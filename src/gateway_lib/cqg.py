"""
Provides Gateway implementation of CQG WebAPI.
"""

import time
from datetime import datetime, timedelta
import asyncio
import logging
import json
from pathlib import Path
import websockets as ws
from threading import RLock, Thread
from uuid import uuid4
from typing import Any, Dict, Optional

from .contract import (Contract, AbstractContract, ContractType, Currency, Exchange, AbstractContractFinder)
from .error import (ErrorCode, ensure_api_ready, raise_undefined_order_reference,
                    raise_duplicated_order_reference)
from .gateway import (AbstractGateway, GatewayEventProcessor)
from .message import (ErrorType, OrderError, ErrorMessage, ExecutionUpdate, OrderUpdate, OrderAPIReady, ConnectionUpdate,
    ConnectionStatus, Position, PositionUpdate, Account, AccountUpdate, ContractDetails, ContractDetailsUpdate, MarketDataBarUpdate)
from .order import (Order, OrderConverter, OrderAction, BUY, SELL, OrderType, TIF, OrderStatus)
from .utils import swap_key_value

from .CQG import prod as cqg_pb

logger = logging.getLogger('cqg')
api_logger = logging.getLogger('cqg.webapi')

CLIENT_ID_PLACEHOLDER = 'GW_CQG'
ORDER_PREFIX_MAX_LENGTH = 10    # max byte for order filtering
DEFAULT_PREFIX_CHAR = '-'

def get_client_order_id(prefix=''):
    """
    Creates a unique order id that could be used for order filtering.
    Max allowed length of `prefix` is 32 bytes.
    Reserve first 10 bytes for filtering.
    """
    _prefix = prefix.ljust(ORDER_PREFIX_MAX_LENGTH+1, DEFAULT_PREFIX_CHAR)
    uid = uuid4().hex  # len: 32, may not be process safe
    return _prefix + uid

def get_cl_order_id_prefix(cl_order_id):
    return cl_order_id[:ORDER_PREFIX_MAX_LENGTH]

def from_timestamp(timestamp) -> datetime:
    seconds = timestamp.seconds
    nanos = timestamp.nanos
    t = datetime.fromtimestamp(seconds)
    return t + timedelta(microseconds=nanos/1000)

def set_timestamp(obj, utc_time: datetime) -> int:
    t = utc_time.timestamp()
    sec = int(t)
    nano = int((t - sec) * pow(10, 9))
    obj.seconds = int(t)
    obj.nanos = nano

def from_decimal(decimal):
    return decimal.significand * pow(10, decimal.exponent)

def set_decimal(obj, num, dp):
    _dp = abs(dp)
    obj.significand = num * pow(10, _dp)
    obj.exponent = _dp

def to_scaled_price(price, contract) -> int:
    # assume price / correct_price_scale always returns an integer
    # and correct_price_scale should never be zero.
    # use round() to handle floating error.
    return round(price / contract.correct_price_scale)

def from_scaled_price(price, contract) -> float:
    return price * contract.correct_price_scale

def serialize(fn):
    def outgoing_msg(*ar, **kw):
        msg = fn(*ar, **kw)
        api_logger.debug("Client message:\n%s" % str(msg))
        return msg.SerializeToString()
    return outgoing_msg

@serialize
def logon_msg(user_name, password,
    client_app_id='WebApiTest', client_version='python-client',
    private_label=None,
    drop_concurrent_session=None, session_settings=None):
    msg = cqg_pb.ClientMsg()
    logon = msg.logon
    logon.user_name = user_name
    logon.password = password
    logon.client_app_id = client_app_id
    logon.client_version = client_version
    logon.protocol_version_major = cqg_pb.ProtocolVersionMajor.Value('PROTOCOL_VERSION_MAJOR')
    logon.protocol_version_minor = cqg_pb.ProtocolVersionMinor.Value('PROTOCOL_VERSION_MINOR')
    logon.max_collapsing_level = cqg_pb.RealTimeCollapsingLevel.REAL_TIME_COLLAPSING_LEVEL_DOM_BBA_TRADES
    if drop_concurrent_session is not None:
        logon.drop_concurrent_session = drop_concurrent_session
    if session_settings is not None:
        # according to .proto enable this might have security risk
        logon.session_settings.extend(session_settings)
    if private_label is not None:
        logon.private_label = private_label
    return msg

@serialize
def logoff_msg(logoff_msg=''):
    msg = cqg_pb.ClientMsg()
    msg.logoff.text_message = logoff_msg
    return msg

@serialize
def ping_msg(token=''):
    msg = cqg_pb.ClientMsg()
    msg.ping.ping_utc_time = int(datetime.utcnow().timestamp())
    if token:
        msg.ping.token = token
    return msg

@serialize
def pong_msg(ping_time_utc, ping_token, pong_time):
    msg = cqg_pb.ClientMsg()
    msg.pong.token = ping_token
    msg.pong.ping_utc_time = ping_time_utc
    msg.pong.pong_utc_time = pong_time
    return msg

@serialize
def restore_session(token):
    client_msg = cqg_pb.ClientMsg()
    client_msg.restore_or_join_session.session_token = token
    return client_msg

@serialize
def request_symbol_msg(req_id: int, symbol: str):
    req = cqg_pb.InformationRequest()
    req.id = req_id
    req.symbol_resolution_request.symbol = symbol
    msg = cqg_pb.ClientMsg()
    msg.information_requests.append(req)
    return msg

@serialize
def subscribe_updates_msg(req_id: int, subscribe: bool):
    req = cqg_pb.TradeSubscription()
    req.id = req_id
    req.subscribe = subscribe
    SCOPE = cqg_pb.TradeSubscription.SubscriptionScope
    for scope in [
        SCOPE.SUBSCRIPTION_SCOPE_ORDERS,
        SCOPE.SUBSCRIPTION_SCOPE_POSITIONS,
        SCOPE.SUBSCRIPTION_SCOPE_COLLATERAL,
    ]:
        req.subscription_scopes.append(scope)
    msg = cqg_pb.ClientMsg()
    msg.trade_subscriptions.append(req)
    return msg

@serialize
def place_order_msg(req_id, acc_id, contract,
    order_type, side, qty, tif, client_order_id,
    limit_price=None, stop_price=None,
    on_behalf_of=None):
    req = cqg_pb.OrderRequest()
    req.request_id = req_id
    order = req.new_order.order
    order.account_id = acc_id
    order.contract_id = contract.contract_id
    # client_order_id is required to be unique,
    # and modified every time any order properties
    # is changed. Thus it is not for storing
    # custom order information.
    order.cl_order_id = client_order_id
    order.order_type = cqg_pb.Order.OrderType.Value(order_type)
    order.duration = cqg_pb.Order.Duration.Value(tif)
    order.side = cqg_pb.Order.Side.Value(side)
    order.is_manual = False
    order.qty.significand = int(qty)
    order.qty.exponent = 0
    order.when_utc_time = int(datetime.timestamp(datetime.now()))
    if limit_price is not None:
        order.scaled_limit_price = to_scaled_price(limit_price, contract)
    if stop_price is not None:
        order.scaled_stop_price = to_scaled_price(stop_price, contract)
    if on_behalf_of:
        req.on_behalf_of_user = on_behalf_of

    msg = cqg_pb.ClientMsg()
    msg.order_requests.append(req)
    return msg

@serialize
def cancel_order_msg(req_id, account_id, broker_order_id, client_order_id, on_behalf_of=''):
    req = cqg_pb.OrderRequest()
    req.request_id = req_id
    if on_behalf_of:
        req.on_behalf_of_user = on_behalf_of
    order = req.cancel_order
    order.order_id = broker_order_id
    order.account_id = account_id
    order.orig_cl_order_id = client_order_id
    orig_prefix = get_cl_order_id_prefix(client_order_id)
    order.cl_order_id = get_client_order_id(orig_prefix)
    set_timestamp(order.when_utc_timestamp, datetime.utcnow())
    msg = cqg_pb.ClientMsg()
    msg.order_requests.append(req)
    return msg

@serialize
def modify_order_msg(req_id, contract, account_id, broker_order_id, client_order_id,
    qty=None, limit_price=None, stop_price=None, tif=None, on_behalf_of=''):
    req = cqg_pb.OrderRequest()
    req.request_id = req_id
    if on_behalf_of:
        req.on_behalf_of_user = on_behalf_of
    order = req.modify_order
    order.order_id = broker_order_id
    order.account_id = account_id
    order.orig_cl_order_id = client_order_id
    orig_prefix = get_cl_order_id_prefix(client_order_id)
    order.cl_order_id = get_client_order_id(orig_prefix)
    set_timestamp(order.when_utc_timestamp, datetime.utcnow())
    if qty is not None:
        # assume qty is integer
        set_decimal(order.qty, num=int(qty), dp=0)
    if limit_price is not None:
        order.scaled_limit_price = to_scaled_price(limit_price, contract)
    if stop_price is not None:
        order.scaled_stop_price = to_scaled_price(stop_price, contract)
    if tif is not None:
        order.duration = cqg_pb.Order.Duration.Value(tif)
    msg = cqg_pb.ClientMsg()
    msg.order_requests.append(req)
    return msg

@serialize
def request_accounts(req_id):
    req = cqg_pb.InformationRequest()
    req.id = req_id
    req.accounts_request.SetInParent()
    msg = cqg_pb.ClientMsg()
    msg.information_requests.append(req)
    return msg

@serialize
def request_historical_orders(req_id, account_id, biz_day):
    req = cqg_pb.InformationRequest()
    req.id = req_id
    request = req.historical_orders_request
    request.from_date = biz_day
    request.account_ids.append(account_id)
    msg = cqg_pb.ClientMsg()
    msg.information_requests.append(req)
    return msg

def request_rate_limit(n_request, time_delta):
    # e.g. 100 request per 24 hours
    #TODO: per session?
    def wraps(fn):
        def impl(*ar, **kw):
            res = fn(*ar, **kw)
        return impl
    return wraps


class CQGOrderConverter(OrderConverter):
    _action_to_native = {
        OrderAction.BUY: 'SIDE_BUY',
        OrderAction.SELL: 'SIDE_SELL',
    }
    _native_to_action = swap_key_value(_action_to_native)

    _orderType_to_native = {
        OrderType.LMT: 'ORDER_TYPE_LMT',
        OrderType.MKT: 'ORDER_TYPE_MKT',
        OrderType.STP: 'ORDER_TYPE_STP',
        OrderType.STP_LMT: 'ORDER_TYPE_STL'
    }
    _native_to_orderType = swap_key_value(_orderType_to_native)

    _tif_to_native = {
        TIF.GTC: 'DURATION_GTC',
        TIF.DAY: 'DURATION_DAY',
    }
    _native_to_tif = swap_key_value(_tif_to_native)

    def convert_from(self, order: Order):
        raise NotImplemented

    def convert_to(self, native) -> Order:
        raise NotImplemented

    def get_native_order_action(self, val):
        return self._action_to_native[val]

    def get_native_order_type(self, val):
        return self._orderType_to_native[val]

    def get_native_tif(self, val):
        return self._tif_to_native[val]

    def get_internal_order_action(self, val):
        return self._native_to_action[val]

    def get_internal_order_type(self, val):
        return self._native_to_orderType[val]

    def get_internal_tif(self, val):
        return self._native_to_tif[val]

class SymbolFinder(AbstractContractFinder):
    def __init__(self) -> None:
        self._symbol_contract_id = {}
        self._req_id_symbol = {}
        self._contract_id_meta = {}

    def reset(self):
        for d in [
            self._symbol_contract_id,
            self._req_id_symbol,
            self._contract_id_meta]:
            d.clear()

    def from_symbol(self, symbol: str):
        """
        Resolve broker native contract by internal symbol name.
        """
        cid = self._symbol_contract_id.get(symbol, None)
        return self._contract_id_meta.get(cid, None)

    def from_contract_id(self, contract_id: int):
        return self._contract_id_meta.get(contract_id, None)

    def from_exchange_symbol(self, exchange: int, contractType: int, symbol: str):
        """
        Resolve broker native contract by exchange symbol and other details.
        """
        pass

    def reset(self):
        self._symbol_contract_id.clear()
        self._req_id_symbol.clear()
        self._contract_id_meta.clear()

    def request_symbol_resolution(self, req_id, symbol):
        self._req_id_symbol[req_id] = symbol

    def process_symbol_resolution(self, req_id, report):
        symbol_lookup = self._req_id_symbol
        symbol = symbol_lookup.get(req_id, None)
        if symbol is None:
            raise Exception(f"Unrecognize report id: {req_id}")
        meta = report.contract_metadata
        if not meta.IsInitialized():
            raise Exception("Contrace meta is not initialized")
        self.register_contract_meta(symbol, meta)

    def register_contract_meta(self, symbol, meta):
        #TODO: utilize common Contract class?
        id_lookup = self._symbol_contract_id
        meta_lookup = self._contract_id_meta
        cid = meta.contract_id
        info = dict(
            id=cid,
            contract_symbol=meta.contract_symbol,
            contract_display_name=meta.title,
            description=meta.description,
            price_scale=meta.correct_price_scale,
            display_scale=meta.display_price_scale,
            tick_size=meta.tick_value,
            exchange=meta.mic,
            exchange_name=meta.mic_description,
            cfi_code=meta.cfi_code,
            currency=meta.currency,
        )
        logger.info(f"Register symbol '{symbol}'. Contract details: {info}")
        id_lookup[symbol] = cid
        meta_lookup[cid] = meta

class OrderManager:
    """
    Manage the internal order reference, and open orders.
    """

    """
    `order reference` is the internal recognized order label.
    It maps to `chain_order_id` which CQG persists the same ID
    even after each order modification.
    """
    def __init__(self):
        self._state_file = None   # persist order reference information
        self._ref_to_id = {}  # internal order reference - chain order id
        self._id_to_ref = {}  # chain order id - internal order reference
        self._native_ids = {}       # link chain_order_id with (order_id, cl_order_id)
        self._pending_order = {}    # cl_order_id - order ref
        self._open_orders = {}  # chain_order_id - OrderUpdate

    def reset(self):
        self._state_file = None   # persist order reference information
        self._ref_to_id.clear()
        self._id_to_ref.clear()
        self._native_ids.clear()
        self._pending_order.clear()
        self._open_orders.clear()

    def update_open_orders(self, order_update: OrderUpdate):
        """ Maintain a collection of open orders.
        """
        broker_oid = order_update.broker_order_id
        status = order_update.status
        if status in [
            OrderStatus.SUBMITTED,
            OrderStatus.PARTIAL_FILLED
        ]:
            self._open_orders[broker_oid] = order_update
        elif broker_oid in self._open_orders:
            self._open_orders.pop(broker_oid)

    def get_open_orders(self) -> dict:
        return self._open_orders.copy()

    def create_or_update_order(self, broker_uoid, broker_oid, cl_oid):
        # create or update order ids in case of creation or modification.
        logger.debug(f"Update order ids:"
            f" chain_order_id='{broker_uoid}'"
            f" order_id='{broker_oid}'"
            f" cl_order_id='{cl_oid}'")
        self._native_ids[broker_uoid] = (broker_oid, cl_oid)
        # link order ref to immutable broker order id
        if cl_oid in self._pending_order:
            ref = self._pending_order.pop(cl_oid)
            self._ref_to_id[ref] = broker_uoid
            self._id_to_ref[broker_uoid] = ref
        self.save_state()

    def add_order(self, order_ref, cl_oid):
        self._pending_order[cl_oid] = order_ref

    def order_exists(self, order_ref):
        return order_ref in self._ref_to_id

    def get_order_reference(self, broker_order_id, default=""):
        return self._id_to_ref.get(broker_order_id, default)

    def get_mutable_order_id(self, order_ref, default=""):
        b_uid = self._ref_to_id.get(order_ref)
        if not b_uid:
            return default
        b_oid, _ = self._native_ids.get(b_uid, (default, default))
        return b_oid

    def get_cl_order_id(self, order_ref, default=""):
        b_uid = self._ref_to_id.get(order_ref)
        if not b_uid:
            return default
        _, cl_oid = self._native_ids.get(b_uid, (default, default))
        return cl_oid

    def set_state_filepath(self, path):
        self._state_file = path

    def save_state(self):
        _file = self._state_file
        try:
            with open(_file, 'w') as fd:
                state = dict(
                    order_id_to_ref=self._id_to_ref.copy(),
                    native_order_ids=self._native_ids.copy(),
                )
                json.dump(state, fd, indent=4)
        except:
            logger.exception("Failed to save state. Skip action.")

    def load_state(self):
        _file = Path(self._state_file)

        if not _file.is_file():
            logger.info(f"Create order reference file '{_file}'")
            with open(_file, 'w') as fd:
                json.dump({
                    'order_id_to_ref': {},
                    'native_order_ids': {},
                }, fd)

        with open(_file, 'r') as fd:
            try:
                state = json.load(fd)
                self._id_to_ref = {k: v for k, v in state['order_id_to_ref'].items()}
                self._native_ids = state['native_order_ids'].copy()
                logger.info(f'Loaded "{_file}".')
                self._ref_to_id = {v: k for k, v in self._id_to_ref.items()}
            except json.decoder.JSONDecodeError as e:
                logger.warning(f'Failed to read "{_file}". Reason: {e}')
                self._id_to_ref = {}
                self._ref_to_id = {}

class AccountManager:
    def __init__(self):
        self._accounts = {}

    def reset(self):
        self._accounts.clear()

    def register_account(self, account_id):
        self._accounts[account_id] = Account(acc_name=str(account_id), cash_balance=0.0, realized_PnL=0.0, unrealized_PnL=0.0, currency='USD')

    def update_account(self, info):
        account_id = info['account_id']
        if account_id in self._accounts:
            acc = self._accounts[account_id]
            acc.cash_balance = info['balance']
            acc.unrealized_PnL = info['floating_pnl']
            acc.currency = info['currency']

    def get_account_update(self) -> AccountUpdate:
        accounts = self._accounts
        return AccountUpdate([accounts[_id] for _id in accounts])

class PositionManager:
    def __init__(self):
        self._positions = {}

    def reset(self):
        self._positions.clear()

    def update_position(self, symbol, net_position):
        self._positions[symbol] = net_position

    def get_position_update(self) -> PositionUpdate:
        positions = self._positions
        return PositionUpdate([
            Position(symbol=sym, broker_symbol=sym, net_position=positions[sym])
            for sym in positions])

def _order_reference_exists(*ar, **kw):
    gateway = ar[0]
    orders = gateway._order_manager
    order_ref = ar[1] if len(ar) > 1 else kw['order_id']
    exists = orders.order_exists(order_ref)
    return order_ref, exists

def _corresponding_broker_oid_exists(*ar, **kw):
    gateway = ar[0]
    hdlr = gateway._event_handler
    mgr = hdlr.order_manager
    internal_oid = ar[1] if len(ar) > 1 else kw['order_id']
    broker_oid = mgr.get_mutable_order_id(internal_oid)
    return internal_oid, broker_oid


class CQGWebAPIv2_6(GatewayEventProcessor):
    """
    CQG Web API v2.6 (production version as of 2021-Jan)

    Initialization: Event callback subscriptions (ref. C# implementation):
    1. LogonResult > AccountReport > TradeSubscription >
    1.1. OrderStatus >
    1.2. PositionUpdate >
    1.3. collateral_statuses > END
    """
    _order_status_map = dict(
        WORKING=OrderStatus.SUBMITTED,
        APPROVED_BY_EXCHANGE=OrderStatus.SUBMITTED,
        CANCELLED=OrderStatus.CANCELLED,
        DISCONNECTED=OrderStatus.CANCELLED,
        FILLED=OrderStatus.FILLED,
        APPROVE_REQUIRED=OrderStatus.INACTIVE,
        ACTIVEAT=OrderStatus.INACTIVE,
        SUSPENDED=OrderStatus.INACTIVE,
        EXPIRED=OrderStatus.REJECTED,
        REJECTED=OrderStatus.REJECTED,
        APPROVE_REJECTED=OrderStatus.REJECTED)

    def __init__(self, gateway):
        super().__init__(gateway)
        self.symbols = SymbolFinder()
        self.order_converter = CQGOrderConverter()
        self.order_manager = OrderManager()
        self.account_manager = AccountManager()
        self.position_manager = PositionManager()
        self.base_time = None
        self.session_timeoffset = -1
        self._logon_init_id = -1

        self._msg_handlers = [
            self._process_user_message,
            self._process_logon_result,
            self._process_logoff_result,
            self._process_order_reject,
            self._process_trade_subscription_status,
            self._process_ping,
            self._process_order_update,
            self._process_information_report,
            self._process_trade_snapshot_completion,
            self._process_collateral_status,
            self._process_position_status,
        ]

        self._info_report_handlers = {
            'symbol_resolution_report': self.__process_symbol_resolution,
            'accounts_report': self.__process_account_report,
            'historical_orders_report': self.__process_historical_order_report,
        }

    def reset(self):
        self.symbols.reset()
        self.order_manager.reset()
        self.account_manager.reset()
        self.position_manager.reset()
        self.base_time = None
        self.session_timeoffset = -1
        self._logon_init_id = -1

    def process_error_event(self, msg: ErrorMessage):
        return msg

    def process_execution_event(self, obj: dict):
        is_historical = obj['is_historical']
        trade = obj['trade']
        exec_id = trade.trade_id
        filled = from_decimal(trade.qty)
        if trade.HasField('trade_utc_timestamp'):
            seconds = trade.trade_utc_timestamp.seconds
            nanos = trade.trade_utc_timestamp.nanos
            t = datetime.fromtimestamp(seconds)
            exec_time = t + timedelta(microseconds=nanos/1000)
            exec_time = from_timestamp(trade.trade_utc_timestamp)
        else:
            exec_time = datetime.min()
        ORDER_SIDE = cqg_pb.Order.Side
        converter = self.order_converter
        side = converter.get_internal_order_action(ORDER_SIDE.Name(trade.side))
        symbol = obj['symbol']
        order_ref = obj['order_ref']
        broker_order_id = obj['broker_order_id']
        cum_qty = obj['cum_qty']
        avg_price = obj['avg_price']
        currency = obj['currency']
        commission = obj['commission']
        price = trade.price_correct
        client_id = self._gw.identity   #TODO: replace hard-code, differentiate manual order

        return ExecutionUpdate(
            client_id=client_id,
            order_ref=order_ref,
            broker_order_id=broker_order_id,
            exec_id=exec_id,
            timestamp=exec_time,
            side=side,
            symbol=symbol,
            filled=filled,
            price=price,
            cum_qty=cum_qty,
            avg_price=avg_price,
            commission=commission,
            currency=currency,
            is_historical=is_historical)

    def process_order_event(self, obj: dict):
        # Duplicated order update not encountered.
        # Handled partial filled.
        is_historical = obj['is_historical']
        order = obj['order']
        order_status = obj['order_status']
        broker_order_id = obj['broker_order_id']
        order_ref = obj['order_ref']
        contract_meta = obj['contract_meta']
        limit_price = obj['limit_price']
        stop_price = obj['stop_price']
        internal_status = obj['internal_status']
        symbol = contract_meta.contract_symbol
        target_qty = from_decimal(order.qty)
        ORDER_TYPE = cqg_pb.Order.OrderType
        converter = self.order_converter
        order_type = converter.get_internal_order_type(ORDER_TYPE.Name(order.order_type))
        ORDER_SIDE = cqg_pb.Order.Side
        order_action = converter.get_internal_order_action(ORDER_SIDE.Name(order.side))
        TIF = cqg_pb.Order.Duration
        tif = converter.get_internal_tif(TIF.Name(order.duration))

        # assuming last transaction status represents an order state.
        order_update = None
        n_trans = len(order_status.transaction_statuses)
        if n_trans > 0:
            last = order_status.transaction_statuses[-1]
            trans_status = last.status
            try:
                STATUS = cqg_pb.TransactionStatus.Status
                status_label = STATUS.Name(trans_status)
                logger.debug(f"Last transaction status of order '{broker_order_id}' is value={status_label}")
            except:
                logger.error(f"Undefined transaction status of order '{broker_order_id}'. value={trans_status}")

            filled_qty = from_decimal(order_status.fill_qty)
            remaining_qty = from_decimal(order_status.remaining_qty)
            if internal_status == OrderStatus.SUBMITTED \
                and filled_qty > 0 and remaining_qty > 0:
                internal_status = OrderStatus.PARTIAL_FILLED

            client_id = self._gw.identity   #TODO: replace hard-code, differentiate manual order

            order_update = OrderUpdate(
                client_id=client_id,
                broker_order_id=broker_order_id,
                order_ref=order_ref,
                order=Order(
                    symbol=symbol,
                    exchange=0, #TODO: handle exchange value
                    contractType=ContractType.Future, #TODO: support other types
                    orderType=order_type,
                    action=order_action,
                    quantity=target_qty,
                    limit_price=limit_price,
                    stop_price=stop_price,
                    tif=tif,
                    outsideRth=True
                ),
                remaining=remaining_qty,
                filled=filled_qty,
                status=internal_status,
                msg="",
                is_historical=is_historical)

            self.order_manager.update_open_orders(order_update)
        return order_update

    def process_connection_event(self, obj: dict):
        return ConnectionUpdate(status=obj['status'])

    def process_order_api_ready_event(self, obj: dict):
        return OrderAPIReady(ready=obj['ready'])

    def process_position_event(self, obj: dict):
        return obj

    def process_account_info_event(self, obj: dict):
        return obj

    def process_contract_details_event(self, obj: dict):
        return obj

    def process_market_data_bar_event(self, obj: dict):
        return obj

    def _process_user_message(self, gw, msg):
        for user_message in msg.user_messages:
            if user_message.IsInitialized():
                msgtype = user_message.message_type
                text = user_message.text
                MSG = cqg_pb.UserMessage.MessageType
                if msgtype == MSG.MESSAGE_TYPE_CRITICAL_ERROR:
                    gw.events.raise_error_event(ErrorMessage(
                        msg=text,
                        code=None,
                        error_type=ErrorType.GENERAL,
                        req_id=None
                    ))
                elif msgtype == MSG.MESSAGE_TYPE_WARNING:
                    logger.warning(text)
                else:
                    logger.info(text)

    def _process_logon_result(self, gw, msg):
        submsg = msg.logon_result
        if submsg.IsInitialized():
            result_code = submsg.result_code
            Code = cqg_pb.LogonResult.ResultCode
            if result_code > 100:
                reason = submsg.text_message
                gw.events.raise_error_event(ErrorMessage(
                    msg=reason,
                    code=result_code,
                    error_type=ErrorType.CONNECTIVITY,
                    req_id=None))
                # logon failure triggers connection close
                gw._connect_failure = True
                gw.close(reason=reason)
            elif result_code == Code.RESULT_CODE_SUCCESS:
                time_pattern = "%Y-%m-%dT%H:%M:%S"
                self.base_time = datetime.strptime(submsg.base_time, time_pattern)
                self.session_timeoffset = self.msec_from_basetime()
                logger.info(f"Session UTC time offset: {self.session_timeoffset}ms")
                logger.info(f"Server time (UTC): {self.msec_to_datetime(submsg.server_time)}")
                offset_server_sec = int((submsg.server_time - self.session_timeoffset) / 1000)
                if offset_server_sec >= 1:
                    logger.warning(
                        f"Local time is behind server time {offset_server_sec}s."
                        " Please consider time sync.")
                gw.set_logon_status(True)
                gw.submit_message(request_accounts(req_id=gw.next_req_id()))

    def _process_logoff_result(self, gw, msg):
        submsg = msg.logged_off
        if submsg.IsInitialized():
            reason = submsg.logoff_reason
            Reason = cqg_pb.LoggedOff.LogoffReason
            try:
                reason_str = Reason.Name(reason)
            except:
                reason_str = str(reason)
            if gw._disconnecting:
                # logoff initiated by client
                reason_msg = 'Logoff initiated by client.'
            else:
                # logoff initiated by server
                msg = submsg.text_message
                gw.events.raise_error_event(ErrorMessage(
                    msg=msg,
                    code=reason_str,
                    error_type=ErrorType.CONNECTIVITY,
                    req_id=None))
                reason_msg = msg
            # close the connection regardless of reason.
            gw.close(reason=reason_msg)

    def _process_information_report(self, gw, msg):
        for report in msg.information_reports:
            if report.IsInitialized():
                req_id = report.id  # same as request id or subscription id
                status = report.status_code
                Code = cqg_pb.InformationReport.StatusCode

                if status > 100:
                    gw.events.raise_error_event(ErrorMessage(
                        msg=report.text_message,
                        code=status,
                        error_type=ErrorType.GENERAL,
                        req_id=req_id
                    ))
                elif status == Code.STATUS_CODE_SUCCESS:
                    for attr_name, hdlr in self._info_report_handlers.items():
                        if report.HasField(attr_name):
                            subject = getattr(report, attr_name)
                            if subject.IsInitialized():
                                hdlr(gw, req_id, subject)
                else:
                    logger.warning(
                        f"Unrecognized information report status: {status}"
                        " Skip report handling.")

    def __process_symbol_resolution(self, gw, req_id, report):
        try:
            self.symbols.process_symbol_resolution(req_id, report)
        except Exception as e:
            logger.warning(f"Error resolving symbol. {e}")

    def __process_account_report(self, gw, req_id, report):
        TYPE = cqg_pb.Brokerage.BrokerageType
        for brokerage in report.brokerages:
            if brokerage.IsInitialized():
                try:
                    broker_type = TYPE.Name(brokerage.type)
                except:
                    broker_type = 'N/A'
                broker_info = dict(
                    broker_id=brokerage.id,
                    broker_name=brokerage.name,
                    broker_type=broker_type,
                )
                for sales in brokerage.sales_series:
                    for acc in sales.accounts:
                        acc_info = dict(
                            account_id=acc.account_id,
                            brokerage_account_id=acc.brokerage_account_id,
                            name=acc.name)
                        logger.info("account info: %s" % {**acc_info, **broker_info})
                        self.account_manager.register_account(acc.account_id)
        self._logon_init_id = gw.subscribe_update()

    def __process_historical_order_report(self, gw, req_id, report):
        # dedicate to get historical executions
        symbols = self.symbols
        mgr = self.order_manager
        for status in report.order_statuses:
            filled_qty = from_decimal(status.fill_qty)
            avg_price = status.avg_fill_price_correct
            broker_order_id = status.chain_order_id
            order = status.order
            if not order.IsInitialized():
                logger.warning(f"Order '{broker_order_id}' is not init")
                continue
            order_ref = mgr.get_order_reference(broker_order_id)
            contract_meta = symbols.from_contract_id(order.contract_id)
            if contract_meta is None:
                # should never be called
                logger.warning(f"Cannot resolve contract for order '{broker_order_id}'")
                continue
            for trans_status in status.transaction_statuses:
                if trans_status.HasField('fill_commission'):
                    commission = trans_status.fill_commission.commission
                else:
                    commission = 0
                for trade in trans_status.trades:
                    gw.events.raise_execution_event(dict(
                        trade=trade,
                        symbol=contract_meta.contract_symbol,
                        order_ref=order_ref,
                        broker_order_id=broker_order_id,
                        currency=contract_meta.currency,
                        cum_qty=filled_qty,
                        avg_price=avg_price,
                        commission=commission,
                        is_historical=True,
                    ))

    def _process_order_reject(self, gw, msg):
        for reject in msg.order_request_rejects:
            if reject.IsInitialized():
                req_id = reject.request_id
                code = reject.reject_code
                text = reject.text_message
                gw.events.raise_error_event(ErrorMessage(
                    msg=text,
                    code=code,
                    error_type=ErrorType.ORDER,
                    req_id=req_id
                ))

    def _process_trade_subscription_status(self, gw, msg):
        for status in msg.trade_subscription_statuses:
            if status.IsInitialized():
                req_id = status.id
                status_code = status.status_code
                text = status.text_message
                if status_code > 100:
                    try:
                        Code = cqg_pb.TradeSubscriptionStatus.StatusCode
                        code_name = Code.Name(status_code)
                    except:
                        code_name = None
                    err_msg = ' '.join(
                        x for x in [
                            "Error with trade subscription.",
                            code_name.replace("STATUS_CODE_", "Code: ") if code_name else None,
                            f"Details: {text}" if text else None,
                        ] if x
                    )
                    gw.events.raise_error_event(ErrorMessage(
                        msg=err_msg,
                        code=status_code,
                        error_type=ErrorType.GENERAL,
                        req_id=req_id
                    ))

    def _process_ping(self, gw, msg):
        ping = msg.ping
        if ping.IsInitialized():
            token = ping.token
            time = ping.ping_utc_time
            timenow = self.msec_from_basetime()
            gw.submit_message(pong_msg(
                ping_time_utc=time,
                ping_token=token,
                pong_time=timenow))

    def _process_order_update(self, gw, msg):
        symbols = self.symbols
        for status in msg.order_statuses:
            if status.IsInitialized():
                # register contracts from order status
                for meta in status.contract_metadata:
                    if meta.IsInitialized():
                        broker_symbol = meta.contract_symbol
                        display_symbol = meta.title
                        symbols.register_contract_meta(broker_symbol, meta)
                        symbols.register_contract_meta(display_symbol, meta)

                # filter orders of relevant to this account
                account_id = status.account_id
                if gw.account_id != account_id:
                    logger.debug("Skip order update from another account.")
                    continue
                # filter valid status
                if status.status <= 0:
                    continue
                # filter update with transactions
                if len(status.transaction_statuses) <= 0:
                    continue

                last = status.transaction_statuses[-1]
                is_current_session = last.trans_utc_time > self.session_timeoffset

                mgr = self.order_manager
                # `chain_order_id` does not change even when order is modified
                # `order_id` is changed by server after each modify
                broker_order_id = status.chain_order_id
                order = status.order
                cl_oid = order.cl_order_id
                mutable_oid = status.order_id

                internal_status = OrderStatus.UNDEFINED
                if status.status > 0:
                    STATUS = cqg_pb.OrderStatus.Status
                    try:
                        status_label = STATUS.Name(status.status)
                        internal_status = self._order_status_map[status_label]
                    except:
                        logger.debug(f"Undefined order status of order '{broker_order_id}'. value={status.status}")
                        continue

                if internal_status == OrderStatus.UNDEFINED:
                    continue

                if not is_current_session:
                    # handle open orders
                    if internal_status in [
                        OrderStatus.SUBMITTED,
                        OrderStatus.INACTIVE,
                    ]:
                        order_ref = mgr.get_order_reference(broker_order_id)
                        # # skip open orders not placed by this gateway instance
                        # if not order_ref:
                        #     continue

                        symbols = self.symbols
                        contract_meta = symbols.from_contract_id(order.contract_id)
                        if contract_meta is None:
                            # should never be called
                            logger.warning(f"Cannot resolve contract for order '{order_ref}'")
                            continue

                        mgr.create_or_update_order(broker_order_id, mutable_oid, cl_oid)

                        try:
                            lmt_price, stop_price = self.__get_price(order, contract_meta)
                        except Exception as e:
                            logger.error(e)
                        else:
                            gw.events.raise_order_event(dict(
                                order=order,
                                order_status=status,
                                broker_order_id=broker_order_id,
                                order_ref=order_ref,
                                contract_meta=contract_meta,
                                limit_price=lmt_price,
                                stop_price=stop_price,
                                internal_status=internal_status,
                                is_historical=True,
                            ))
                else:
                    # do not raise order/execution events happened before session starts
                    STATUS = cqg_pb.TransactionStatus.Status
                    err_map = {
                        STATUS.REJECTED: "Place order rejected.",
                        STATUS.REJECT_CANCEL: "Cancel order rejected.",
                        STATUS.REJECT_MODIFY: "Modify order rejected.",
                    }
                    if last.status in err_map:
                        #TODO: provide information which order is rejected
                        gw.events.raise_error_event(ErrorMessage(
                            msg=f"{err_map[last.status]}" \
                                f" Message: {status.reject_message}",
                            code=last.status,
                            error_type=ErrorType.ORDER,
                            req_id=None))
                        continue

                    mgr.create_or_update_order(broker_order_id, mutable_oid, cl_oid)
                    order_ref = mgr.get_order_reference(broker_order_id)

                    symbols = self.symbols
                    contract_meta = symbols.from_contract_id(order.contract_id)
                    if contract_meta is None:
                        # should never be called
                        logger.warning(f"Cannot resolve contract for order '{order_ref}'")
                        continue

                    try:
                        lmt_price, stop_price = self.__get_price(order, contract_meta)
                    except Exception as e:
                        logger.error(e)
                    else:
                        gw.events.raise_order_event(dict(
                            order=order,
                            order_status=status,
                            broker_order_id=broker_order_id,
                            order_ref=order_ref,
                            contract_meta=contract_meta,
                            limit_price=lmt_price,
                            stop_price=stop_price,
                            internal_status=internal_status,
                            is_historical=False,
                        ))

                        # assuming execution info, i.e. `trans_status.trades`
                        # exists in valid order status
                        filled_qty = from_decimal(status.fill_qty)
                        avg_price = status.avg_fill_price_correct
                        for trans_status in status.transaction_statuses:
                            if trans_status.HasField('fill_commission'):
                                commission = trans_status.fill_commission.commission
                            else:
                                commission = 0
                            for trade in trans_status.trades:
                                gw.events.raise_execution_event(dict(
                                    trade=trade,
                                    symbol=contract_meta.contract_symbol,
                                    order_ref=order_ref,
                                    broker_order_id=broker_order_id,
                                    currency=contract_meta.currency,
                                    cum_qty=filled_qty,
                                    avg_price=avg_price,
                                    commission=commission,
                                    is_historical=False,
                                ))

    def __get_price(self, order, contract):
        ORDER_TYPE = cqg_pb.Order.OrderType
        converter = self.order_converter
        order_type = converter.get_internal_order_type(ORDER_TYPE.Name(order.order_type))
        lmt_price, stop_price = None, None
        if order_type in [OrderType.LMT, OrderType.STP_LMT]:
            if order.scaled_limit_price == 0:
                raise Exception("Limit order does not have `scaled_limit_price` set.")
            else:
                lmt_price = from_scaled_price(order.scaled_limit_price, contract)
        if order_type in [OrderType.STP, OrderType.STP_LMT]:
            if order.scaled_stop_price == 0:
                raise Exception("Stop order does not have `scaled_stop_price` set.")
            else:
                stop_price = from_scaled_price(order.scaled_stop_price, contract)
        return lmt_price, stop_price

    def _process_trade_snapshot_completion(self, gw, msg):
        SCOPE = cqg_pb.TradeSubscription.SubscriptionScope
        for completion in msg.trade_snapshot_completions:
            _id = completion.subscription_id
            for scope_id in completion.subscription_scopes:
                _scope = scope_id
                try:
                    _scope = SCOPE.Name(scope_id)
                except:
                    pass
                logger.debug("Trade snapshot completed"
                    f" with subscription id = {_id}"
                    f" and scope = {_scope}")

                if _id == self._logon_init_id and scope_id == SCOPE.SUBSCRIPTION_SCOPE_COLLATERAL:
                    # logon event subscription done
                    # publish connection ready
                    gw._on_logon_procedure_done()

    def _process_collateral_status(self, gw, msg):
        accounts = self.account_manager
        for status in msg.collateral_statuses:
            if status.IsInitialized():
                accounts.update_account(dict(
                    account_id = status.account_id,
                    currency = status.currency,
                    balance = status.purchasing_power,
                    floating_pnl = status.ote,  # only include futures pnl
                    #TODO: there is no realized pnl
                ))

    def _process_position_status(self, gw, msg):
        mgr = self.position_manager
        symbols = self.symbols
        for status in msg.position_statuses:
            if status.IsInitialized():
                cid = status.contract_id
                meta = symbols.from_contract_id(cid)
                if meta is not None:
                    symbol = meta.contract_symbol
                    net_pos = 0
                    for pos in status.open_positions:
                        _dir = -1 if pos.is_short else 1
                        net_pos += _dir * from_decimal(pos.qty)
                    mgr.update_position(symbol, net_pos)

    def process_message(self, msg):
        gw = self._gw
        handlers = self._msg_handlers
        for hdlr in handlers:
            hdlr(gw, msg)

    def msec_from_basetime(self, time=None):
        """
        Convert datetime to offset from given `base_time` in millisecond.
        """
        _time = time if time else datetime.utcnow()
        offset = _time - self.base_time
        return int(offset.total_seconds() * 1000)

    def msec_to_datetime(self, msec):
        """
        Convert time offset to datetime.
        """
        return self.base_time + timedelta(milliseconds=msec)

class CQGGateway(AbstractGateway):

    READ_TIMEOUT: int = 1

    def __init__(self, url, account_id, username, password,
        client_app_id, client_version, private_label=None, name=None,
        state_filepath: str = None):
        _name = name if name else f'{username}@{url}'
        super().__init__(name=_name, event_handler=CQGWebAPIv2_6(self))
        self._event_handler = self.events._event_handler
        self._order_converter = self._event_handler.order_converter
        self._symbols = self._event_handler.symbols
        self._order_manager = self._event_handler.order_manager
        self._state_file = f'{self.name}-cqg.json' if state_filepath is None else state_filepath
        self._lock = RLock()
        self._conn = None
        self._loop = None
        self._url = url
        self._account_id = account_id
        self._username = username
        self._password = password
        self._client_app_id= client_app_id
        self._client_version= client_version
        self._private_label = private_label
        self._connect_id: int = None
        self._read_timeout = self.READ_TIMEOUT
        self._socket: Optional[ws.client.WebSocketClientProtocol] = None
        self._link_up = False
        self._connect_failure = False
        self._is_logon = False
        self._disconnecting = False
        self._connecting = False
        self._order_api_ready = False
        self._closing = False
        self._req_id = 0

    def __str__(self):
        return f"CQGGateway('{self.name}')"

    def set_link_status(self, value):
        self._link_up = value

    def set_logon_status(self, value, force_publish=False, **kwargs):
        changed = self._is_logon != value
        self._is_logon = value
        # only publish disconnect status
        if not value and (changed or force_publish):
            kwargs['status'] = ConnectionStatus.CONNECTED if value else ConnectionStatus.DISCONNECTED
            self.events.raise_connection_event(kwargs)

    def set_order_api_ready(self, value):
        old = self._order_api_ready
        self._order_api_ready = value
        if old != value:
            self.events.raise_order_api_ready_event(dict(
                ready=self.can_manipulate_order))

    @property
    def account_id(self):
        return self._account_id

    @property
    def identity(self):
        """ Returns the identity recognized by the execution venue.
        This is use to differentiate orders placed by this gateway.
        """
        return CLIENT_ID_PLACEHOLDER

    def save_state(self):
        """ Store the state of gateway to file.
        """
        try:
            self._order_manager.save_state()
        except:
            logger.exception("Unhandled exception when saving state to file")

    def load_state(self):
        """ Restore the state of gateway from file.
        """
        try:
            self._order_manager.load_state()
        except:
            logger.exception("Unhandled exception when loading state from file")

    def connect(self):
        """ Connect to broker.
        """
        if not self.is_healthy and not self._connecting:
            self._connecting = True
            logger.debug("Going to establish connection.")
            self._order_manager.set_state_filepath(self._state_file)
            self.load_state()
            thread = Thread(target=self._start_event_loop, daemon=False)
            thread.start()

    def disconnect(self):
        """ Disconnect from broker.
        """
        if self.is_healthy and not self._disconnecting:
            # user initiate logoff will trigger `close()`
            self._disconnecting = True
            logger.debug("Begin to logoff and close the connection.")
            self.submit_message(logoff_msg())
        elif self._link_up:
            # ensure `disconnect()` can close the socket
            self.close(reason='Close socket by user disconnect.')

    @property
    def is_healthy(self):
        """ Returns True if connectivity with broker is healthy.
        """
        return self._conn is not None and self._link_up and self._is_logon

    @property
    def can_manipulate_order(self):
        """ Returns True if gateway is healthy and is ready for order API calls.
        """
        return self.is_healthy and self._order_api_ready

    @ensure_api_ready(error_type=ErrorType.ORDER, attr_ready='can_manipulate_order')
    @raise_duplicated_order_reference(_order_reference_exists)
    def place_order(self, order_id: str, order: Order, order_prefix='', on_behalf_of=''):
        """ Place order.
        """
        symbols = self._symbols
        contract = symbols.from_symbol(order.symbol)
        if contract is None:
            #TODO: add order id in error message?
            self.events.raise_error_event(ErrorMessage(
                msg=f"Contract not found for symbol: '{order.symbol}'",
                code=ErrorCode.GTW_SYMBOL_RESOLVE_ERROR,
                error_type=ErrorType.ORDER,
                req_id=None
            ))
            return

        if order_prefix and len(order_prefix.encode('utf-8')) > 10:
            self.events.raise_error_event(ErrorMessage(
                msg=f"Length of order prefix '{order_prefix}' exceeded 10.",
                code=ErrorCode.GTW_SKIPPED_API_CALL,
                error_type=ErrorType.ORDER,
                req_id=None
            ))
            return

        converter = self._order_converter
        side = converter.get_native_order_action(order.action)
        orderType = converter.get_native_order_type(order.orderType)
        tif = converter.get_native_tif(order.tif)
        acc_id = self._account_id
        _limit_price = None
        _stop_price = None
        if order.orderType in [OrderType.STP, OrderType.STP_LMT]:
            _stop_price = order.stop_price
        if order.orderType in [OrderType.LMT, OrderType.STP_LMT]:
            _limit_price = order.limit_price

        cl_oid = get_client_order_id(order_prefix)
        orders = self._order_manager
        orders.add_order(order_id, cl_oid)

        msg = place_order_msg(
            req_id=self.next_req_id(),
            acc_id=acc_id,
            contract=contract,
            order_type=orderType,
            side=side,
            qty=order.quantity,
            tif=tif,
            client_order_id=cl_oid,
            limit_price=_limit_price,
            stop_price=_stop_price,
            on_behalf_of=on_behalf_of
        )
        self.submit_message(msg)

    @ensure_api_ready(error_type=ErrorType.ORDER, attr_ready='can_manipulate_order')
    @raise_undefined_order_reference(_corresponding_broker_oid_exists)
    def cancel_order(self, order_id: str, on_behalf_of=''):
        """ Cancel open order.
        """
        hdlr = self._event_handler
        mgr = hdlr.order_manager
        internal_oid = order_id
        # `broker_oid` and `cl_oid` must exists
        # given it passed the `raise_undefined_order_reference` check
        broker_oid = mgr.get_mutable_order_id(internal_oid)
        cl_oid = mgr.get_cl_order_id(internal_oid)

        msg = cancel_order_msg(
            req_id=self.next_req_id(),
            account_id=self._account_id,
            broker_order_id=broker_oid,
            client_order_id=cl_oid,
            on_behalf_of=on_behalf_of)
        self.submit_message(msg)

    @ensure_api_ready(error_type=ErrorType.ORDER, attr_ready='can_manipulate_order')
    @raise_undefined_order_reference(_order_reference_exists)
    def modify_order(self, order_id: str, order: Order, on_behalf_of=''):
        """ Modify open order.
        """
        symbols = self._symbols
        contract = symbols.from_symbol(order.symbol)
        if contract is None:
            #TODO: add order id in error message?
            self.events.raise_error_event(ErrorMessage(
                msg=f"Contract not found for symbol: '{order.symbol}'",
                code=ErrorCode.GTW_SYMBOL_RESOLVE_ERROR,
                error_type=ErrorType.ORDER,
                req_id=None
            ))
            return

        hdlr = self._event_handler
        mgr = hdlr.order_manager
        internal_oid = order_id
        # `broker_oid` and `cl_oid` must exists
        # given it passed the `raise_undefined_order_reference` check
        broker_oid = mgr.get_mutable_order_id(internal_oid)
        cl_oid = mgr.get_cl_order_id(internal_oid)
        msg = None
        _lmt_price = None
        _stop_price = None
        if order.orderType == OrderType.STP:
            _stop_price = order.stop_price
        elif order.orderType == OrderType.LMT:
            _lmt_price = order.limit_price
        elif order.orderType == OrderType.STP_LMT:
            _stop_price = order.stop_price
            _lmt_price = order.limit_price

        converter = self._order_converter
        tif = converter.get_native_tif(order.tif)

        msg = modify_order_msg(
            req_id=self.next_req_id(),
            contract=contract,
            account_id=self._account_id,
            broker_order_id=broker_oid,
            client_order_id=cl_oid,
            qty=order.quantity,
            limit_price=_lmt_price,
            stop_price=_stop_price,
            tif=tif,
            on_behalf_of=on_behalf_of
        )
        self.submit_message(msg)

    @ensure_api_ready(error_type=ErrorType.GENERAL)
    def request_market_data_bar(self, symbol, exchange, contract_month,
        local_symbol=None, resolution=None,
        start_date=None, end_date=None, user_data=None):
        """
        :param symbol:
        :param exchange:
        :param contract_month:
        :param local_symbol:
        :param resolution: Bar resolution in seconds, if none use the default for the connection
        :param start_date: If None assume now
        :param end_date: If None assume real time data is required
        :param user_data: User data to be returned in every call back
        :return:
        """
        pass

    @ensure_api_ready(error_type=ErrorType.GENERAL)
    def request_open_orders(self):
        """ Request open orders placed by this client.
        """
        mgr = self._order_manager
        events = self.events
        for _, obj in mgr.get_open_orders().items():
            clone = obj.copy()
            clone.is_historical = True
            events.raise_event(events._on_order_update,
                lambda x: x, clone, self)

    @ensure_api_ready(error_type=ErrorType.GENERAL)
    def request_positions(self):
        """ Request positions.
        """
        mgr = self._event_handler.position_manager
        events = self.events
        update = mgr.get_position_update()
        events.raise_event(events._on_position,
            lambda x: x, update, self)

    @ensure_api_ready(error_type=ErrorType.GENERAL)
    def request_account_info(self):
        """ Request account information.
        """
        mgr = self._event_handler.account_manager
        events = self.events
        update = mgr.get_account_update()
        events.raise_event(events._on_account_info,
            lambda x: x, update, self)

    @ensure_api_ready(error_type=ErrorType.GENERAL)
    def request_contract_details(self, **kwargs):
        """ Request contract information
        """
        logger.debug(f"request contract. {locals()}")
        req_id = self.next_req_id()
        symbol = kwargs['symbol']
        self._event_handler.symbols.request_symbol_resolution(req_id, symbol)
        self.submit_message(request_symbol_msg(req_id, symbol))

    @ensure_api_ready(error_type=ErrorType.GENERAL)
    def request_executions(self):
        """ Request executions.
        """
        req_id = self.next_req_id()
        hdlr = self._event_handler
        start_time = datetime.utcnow() - timedelta(days=1)
        start_from = hdlr.msec_from_basetime(start_time)
        msg = request_historical_orders(req_id, self.account_id, start_from)
        self.submit_message(msg)

    @ensure_api_ready(error_type=ErrorType.GENERAL)
    def ping(self):
        """ Ping the server
        """
        self.submit_message(ping_msg('roger'))

    @ensure_api_ready(error_type=ErrorType.GENERAL)
    def subscribe_update(self):
        """
        List open orders. end with trade_snapshot_completions.
        """
        req_id = self.next_req_id()
        logger.debug(f'subscribe update, id={req_id}')
        self.submit_message(subscribe_updates_msg(req_id, True))
        return req_id

    @ensure_api_ready(error_type=ErrorType.GENERAL)
    def unsubscribe_update(self, req_id):
        logger.debug(f'unsubscribe update with id={req_id}')
        self.submit_message(subscribe_updates_msg(req_id, False))

    def _start_event_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._conn = asyncio.ensure_future(self._reader())
        loop.run_forever()
        logger.info("Gracefully disconnected.")

    #TODO: implement and test network error/timeout reconnect
    async def _reader(self):
        loop = self._loop
        endpoint = self._url
        timeout = self._read_timeout
        dispatch = self._event_handler.process_message
        try:
            logger.info(f"Connecting to {endpoint}")
            async with ws.connect(endpoint, loop=loop) as socket:
                self._on_connected(socket)
                try:
                    await self._logon()

                    while True:
                        try:
                            data = await asyncio.wait_for(socket.recv(), timeout=timeout)
                        except asyncio.TimeoutError:
                            # frequent logging of the timeout is meaningless
                            pass
                        except asyncio.CancelledError:
                            logger.info(f"Going to disconnect from {endpoint}")
                            break
                        else:
                            try:
                                msg = cqg_pb.ServerMsg()
                                msg.ParseFromString(data)
                                api_logger.debug("Server message:\n%s" % msg)
                            except Exception as e:
                                logger.warning("Unrecognized incoming data: %s Exception: %s" % (
                                    str(data), str(e)))
                            else:
                                try:
                                    dispatch(msg)
                                except:
                                    logger.exception("Unhandled exception when dispatching message.")
                except ws.ConnectionClosed:
                    logger.error('Connection closed by server.')
        except Exception:
            logger.exception(f"Error connecting to {endpoint}")
            self._connect_failure = True
        finally:
            self._exit_loop()

    async def send_message(self, msg, retry_count=None):
        socket = self._socket
        if socket:
            await socket.send(msg)
        elif isinstance(retry_count, int) and retry_count > 0:
            await asyncio.sleep(1)
            await self.send_message(msg, retry_count - 1)

    def submit_message(self, msg, retry_count=None):
        logger.debug(f"Send message (retry={retry_count}): {msg}")
        asyncio.run_coroutine_threadsafe(self.send_message(msg, retry_count), self._loop)

    def next_req_id(self):
        with self._lock:
            self._req_id += 1
            return self._req_id

    def close(self, reason=''):
        """ Closes socket connection """
        if self._closing:
            logger.debug(f"Already disconnecting websocket. No action will be performed.")
            return
        try:
            self._closing = True
            if self._conn:
                logger.debug(f"Disconnect websocket. Reason: {reason}")
                self._conn.cancel()
        except:
            logger.exception("Unhandled exception when disconnecting websocket."
                " Force exit loop.")
            self._exit_loop()

    def _exit_loop(self):
        try:
            if self._loop:
                self._loop.stop()
                logger.debug("Gracefully stopped event loop")
        except:
            logger.exception("Unhandled exception when stopping event loop")
        finally:
            self._socket = None
            self._conn = None
            self.set_order_api_ready(False)
            self.set_link_status(False)
            self.set_logon_status(False, force_publish=self._connect_failure)
            self._disconnecting = False
            self._connecting = False
            self._closing = False
            self._req_id = 0
            self._connect_failure = False
            self._event_handler.reset()
            self._loop = None

    async def _logon(self):
        logger.debug(f'logon with {self._username}')
        msg = logon_msg(self._username, self._password,
            client_app_id=self._client_app_id,
            client_version=self._client_version,
            private_label=self._private_label,
            drop_concurrent_session=True)
        await self.send_message(msg, retry_count=5)

    def _on_connected(self, socket):
        """ On websocket connected """
        self._socket = socket
        self.set_link_status(True)

    def _on_logon_procedure_done(self):
        """ On account logon procedure completed """
        self._connecting = False
        # this connection event will notify client
        # this gateway is ready to place order
        self.events.raise_connection_event(dict(
            status=ConnectionStatus.CONNECTED))
        self.set_order_api_ready(True)