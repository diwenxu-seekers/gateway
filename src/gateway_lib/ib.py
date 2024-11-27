""" Provides Gatway implementation of Interactive Brokers.
"""
from datetime import datetime, timedelta, timezone
import json
import logging
import os.path
import sys
import threading
from threading import RLock
from typing import Any, Dict
from functools import wraps
import traceback

import ibapi.commission_report as ib_commission
import ibapi.common as ibc
import ibapi.contract as ib_contract
import ibapi.execution as ib_exec
import ibapi.order as ibo
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from pytz import utc

from .contract import (Contract, AbstractContract, ContractType, Currency, Exchange, AbstractContractFinder)
from .error import (ErrorCode, ensure_api_ready, raise_undefined_order_reference,
                    raise_duplicated_order_reference)
from .gateway import (AbstractGateway, GatewayEventProcessor)
from .message import (ErrorType, OrderError, ErrorMessage, ExecutionUpdate, OrderUpdate,
    OrderAPIReady, ConnectionUpdate, ConnectionStatus, Position, PositionUpdate,
    Account, AccountUpdate, ContractDetails, ContractDetailsUpdate, MarketDataBarUpdate,
    OpenOrdersUpdate)
from .order import (Order, OrderConverter, OrderAction, BUY, SELL, OrderType, TIF, OrderStatus)
from .utils import swap_key_value

logger = logging.getLogger(__name__)


def handle_broken_pipe(api_call):
    @wraps(api_call)
    def args(*ar, **kw):
        gateway = ar[0]
        try:
            return api_call(*ar, **kw)
        except BrokenPipeError:
            logger.exception(f"Broken pipe when calling '{api_call.__name__}'.")
            gateway.disconnect()
    return args


def _order_reference_exists(*ar, **kw):
    gateway = ar[0]
    orders = getattr(gateway, 'order_ref_to_id')
    order_ref = ar[1] if len(ar) > 1 else kw['order_id']
    exists = order_ref in orders
    return order_ref, exists


class ExecutionBucket:
    """
    Cache of execution events.
    """
    def __init__(self, reqId, commissions):
        self._executions = {}
        self._commissions = commissions
        self._reqId = reqId
        self._historical = reqId != -1
        self.onNewEvent = None
        self.events = []

    def clear(self):
        self._executions.clear()
        self.events.clear()

    def update_execution(self, execObj):
        commissions = self._commissions
        execId = execObj["execution"].execId
        if execId in commissions:
            d = commissions[execId]
            execObj['is_historical'] = self._historical
            execObj['commission'] = d["commission"]
            execObj['currency'] = d["currency"]
            self.onNewEvent(execObj)
        else:
            self._executions[execId] = execObj

    def notify_commission(self, execId):
        executions = self._executions
        if execId in executions:
            execObj = executions.pop(execId)
            commission = self._commissions[execId]
            execObj['is_historical'] = self._historical
            execObj['commission'] = commission["commission"]
            execObj['currency'] = commission["currency"]
            self.onNewEvent(execObj)


class ExecutionManager:
    """
    Manage the execution caches upon receiving executions and commissions updates.
    """
    def __init__(self, dispatch):
        self._buckets = {}
        self._commissions = {}
        self._dispatch = dispatch
        rtBucket = ExecutionBucket(
            reqId=-1,
            commissions=self._commissions)
        rtBucket.onNewEvent = dispatch
        self._buckets[-1] = rtBucket

    def clear(self):
        self._commissions.clear()
        buckets = self._buckets
        reqIds = [x for x in buckets if x >=0]
        for x in reqIds:
            buckets[x].clear()
            del buckets[x]
        buckets[-1].clear()

    def request_historical(self, reqId):
        bucket = ExecutionBucket(
            reqId=reqId,
            commissions=self._commissions)
        bucket.onNewEvent = lambda e: bucket.events.append(e)
        self._buckets[reqId] = bucket

    def notify_historical_end(self, reqId):
        #TODO: implement dispatch collection or partial dispatch
        buckets = self._buckets
        if reqId in buckets:
            bucket = buckets.pop(reqId)
            events = bucket.events
            dispatch = self._dispatch
            for x in events:
                dispatch(x)
            bucket.clear()
        else:
            logger.warning(f"Unhandled reqId: {reqId}.")

    def update_execution(self, reqId, execObj):
        buckets = self._buckets
        if reqId in buckets:
            buckets[reqId].update_execution(execObj)
        else:
            logger.warning(f"Unhandled reqId: {reqId}. Skip execution update: {execObj}")

    def update_commission(self, execId, commission):
        # assume execId is unique, i.e. no two different executions shares the same id
        # note `_commissions` will slowly leak memory
        # This allows execution update to proceed using commission cache.
        self._commissions[execId] = commission
        buckets = self._buckets
        for reqId in buckets:
            buckets[reqId].notify_commission(execId)


class IB_API(EClient, EWrapper):
    def __init__(self, gateway: AbstractGateway, events):
        self._events = events
        self._gw = gateway
        self.nextValidOrderId = None
        self.next_request_id = 1
        self._ib_server_connected = False  # IB doc claims API calls before receiving the first valid id might be dropped.
        self._is_logon = False
        self._req_open_orders = False
        self.position_snapshot = {}
        self.account_snapshot = {}
        self._executionManager = ExecutionManager(dispatch=events.raise_execution_event)
        self.open_order_snapshot = {}
        self.open_order_update = []
        self.contract_details_snapshot = []
        self._lock = RLock()
        EWrapper.__init__(self)
        EClient.__init__(self, self)

    @property
    def is_healthy(self):
        # received `nextValidId` callback and gateway is connected to TWS
        return self.is_logon and self.isConnected()

    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    def nextReqId(self):
        with self._lock:
            oid = self.next_request_id
            self.next_request_id += 1
            return oid

    @property
    def ib_server_connected(self):
        return self._ib_server_connected

    @ib_server_connected.setter
    def ib_server_connected(self, val):
        old = self._ib_server_connected
        self._ib_server_connected = val
        if old != val:
            self._events.raise_order_api_ready_event(dict(
                ready=self._gw.can_manipulate_order))

    @property
    def is_logon(self):
        return self._is_logon

    @is_logon.setter
    def is_logon(self, val):
        self._is_logon = val
        # `RetryGateway` relies on receiving every DISCONNECTED event to trigger reconnect operation.
        self._events.raise_connection_event(dict(
            status=ConnectionStatus.CONNECTED if self._gw.is_healthy else ConnectionStatus.DISCONNECTED))

    ########################
    # Implement IB interface
    ########################

    def reset(self):
        super().reset()
        self.nextValidOrderId = None
        self._req_open_orders = False
        self.ib_server_connected = False
        self.next_request_id = 1
        self.position_snapshot.clear()
        self.account_snapshot.clear()
        self.open_order_snapshot.clear()
        self.open_order_update.clear()
        self.contract_details_snapshot.clear()
        self._executionManager.clear()

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        self._gw.set_connecting(False)
        self.is_logon = True
        self.ib_server_connected = True

    def error(self, reqId: ibc.TickerId, errorCode: int, errorString: str):
        ## Order Error Code
        # 200
        # 201
        # 202
        # 10147
        # 10149
        # 103   - Duplicate order ID - something wrong in order id
        # 10149 - Invalid order id
        # 107   - Cannot transmit incomplete order
        # 110   - The price does not conform to the minimum price variation for this contract.
        # 116   - The order cannot be transmitted to a dead exchange.
        # 201   - Order rejected - Reason:
        # 105   - Order being modified does not match original order
        # 329   - Order modify failed. Cannot change to the new order type.

        ## Connection Error Code
        # 501   - Already Connected
        # 502   - Could not connect to TWS
        # 503   - Problem between API version and the version of TWS
        # 504   - Request made to broken connection probably due to unhandled client exception

        super().error(reqId, errorCode, errorString)
        publish = True
        if errorCode in [1100, 1300]:
            # 1100  - Connection between IB and TWS/GW was lost. (including: internet issue, nightly reset, competing session)
            # 1300  - TWS/GW port number changed.
            publish = self.ib_server_connected
            self.ib_server_connected = False
        elif errorCode in [1101, 1102]:
            # 1101  - Connection between IB and TWS/GW restored, and need to re-submit market data request.
            # 1102  - Connection between IB and TWS/GW restored, and no need to re-submit market data request.
            publish = not self.ib_server_connected
            self.ib_server_connected = True
        elif errorCode in [2104, 2106, 2158]:
            # called when TWS connected, not for IBGateway
            # no need to propagate this message unless for data subscription callback
            # 2104 - Market data farm connection is OK:<farm_id>
            # 2106 - HMDS data farm connection is OK:<farm_id>
            # 2158 - Sec-def data farm connection is OK:<farm_id>
            publish = False
        elif errorCode in [202] and errorString == 'Order Canceled - reason:':
            # 202 - order cancelled by user (TWS or API)
            # since it would trigger OrderUpdate message.
            # there is no need to propagate the same message as error.
            publish = False

        if publish:
            order_ref_lookup = self._gw.order_id_to_ref
            if order_ref_lookup is not None \
                and reqId is not None \
                and reqId in order_ref_lookup:
                msg = OrderError(
                    order_id=order_ref_lookup[reqId],
                    msg=errorString,
                    code=errorCode,
                    req_id=reqId)
            else:
                msg = ErrorMessage(msg=errorString, code=errorCode, req_id=reqId)
            self._events.raise_error_event(msg)

    def openOrder(self, orderId, contract: ib_contract.Contract, order: ibo.Order,
                  orderState):
        super().openOrder(orderId, contract, order, orderState)
        orderId = self._use_permId(order.clientId, orderId, order.permId)
        self.open_order_snapshot[orderId] = (contract, order, orderState)

    def orderStatus(self, orderId: ibc.OrderId, status: str, filled: float,
                    remaining: float, avgFillPrice: float, permId: int,
                    parentId: int, lastFillPrice: float, clientId: int,
                    whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining,
                            avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        orderId = self._use_permId(clientId, orderId, permId)
        if self.nextValidOrderId is None:
            return
        args = locals()
        args['is_historical'] = self._req_open_orders
        self._events.raise_order_event(locals())

    def reqExecutions(self, reqId, execFilter):
        super().reqExecutions(reqId, execFilter)
        self._executionManager.request_historical(reqId)

    def execDetailsEnd(self, reqId):
        super().execDetailsEnd(reqId)
        self._executionManager.notify_historical_end(reqId)

    def execDetails(self, reqId: int, contract: ib_contract.Contract, execution: ib_exec.Execution):
        super().execDetails(reqId, contract, execution)
        execObj = locals()
        self._executionManager.update_execution(reqId, execObj)

    def commissionReport(self, commissionReport: ib_commission.CommissionReport):
        super().commissionReport(commissionReport)
        self._executionManager.update_commission(
            commissionReport.execId,
            {'commission': commissionReport.commission, 'currency': commissionReport.currency})

    def connectionClosed(self):
        super().connectionClosed()
        self._gw._on_connection_closed()

    def position(self, account: str, contract: ib_contract.Contract, position: float,
                 avgCost: float):
        super().position(account, contract, position, avgCost)
        _sym = contract.localSymbol
        self.position_snapshot[_sym] = (contract, position)

    def positionEnd(self):
        super().positionEnd()
        self.cancelPositions()
        self._events.raise_position_event(self.position_snapshot)

    def accountSummary(self, reqId: int, account: str, tag: str, value: str,
                       currency: str):
        super().accountSummary(reqId, account, tag, value, currency)
        if account not in self.account_snapshot:
            self.account_snapshot[account] = {}
        self.account_snapshot[account][tag] = value

    def accountSummaryEnd(self, reqId: int):
        super().accountSummaryEnd(reqId)
        self.cancelAccountSummary(reqId)
        self._events.raise_account_info_event(self.account_snapshot)

    def contractDetails(self, reqId: int, contractDetails: ib_contract.ContractDetails):
        super().contractDetails(reqId, contractDetails)
        self.contract_details_snapshot.append(contractDetails)

    def contractDetailsEnd(self, reqId: int):
        super().contractDetailsEnd(reqId)
        self._events.raise_contract_details_event(self.contract_details_snapshot)
        self.contract_details_snapshot.clear()

    def historicalData(self, reqId: int, bar: ibc.BarData):
        super().historicalData(reqId, bar)
        subscription: IbMarketDataSubscription = self._gw.get_market_data_subscription(reqId)
        last_bar = subscription.last_historical_bar
        subscription.last_historical_bar = bar

        if last_bar:
            msg = {
                'reqId': reqId,
                'time': int(last_bar.date),
                'open_': last_bar.open,
                'high': last_bar.high,
                'low': last_bar.low,
                'close': last_bar.close,
                'volume': last_bar.volume
            }
            self._events.raise_market_data_bar_event(msg)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        subscription: IbMarketDataSubscription = self._gw.get_market_data_subscription(reqId)
        subscription.notify_live()

    def realtimeBar(self, reqId: ibc.TickerId, time: int, open_: float, high: float, low: float, close: float,
                    volume: int, wap: float, count: int):
        super().realtimeBar(reqId, time, open_, high, low, close, volume, wap, count)
        subscription = self._gw.get_market_data_subscription(reqId)
        subscription.build_bar(time, open_, high, low, close, volume)
        bar = subscription.checkout_bar()
        if bar:
            msg = {
                'reqId': reqId,
                'time': int(bar.time),
                'open_': bar.open_,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume
            }
            self._events.raise_market_data_bar_event(msg)

    def reqOpenOrders(self):
        self._req_open_orders = True
        super().reqOpenOrders()
        self.open_order_update.clear()

    def openOrderEnd(self):
        self._req_open_orders = False
        super().openOrderEnd()
        if self._is_logon:
            self._events.raise_open_order_end_event(dict(
                client_id=self._gw.client_id,
                open_orders=self.open_order_update.copy()))

    def _use_permId(self, clientId, orderId, permId):
        if clientId == 0 and orderId == 0:
            logger.debug(f"Set orderId to permId ({permId})")
            return permId
        return orderId


class IBContract(AbstractContract):
    _contract_type = {
        ContractType.Future: 'FUT',
        ContractType.Stock: 'STK',
    }
    _native_to_contractType = swap_key_value(_contract_type)

    _exch_to_native = {
        Exchange.GLOBEX: 'GLOBEX',
        Exchange.CME: 'CME',
        Exchange.COMEX: 'COMEX',
        Exchange.NYMEX: 'NYMEX',
        Exchange.HKFE: 'HKFE',
        Exchange.SEHK: 'SEHK'}
    _native_to_exch = swap_key_value(_exch_to_native)

    _ccy_to_native = {Currency.USD: 'USD', Currency.HKD: 'HKD'}
    _native_to_ccy = swap_key_value(_ccy_to_native)

    def create(self, contract: Contract):
        ctype = contract.contract_type
        if ctype == ContractType.Future:
            obj = ib_contract.Contract()
            obj.localSymbol = contract.symbol
            obj.secType = self.get_native_contract_type(ctype)
            obj.exchange = self.get_native_exchange(contract.exchange)
            # obj.currency = self.get_native_currency(contract.currency)    # optional for FUT and STK
            # obj.lastTradeDateOrContractMonth = contract.contract_month    # optional if local symbol is used
            return obj
        raise NotImplementedError()

    def get_native_currency(self, val):
        return IBContract._ccy_to_native[val]

    def get_native_exchange(self, val):
        return IBContract._exch_to_native[val]

    def get_native_contract_type(self, val):
        return IBContract._contract_type[val]

    def get_internal_currency(self, val):
        return IBContract._native_to_ccy[val]

    def get_internal_exchange(self, val):
        return IBContract._native_to_exch[val]

    def get_internal_contract_type(self, val):
        return IBContract._native_to_contractType[val]


class IBOrderConverter(OrderConverter):
    _action_to_native = {
        OrderAction.BUY: 'BUY',
        OrderAction.SELL: 'SELL',
    }
    _native_to_action = swap_key_value(_action_to_native)
    _orderType_to_native = {
        OrderType.LMT: 'LMT',
        OrderType.MKT: 'MKT',
        OrderType.STP: 'STP',
        OrderType.STP_LMT: 'STP LMT',
    }
    _native_to_orderType = swap_key_value(_orderType_to_native)
    _tif_to_native = {
        TIF.GTC: 'GTC',
        TIF.DAY: 'DAY',
        TIF.GTD: 'GTD',
    }
    _native_to_tif = swap_key_value(_tif_to_native)

    def convert_from(self, order: Order):
        act = self.get_native_order_action(order.action)
        orderType = self.get_native_order_type(order.orderType)
        tif = self.get_native_tif(order.tif)
        obj = ibo.Order()
        if order.orderType == OrderType.LMT:
            obj.lmtPrice = order.limit_price
        elif order.orderType == OrderType.STP:
            obj.auxPrice = order.stop_price
        elif order.orderType == OrderType.STP_LMT:
            obj.lmtPrice = order.limit_price
            obj.auxPrice = order.stop_price
        obj.orderType = orderType
        obj.action = act
        obj.totalQuantity = order.quantity
        obj.tif = tif
        obj.outsideRth = order.outsideRth
        obj.goodTillDate = order.goodTillDate
        # get rid of IB warning since TWS 983
        obj.eTradeOnly = False
        obj.firmQuoteOnly = False
        return obj

    def convert_to(self, native) -> Order:
        contract: ib_contract.Contract = native[0]
        order: ibo.Order = native[1]
        contract_converter = IBContract()
        _exchange = contract_converter.get_internal_exchange(contract.exchange)
        _contractType = contract_converter.get_internal_contract_type(contract.secType)
        _orderType = self.get_internal_order_type(order.orderType)
        _action = self.get_internal_order_action(order.action)
        _tif = self.get_internal_tif(order.tif)
        _outsideRth = order.outsideRth
        _goodtilldate = order.goodTillDate
        _lmt_price = 0.0
        _stop_price = 0.0
        if OrderType.MKT == _orderType:
            _lmt_price = 0.0
        elif OrderType.LMT == _orderType:
            _lmt_price = order.lmtPrice
        elif OrderType.STP == _orderType:
            _stop_price = order.auxPrice
        elif OrderType.STP_LMT == _orderType:
            _lmt_price = order.lmtPrice
            _stop_price = order.auxPrice
        return Order(symbol=contract.localSymbol, exchange=_exchange, contractType=_contractType,
            orderType=_orderType, action=_action, quantity=order.totalQuantity, limit_price=_lmt_price,
            tif=_tif, stop_price=_stop_price, outsideRth=_outsideRth, goodTillDate=_goodtilldate)

    def get_native_order_action(self, val):
        return IBOrderConverter._action_to_native[val]

    def get_native_order_type(self, val):
        return IBOrderConverter._orderType_to_native[val]

    def get_native_tif(self, val):
        return IBOrderConverter._tif_to_native[val]

    def get_internal_order_action(self, val):
        return IBOrderConverter._native_to_action[val]

    def get_internal_order_type(self, val):
        return IBOrderConverter._native_to_orderType[val]

    def get_internal_tif(self, val):
        return IBOrderConverter._native_to_tif[val]


class IBGatewayEventProcessor(GatewayEventProcessor):
    # skipped PendingSubmit, PendingCancel
    _order_status_map = dict(
        PreSubmitted=OrderStatus.SUBMITTED,  # e.g. STOP order or other trigger based order
        Submitted=OrderStatus.SUBMITTED,
        Cancelled=OrderStatus.CANCELLED,
        Filled=OrderStatus.FILLED,
        Inactive=OrderStatus.INACTIVE)

    _ib_time_format = '%Y%m%d  %H:%M:%S'

    def __init__(self, gateway):
        super().__init__(gateway)
        #TODO: 1) consider persist order status filter in a database
        # otherwise disconnect or crash would lose all records
        # and thus the duplicate check would failed.
        # 2) consider replacing all caches used by gateway with the same database
        self._prev_order_status_filter = dict()
        self._order_converter = IBOrderConverter()

    def reset(self):
        self._prev_order_status_filter.clear()

    def process_error_event(self, msg: ErrorMessage):
        return msg

    def process_execution_event(self, obj: dict):
        is_historical = obj['is_historical']
        _exec: ib_exec.Execution = obj['execution']
        _commission = obj['commission']
        _currency = obj['currency']
        _contract = obj['contract']
        _symbol = _contract.localSymbol  # TODO: resolve internal symbol mapping
        exec_id = _exec.execId
        avg_price = _exec.avgPrice
        broker_order_id = _exec.orderId
        order_ref = self._gw.order_id_to_ref.get(broker_order_id, None)
        qty = _exec.shares
        _side = None
        if _exec.side == 'BOT':
            _side = BUY
        elif _exec.side == 'SLD':
            _side = SELL
        dt = datetime.strptime(_exec.time, IBGatewayEventProcessor._ib_time_format)
        _dt = dt.replace(tzinfo=timezone(timedelta(hours=0)))  # assume ib is set to UTC
        res = ExecutionUpdate(client_id=_exec.clientId, order_ref=order_ref, broker_order_id=broker_order_id,
                              exec_id=exec_id, timestamp=_dt,
                              side=_side, symbol=_symbol, filled=qty, price=_exec.price,
                              cum_qty=_exec.cumQty, avg_price=avg_price, commission=_commission, currency=_currency,
                              is_historical=is_historical)
        return res

    def process_order_event(self, obj: dict):
        """
        Handled duplicated order update.
        Handled partial filled.
        """
        is_historical = obj['is_historical']
        broker_order_id = obj['orderId']
        status = obj['status']
        _status = IBGatewayEventProcessor._order_status_map.get(status, OrderStatus.UNDEFINED)
        if _status is OrderStatus.UNDEFINED:
            return None

        remaining = obj['remaining']
        filled = obj['filled']
        order = None
        limit_price = None
        stop_price = None
        qty = None
        order_info = self._gw.api.open_order_snapshot.get(broker_order_id, None)
        tif = None
        outsideRth = None
        orderType = None
        if order_info is not None:
            try:
                order = self._order_converter.convert_to(order_info)
            except Exception as e:
                tb = traceback.format_exc()
                logger.warning(f"Failed to convert order. Reason: {e}\n{tb}")
                return None
            else:
                limit_price = order.limit_price
                stop_price = order.stop_price
                qty = order.quantity
                tif = order.tif
                outsideRth = order.outsideRth
                orderType = order.orderType

        if not is_historical:
            cache = self._prev_order_status_filter
            outdated = [oid for oid in cache if cache[oid][0] < datetime.now() - timedelta(days=1)]
            for oid in outdated:
                logger.info(f'Remove outdated order update: {oid}, {cache[oid]}')
                cache.pop(oid)
            current = (datetime.now(),
                limit_price, stop_price, qty, status, remaining, orderType, filled, tif, outsideRth)
            logger.info(f'New order update: {broker_order_id}, {current}')
            if broker_order_id in cache:
                prev_order = cache[broker_order_id]
                if current[1:] == prev_order[1:]:
                    logger.warning(f"Skip duplicated order update: {broker_order_id}, {current[0]}, prev: {prev_order}")
                    return None
            cache[broker_order_id] = current

        if (_status == OrderStatus.SUBMITTED
                and remaining > 0
                and filled > 0):
            _status = OrderStatus.PARTIAL_FILLED
        msg = ''
        order_ref = self._gw.order_id_to_ref.get(broker_order_id, None)
        client_id = obj['clientId']
        res = OrderUpdate(
            client_id=client_id,
            broker_order_id=broker_order_id,
            order_ref=order_ref,
            order=order,
            remaining=remaining,
            filled=filled,
            status=_status,
            msg=msg,
            is_historical=is_historical)

        if is_historical:
            self._gw.api.open_order_update.append(res)

        return res

    def process_connection_event(self, obj: dict):
        return ConnectionUpdate(status=obj['status'])

    def process_order_api_ready_event(self, obj: dict):
        return OrderAPIReady(ready=obj['ready'])

    def process_position_event(self, obj: dict):
        _list = []
        for broker_sym in obj:
            contract, pos = obj[broker_sym]
            _list.append(Position(symbol=contract.symbol, broker_symbol=broker_sym, net_position=pos))
        return PositionUpdate(_list)

    def process_account_info_event(self, obj: dict):
        _list = []
        for acc in obj:
            _d = obj[acc]
            _list.append(Account(acc_name=acc,
                                 cash_balance=_d['TotalCashBalance'],
                                 realized_PnL=_d['RealizedPnL'], unrealized_PnL=_d['UnrealizedPnL'],
                                 currency=_d['Currency']))
        return AccountUpdate(_list)

    def process_contract_details_event(self, obj: dict):
        _list = []
        for contract in obj:
            c = contract.contract
            _list.append(ContractDetails(symbol=c.symbol, local_symbol=c.localSymbol, exchange=c.exchange,
                                         last_trading_date=c.lastTradeDateOrContractMonth,
                                         contract_month=contract.contractMonth, trading_class=c.tradingClass,
                                         timezone=contract.timeZoneId))
        return ContractDetailsUpdate(_list)

    def process_market_data_bar_event(self, obj: dict):
        req_id = obj['reqId']
        subscription = self._gw.get_market_data_subscription(req_id)
        update = MarketDataBarUpdate(subscription.resolution, obj['time'] + subscription.resolution, obj['open_'],
                                     obj['high'], obj['low'], obj['close'], obj['volume'], subscription.user_data)
        return update

    def process_open_order_end_event(self, obj: dict):
        client_id = obj['client_id']
        orders = obj['open_orders']
        update = OpenOrdersUpdate(orders)
        update.client_id = client_id
        return update


class IBContractFinder(AbstractContractFinder):
    def __init__(self):
        self._contract_loader = IBContract()

    def from_symbol(self, symbol: str):
        """
        Resolve broker native contract by internal symbol name.
        """
        raise NotImplementedError("Need to resolve the contract to be traded from internal symbol")

    def from_exchange_symbol(self, exchange: int, contractType: int, symbol: str):
        """
        Resolve broker native contract by exchange symbol and other details.
        """
        return self._contract_loader.create(Contract(contractType, symbol, exchange))


class IbMarketDataSubscription:
    ONE_HOUR = timedelta(hours=1)
    SECONDS_TO_DURATION_STRING: Dict[int, str] = {1: '1 sec',
                                                  5: '5 secs',
                                                  15: '15 secs',
                                                  30: '30 secs',
                                                  60: '1 min',
                                                  2 * 60: '2 mins',
                                                  3 * 60: '3 mins',
                                                  5 * 60: '5 mins',
                                                  15 * 60: '15 mins',
                                                  30 * 60: '30 mins',
                                                  60 * 60: '1 hour',
                                                  24 * 60 * 60: '1 day'}

    class Bar:
        def __init__(self, time):
            self.time: int = time
            self.open_: float = None
            self.high: float = -sys.maxsize
            self.low: float = sys.maxsize
            self.close: float = None
            self.volume: int = 0

        def append(self, open_: float, high: float, low: float, close: float, volume: int):
            if self.open_ is None:
                self.open_ = open_
            self.high = max(self.high, high)
            self.low = min(self.low, low)
            self.close = close
            self.volume += volume

    def __init__(self, req_id: int, contract: ib_contract.Contract, resolution: int, start_date: datetime,
                 end_date: datetime, user_data: Any):
        """

        :param req_id:
        :param contract:
        :param resolution: Resolution in seconds
        :param user_data:
        """
        if int(self.ONE_HOUR.total_seconds()) % resolution != 0:
            raise ValueError(f'Bar data resolution must be a factor of one hour, required resolution: '
                             f'{resolution} seconds')

        self._req_id = req_id
        self._contract = contract
        self._resolution = resolution
        self._start_date = start_date
        self._end_date = end_date
        self._user_data = user_data
        self._live_subscription = None
        self._next_bar_start = None
        self._current_bar = None
        self._last_live_bar = None

        self.last_historical_bar: ibc.BarData = None

    @property
    def contract(self):
        return self._contract

    @property
    def end_date(self):
        return self._end_date

    @property
    def req_id(self):
        return self._req_id

    @property
    def resolution(self):
        return self._resolution

    @resolution.setter
    def resolution(self, value: int):
        self._resolution = value

    @property
    def start_date(self):
        return self._start_date

    @property
    def user_data(self):
        return self._user_data

    def bind_live_subscription(self, subscription):
        self._live_subscription = subscription

    def build_bar(self, time: int, open_: float, high: float, low: float, close: float, volume: int):
        if self._current_bar is None:
            return

        if time >= self._next_bar_start:
            self._last_live_bar = self._current_bar
            self._current_bar = self.Bar(self._next_bar_start)
            self._next_bar_start += self.resolution

        self._current_bar.append(open_, high, low, close, volume)

    def checkout_bar(self):
        try:
            return self._last_live_bar
        finally:
            self._last_live_bar = None

    def notify_live(self):
        if self._live_subscription is not None:
            self._live_subscription.start_live(self.last_historical_bar)

    def start_live(self, last_historical_bar: ibc.BarData):
        bar_time = int(last_historical_bar.date)
        self._next_bar_start = bar_time + self.resolution
        self._current_bar = self.Bar(bar_time)
        self._current_bar.open_ = last_historical_bar.open
        self._current_bar.high = last_historical_bar.high
        self._current_bar.low = last_historical_bar.low
        self._current_bar.close = last_historical_bar.close
        self._current_bar.volume = last_historical_bar.volume


class IBGateway(AbstractGateway):
    IB_TRADES = 'TRADES'
    MAX_ID = 999999
    next_client_id = 3000  # default client id begins from 3000

    def __init__(self, name: str, host: str, port: int, client_id: int = None, state_filepath: str = None):
        event_handler = IBGatewayEventProcessor(self)
        super().__init__(name=name, event_handler=event_handler)
        self.api = IB_API(self, self.events)
        self._contract_finder = IBContractFinder()
        self._order_converter = IBOrderConverter()
        self.host = host
        self.port = port
        self.client_id = client_id
        if self.client_id is None:
            self.client_id = IBGateway.next_client_id
            IBGateway.next_client_id = IBGateway.next_client_id + 1

        self.order_ref_to_id = {}  # map internal order reference to broker native order id
        self.order_id_to_ref = {}  # map broker native order id to internal order reference
        self.state_file = f'{self.name}-ib.json' if state_filepath is None else state_filepath
        try:
            self.load_state()
        except:
            logger.warning(f'Failed to load state from "{self.state_file}".')

        self._market_data_subscriptions: Dict[int, IbMarketDataSubscription] = dict()
        self._closing = False
        self._disconnecting = False
        self._connecting = False

    def set_disconnecting(self, value):
        self._disconnecting = value

    def set_connecting(self, value):
        self._connecting = value

    def __repr__(self):
        return f"IBGateway('{self.name}', '{self.host}', {self.port}, {self.client_id})"

    def __str__(self):
        return f"IBGateway('{self.name}')"

    def save_state(self):
        _file = self.state_file
        with open(_file, 'w') as fd:
            state = dict(
                order_id_to_ref=self.order_id_to_ref.copy(),
            )
            json.dump(state, fd, indent=4)

    def get_market_data_subscription(self, req_id: int) -> IbMarketDataSubscription:
        try:
            return self._market_data_subscriptions[req_id]
        except KeyError:
            return None

    def load_state(self):
        _file = self.state_file

        if not os.path.isfile(_file):
            logger.info(f'Order reference file "{_file}" not found, create a new one...')
            with open(_file, 'w') as fd:
                json.dump({'order_id_to_ref': {}}, fd)

        with open(_file, 'r') as fd:
            try:
                state = json.load(fd)
                self.order_id_to_ref = {int(k): v for k, v in state['order_id_to_ref'].items()}
                logger.info(f'Loaded "{_file}".')
                self.order_ref_to_id = {v: k for k, v in self.order_id_to_ref.items()}
            except json.decoder.JSONDecodeError:
                logger.warning(f'Failed to loaded "{_file}".')
                self.order_id_to_ref = None
                self.order_ref_to_id = None

    def _create_message_loop(self, name: str):
        self._thread = threading.Thread(target=self._reader)
        self._thread.name = name
        self._thread.daemon = False
        return self._thread

    def _reader(self):
        try:
            self.api.run()
            logger.info('Reader finished gracefully.')
        except:
            logger.exception('Unhandled exception in Reader.')

    #############################
    # Implement gateway interface
    #############################

    @property
    def identity(self):
        """ Returns the identity recognized by the execution venue.
        This is use to differentiate orders placed by this gateway.
        """
        return self.client_id

    @handle_broken_pipe
    def connect(self):
        """ Connect to broker.
        """
        if not self.is_healthy and not self._connecting \
            and not self.api.isConnected():
            self._connecting = True
            logger.debug('Going to establish connection.')
            try:
                self.api.connect(self.host, self.port, self.client_id)
            except:
                # Exception could be raised in `ibapi` module:
                # self.conn could be None (ibapi/client.py line 149)
                msg = "Error establishing connection."
                logger.exception(msg)
                self.close(reason=msg)
            else:
                try:
                    t = self._create_message_loop(f'IBAPI_MSG_HANDLER_{self}')
                    t.start()
                except:
                    msg = "Error creating message reader."
                    logger.exception(msg)
                    self.close(reason=msg)

    @handle_broken_pipe
    def disconnect(self):
        """ Disconnect from broker.
        """
        reason=""
        if self.is_healthy and not self._disconnecting:
            self._disconnecting = True
            logger.debug('Going to disconnect.')
            reason="User disconnect."
        elif self.api.isConnected():
            reason = "Close connection by user."
        if reason:
            self.close(reason=reason)

    def close(self, reason=''):
        """ Closes connection """
        if self._closing:
            logger.debug(f"Already disconnecting TWS. No action will be performed.")
            return

        logger.debug(f"Disconnect socket. Reason: {reason}")
        self._closing = True
        if self.api.conn is None:
            self._clean_exit()
        else:
            try:
                # normal disconnect, should trigger `_on_connection_closed`
                self.api.disconnect()
            except:
                logger.exception("Unhandled exception when disconnecting TWS. Continue to reset states.")
                self._clean_exit()

    def _on_connection_closed(self):
        """ Handles callback from IB API disconnect when `conn` is not None.

        This includes:
         - user request disconnect
         - socket error when reading msg
        """
        self._clean_exit()

    def _clean_exit(self):
        self.api.reset()
        self._closing = False
        self._disconnecting = False
        self._connecting = False
        self.api.is_logon = False

    @property
    def is_healthy(self):
        """ Returns True if connectivity with broker is healthy.
        """
        return self.api.is_healthy

    @property
    def can_manipulate_order(self):
        """ Returns True if gateway is healthy and is ready for order API calls.
        """
        return self.is_healthy and self.api.ib_server_connected

    #TODO: order prefix and on_behalf_of are not implemented
    @ensure_api_ready(error_type=ErrorType.ORDER, attr_ready='can_manipulate_order')
    @raise_duplicated_order_reference(_order_reference_exists)
    @handle_broken_pipe
    def place_order(self, order_id: str, order: Order,
        order_prefix='', on_behalf_of=''):
        """ Place order.
        """
        broker_order_id = self.api.nextOrderId()
        self.order_ref_to_id[order_id] = broker_order_id
        self.order_id_to_ref[broker_order_id] = order_id
        try:
            self.save_state()
            self.api.placeOrder(broker_order_id,
                                self._contract_finder.from_exchange_symbol(order.exchange, order.contractType,
                                                                           order.symbol),
                                self._order_converter.convert_from(order))
        except:
            text = f"Failed to save state to '{self.state_file}'. Skip placing order: {order}"
            logger.exception(text)
            msg = ErrorMessage(msg=text, code=0, req_id=broker_order_id, error_type=ErrorType.ORDER)
            self.events.raise_error_event(msg)

    @ensure_api_ready(error_type=ErrorType.ORDER, attr_ready='can_manipulate_order')
    @raise_undefined_order_reference(_order_reference_exists)
    @handle_broken_pipe
    def cancel_order(self, order_id: str,
        on_behalf_of=''):
        """ Cancel open order.
        """
        broker_order_id = self.order_ref_to_id[order_id]
        logger.debug(f'Cancel order: ref={order_id} order_id={broker_order_id}')
        self.api.cancelOrder(broker_order_id)

    @ensure_api_ready(error_type=ErrorType.ORDER, attr_ready='can_manipulate_order')
    @raise_undefined_order_reference(_order_reference_exists)
    @handle_broken_pipe
    def modify_order(self, order_id: str, order: Order,
        on_behalf_of=''):
        """ Modify open order.
        """
        broker_order_id = self.order_ref_to_id[order_id]
        logger.debug(f'Modify order: ref={order_id} order_id={broker_order_id}')
        self.api.placeOrder(broker_order_id,
                            self._contract_finder.from_exchange_symbol(order.exchange, order.contractType,
                                                                       order.symbol),
                            self._order_converter.convert_from(order))

    @ensure_api_ready(error_type=ErrorType.GENERAL)
    @handle_broken_pipe
    def request_market_data_bar(self, symbol: str, exchange: str, contract_month: str, local_symbol: str = None,
                                resolution: int = 60, start_date: datetime = None, end_date: datetime = None,
                                user_data=None):
        contract = ib_contract.Contract()

        contract.symbol = symbol
        contract.exchange = exchange
        contract.secType = ContractType.to_str(ContractType.Future)
        contract.lastTradeDateOrContractMonth = contract_month
        contract.localSymbol = local_symbol

        live_subscription = None
        if end_date is None:
            req_id = self.api.nextReqId()
            live_subscription = IbMarketDataSubscription(req_id, contract, resolution, None, None, user_data)
            self._market_data_subscriptions[req_id] = live_subscription
            self.api.reqRealTimeBars(req_id, contract, 0, self.IB_TRADES, False, None)
            end_date = datetime.now()

        if start_date is None:
            start_date = datetime.now() - timedelta(hours=1)

        start_date = start_date.astimezone(tz=utc)
        end_date = end_date.astimezone(tz=utc)
        duration = end_date - start_date

        req_id = self.api.nextReqId()
        historical_subscription = IbMarketDataSubscription(req_id, contract, resolution, start_date, end_date,
                                                           user_data)
        historical_subscription.bind_live_subscription(live_subscription)
        self._market_data_subscriptions[req_id] = historical_subscription
        self.api.reqHistoricalData(req_id, contract, '', f'{int(duration.days)} D',
                                   IbMarketDataSubscription.SECONDS_TO_DURATION_STRING[resolution],
                                   self.IB_TRADES, 0, 2, False, None)

    @ensure_api_ready(error_type=ErrorType.GENERAL)
    @handle_broken_pipe
    def request_open_orders(self):
        """ Request open orders placed by this client.
        """
        self.api.reqOpenOrders()

    @ensure_api_ready(error_type=ErrorType.GENERAL)
    @handle_broken_pipe
    def request_positions(self):
        """ Request positions.
        """
        self.api.reqPositions()

    @ensure_api_ready(error_type=ErrorType.GENERAL)
    @handle_broken_pipe
    def request_account_info(self):
        """ Request account information.
        """
        self.api.reqAccountSummary(reqId=self.api.nextReqId(), groupName='All', tags='$LEDGER:USD')

    @ensure_api_ready(error_type=ErrorType.GENERAL)
    @handle_broken_pipe
    def request_contract_details(self, **kwargs):
        """ Request contract details.
        """
        self.api.reqContractDetails(reqId=self.api.nextReqId(), **kwargs)

    @ensure_api_ready(error_type=ErrorType.GENERAL)
    @handle_broken_pipe
    def request_executions(self):
        """ Request executions.
        """
        self.api.reqExecutions(reqId=self.api.nextReqId(), execFilter=ib_exec.ExecutionFilter())

    @ensure_api_ready(error_type=ErrorType.GENERAL)
    @handle_broken_pipe
    def ping(self):
        """ Ping the server
        """
        self.api.reqCurrentTime()

    @ensure_api_ready(error_type=ErrorType.GENERAL)
    @handle_broken_pipe
    def retry_request(self, req_id):
        """ Retry an IB request

        :param req_id:
        :return:
        """
        subscription = self.get_market_data_subscription(req_id)
        if subscription:
            logging.info(f'Retry a market data request, reqId: {req_id}')
            contract = subscription.contract
            self.request_market_data_bar(contract.symbol, contract.exchange, contract.lastTradeDateOrContractMonth,
                                         local_symbol=contract.localSymbol, resolution=subscription.resolution,
                                         start_date=subscription.start_date, end_date=subscription.end_date,
                                         user_data=subscription.user_data)
        else:
            logging.warning(f"Couldn't find and request to retry, ID: {req_id}")
