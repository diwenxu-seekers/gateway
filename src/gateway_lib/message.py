import datetime
import json
from functools import partial
from typing import Union, Optional

_SEP = '__'

_pprint = partial(json.dumps, indent=4)

_datetime_format = '%Y-%m-%dT%H:%M:%S.%Z'


class RequestTag:
    __slots__ = ('req_id', 'client_id')

    def __init__(self, req_id: int, client_id: str):
        self.req_id = req_id
        self.client_id = client_id

    def __str__(self):
        return f'req#: {self.req_id}, client_id: {self.client_id}'


class Request:
    __slots__ = ('tag', 'uid')

    def __init__(self, tag: RequestTag):
        self.tag = tag
        self.uid = _SEP.join([str(self.tag.client_id), str(self.tag.req_id)])

    def __str__(self):
        return _pprint(self.to_dict())

    def to_dict(self):
        return dict(type=type(self).__name__, uid=self.uid)


class GatewayRequest(Request):
    __slots__ = ('gateway_id',)

    def __init__(self, tag: RequestTag, gateway_id: str):
        super().__init__(tag)
        self.gateway_id = gateway_id
        self.uid = _SEP.join([str(self.tag.client_id), str(self.tag.req_id), gateway_id])

    def to_dict(self):
        d0 = super().to_dict()
        d1 = dict(gateway_id=self.gateway_id)
        return {**d0, **d1}


class ConnectRequest(GatewayRequest):
    __slots__ = ('broker', 'kwargs')

    def __init__(self, tag: RequestTag, broker, gateway_id: str, kwargs: dict):
        super().__init__(tag, gateway_id)
        self.broker = broker
        self.kwargs = kwargs

    def to_dict(self):
        d0 = super().to_dict()
        d1 = dict(broker=self.broker, kwargs=self.kwargs)
        return {**d0, **d1}


class DisconnectRequest(GatewayRequest):
    def __init__(self, tag: RequestTag, gateway_id: str):
        super().__init__(tag, gateway_id)


class OrderRequest(GatewayRequest):
    __slots__ = ('order_id', 'order')

    def __init__(self, tag: RequestTag, gateway_id: str, order_id: str, order):
        super().__init__(tag, gateway_id)
        self.order_id = order_id
        self.order = order
        self.uid = _SEP.join([str(tag), order_id])

    def to_dict(self):
        d0 = super().to_dict()
        d1 = dict(order_id=self.order_id, order=str(self.order))
        return {**d0, **d1}


class OrderCancelRequest(GatewayRequest):
    __slots__ = ('order_id',)

    def __init__(self, tag: RequestTag, gateway_id: str, order_id: str):
        super().__init__(tag, gateway_id)
        self.order_id = order_id
        self.uid = _SEP.join([str(tag), order_id])

    def to_dict(self):
        d0 = super().to_dict()
        d1 = dict(order_id=self.order_id)
        return {**d0, **d1}


class GatewaySummaryRequest(Request):
    def __init__(self, tag: RequestTag):
        super().__init__(tag)


class Update:
    __slots__ = ('gateway_id', 'client_id')

    def __init__(self, gateway_id: str = None, client_id: str = None):
        self.gateway_id = gateway_id
        self.client_id = client_id

    def __str__(self):
        return _pprint(self.to_dict())

    def to_dict(self):
        return dict(type=type(self).__name__, client_id=self.client_id, gateway_id=self.gateway_id)


class OrderUpdate(Update):
    __slots__ = ('order_ref', 'broker_order_id', 'order', 'status', 'remaining', 'filled', 'msg', 'is_historical')

    def __init__(self, client_id: int, order_ref: str, broker_order_id, status: str,
        remaining: float, filled: float, msg: str=None, order=None, is_historical: bool=False):
        super().__init__(client_id=client_id)
        self.status = status
        self.msg = msg                      # msg is used to provide more information about abnormal status
        self.broker_order_id = broker_order_id
        self.order = order
        self.order_ref = order_ref
        self.remaining = remaining
        self.filled = filled
        self.is_historical = is_historical  # True if order update is not triggered by place/modify/cancel order.

    def to_dict(self):
        d0 = super().to_dict()
        d1 = dict(order_ref=self.order_ref, broker_order_id=self.broker_order_id,
            order=str(self.order),
            remaining=self.remaining, filled=self.filled,
            status=self.status, msg=self.msg, is_historical=self.is_historical)
        return {**d0, **d1}

    def copy(self):
        clone = OrderUpdate(
            client_id=self.client_id,
            order_ref=self.order_ref,
            broker_order_id=self.broker_order_id,
            status=self.status,
            remaining=self.remaining,
            filled=self.filled,
            msg=self.msg,
            order=self.order.copy(),
            is_historical=self.is_historical)
        clone.gateway_id = self.gateway_id
        return clone


class ExecutionUpdate(Update):
    __slots__ = ('exec_id', 'timestamp', 'order_ref', 'broker_order_id', 'side', 'symbol', 'filled', 'price', 'cum_qty',
                 'avg_price', 'commission', 'currency', 'is_historical')

    def __init__(self, client_id, exec_id, timestamp, order_ref: str, broker_order_id, side, symbol, filled: int, price, cum_qty,
                 avg_price: float, commission: float, currency: str, is_historical: bool=False):
        super().__init__(client_id=client_id)
        self.order_ref = order_ref
        self.exec_id = exec_id
        self.timestamp = timestamp
        self.side = side
        self.symbol = symbol
        self.filled = filled
        self.price = price
        self.avg_price = avg_price
        self.broker_order_id = broker_order_id
        self.commission = commission
        self.currency = currency
        self.cum_qty = cum_qty
        self.is_historical = is_historical

    def to_dict(self):
        d0 = super().to_dict()
        d1 = dict(order_ref=self.order_ref, broker_order_id=self.broker_order_id,
                  timestamp=datetime.datetime.strftime(self.timestamp, _datetime_format),
                  exec_id=self.exec_id, side=self.side, symbol=self.symbol, filled=self.filled, price=self.price,
                  cum_qty=self.cum_qty, avg_price=self.avg_price, commission=self.commission, currency=self.currency,
                  is_historical=self.is_historical)
        return {**d0, **d1}

    def copy(self):
        clone = ExecutionUpdate(
            client_id=self.client_id,
            exec_id=self.exec_id,
            timestamp=self.timestamp,
            order_ref=self.order_ref,
            broker_order_id=self.broker_order_id,
            side=self.side,
            symbol=self.symbol,
            filled=self.filled,
            price=self.price,
            cum_qty=self.cum_qty,
            avg_price=self.avg_price,
            commission=self.commission,
            currency=self.currency,
            is_historical=self.is_historical
        )
        clone.gateway_id = self.gateway_id
        return clone


class ConnectionStatus:
    CONNECTED = 'connected'
    DISCONNECTED = 'disconnected'


class ConnectionUpdate(Update):
    __slots__ = ('status')

    def __init__(self, status: str):
        super().__init__()
        self.status = status

    def to_dict(self):
        d0 = super().to_dict()
        d1 = dict(status=self.status)
        return {**d0, **d1}

class OrderAPIReady(Update):
    """
    `ready` is true indicates the gateway instance accepts order operations.
    """
    __slots__ = ('ready')

    def __init__(self, ready: bool):
        super().__init__()
        self.ready = ready

    def to_dict(self):
        d0 = super().to_dict()
        d1 = dict(ready=self.ready)
        return {**d0, **d1}

class Healthiness:
    HEALTHY = 'OK'
    NOT_HEALTHY = 'NOT OK'


class HealthinessUpdate(Update):
    __slots__ = ('status')

    def __init__(self, status: str):
        super().__init__()
        self.status = status

    def to_dict(self):
        d0 = super().to_dict()
        d1 = dict(status=self.status)
        return {**d0, **d1}


class Position:
    __slots__ = ('symbol', 'broker_symbol', 'net_position')

    def __init__(self, symbol: str, broker_symbol: str, net_position: float):
        self.symbol = symbol
        self.broker_symbol = broker_symbol
        self.net_position = net_position

    def to_dict(self):
        return dict(symbol=self.symbol, broker_symbol=self.broker_symbol, net_position=self.net_position)


class PositionUpdate(Update):
    __slots__ = ('positions')

    def __init__(self, positions: list):
        super().__init__()
        self.positions = positions

    def to_dict(self):
        d0 = super().to_dict()
        d1 = dict(positions=[dict(broker_symbol=p.broker_symbol, net_position=p.net_position) for p in self.positions])
        return {**d0, **d1}


class OpenOrdersUpdate(Update):
    __slots__ = ('open_orders')

    def __init__(self, open_orders: list):
        super().__init__()
        self.open_orders = open_orders

    def to_dict(self):
        d0 = super().to_dict()
        d1 = dict(orders=[x.to_dict() for x in self.open_orders])
        return {**d0, **d1}


class Account:
    __slots__ = ('acc_name', 'cash_balance', 'realized_PnL', 'unrealized_PnL', 'currency')

    def __init__(self, acc_name: str, cash_balance: float, realized_PnL: float, unrealized_PnL: float, currency: str):
        self.acc_name = acc_name
        self.cash_balance = cash_balance
        self.realized_PnL = realized_PnL
        self.unrealized_PnL = unrealized_PnL
        self.currency = currency

    def to_dict(self):
        return {s: getattr(self, s, None) for s in self.__slots__}


class AccountUpdate(Update):
    __slots__ = ('accounts',)

    def __init__(self, accounts: list):
        super().__init__()
        self.accounts = accounts

    def to_dict(self):
        d0 = super().to_dict()
        d1 = dict(account=[a.to_dict() for a in self.accounts])
        return {**d0, **d1}


class ContractDetails:
    __slots__ = ('symbol', 'exchange', 'local_symbol', 'last_trading_date', 'contract_month', 'trading_class',
                 'timezone')

    def __init__(self, symbol: str, exchange: str, local_symbol: str, last_trading_date: str, contract_month: str,
                 trading_class: str, timezone: str):
        self.symbol = symbol
        self.exchange = exchange
        self.local_symbol = local_symbol
        self.last_trading_date = last_trading_date
        self.contract_month = contract_month
        self.trading_class = trading_class
        self.timezone = timezone

    def to_dict(self):
        return {s: getattr(self, s, None) for s in self.__slots__}


class ContractDetailsUpdate(Update):
    __slots__ = ('contracts',)

    def __init__(self, contracts: list):
        super().__init__()
        self.contracts = contracts

    def to_dict(self):
        d0 = super().to_dict()
        d1 = dict(contract=[a.to_dict() for a in self.contracts])
        return {**d0, **d1}


class MarketDataBarUpdate(Update):
    __slots__ = ('resolution', 'timestamp', 'open_', 'high', 'low', 'close', 'volume', 'user_data')

    def __init__(self, resolution, timestamp, open_, high, low, close, volume, user_data):
        super().__init__()
        self.resolution = resolution
        self.timestamp = timestamp
        self.open_ = open_
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.user_data = user_data

    def to_dict(self):
        d0 = super().to_dict()
        d1 = {s: getattr(self, s, None) for s in self.__slots__ if s != 'user_data'}
        return {**d0, **d1}


class ErrorType:
    GENERAL = 'General'
    ORDER = 'Order'
    CONNECTIVITY = 'Connectivity'


class ErrorMessage(Update):
    __slots__ = ('msg', 'error_type', 'code', 'req_id')

    def __init__(self, msg: str, code: Optional[Union[int, str]]=None, error_type: str = ErrorType.GENERAL, req_id: int = None):
        super().__init__()
        self.msg = msg
        self.code = code
        self.error_type = error_type
        self.req_id = req_id

    def to_dict(self):
        d0 = super().to_dict()
        d1 = dict(error_type=self.error_type, code=self.code, msg=self.msg, req_id=self.req_id)
        return {**d0, **d1}


class OrderError(ErrorMessage):
    __slots__ = ('order_id')

    def __init__(self, order_id: str, msg: str, code=None, req_id: int = None):
        super().__init__(msg, code, ErrorType.ORDER, req_id)
        self.order_id = order_id

    def to_dict(self):
        d0 = super().to_dict()
        d1 = dict(order_id=self.order_id)
        return {**d0, **d1}


class ConnectivityError(ErrorMessage):
    def __init__(self, msg: str, code=None):
        super().__init__(msg, code, ErrorType.CONNECTIVITY)
