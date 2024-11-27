import abc
import math
from collections import deque
from .contract import (Contract, Exchange, ContractType)

class OrderStatus:
    UNDEFINED = 'UNDEFINED'
    SUBMITTED = 'SUBMITTED'             # order is active on execution venue
    FILLED = 'FILLED'                   # order is fully filled
    PARTIAL_FILLED = 'PARTIAL_FILLED'   # order is partially filled
    CANCELLED = 'CANCELLED'             # order is cancelled by internal/external parties
    INACTIVE = 'INACTIVE'               # order is suspended on execution venue
    REJECTED = 'REJECTED'               # order is rejected by internal/broker/venue


class OrderType:
    MKT = 0
    LMT = 1
    STP = 2
    STP_LMT = 3

    @staticmethod
    def to_str(val):
        if val == OrderType.MKT:
            return 'MKT'
        elif val == OrderType.LMT:
            return 'LMT'
        elif val == OrderType.STP:
            return 'STP'
        elif val == OrderType.STP_LMT:
            return 'STP_LMT'

BUY = 'BUY'
SELL = 'SELL'

class OrderAction:
    BUY = 0
    SELL = 1

    @staticmethod
    def to_str(val):
        if val == OrderAction.BUY:
            return BUY
        elif val == OrderAction.SELL:
            return SELL


class TIF:
    GTC = 0
    DAY = 1
    GTD = 2

    @staticmethod
    def to_str(val):
        if val == TIF.GTC:
            return 'GTC'
        elif val == TIF.GTD:
            return 'GTD'
        elif val == TIF.DAY:
            return 'DAY'

class Order:
    __slot__ = [
        'symbol',
        'exchange',     # for CQG exchange is not necessary to resolve contract
        'contractType',
        'orderType',    # MKT, LMT, STP
        'action',       # B/S
        'quantity',
        'limit_price',  # limit price; e.g. LMT, STP_LMT
        'stop_price',   # stop price; e.g. STP, STP_LMT
        'tif',          # TIF
        'outsideRth',   # outside regular trading hour
        'goodTillDate', # Format: 20060505 08:00:00 {time zone}
        'extra',        # dictionary of additional order properties
    ]

    def __init__(self, symbol: str, exchange: int, contractType: int, orderType: int, action: int, quantity: float,
        limit_price: float=None, tif: int=TIF.GTC, stop_price: float=None, outsideRth=False, goodTillDate=""):
        self.symbol = symbol
        self.exchange = exchange
        self.contractType = contractType
        self.orderType = orderType
        self.action = action
        self.quantity = quantity
        self.limit_price = limit_price
        self.stop_price = stop_price
        self.tif = tif
        self.outsideRth = outsideRth
        self.goodTillDate = goodTillDate

    @property
    def price(self):
        """
        [Depreciated] For backward compatible only.
        """
        _type = self.orderType
        if _type == OrderType.LMT:
            return self.limit_price
        elif _type == OrderType.STP:
            return self.stop_price
        elif _type == OrderType.STP_LMT:
            return self.stop_price
        return 0.0

    def __repr__(self):
        return f"Order('{self.symbol}', {self.exchange}, {self.contractType}, {self.orderType}, {self.action}, {self.quantity}," \
            f" {self.limit_price}, {self.tif}, {self.stop_price}, {self.outsideRth}, {self.goodTillDate})"

    def __str__(self):
        action = OrderAction.to_str(self.action)
        ordertype = OrderType.to_str(self.orderType)
        tif = TIF.to_str(self.tif)
        if self.tif == TIF.GTD:
            tif = f'{tif} {self.goodTillDate}'
        _price_map = {
            OrderType.LMT: f'@{self.limit_price}',
            OrderType.STP: f'@{self.stop_price}',
            OrderType.STP_LMT: f'@{self.limit_price} if {self.stop_price}',
        }
        _price_str = _price_map.get(self.orderType, '')
        return ' '.join([
            f"{ordertype} {action} {self.quantity} {self.symbol}",
            _price_str,
            tif
        ])

    def strcmp(self, order_str):
        """
        Check if the order equals to a given order string presentation.
        """

        def __safe_cmp_f(a, b):
            try:
                x = float(b)
                return math.isclose(a, x)
            except:
                return False
        try:
            if not order_str:
                return False
            tokens = deque(order_str.split(' '))
            token = tokens.popleft()
            if OrderType.to_str(self.orderType) != token:
                return False
            token = tokens.popleft()
            if OrderAction.to_str(self.action) != token:
                return False
            token = tokens.popleft().strip()
            if self.quantity is None:
                if str(self.quantity) != token:
                    return False
            else:
                if not __safe_cmp_f(self.quantity, token):
                    return False
            token = tokens.popleft()
            if self.symbol != token:
                return False

            if self.orderType == OrderType.MKT:
                tokens.popleft()    # pop one empty space
            if self.orderType == OrderType.LMT:
                token = tokens.popleft().strip('@ ')
                if self.limit_price is None:
                    if str(self.limit_price) != token:
                        return False
                else:
                    if not __safe_cmp_f(self.limit_price, token):
                        return False
            elif self.orderType == OrderType.STP:
                token = tokens.popleft().strip('@ ')
                if self.stop_price is None:
                    if str(self.stop_price) != token:
                        return False
                else:
                    if not __safe_cmp_f(self.stop_price, token):
                        return False
            elif self.orderType == OrderType.STP_LMT:
                token = tokens.popleft().strip('@ ')
                if self.limit_price is None:
                    if str(self.limit_price) != token:
                        return False
                else:
                    if not __safe_cmp_f(self.limit_price, token):
                        return False
                tokens.popleft()    # pop ' if '
                token = tokens.popleft().strip('@ ')
                if self.stop_price is None:
                    if str(self.stop_price) != token:
                        return False
                else:
                    if not __safe_cmp_f(self.stop_price, token):
                        return False
            token = tokens.popleft()
            if TIF.to_str(self.tif) != token:
                return False
            if self.tif == TIF.GTD:
                if self.goodTillDate != ' '.join(tokens):
                    return False
            return True
        except:
            return False


    def copy(self):
        """
        Return a deep copy of the current instance.
        """
        return Order(
            symbol=self.symbol,
            exchange=self.exchange,
            contractType=self.contractType,
            orderType=self.orderType,
            action=self.action,
            quantity=self.quantity,
            limit_price=self.limit_price,
            tif=self.tif,
            stop_price=self.stop_price,
            outsideRth=self.outsideRth,
            goodTillDate=self.goodTillDate
        )


class OrderConverter(abc.ABC):
    @abc.abstractmethod
    def convert_from(self, order: Order):
        pass

    @abc.abstractmethod
    def convert_to(self, native) -> Order:
        pass

    @abc.abstractmethod
    def get_native_order_action(self, val: OrderAction):
        pass

    @abc.abstractmethod
    def get_native_order_type(self, val: OrderType):
        pass

    @abc.abstractmethod
    def get_native_tif(self, val: TIF):
        pass

    @abc.abstractmethod
    def get_internal_order_action(self, val):
        pass

    @abc.abstractmethod
    def get_internal_order_type(self, val):
        pass

    @abc.abstractmethod
    def get_internal_tif(self, val):
        pass