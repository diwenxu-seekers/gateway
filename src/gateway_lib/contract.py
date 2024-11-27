import abc
from .utils import get_item, swap_key_value

class ContractType:
    Future = 0
    Stock = 1

    __str = {Future: 'FUT', Stock: 'STK'}
    __istr = swap_key_value(__str)
    to_str = lambda v, d=None: get_item(ContractType.__str, v, d)
    from_str = lambda v, d=None: get_item(ContractType.__istr, v, d)


class Currency:
    USD = 0
    CNH = 1
    HKD = 2

    __str = {USD: 'USD', CNH: 'CNH', HKD: 'HKD'}
    __istr = swap_key_value(__str)
    to_str = lambda v, d=None: get_item(Currency.__str, v, d)
    from_str = lambda v, d=None: get_item(Currency.__istr, v, d)


class Exchange:
    UNDEFINED = 0
    GLOBEX = 1
    CME = 2
    COMEX = 6
    NYMEX = 3
    HKFE = 4
    SEHK = 5

    __str = {
        UNDEFINED: 'Undefined',
        GLOBEX: 'GLOBEX',
        CME: 'CME',
        COMEX: 'COMEX',
        NYMEX: 'NYMEX',
        HKFE: 'HKFE',
        SEHK: 'SEHK'}
    __istr = swap_key_value(__str)
    to_str = lambda v, d=None: get_item(Exchange.__str, v, d)
    from_str = lambda v, d=None: get_item(Exchange.__istr, v, d)


class Contract:
    __slots__ = ['contract_type', 'symbol', 'exchange', 'currency', 'contract_month']

    def __init__(self, contract_type: int, symbol: str, exchange: int, currency: int=Currency.USD, contract_month: str=None):
        self.contract_type = contract_type
        self.symbol = symbol
        self.exchange = exchange
        self.currency = currency
        self.contract_month = contract_month

    def __str__(self):
        return self.symbol


class AbstractContract(abc.ABC):
    @abc.abstractmethod
    def create(self, contract: Contract):
        pass

    @abc.abstractmethod
    def get_native_currency(self, val):
        pass

    @abc.abstractmethod
    def get_native_exchange(self, val):
        pass

    @abc.abstractmethod
    def get_native_contract_type(self, val):
        pass

    @abc.abstractmethod
    def get_internal_currency(self, val):
        pass

    @abc.abstractmethod
    def get_internal_exchange(self, val):
        pass

    @abc.abstractmethod
    def get_internal_contract_type(self, val):
        pass

class AbstractContractFinder(abc.ABC):
    """ Create the broker native contract object.
    """
    @abc.abstractmethod
    def from_symbol(self, symbol: str):
        """
        Resolve broker native contract by internal symbol name.
        """
        pass

    @abc.abstractmethod
    def from_exchange_symbol(self, exchange: int, contractType: int, symbol: str):
        """
        Resolve broker native contract by exchange symbol and other details.
        """
        pass