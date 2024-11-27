import abc
import logging
from typing import List, Tuple

from .order import Order

logger = logging.getLogger(__name__)


class GatewayEventProcessor:
    def __init__(self, gateway):
        self._gw = gateway

    def process_error_event(self, obj: dict):
        return None

    def process_execution_event(self, obj: dict):
        return None

    def process_order_event(self, obj: dict):
        return None

    def process_connection_event(self, obj: dict):
        return None

    def process_position_event(self, obj: dict):
        return None

    def process_account_info_event(self, obj: dict):
        return None

    def process_contract_details_event(self, obj: dict):
        return None

    def process_market_data_event(self, obj: dict):
        return None

    def process_open_order_end_event(self, obj: dict):
        return None


class GatewayEvent:
    """ Defines the gateway events.
    """

    def __init__(self,
                 gateway,
                 event_handler: GatewayEventProcessor,
                 on_connection_update=None,
                 on_order_update=None,
                 on_execution=None,
                 on_error=None,
                 on_order_api_ready=None,
                 on_account_info=None,
                 on_position=None,
                 on_contract_details=None,
                 on_market_data_bar=None,
                 on_open_order_end=None):
        """
        Instantiates a set of gateway callbacks for client's subscriptions.

        `on_connection_update`: client can safely assume all API and callbacks are ready when status is CONNECTED.

        `on_open_order_end`: A callback to receive a list of `OrderUpdate` instances of the open orders. It is used to synchronize open orders between the client and the broker in the event of disconnection between the two.
        """
        self._gw = gateway
        self._event_handler = event_handler
        self._on_connection_update = on_connection_update
        self._on_order_update = on_order_update
        self._on_execution = on_execution
        self._on_error = on_error
        self._on_account_info = on_account_info
        self._on_position = on_position
        self._on_contract_details = on_contract_details
        self._on_market_data_bar = on_market_data_bar
        self._on_order_api_ready = on_order_api_ready
        #TODO: `on_open_order_end` is ONLY implemented in IB
        self._on_open_order_end = on_open_order_end

    def raise_event(self, callback, event_handler, data, src):
        if callback is not None:
            try:
                _event = event_handler(data)
                if _event is None:
                    logger.warning(f'Suppressed event {data}')
                    return
                _event.gateway_id = src.name
                logger.debug(_event)
                callback(src, _event)
            except:
                logger.exception('Unhandled exception when processing event.')

    def dispose(self):
        """ Release resources.
        """
        self._on_connection_update = None
        self._on_order_update = None
        self._on_execution = None
        self._on_error = None
        self._on_account_info = None
        self._on_position = None
        self._on_contract_details = None
        self._on_market_data_bar = None
        self._on_order_api_ready = None
        self._on_open_order_end = None

    def on_error(self, handler):
        self._on_error = handler

    def on_execution(self, handler):
        self._on_execution = handler

    def on_order_update(self, handler):
        self._on_order_update = handler

    def on_order_api_ready(self, handler):
        self._on_order_api_ready = handler

    def on_connection_update(self, handler):
        self._on_connection_update = handler

    def on_position_update(self, handler):
        self._on_position = handler

    def on_account_info_update(self, handler):
        self._on_account_info = handler

    def on_contract_details_update(self, handler):
        self._on_contract_details = handler

    def on_market_data_bar(self, handler):
        self._on_market_data_bar = handler

    def on_open_order_end(self, handler):
        self._on_open_order_end = handler

    def raise_error_event(self, obj: dict):
        self.raise_event(self._on_error,
            self._event_handler.process_error_event, obj, self._gw)

    def raise_execution_event(self, obj: dict):
        self.raise_event(self._on_execution,
            self._event_handler.process_execution_event, obj, self._gw)

    def raise_order_event(self, obj: dict):
        self.raise_event(self._on_order_update,
            self._event_handler.process_order_event, obj, self._gw)

    def raise_order_api_ready_event(self, obj: dict):
        self.raise_event(self._on_order_api_ready,
            self._event_handler.process_order_api_ready_event, obj, self._gw)

    def raise_connection_event(self, obj: dict):
        self.raise_event(self._on_connection_update,
            self._event_handler.process_connection_event, obj, self._gw)

    def raise_position_event(self, obj: dict):
        self.raise_event(self._on_position,
            self._event_handler.process_position_event, obj, self._gw)

    def raise_account_info_event(self, obj: dict):
        self.raise_event(self._on_account_info,
            self._event_handler.process_account_info_event, obj, self._gw)

    def raise_contract_details_event(self, obj: list):
        self.raise_event(self._on_contract_details,
            self._event_handler.process_contract_details_event, obj, self._gw)

    def raise_market_data_bar_event(self, obj: list):
        self.raise_event(self._on_market_data_bar,
            self._event_handler.process_market_data_bar_event, obj, self._gw)

    def raise_open_order_end_event(self, obj: dict):
        self.raise_event(self._on_open_order_end,
            self._event_handler.process_open_order_end_event, obj, self._gw)


class AbstractGateway(abc.ABC):
    """
    Defines an event-driven non-blocking API to access execution systems.
    """
    def __init__(self, name: str, event_handler: GatewayEventProcessor):
        self.name = name
        self.events = GatewayEvent(self, event_handler)

    @property
    @abc.abstractmethod
    def identity(self):
        """ Returns the identity recognized by the execution venue.
        This is use to differentiate orders placed by this gateway.
        """
        pass

    @abc.abstractmethod
    def close(self, reason=''):
        """ Closes connection """
        pass

    @abc.abstractmethod
    def save_state(self):
        """ Store the state of gateway to file.
        """
        pass

    @abc.abstractmethod
    def load_state(self):
        """ Restore the state of gateway from file.
        """
        pass

    @abc.abstractmethod
    def connect(self):
        """ Connect to broker.
        """
        pass

    @abc.abstractmethod
    def disconnect(self):
        """ Disconnect from broker.
        """
        pass

    @property
    @abc.abstractmethod
    def is_healthy(self):
        """ Returns True if connectivity with broker is healthy.
        Healthy means socket connection is UP and account logon successfully.
        """
        pass

    @property
    @abc.abstractmethod
    def can_manipulate_order(self):
        """ Returns True if allowed to make order API calls.

        Related event: on_order_api_ready
        """
        pass

    @abc.abstractmethod
    def place_order(self, order_id: str, order: Order,
        order_prefix='', on_behalf_of=''):
        """ Place order.

        order_prefix (str|None): assign a prefix for filtering orders
        on_behalf_of (str|None): to compile with CME FIX tag 50
        """
        pass

    @abc.abstractmethod
    def cancel_order(self, order_id: str, on_behalf_of=''):
        """ Cancel open order.
        """
        pass

    @abc.abstractmethod
    def modify_order(self, order_id: str, order: Order, on_behalf_of=''):
        """ Modify open order.
        """
        pass

    @abc.abstractmethod
    def request_market_data_bar(self, symbol, exchange, contract_month, local_symbol=None, resolution=None,
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

    @abc.abstractmethod
    def request_open_orders(self):
        """ Request open orders placed by this client.
        """
        pass

    @abc.abstractmethod
    def request_positions(self):
        """ Request positions.
        """
        pass  # reqPositions

    @abc.abstractmethod
    def request_account_info(self):
        """ Request account information.
        """
        pass  # reqAccountSummary

    @abc.abstractmethod
    def request_contract_details(self, **kwargs):
        """ Request contract information
        """
        pass  # reqContractDetails

    @abc.abstractmethod
    def request_executions(self):
        """ Request executions.
        """
        pass  # reqExecutions

    @abc.abstractmethod
    def ping(self):
        """ Ping the server
        """
        pass  # reqExecutions
