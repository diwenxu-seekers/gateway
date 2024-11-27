import logging
from datetime import datetime, timedelta
from time import sleep
from threading import Thread, RLock

from .gateway import AbstractGateway
from .order import Order
from .message import (ErrorType, OrderError, ErrorMessage, ExecutionUpdate, OrderUpdate, ConnectionUpdate, ConnectionStatus,
                      Position, PositionUpdate, Account, AccountUpdate, ContractDetails, ContractDetailsUpdate,
                      MarketDataBarUpdate)

logger = logging.getLogger(__name__)

_lock = RLock()

def safe_execute(lock):
    """ Thread safe lock.
    """
    def wraps(fn):
        def args(*ar, **kw):
            with lock:
                return fn(*ar, **kw)
        return args
    return wraps


class RetryGateway(AbstractGateway):
    """
    Gateway wrapper that handles reconnection and
    errors that could possibly resolve by retry.
    It also provides thread-safe gateway API.
    """
    def __init__(self, gateway: AbstractGateway):
        super().__init__(name=gateway.name, event_handler=None)
        self._gateway = gateway
        self._retry_interval = 60   # sec
        self._max_retry = -1        # -ve number to retry forever
        self._retry = 0
        self._request_disconnect = False
        self._wire_events()

    def __str__(self) -> str:
        return self._gateway.name

    def configure(self, max_retry=None, retry_interval=None):
        if max_retry is not None:
            self._max_retry = max_retry
        if retry_interval is not None:
            self._retry_interval = retry_interval
        logger.info(f"Configure auto reconnect for '{self._gateway.name}'."
            f" max_retry={self._max_retry} interval={self._retry_interval}")

    def _wire_events(self):
        events = self.events
        gw = self._gateway.events
        gw.on_connection_update(self._handle_disconnect)
        gw.on_error(self._propagate_event(events, '_on_error'))
        gw.on_order_update(self._propagate_event(events, '_on_order_update'))
        gw.on_order_api_ready(self._propagate_event(events, '_on_order_api_ready'))
        gw.on_execution(self._propagate_event(events, '_on_execution'))
        gw.on_contract_details_update(self._propagate_event(events, '_on_contract_details'))
        gw.on_account_info_update(self._propagate_event(events, '_on_account_info'))
        gw.on_position_update(self._propagate_event(events, '_on_position'))
        gw.on_market_data_bar(self._propagate_event(events, '_on_market_data_bar'))
        gw.on_open_order_end(self._propagate_event(events, '_on_open_order_end'))

    def _propagate_event(self, obj, handler_name):
        def _propagate(src, event):
            callback = getattr(obj, handler_name)
            if callback:
                callback(src, event)
        return _propagate

    def _handle_disconnect(self, src, event: ConnectionUpdate):
        gw = self._gateway
        events = self.events
        handler = events._on_connection_update
        events.raise_event(handler, lambda x: x, event, gw)

        # attempt reconnect
        if event.status == ConnectionStatus.DISCONNECTED:
            if self._request_disconnect:
                self._request_disconnect = False
                logger.debug(f'Skipped auto reconnect because user initiated disconnect request.')
            elif self._max_retry < 0 or self._retry < self._max_retry:
                interval = self._retry_interval
                self._retry = self._retry + 1
                logger.info(f'Auto reconnect {src} after {interval}s.')
                try:
                    msg = f"Reconnect to {src}. retry={self._retry}"
                    t = Thread(name='ReconnectTimer',
                        target=self._reconnect, args=(interval, msg))
                    t.start()
                except Exception as e:
                    self._raise_error(str(e))
            elif self._retry >= self._max_retry:
                logger.info("Auto reconnect reached max number of retry.")
        elif event.status == ConnectionStatus.CONNECTED:
            self._retry = 0

    def _reconnect(self, delay, msg):
        try:
            sleep(delay)
            # if disconnect() is called when sleeping
            # do not connect()
            if not self._request_disconnect:
                logger.info(msg)
                self.connect()
        except:
            logger.exception("Unhandled exception.")
        finally:
            logger.debug("Thread done.")
            self._request_disconnect = False

    def _raise_error(self, msg):
        callback = self.events._on_error
        msg = ErrorMessage(
            msg=msg,
            code=None,
            error_type=ErrorType.CONNECTIVITY,
            req_id=None)
        self.events.raise_event(callback, lambda x: x, msg, self)


    ## Implement Gateway Interface

    @property
    def identity(self):
        """ Returns the identity recognized by the execution venue.
        This is use to differentiate orders placed by this gateway.
        """
        return self._gateway.identity

    @safe_execute(_lock)
    def save_state(self):
        """ Store the state of gateway to file.
        """
        self._gateway.save_state()

    @safe_execute(_lock)
    def load_state(self):
        """ Restore the state of gateway from file.
        """
        self._gateway.load_state()

    @safe_execute(_lock)
    def connect(self):
        """ Connect to broker.
        """
        self._gateway.connect()

    @safe_execute(_lock)
    def disconnect(self):
        """ Disconnect from broker.
        """
        self._request_disconnect = True
        self._gateway.disconnect()

    @safe_execute(_lock)
    def close(self, reason=''):
        """ Closes connection.
        """
        self._gateway.close(reason=reason)

    @property
    def is_healthy(self):
        """ Returns True if connectivity with broker is healthy.
        """
        return self._gateway.is_healthy

    @property
    def can_manipulate_order(self):
        """ Returns True if gateway is healthy and is ready for order API calls.
        """
        return self._gateway.can_manipulate_order

    @safe_execute(_lock)
    def place_order(self, order_id: str, order: Order,
        order_prefix='', on_behalf_of=''):
        """ Place order.
        """
        self._gateway.place_order(order_id, order, order_prefix, on_behalf_of)

    @safe_execute(_lock)
    def cancel_order(self, order_id: str, on_behalf_of=''):
        """ Cancel open order.
        """
        self._gateway.cancel_order(order_id, on_behalf_of=on_behalf_of)

    @safe_execute(_lock)
    def modify_order(self, order_id: str, order: Order, on_behalf_of=''):
        """ Modify open order.
        """
        self._gateway.modify_order(order_id, order, on_behalf_of=on_behalf_of)

    @safe_execute(_lock)
    def request_market_data_bar(self, symbol, exchange, contract_month, local_symbol=None, resolution=None,
                                start_date=None, end_date=None, user_data=None):
        """ Request market data.
        """
        self._gateway.request_market_data_bar(**locals())

    @safe_execute(_lock)
    def request_open_orders(self):
        """ Request open orders placed by this client.
        """
        self._gateway.request_open_orders()

    @safe_execute(_lock)
    def request_positions(self):
        """ Request positions.
        """
        self._gateway.request_positions()

    @safe_execute(_lock)
    def request_account_info(self):
        """ Request account information.
        """
        self._gateway.request_account_info()

    @safe_execute(_lock)
    def request_contract_details(self, **kwargs):
        """ Request contract information
        """
        self._gateway.request_contract_details(**kwargs)

    @safe_execute(_lock)
    def request_executions(self):
        """ Request executions.
        """
        self._gateway.request_executions()

    @safe_execute(_lock)
    def ping(self):
        """ Ping the server
        """
        self._gateway.ping()


