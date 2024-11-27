from .ib import IBGateway
from .gateway import (AbstractGateway)
import abc
import json
import queue
import logging
import threading
from typing import (List, Dict, Any, Optional)

from .message import (
    Request, GatewayRequest, ConnectRequest, DisconnectRequest, OrderRequest, OrderCancelRequest,
    GatewaySummaryRequest,
    ConnectivityError, OrderError, ErrorMessage,
    ExecutionUpdate, OrderUpdate, ConnectionUpdate, ConnectionStatus,
    Position, PositionUpdate,
    Account, AccountUpdate,
    HealthinessUpdate, Healthiness)

logger = logging.getLogger(__name__)

class Broker:
    IB = 'IB'   # InteractiveBrokers


class ManagerMessageProcessor(abc.ABC):
    @abc.abstractmethod
    def process_incoming(self, msg) -> (str, object):
        """ Translate incoming message object to internal object.
        """
        pass

    @abc.abstractmethod
    def process_outgoing(self, obj):
        """ Translate internal object to outgoing message object.
        """
        pass

class MessageRouter:

    def get_key(self, gateway_id: str):
        return gateway_id

    def __init__(self):
        self.des_lookup = {}

    def destinations(self, gateway_id):
        return self.des_lookup[gateway_id]

    def register_client(self, gateway_id: str, client_id: str):
        key = self.get_key(gateway_id)
        if key not in self.des_lookup:
            self.des_lookup[key] = set()
        self.des_lookup[key].add(client_id)

    def unregister_client(self, gateway_id: str, client_id: str):
        key = self.get_key(gateway_id)
        if key in self.des_lookup:
            self.des_lookup[key].discard(client_id)

class GatewayManager:
    """ Manages multiple Gateway instances.
    """

    def __init__(self, message_handler: ManagerMessageProcessor, event_handler):
        self._gateways: Dict[str, AbstractGateway] = {}
        self._message_handler = message_handler
        _queue_size = 100
        self._rx = queue.Queue(_queue_size)
        self._tx = queue.Queue(_queue_size)
        self._thread = None
        self._terminate = False
        self._event_handler = event_handler
        self.router = MessageRouter()

    def __enter__(self):
        self.run()
        return self

    def __exit__(self, *exc_info):
        self.exit()

    def run(self):
        self._terminate = False
        self._thread = threading.Thread(target=self._loop)
        self._thread.name = 'ManagerLoop'
        self._thread.daemon = False
        self._thread.start()

    def exit(self):
        self._terminate = True
        self._thread.join()

    def add_gateway(self, gateway_id: str, broker: str, kwargs: dict):
        _gw = None
        if broker == Broker.IB:
            # Mandatory: name, host, port;  Optional: client_id
            _host = kwargs['host']
            _port = kwargs['port']
            _client_id = kwargs.get('client_id', None)
            _gw = IBGateway(name=gateway_id, host=_host, port=_port, client_id=_client_id)
            self._gateways.update({gateway_id: _gw})
        self._wire_gateway_callbacks(_gw)
        return _gw

    def remove_gateway(self, gateway_id: str):
        return self._gateways.pop(gateway_id)

    def get(self, gateway_id: str) -> Optional[AbstractGateway]:
        return self._gateways.get(gateway_id, None)

    def send(self, msg):
        """ Accept incoming message from client.
        """
        try:
            logger.info(f'Receive {msg}')
            self._rx.put_nowait(msg)
        except queue.Full:
            self._sink(self, ErrorMessage(msg='Incoming message queue is full. Please try again later.'))

    def _dispatch(self, msg):
        """ Dispatch outgoing message to client.
        """
        # logger.info(str(msg))
        if self._event_handler is not None:
            self._event_handler(msg)

    def _post(self, request: Request):
        """ Post request to gateway.
        """
        if isinstance(request, GatewayRequest):
            gw_id = request.gateway_id
            if gw_id not in self._gateways and isinstance(request, ConnectRequest):
                self.add_gateway(gateway_id=gw_id, broker=request.broker, kwargs=request.kwargs)

            gw = self.get(gw_id)
            if gw is None:
                logging.warning(f'Gateway id "{gw_id}" not found.')
            else:
                cid = request.tag.client_id
                if isinstance(request, ConnectRequest):
                    self.router.register_client(gw_id, cid)
                    if len(self.router.destinations(gw_id)) > 1:
                        _status = ConnectionStatus.DISCONNECTED
                        if gw.is_healthy:
                            _status = ConnectionStatus.CONNECTED
                        _update = ConnectionUpdate(status=_status)
                        _update.gateway_id = gw_id
                        _update.client_id = cid
                        self._sink(self, _update)
                    else:
                        gw.connect()
                elif isinstance(request, DisconnectRequest):
                    if len(self.router.destinations(gw_id)) > 1:
                        self.router.unregister_client(gw_id, cid)
                        _update = ConnectionUpdate(status=ConnectionStatus.DISCONNECTED)
                        _update.gateway_id = gw_id
                        _update.client_id = cid
                        self._sink(self, _update)
                    else:
                        gw.disconnect()
                elif isinstance(request, OrderRequest):
                    gw.place_order(request.order_id, request.order)
                elif isinstance(request, OrderCancelRequest):
                    gw.cancel_order(request.order_id)
                else:
                    logging.warning(f'Unknown request of type "{type(request)}"')
        else:
            if isinstance(request, GatewaySummaryRequest):
                info = {gw.name: gw.is_healthy for gw in self._gateways.values()}
                logging.info(str(info))

    def _process_incoming_msg(self):
        # post request to corresponding gateway
        while True:
            try:
                msg = self._rx.get(block=True, timeout=0.2)
                req = self._message_handler.process_incoming(msg)
                if req is not None:
                    self._post(req)
            except queue.Empty:
                break

    def _process_outgoing_msg(self):
        # dispatch update to corresponding clients
        while True:
            try:
                update = self._tx.get(block=True, timeout=0.2)
                cid = update.client_id
                if cid is None:
                    for cid in self.router.destinations(update.gateway_id):
                        update.client_id = cid
                        msg = self._message_handler.process_outgoing(update)
                        self._dispatch(msg)
                else:
                    msg = self._message_handler.process_outgoing(update)
                    self._dispatch(msg)
            except queue.Empty:
                break

    def _loop(self):
        try:
            while True:
                if self._terminate:
                    break
                self._process_incoming_msg()
                self._process_outgoing_msg()
        except:
            logger.exception('Unhandled exception in ManagerLoop.')
        finally:
            [gw.disconnect() for gw in self._gateways.values() if gw.is_healthy]
            self._process_outgoing_msg()    # clear all outstanding messages
            logger.info('Loop finished.')

    def _sink(self, src, event):
        try:
            # logger.info(f'Receive {src} {event}')
            self._tx.put_nowait(event)
        except queue.Full:
            pass

    def _wire_gateway_callbacks(self, gw):
        gw.events.on_error(self._sink)
        gw.events.on_connection_update(self._sink)
        gw.events.on_order_update(self._sink)
        gw.events.on_execution(self._sink)
        gw.events.on_account_info_update(self._sink)
        gw.events.on_position_update(self._sink)