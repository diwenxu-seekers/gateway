import logging
from logging.handlers import RotatingFileHandler
import signal
import queue
import threading
import json
import time

import gateway_lib as GL

logger = logging.getLogger(__name__)

REQ_TERMINATE = 2

class SampleMessageProcessor(GL.ManagerMessageProcessor):
    def __init__(self):
        self.req_id = 0

    def next_req_id(self):
        self.req_id = self.req_id + 1
        return self.req_id

    def next_tag(self, client_id: str):
        return GL.RequestTag(req_id=self.next_req_id(), client_id=client_id)

    def create_order_request(self, tag, gw_id, order_type, side, symbol, exchange, contract_type, quantity, price, ref: str=''):
        order = GL.Order(symbol=symbol, exchange=exchange, contractType=contract_type,
            orderType=order_type, action=side, quantity=quantity, limit_price=price,
            tif=GL.TIF.GTC)
        _oid = ref if ref else f'ref_{self.req_id}'
        return GL.OrderRequest(tag, gw_id, _oid, order)

    def process_incoming(self, msg: dict):
        req = None
        try:
            cmd = msg.get('cmd', None)
            client_id = msg['cid']
            if cmd == 'connect':
                gw_id = msg['gid']
                broker = msg['broker']
                host = msg['host']
                port = msg['port']
                ib_client_id = msg.get('client_id', None)
                req = GL.ConnectRequest(tag=self.next_tag(client_id), gateway_id=gw_id,
                    broker=broker,
                    kwargs=dict(host=host, port=port, client_id=ib_client_id))
            elif cmd == 'disconnect':
                gw_id = msg['gid']
                req = GL.DisconnectRequest(tag=self.next_tag(client_id), gateway_id=gw_id)
            elif cmd == 'place order':
                gw_id = msg['gid']
                side = msg['side']
                _side = GL.OrderAction.BUY
                if side == 'sell':
                    _side = GL.OrderAction.SELL
                order_type = msg['order_type']
                _type = GL.OrderType.MKT
                if order_type == 'lmt':
                    _type = GL.OrderType.LMT
                symbol = msg['symbol']
                qty = msg['qty']
                price = msg.get('price', 0)
                order_ref = msg['order_ref']
                exchange = msg['exchange']
                contract_type = msg['contract_type']
                req = self.create_order_request(self.next_tag(client_id), gw_id, _type, _side, symbol, exchange, contract_type, qty, price, order_ref)
            elif cmd == 'cancel order':
                gw_id = msg['gid']
                order_ref = msg['order_ref']
                req = GL.OrderCancelRequest(self.next_tag(client_id), gw_id, order_ref)
            elif cmd == 'status':
                req = GL.GatewaySummaryRequest(self.next_tag(client_id))
        except:
            logger.exception(f'Unhandled exception when processing message "{msg}"')
        logger.info(f'Request: {req if req else json.dumps(msg, indent=4)}')
        return req

    def process_outgoing(self, update):
        # logger.info(f'Outgoing obj: {update}')
        return update.to_dict()

def config_logger():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter('[%(asctime)s] [%(name)s] [%(thread)d] [%(threadName)s] [%(levelname)s] [%(funcName)s] %(message)s'))
    fh = RotatingFileHandler('runmgr.log', maxBytes=2*1000*1000, backupCount=10)
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] [%(funcName)s] %(message)s'))
    root.addHandler(sh)
    root.addHandler(fh)

def keyboardInterruptHandler(signal, frame):
    logger.info("KeyboardInterrupt (ID: {}) has been caught. Cleaning up...".format(signal))
    exit(0)

signal.signal(signal.SIGINT, keyboardInterruptHandler)

requested_disconnection = set()
connections = dict()

def capture_connection_requests(msg):
    global requested_disconnection
    global connections
    try:
        cmd = msg['cmd']
        gid = msg['gid']
        if cmd == 'connect':
            connections[gid] = msg
        if cmd == 'disconnect':
            requested_disconnection.add(gid)
    except:
        pass

def auto_reconnect(q, wait_sec: int=10, max_retry: int=-1):
    retry_cnts = {}
    def reconnect_handler(event):
        nonlocal retry_cnts
        global requested_disconnection
        global connections
        try:
            if event['type'] == 'ConnectionUpdate':
                gid = event['gateway_id']
                status = event['status']
                if status == GL.ConnectionStatus.DISCONNECTED:
                    if gid in requested_disconnection:
                        logger.debug(f'Skipped auto reconnect because user initiated disconnect request.')
                        requested_disconnection.remove(gid)
                    else:
                        if gid not in retry_cnts:
                            retry_cnts[gid] = 0
                        retry_cnt = retry_cnts[gid]
                        if max_retry < 0 or retry_cnt < max_retry:
                            retry_cnt = retry_cnt + 1
                            logger.info(f'Auto reconnect after {wait_sec}s. Retry: {retry_cnt}')
                            retry_cnts[gid] = retry_cnt
                            time.sleep(wait_sec)
                            req = connections.get(gid, None)
                            if req is None:
                                pass
                            else:
                                q.put(req)
                elif status == GL.ConnectionStatus.CONNECTED:
                    retry_cnts[gid] = 0
        except:
            pass
    return reconnect_handler

def oms_loop(q: queue.Queue):
    reconnect = auto_reconnect(q, wait_sec=10, max_retry=-1)

    def on_events(event):
        logger.info(json.dumps(event, indent=4))
        reconnect(event)

    with GL.GatewayManager(message_handler=SampleMessageProcessor(), event_handler=on_events) as oms:
        while True:
            try:
                msg = q.get(block=True, timeout=0.2)
                capture_connection_requests(msg)
                sig = msg.get('signal', None)
                if sig is not None and sig == REQ_TERMINATE:
                    break
                oms.send(msg)
            except queue.Empty:
                pass

def wait_user_input(q: queue.Queue):
    s = input('Please enter command:\n')
    logger.debug(f"user entered: '{s}'")
    ss = f'dict({s.strip()})'
    try:
        obj = eval(ss)
        q.put(obj)
    except:
        logger.warning(f'Invalid input "{s}"')

def start_interactive():
    mq = queue.Queue()
    try:
        t = threading.Thread(name='OMSLoop', target=oms_loop, args=(mq,))
        t.start()
        while True:
            wait_user_input(mq)
    except (KeyboardInterrupt, SystemExit):
        mq.put(dict(signal=REQ_TERMINATE))
    finally:
        logger.info('Program exit.')

if __name__ == "__main__":
    config_logger()
    start_interactive()