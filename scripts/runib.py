import time
import logging
from logging.handlers import RotatingFileHandler
import signal
import sys
import queue
import threading
import pathlib
from functools import partial

import gateway_lib as GL

logger = logging.getLogger(__name__)

(RSP_DISCONNECT, RSP_CONNECT,
    REQ_DISCONNECT, REQ_CONNECT, REQ_STATUS, REQ_TERMINATE,
    REQ_OPEN_ORDERS, REQ_POSITIONS, REQ_EXECUTIONS, REQ_ACC_INFO) = range(10)

_client_id = 'ib_tester'
_gateway_id = 'ib01'
_req_id = 0
_ib_host='ibtws'
_ib_port=9002
_ib_client_id=3000
_gateway_state_file = f'{_gateway_id}_runib.json'

def next_req_id():
    global _req_id
    _req_id = _req_id + 1
    return _req_id

def config_logger():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter('[%(asctime)s] [%(name)s] [%(threadName)s] [%(levelname)s] [%(funcName)s] %(message)s'))
    root.addHandler(sh)

    fh = RotatingFileHandler('runib.log', maxBytes=2*1000*1000, backupCount=10)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter('[%(asctime)s] [%(name)s] [%(thread)d] [%(levelname)s] [%(funcName)s] %(message)s'))
    root.addHandler(fh)

    ib = logging.getLogger('ibapi')
    ib.setLevel(logging.INFO)

def keyboardInterruptHandler(signal, frame):
    logger.info("KeyboardInterrupt (ID: {}) has been caught. Cleaning up...".format(signal))
    exit(0)

signal.signal(signal.SIGINT, keyboardInterruptHandler)

def debug_handler(src, event):
    detail = f'src={src} event={event}'
    logger.info(f'{detail}\n')

def create_order(exchange, contractType, order_type, side, symbol, quantity, limit_price=0, stop_price=0, ref: str=''):
    order = GL.Order(symbol=symbol, exchange=exchange, contractType=contractType,
        orderType=order_type, action=side, quantity=quantity, limit_price=limit_price, stop_price=stop_price,
        tif=GL.TIF.GTC)
    tag = GL.RequestTag(next_req_id(), _client_id)
    _oid = ref if ref else f'order_ref_{_req_id}'
    return GL.OrderRequest(tag, _gateway_id, _oid, order)


def gateway_loop(q: queue.Queue):
    cfg = GL.GatewayConfig
    gw = GL.GatewayFactory.create({
        cfg.TYPE: cfg.GATEWAY_IB,
        cfg.NAME: _gateway_id,
        cfg.HOST: _ib_host,
        cfg.PORT: _ib_port,
        cfg.CLIENT_ID: _ib_client_id,
        cfg.STATE_FILE_PATH: _gateway_state_file,
        cfg.RECONNECT_MAX_RETRY: -1,
        cfg.RECONNECT_INTERVAL: 20})
    gw.events.on_error(debug_handler)
    gw.events.on_connection_update(debug_handler)
    gw.events.on_order_update(debug_handler)
    gw.events.on_execution(debug_handler)
    gw.events.on_account_info_update(debug_handler)
    gw.events.on_position_update(debug_handler)

    cont = True
    while cont:
        try:
            req = q.get(block=True, timeout=0.2)
            if req == REQ_CONNECT:
                gw.connect()
            elif req == REQ_DISCONNECT:
                gw.disconnect()
            elif req == REQ_STATUS:
                logger.info(f'{gw} connection {"is" if gw.is_healthy else "is not"} healthy.')
            elif req == REQ_TERMINATE:
                # gw.events.on_error(None)
                # gw.events.on_connection_update(None)
                # gw.events.on_order_update(None)
                # gw.events.on_execution(None)
                cont = False
                gw.disconnect()
            elif isinstance(req, GL.OrderRequest):
                gw.place_order(req.order_id, req.order)
            elif isinstance(req, GL.OrderCancelRequest):
                gw.cancel_order(req.order_id)
            elif req == REQ_OPEN_ORDERS:
                gw.request_open_orders()
            elif req == REQ_POSITIONS:
                gw.request_positions()
            elif req == REQ_EXECUTIONS:
                gw.request_executions()
            elif req == REQ_ACC_INFO:
                gw.request_account_info()
            q.task_done()
        except queue.Empty:
            pass
        except Exception:
            logger.exception('Unhandled exception in gateway implementation.')
    logger.info('Gateway loop end.')

def place_order(cmd, order_type: str):
    action = None
    if cmd.startswith('buy'):
        action = GL.OrderAction.BUY
    elif cmd.startswith('sell'):
        action = GL.OrderAction.SELL

    order = None
    try:
        formatter = {}
        _new_order = None
        if order_type == 'market':
            formatter = dict(
                exchange=lambda x: GL.Exchange.from_str(x),
                symbol=lambda x:x,
                quantity=lambda x: float(x),
                ref=lambda x: x)
            _new_order = partial(create_order, contractType=GL.ContractType.Future, order_type=GL.OrderType.MKT, side=action, limit_price=0)
        elif order_type == 'limit':
            _type = GL.OrderType.LMT
            formatter = dict(
                exchange=lambda x: GL.Exchange.from_str(x),
                symbol=lambda x:x,
                quantity=lambda x: float(x),
                limit_price=lambda x: float(x),
                ref=lambda x: x)
            _new_order = partial(create_order, contractType=GL.ContractType.Future, order_type=_type, side=action)
        elif order_type == 'stop':
            _type = GL.OrderType.STP
            formatter = dict(
                exchange=lambda x: GL.Exchange.from_str(x),
                symbol=lambda x:x,
                quantity=lambda x: float(x),
                stop_price=lambda x: float(x),
                ref=lambda x: x)
            _new_order = partial(create_order, contractType=GL.ContractType.Future, order_type=_type, side=action)
        elif order_type == 'stop limit':
            _type = GL.OrderType.STP_LMT
            formatter = dict(
                exchange=lambda x: GL.Exchange.from_str(x),
                symbol=lambda x:x,
                quantity=lambda x: float(x),
                limit_price=lambda x: float(x),
                stop_price=lambda x: float(x),
                ref=lambda x: x)
            _new_order = partial(create_order, contractType=GL.ContractType.Future, order_type=_type, side=action)
        req = input(f'Please enter order details (format: {",".join(x for x in formatter)}):')
        tokens = {k: formatter[k](v.strip()) for k, v in zip(formatter, (t.strip() for t in req.split(',')))}
        order = _new_order(**tokens)
    except:
        logger.exception('Failed to parse order input.')
    return order

def wait_user_input(q: queue.Queue):
    s = input('Please enter command:\n')
    logger.debug(f"user entered: '{s}'")
    ss = s.strip()
    if ss == 'exit':
        q.put(REQ_TERMINATE)
        sys.exit('User requested system exit.')
    elif ss.startswith('status'):
        q.put(REQ_STATUS)
    elif ss.startswith('connect'):
        q.put(REQ_CONNECT)
    elif ss.startswith('disconnect'):
        q.put(REQ_DISCONNECT)
    elif ss.startswith('buy') or ss.startswith('sell'):
        order_type = 'market'
        if 'stop limit' in ss:
            order_type = 'stop limit'
        elif 'limit' in ss:
            order_type = 'limit'
        elif 'stop' in ss:
            order_type = 'stop'
        req = place_order(ss, order_type)
        if req is not None:
            q.put(req)
    elif ss.startswith('cancel'):
        ref = input(f'Please enter order ref:')
        tag = GL.RequestTag(next_req_id(), _client_id)
        req = GL.OrderCancelRequest(tag, _gateway_id, ref)
        q.put(req)
    elif ss == 'orders':
        q.put(REQ_OPEN_ORDERS)
    elif ss == 'positions':
        q.put(REQ_POSITIONS)
    elif ss == 'trades':
        q.put(REQ_EXECUTIONS)
    elif ss == 'account':
        q.put(REQ_ACC_INFO)
    else:
        logger.info(f"Unrecognized command: '{s}'")

if __name__ == "__main__":
    mq = queue.Queue()
    config_logger()

    try:
        t = threading.Thread(name='GatewayLoop', target=gateway_loop, args=(mq,))
        t.start()
        while True:
            wait_user_input(mq)
    except (KeyboardInterrupt, SystemExit):
        mq.put(REQ_TERMINATE)
        raise
    finally:
        logger.info('Program exit.')