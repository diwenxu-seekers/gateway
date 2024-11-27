import logging
from logging.handlers import TimedRotatingFileHandler
import sys
import queue
import threading
from datetime import datetime
import time
from functools import partial
from contextlib import contextmanager
from traceback import print_exc


import gateway_lib as GL

logger = logging.getLogger(__name__)

sender_id = GL.OrderSubSenderID.create(r'P:\dist\shifts.txt')
order_prefix = 'GWC'

def get_on_behalf():
    on_behalf=''
    if sender_id:
        on_behalf = sender_id.active()
        logger.info(f"On behalf of '{on_behalf}'")
    return on_behalf

def set_broker_log_level(level):
    for name in ['ibapi', 'websockets']:
        logger = logging.getLogger(name)
        if level:
            logger.setLevel(level)
        else:
            logger.propagate = False

def config_logging(log_file=None,
    display_level=logging.INFO, file_level=logging.DEBUG):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    if display_level:
        streamHandler = logging.StreamHandler()
        fmt = '[%(asctime)s] [%(name)s] [%(threadName)s] [%(levelname)s] [%(funcName)s] %(message)s'
        formatter = logging.Formatter(fmt)
        streamHandler.setFormatter(formatter)
        streamHandler.setLevel(display_level)
        root.addHandler(streamHandler)

    if log_file:
        from pathlib import Path
        from datetime import time

        filepath = Path(log_file)
        if filepath.parent == Path('.'):
            filepath = Path.cwd() / filepath

        fmt_str = '%(asctime)s;%(levelname)s;%(name)s;pid-%(process)d;tid-%(thread)d;%(funcName)s;%(message)s'
        import os
        os.makedirs(os.path.dirname(str(filepath)), exist_ok=True)
        handler = logging.handlers.TimedRotatingFileHandler(
            str(filepath), when='W6', interval=1,
            atTime=time(hour=0, minute=0))
        handler.setFormatter(logging.Formatter(fmt_str))
        handler.setLevel(file_level)
        root.addHandler(handler)

    set_broker_log_level(level=None)

def debug_handler(src, event):
    detail = f'src={src} event={event}'
    logger.info(f'{detail}\n')

gateway_id = 'gateway-console'
client_id = 0   # default constant. For compatability with IB

def create_cqg(retry=-1):
    host_name = 'wss://demoapi.cqg.com:443'
    user_name = 'FYuenWAPI'
    password = 'pass'
    client_app_id = 'WaveRiderCapitalWT'
    client_version = 'python-client'
    private_label = 'WaveRiderCapital'
    account_id = 17018315
    cfg = GL.GatewayConfig
    return GL.GatewayFactory.create({
        cfg.TYPE: cfg.GATEWAY_CQG,
        cfg.NAME: gateway_id,
        cfg.HOST: host_name,
        cfg.ACCOUNT_ID: account_id,
        cfg.USER: user_name,
        cfg.PASSWORD: password,
        cfg.APP_ID: client_app_id,
        cfg.VERSION: client_version,
        cfg.PRIVATE_LABEL: private_label,
        cfg.RECONNECT_MAX_RETRY: retry,
        cfg.RECONNECT_INTERVAL: 10})

def create_ib(retry=-1):
    _ib_host='ibtws'
    _ib_port=10000
    _ib_client_id=3000

    cfg = GL.GatewayConfig
    return GL.GatewayFactory.create({
        cfg.TYPE: cfg.GATEWAY_IB,
        cfg.NAME: gateway_id,
        cfg.HOST: _ib_host,
        cfg.PORT: _ib_port,
        cfg.CLIENT_ID: _ib_client_id,
        cfg.RECONNECT_MAX_RETRY: retry,
        cfg.RECONNECT_INTERVAL: 10})

def wait_true(arg, until, timeout=10):
    start = time.time()
    while not until(arg):
        time.sleep(1)
        if time.time() - start > timeout:
            raise Exception("Timeout")

(RSP_DISCONNECT, RSP_CONNECT,
    REQ_DISCONNECT, REQ_CONNECT, REQ_STATUS, REQ_TERMINATE,
    REQ_OPEN_ORDERS, REQ_POSITIONS, REQ_EXECUTIONS, REQ_ACC_INFO,
    REQ_LOAD_SYMBOL, REQ_MOD_ORDER) = range(12)

orders = {}

_req_id = 0
def next_req_id():
    global _req_id
    _req_id = _req_id + 1
    return _req_id

def get_order_reference():
    utc_timestamp = datetime.utcnow().timestamp()
    return str(utc_timestamp)

def create_order(exchange, contractType, order_type, side, symbol, quantity, limit_price=0, stop_price=0, ref: str=''):
    order = GL.Order(symbol=symbol, exchange=exchange, contractType=contractType,
        orderType=order_type, action=side, quantity=quantity, limit_price=limit_price, stop_price=stop_price,
        tif=GL.TIF.GTC, outsideRth=True)
    tag = GL.RequestTag(req_id=next_req_id(), client_id=client_id)
    _ref = ref if ref else get_order_reference()
    orders[_ref] = order
    return GL.OrderRequest(tag, gateway_id, _ref, order)

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
                quantity=lambda x: float(x))
            _new_order = partial(create_order, contractType=GL.ContractType.Future, order_type=GL.OrderType.MKT, side=action, limit_price=0, ref="")
        elif order_type == 'limit':
            _type = GL.OrderType.LMT
            formatter = dict(
                exchange=lambda x: GL.Exchange.from_str(x),
                symbol=lambda x:x,
                quantity=lambda x: float(x),
                limit_price=lambda x: float(x))
            _new_order = partial(create_order, contractType=GL.ContractType.Future, order_type=_type, side=action, ref="")
        elif order_type == 'stop':
            _type = GL.OrderType.STP
            formatter = dict(
                exchange=lambda x: GL.Exchange.from_str(x),
                symbol=lambda x:x,
                quantity=lambda x: float(x),
                stop_price=lambda x: float(x))
            _new_order = partial(create_order, contractType=GL.ContractType.Future, order_type=_type, side=action, ref="")
        elif order_type == 'stop limit':
            _type = GL.OrderType.STP_LMT
            formatter = dict(
                exchange=lambda x: GL.Exchange.from_str(x),
                symbol=lambda x:x,
                quantity=lambda x: float(x),
                stop_price=lambda x: float(x),
                limit_price=lambda x: float(x))
            _new_order = partial(create_order, contractType=GL.ContractType.Future, order_type=_type, side=action, ref="")
        req = input(f'Please enter order details (format: {",".join(x for x in formatter)}):')
        tokens = {k: formatter[k](v.strip()) for k, v in zip(formatter, (t.strip() for t in req.split(',')))}
        print('user input: ', tokens)
        order = _new_order(**tokens)
    except:
        logger.exception('Failed to parse order input.')
    return order

def loop(create_gw, q):
    gw = create_gw()
    gw.events.on_connection_update(debug_handler)
    def update_order_info(src, evt: GL.OrderUpdate):
        debug_handler(src, evt)
        orders[evt.order_ref] = evt.order
    gw.events.on_order_update(update_order_info)
    gw.events.on_error(debug_handler)
    gw.events.on_execution(debug_handler)
    gw.events.on_account_info_update(debug_handler)
    gw.events.on_position_update(debug_handler)
    gw.events.on_order_api_ready(debug_handler)

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
                gw.events.on_error(None)
                gw.events.on_connection_update(None)
                gw.events.on_order_update(None)
                gw.events.on_execution(None)
                cont = False
                gw.disconnect()
            elif isinstance(req, GL.OrderRequest):
                gw.place_order(req.order_id, req.order, order_prefix=order_prefix, on_behalf_of=get_on_behalf())
            elif isinstance(req, GL.OrderCancelRequest):
                gw.cancel_order(req.order_id, on_behalf_of=get_on_behalf())
            elif req == REQ_MOD_ORDER:
                order_ref = q.get(block=True, timeout=0.2)
                order = orders.get(order_ref, None)
                if order is None:
                    logger.error(f"Order not found for '{order_ref}'")
                else:
                    qty = q.get(block=True, timeout=0.2)
                    if qty:
                        order.quantity = int(qty)
                    price = q.get(block=True, timeout=0.2)
                    if price:
                        if order.orderType == GL.OrderType.STP:
                            order.stop_price = float(price)
                        elif order.orderType == GL.OrderType.LMT:
                            order.limit_price = float(price)
                        elif order.orderType == GL.OrderType.STP_LMT:
                            order.stop_price = float(price)
                            price2 = q.get(block=True, timeout=0.2)
                            if price2:
                                order.limit_price = float(price2)
                    if qty or price:
                        gw.modify_order(order_ref, order, on_behalf_of=get_on_behalf())
            elif req == REQ_LOAD_SYMBOL:
                sym = q.get(block=True, timeout=0.2)
                gw.request_contract_details(symbol=sym)
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
    logger.info("Finished.")


def wait_user_input(q: queue.Queue):
    while True:
        s = input('Please enter command:\n')
        logger.debug(f"user entered: '{s}'")
        ss = s.strip()
        if ss == 'exit':
            q.put(REQ_TERMINATE)
            time.sleep(2)
            sys.exit('User requested system exit.')
        elif 'help' in ss:
            print('List of commands:')
            print('\n'.join(f"{i+1}) {cmd}" for i, cmd in enumerate([
                'connect',
                'disconnect',
                'buy market',
                'sell market',
                'buy limit',
                'sell limit',
                'buy stop',
                'sell stop',
                'cancel',
                'modify',
                'orders',
                'trades',
                'positions'])))
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
            tag = GL.RequestTag(next_req_id(), client_id)
            req = GL.OrderCancelRequest(tag, gateway_id, ref)
            q.put(req)
        elif ss.startswith('modify'):
            entered = input(f'Please enter details (order ref,qty,(stop|limit|stop,limit)):')
            tag = GL.RequestTag(next_req_id(), client_id)
            q.put(REQ_MOD_ORDER)
            for token in entered.split(','):
                q.put(token)
        elif ss == 'orders':
            q.put(REQ_OPEN_ORDERS)
        elif ss == 'positions':
            q.put(REQ_POSITIONS)
        elif ss == 'trades':
            q.put(REQ_EXECUTIONS)
        elif ss == 'account':
            q.put(REQ_ACC_INFO)
        elif ss == 'symbol':
            sym = input(f'Please enter symbol:')
            q.put(REQ_LOAD_SYMBOL)
            q.put(sym)
        else:
            logger.info(f"Unrecognized command: '{s}'")


def start_interactive(create_gw, mq):
    config_logging(
        display_level=logging.INFO,
        log_file='console.log')

    logger.info(f"Path of gateway_lib: '{GL.__file__}'")
    t = threading.Thread(name='GatewayLoop', target=loop, args=(create_gw, mq,))
    t.start()
    time.sleep(1)
    wait_user_input(mq)

if __name__ == "__main__":
    mq = queue.Queue()
    try:
        broker = create_cqg
        start_interactive(broker, mq)
    except KeyboardInterrupt:
        logger.warning("Interrupted")
    except Exception:
        logger.exception('Main caught exception')
    finally:
        logger.info('Exit.')