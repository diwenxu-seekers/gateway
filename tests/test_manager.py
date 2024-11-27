import pytest
import time
import logging
import json
import uuid
import pathlib

from .common import get_settings, get_front_month
import gateway_lib as GL

logger = logging.getLogger(__name__)

settings = get_settings()
ib_host = settings['IB_dev']['host']
ib_port = settings['IB_dev']['port']
ib_client_id = 3334

def config_logger():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter('[%(asctime)s] [%(name)s] [%(thread)d] [%(threadName)s] [%(levelname)s] [%(funcName)s] %(message)s'))
    root.addHandler(sh)

def clean_state_file(gw: GL.IBGateway):
    f = pathlib.Path(gw.state_file)
    if f.is_file():
        f.unlink()

class SampleMessageProcessor(GL.ManagerMessageProcessor):
    def __init__(self):
        self.req_id = 0

    def next_req_id(self):
        self.req_id = self.req_id + 1
        return self.req_id

    def next_tag(self, client_id: str):
        return GL.RequestTag(req_id=self.next_req_id(), client_id=client_id)

    def create_order_request(self, tag, gw_id, order_type, side, symbol, exchange, contract_type, quantity, price, ref: str=''):
        _exch = GL.Exchange.from_str(exchange)
        _ctype = GL.ContractType.from_str(contract_type)
        order = GL.Order(symbol=symbol, exchange=_exch, contractType=_ctype,
            orderType=order_type, action=side, quantity=quantity, price=price,
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
        msg = json.dumps(update.to_dict(), indent=4)
        return msg

def on_events(event):
    logger.info(event)

u_order_ref = lambda: str(uuid.uuid4())

def basic_connection():
    with GL.GatewayManager(message_handler=SampleMessageProcessor(), event_handler=on_events) as oms:
        oms.send(dict(cid='C1',cmd='connect',
            broker='IB',gid='ib01',host=ib_host,port=ib_port,client_id=3333))
        time.sleep(5)
        oms.send(dict(cid='C1',cmd='disconnect',gid='ib01'))
        time.sleep(5)

def multi_client_connection():
    with GL.GatewayManager(message_handler=SampleMessageProcessor(), event_handler=on_events) as oms:
        oms.send(dict(cid='C1',cmd='connect',broker='IB',gid='ib01',host=ib_host,port=ib_port,client_id=ib_client_id))
        time.sleep(5)
        oms.send(dict(cid='C2',cmd='connect',broker='IB',gid='ib01',host=ib_host,port=ib_port,client_id=ib_client_id))
        time.sleep(5)
        oms.send(dict(cid='C1',cmd='disconnect',gid='ib01'))
        time.sleep(5)
        oms.send(dict(cid='C2',cmd='disconnect',gid='ib01'))
        time.sleep(5)

def multi_gateway_connection():
    with GL.GatewayManager(message_handler=SampleMessageProcessor(), event_handler=on_events) as oms:
        oms.send(dict(cid='C1',cmd='connect',broker='IB',gid='ib01',host=ib_host,port=ib_port,client_id=ib_client_id))
        time.sleep(5)
        oms.send(dict(cid='C1',cmd='connect',broker='IB',gid='ib02',host=ib_host,port=10000,client_id=ib_client_id))
        time.sleep(5)

def market_orders():
    with GL.GatewayManager(message_handler=SampleMessageProcessor(), event_handler=on_events) as oms:
        oms.send(dict(cid='C1',cmd='connect',
            broker='IB',gid='ib01',host=ib_host,port=ib_port,client_id=ib_client_id))
        time.sleep(5)
        oms.send(dict(cid='C1',gid='ib01',cmd='place order',
            side='buy',order_type='mkt',symbol=get_front_month('NQ'),exchange='GLOBEX',contract_type='FUT',qty=1,order_ref=u_order_ref()))
        time.sleep(5)
        oms.send(dict(cid='C1',gid='ib01',cmd='place order',
            side='sell',order_type='mkt',symbol=get_front_month('NQ'),exchange='GLOBEX',contract_type='FUT',qty=1,order_ref=u_order_ref()))
        time.sleep(5)
        oms.send(dict(cid='C1',cmd='disconnect',gid='ib01'))
        time.sleep(5)
        clean_state_file(oms.get('ib01'))

def limit_orders():
    with GL.GatewayManager(message_handler=SampleMessageProcessor(), event_handler=on_events) as oms:
        oms.send(dict(cid='C1',cmd='connect',
            broker='IB',gid='ib01',host=ib_host,port=ib_port,client_id=ib_client_id))
        time.sleep(5)
        order_ref = u_order_ref()
        oms.send(dict(cid='C1',gid='ib01',cmd='place order',
            side='buy',order_type='lmt',symbol=get_front_month('NQ'),exchange='GLOBEX',contract_type='FUT',qty=1,price=7000,order_ref=order_ref))
        time.sleep(5)
        oms.send(dict(cid='C1',gid='ib01',cmd='cancel order',order_ref=order_ref))
        time.sleep(5)
        oms.send(dict(cid='C1',cmd='disconnect',gid='ib01'))
        time.sleep(5)
        clean_state_file(oms.get('ib01'))

if __name__ == '__main__':
    config_logger()
    # basic_connection()
    market_orders()