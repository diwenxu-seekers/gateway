import logging
import gateway_lib as GL
import time
import json
import random
from collections import deque

logger = logging.getLogger(__name__)

_gateway_id = '123'

_order_list = {}


def wait_status(order_id, statuses):
    _status = _order_list.get(str(order_id), None)
    while not _status in statuses:
        time.sleep(0.2)
        _status = _order_list.get(str(order_id), None)

def debug_handler(*ar, **kw):
    print(str(kw['event']))
    event = kw['event']
    if isinstance(event, GL.OrderUpdate):
        status = event.status
        oref = event.order_ref
        _order_list.update({oref: status})

def stress1(gw: GL.AbstractGateway):
    gw.connect()
    T = 100
    order_id = 0
    price = 7600
    while not gw.is_healthy:
        print('not healthy')
        time.sleep(0.5)
        pass
    for _ in range(T):
        order_id = order_id + 1
        lmt_order = GL.Order(symbol='NQ', orderType=GL.OrderType.LMT, action=GL.OrderAction.BUY, quantity=1, limit_price=price + random.randint(-1, 1) * random.randint(1,10) * 0.25, tif=GL.TIF.GTC)
        gw.place_order(str(order_id), lmt_order)
        wait_status(order_id, [GL.OrderStatus.SUBMITTED, GL.OrderStatus.FILLED, GL.OrderStatus.INACTIVE])
        gw.cancel_order(str(order_id))
        # wait_status(order_id, GL.OrderStatus.CANCELLED)
    time.sleep(1)
    gw.disconnect()
    print('total:', len(_order_list), 'cancelled:', [x for x in _order_list if _order_list[x] == GL.OrderStatus.CANCELLED])

def stress2(gw: GL.AbstractGateway):
    gw.connect()
    T = 100
    order_id = 0
    price = 7600
    time_comsumption = deque()
    while not gw.is_healthy:
        print('not healthy')
        time.sleep(0.5)
        pass
    for _ in range(T):
        order_id = order_id + 1
        lmt_order = GL.Order(symbol='NQ', orderType=GL.OrderType.LMT, action=GL.OrderAction.BUY, quantity=1, limit_price=price, tif=GL.TIF.GTC)
        t1 = time.time()
        gw.place_order(str(order_id), lmt_order)
        wait_status(order_id, [GL.OrderStatus.SUBMITTED, GL.OrderStatus.FILLED, GL.OrderStatus.INACTIVE])
        t2 = time.time()
        time_comsumption.append(t2 -t1)
        gw.cancel_order(str(order_id))
        # wait_status(order_id, GL.OrderStatus.CANCELLED)
    time.sleep(1)
    print(f'avg. latency from client.place_order to ack_from_ib: {sum(time_comsumption)/len(time_comsumption)}s, {time_comsumption}' )
    gw.disconnect()


if __name__ == "__main__":
    gw = GL.IBGateway(_gateway_id, host='ibtws', port=9002, client_id=3000)
    gw.events.on_error(debug_handler)
    gw.events.on_connection_update(debug_handler)
    gw.events.on_order_update(debug_handler)
    gw.events.on_execution(debug_handler)
    gw.events.on_account_info_update(debug_handler)
    gw.events.on_position_update(debug_handler)
    stress2(gw)
