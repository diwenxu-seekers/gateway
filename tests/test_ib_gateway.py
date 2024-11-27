import pytest
import logging
import time
import pathlib
import datetime

from .common import get_settings, get_front_month
import gateway_lib as GL

error_msgs = list()
connection_msgs = list()
order_msgs = list()
execution_msgs = list()

gateway_id = 'ib01'
settings = get_settings()
ib_host = settings['IB_dev']['host']
ib_port = settings['IB_dev']['port']
ib_client_id = 3334
gateway_state_file = f'{gateway_id}_unittest.json'

def error_handler(src, event):
    error_msgs.append(event)

def connection_handler(src, event):
    connection_msgs.append(event)

def order_handler(src, event):
    order_msgs.append(event)

def execution_handler(src, event):
    execution_msgs.append(event)

def config_logger():
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] [%(funcName)s] %(message)s'))
    root.addHandler(sh)

def init_test_states():
    global error_msgs
    global connection_msgs
    global order_msgs
    global execution_msgs
    error_msgs.clear()
    connection_msgs.clear()
    order_msgs.clear()
    execution_msgs.clear()

def clean_state_file(gw: GL.IBGateway):
    f = pathlib.Path(gw.state_file)
    if f.is_file():
        f.unlink()

@pytest.fixture
def gateway():
    config_logger()
    init_test_states()
    gw = GL.IBGateway(gateway_id, host=ib_host, port=ib_port, client_id=ib_client_id, state_filepath=gateway_state_file)
    gw.events.on_error(error_handler)
    gw.events.on_connection_update(connection_handler)
    gw.events.on_order_update(order_handler)
    gw.events.on_execution(execution_handler)
    yield gw
    gw.disconnect()
    clean_state_file(gw)

@pytest.fixture
def market_buy_order():
    return GL.Order(symbol=get_front_month('NQ'), exchange=GL.Exchange.CME, contractType=GL.ContractType.Future, orderType=GL.OrderType.MKT, action=GL.OrderAction.BUY, quantity=1, limit_price=0, tif=GL.TIF.GTC)

@pytest.fixture
def market_sell_order():
    return GL.Order(symbol=get_front_month('NQ'), exchange=GL.Exchange.CME, contractType=GL.ContractType.Future, orderType=GL.OrderType.MKT, action=GL.OrderAction.SELL, quantity=1, limit_price=0, tif=GL.TIF.GTC)

def test_connect(gateway):
    gateway.connect()
    time.sleep(2)
    assert len(connection_msgs) == 1
    update = connection_msgs[0]
    assert isinstance(update, GL.ConnectionUpdate)
    assert update.status == GL.ConnectionStatus.CONNECTED
    gateway.disconnect()
    time.sleep(2)

def test_disconnect(gateway):
    gateway.connect()
    time.sleep(2)
    init_test_states()

    gateway.disconnect()
    time.sleep(2)
    assert len(connection_msgs) == 1
    update = connection_msgs[0]
    assert isinstance(update, GL.ConnectionUpdate)
    assert update.status == GL.ConnectionStatus.DISCONNECTED

def test_connection_healthiness(gateway):
    init_test_states()
    assert gateway.is_healthy == False
    gateway.connect()
    time.sleep(2)
    assert gateway.is_healthy == True
    gateway.disconnect()
    time.sleep(2)
    assert gateway.is_healthy == False

def test_place_market_order(gateway: GL.AbstractGateway, market_buy_order: GL.Order, market_sell_order: GL.Order):
    init_test_states()
    gateway.connect()
    time.sleep(2)
    order_id = 0

    order_id += 1
    order_ref = str(order_id)
    gateway.place_order(order_ref, market_buy_order)
    time.sleep(5)

    assert len(order_msgs) > 0
    update = order_msgs[-1]
    assert isinstance(update, GL.OrderUpdate)
    assert update.order_ref == order_ref
    assert update.filled == 1
    assert update.status == GL.OrderStatus.FILLED
    assert len(execution_msgs) == 1
    update = execution_msgs[0]
    assert isinstance(update, GL.ExecutionUpdate)
    assert update.order_ref == order_ref
    assert update.filled == 1

    init_test_states()

    order_id += 1
    order_ref = str(order_id)
    gateway.place_order(str(order_id), market_sell_order)
    time.sleep(5)

    assert len(order_msgs) > 0
    update = order_msgs[-1]
    assert isinstance(update, GL.OrderUpdate)
    assert update.order_ref == order_ref
    assert update.filled == 1
    assert update.status == GL.OrderStatus.FILLED
    assert len(execution_msgs) == 1
    update = execution_msgs[0]
    assert isinstance(update, GL.ExecutionUpdate)
    assert update.order_ref == order_ref
    assert update.filled == 1