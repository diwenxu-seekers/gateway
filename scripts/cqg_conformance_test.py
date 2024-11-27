import logging
from logging.handlers import RotatingFileHandler
import signal
import sys
import queue
import threading
from pathlib import Path
from datetime import datetime, timezone
import time
from functools import partial
from contextlib import contextmanager
from traceback import print_exc
import unittest

import gateway_lib as GL

logger = logging.getLogger(__name__)

test_output_dir = Path('out') / 'conformance_test'

url = 'wss://demoapi.cqg.com:443'
user_name = 'FYuenWAPI'
password = 'pass'
client_app_id = 'WebApiTest'
client_version = 'python-client'
account_id = 17018315
gateway_label = 'cqg_test'

def setup_logger(logger_name, log_file=None, level=logging.INFO, filemode='w'):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('[%(asctime)s] [%(name)s] [%(threadName)s] [%(levelname)s] [%(funcName)s] %(message)s')
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    l.setLevel(level)
    l.addHandler(streamHandler)
    if log_file:
        fileHandler = logging.FileHandler(log_file, mode=filemode)
        fileHandler.setFormatter(formatter)
        l.addHandler(fileHandler)

def release_logger(logger_name):
    logger = logging.getLogger(logger_name)
    handlers = logger.handlers[:]
    for handler in handlers:
        handler.close()
        logger.removeHandler(handler)

def set_debug_mode():
    setup_logger(__name__)
    setup_logger('cqg', level=logging.DEBUG)
    setup_logger('cqg.webapi', level=logging.DEBUG)

def debug_handler(src, event):
    detail = f'src={src} event={event}'
    logger.info(f'{detail}\n')

def get_order_reference():
    utc_timestamp = datetime.utcnow().timestamp()
    return str(utc_timestamp)

def wait_true(arg, until, timeout=10):
    start = time.time()
    while not until(arg):
        time.sleep(1)
        if time.time() - start > timeout:
            raise Exception("Timeout")

@contextmanager
def open_session(url, account_id, user_name, password,
    client_app_id, client_version, name=None):
    logger.debug('begin')
    gw = GL.CQGGateway(
            url=url,
            account_id=account_id,
            username=user_name,
            password=password,
            client_app_id=client_app_id,
            client_version=client_version,
            name=name)
    try:
        yield gw
    except Exception as e:
        logger.exception("Unhandled exception.")
    finally:
        gw.close()
        wait_true(gw, until=lambda gw: gw._loop == None)
        logger.debug('done')

class TestTemplate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        output = test_output_dir / f'{cls.__name__}.log'
        if output.is_file():
            output.unlink()
        logging.Formatter.converter = time.gmtime
        setup_logger(__name__, output, logging.INFO, filemode='a')
        setup_logger('cqg.webapi', output, logging.DEBUG, filemode='a')
        logger.info(f"Test case '{cls.__name__}' begins.")

    @classmethod
    def tearDownClass(cls):
        logger.info(f"Test case '{cls.__name__}' ends.")
        release_logger(__name__)
        release_logger('cqg.webapi')

## Test Cases

class SuccessfulLogonLogoff(TestTemplate):
    def test_successful_logon_logoff(self):
        with open_session(url, account_id, user_name, password,
            client_app_id, client_version, name=gateway_label) as gw:
            gw.connect()
            wait_true(gw, until=lambda gw: gw.is_healthy)
            gw.disconnect()

class InvalidLogon_UserName(TestTemplate):
    def test_invalid_login(self):
        with open_session(url, account_id, 'invalid user', password,
            client_app_id, client_version, name=gateway_label) as gw:
            err = []
            gw.events.on_error(lambda src, event: err.append(event.msg))
            gw.connect()
            wait_true(err, until=lambda ar: len(ar))
            gw.close()

class InvalidLogon_Password(TestTemplate):
    def test_invalid_login(self):
        with open_session(url, account_id, user_name, 'invalid password',
            client_app_id, client_version, name=gateway_label) as gw:
            err = []
            gw.events.on_error(lambda src, event: err.append(event.msg))
            gw.connect()
            wait_true(err, until=lambda ar: len(ar))
            gw.close()

class InvalidLogon_AppID(TestTemplate):
    def test_invalid_login(self):
        with open_session(url, account_id, user_name, password,
            'invalid app id', client_version, name=gateway_label) as gw:
            err = []
            gw.events.on_error(lambda src, event: err.append(event.msg))
            gw.connect()
            wait_true(err, until=lambda ar: len(ar))
            gw.close()

class Concurrent_Session(TestTemplate):
    def test_concurrent_logon(self):
        with open_session(url, account_id, user_name, password,
            client_app_id, client_version, name=gateway_label) as gw:
            status = []
            gw.events.on_connection_update(lambda src, event: status.append(event.status))
            gw.connect()
            wait_true(status, until=lambda status: status and status[-1] == GL.ConnectionStatus.CONNECTED, timeout=20)
            with open_session(url, account_id, user_name, password,
                client_app_id, client_version, name=gateway_label) as gw2:
                status2 = []
                gw2.events.on_connection_update(lambda src, event: status2.append(event.status))
                gw2.connect()
                wait_true(gw2, until=lambda gw: gw.is_healthy)
                gw2.disconnect()
                wait_true(status2, until=lambda status: status[-1] == GL.ConnectionStatus.DISCONNECTED)
            wait_true(status, until=lambda status: status[-1] == GL.ConnectionStatus.DISCONNECTED)

class SuccessfulSymbolResolution(TestTemplate):
    def test_resolve_symbol(self):
        with open_session(url, account_id, user_name, password,
            client_app_id, client_version, name=gateway_label) as gw:
            status = []
            gw.events.on_connection_update(lambda src, event: status.append(event.status))
            gw.connect()
            wait_true(status, until=lambda status: status and status[-1] == GL.ConnectionStatus.CONNECTED, timeout=20)
            gw.request_contract_details(symbol='F.US.ZUC')
            time.sleep(10)
            gw.disconnect()


class SuccessfulOrderRequest(TestTemplate):
    def test_place_order(self):
        with open_session(url, account_id, user_name, password,
            client_app_id, client_version, name=gateway_label) as gw:
            conn = []
            gw.events.on_connection_update(lambda src, event: conn.append(event.status))
            orders = []
            gw.events.on_order_update(lambda src, event: orders.append(event))
            gw.events.on_error(lambda _, evt: logger.error(evt))
            def wait_order_status(status, timeout=20):
                wait_true(orders,
                    until=lambda x: x and x[-1].status == status,
                    timeout=timeout)
                orders.clear()

            gw.connect()
            wait_true(conn,
                until=lambda x: x and x[-1] == GL.ConnectionStatus.CONNECTED,
                timeout=20)

            sym1, sym2, sym3 = 'F.US.ZUW', 'F.US.ZUC', 'F.US.ZUI'
            logger.info(', '.join(f"Symbol_{i+1}='{x}'"
                for i, x in enumerate([sym1, sym2, sym3])))
            gw.request_contract_details(symbol=sym1)
            gw.request_contract_details(symbol=sym2)
            gw.request_contract_details(symbol=sym3)

            time.sleep(10)
            subscription_id = gw.subscribe_update()

            time.sleep(10)
            gw.place_order(order_id=get_order_reference(),
                order=GL.Order(
                    symbol=sym1,
                    exchange=GL.Exchange.GLOBEX,
                    contractType=GL.ContractType.Future,
                    orderType=GL.OrderType.MKT,
                    action=GL.OrderAction.SELL,
                    quantity=1,
                    price=None,
                    tif=GL.TIF.DAY))
            wait_order_status(GL.OrderStatus.FILLED)

            gw.place_order(order_id=get_order_reference(),
                order=GL.Order(
                    symbol=sym1,
                    exchange=GL.Exchange.GLOBEX,
                    contractType=GL.ContractType.Future,
                    orderType=GL.OrderType.MKT,
                    action=GL.OrderAction.BUY,
                    quantity=1,
                    price=None,
                    tif=GL.TIF.GTC))
            wait_order_status(GL.OrderStatus.FILLED)

            gw.place_order(order_id=get_order_reference(),
                order=GL.Order(
                    symbol=sym2,
                    exchange=GL.Exchange.GLOBEX,
                    contractType=GL.ContractType.Future,
                    orderType=GL.OrderType.LMT,
                    action=GL.OrderAction.BUY,
                    quantity=1,
                    price=6.5,
                    tif=GL.TIF.DAY))
            wait_order_status(GL.OrderStatus.FILLED)

            gw.place_order(order_id=get_order_reference(),
                order=GL.Order(
                    symbol=sym2,
                    exchange=GL.Exchange.GLOBEX,
                    contractType=GL.ContractType.Future,
                    orderType=GL.OrderType.LMT,
                    action=GL.OrderAction.SELL,
                    quantity=1,
                    price=6.0,
                    tif=GL.TIF.GTC))
            wait_order_status(GL.OrderStatus.FILLED)

            gw.place_order(order_id=get_order_reference(),
                order=GL.Order(
                    symbol=sym3,
                    exchange=GL.Exchange.GLOBEX,
                    contractType=GL.ContractType.Future,
                    orderType=GL.OrderType.STP,
                    action=GL.OrderAction.SELL,
                    quantity=1,
                    trigger=136,
                    tif=GL.TIF.GTC))
            wait_order_status(GL.OrderStatus.SUBMITTED)

            gw.place_order(order_id=get_order_reference(),
                order=GL.Order(
                    symbol=sym3,
                    exchange=GL.Exchange.GLOBEX,
                    contractType=GL.ContractType.Future,
                    orderType=GL.OrderType.STP,
                    action=GL.OrderAction.BUY,
                    quantity=1,
                    trigger=137,
                    tif=GL.TIF.GTC))
            wait_order_status(GL.OrderStatus.SUBMITTED)

            time.sleep(10)
            gw.unsubscribe_update(subscription_id)

            time.sleep(10)
            gw.subscribe_update()

            time.sleep(10)
            gw.disconnect()


class ModifyOrder(TestTemplate):
    def test_modify_order(self):
        with open_session(url, account_id, user_name, password,
            client_app_id, client_version, name=gateway_label) as gw:
            gw.events.on_error(lambda _, evt: logger.error(evt))
            conn = []
            gw.events.on_connection_update(lambda _, evt: conn.append(evt.status))
            orders = []
            gw.events.on_order_update(lambda _, evt: orders.append(evt))
            def wait_order_status(status, timeout=20):
                orders.clear()
                wait_true(orders,
                    until=lambda x: x and x[-1].status == status,
                    timeout=timeout)

            gw.connect()
            wait_true(conn,
                until=lambda x: x and x[-1] == GL.ConnectionStatus.CONNECTED,
                timeout=20)

            sym = 'F.US.ZUI'
            gw.request_contract_details(symbol=sym)

            time.sleep(10)
            order_ref = get_order_reference()
            gw.place_order(order_id=order_ref,
                order=GL.Order(
                    symbol=sym,
                    exchange=GL.Exchange.GLOBEX,
                    contractType=GL.ContractType.Future,
                    orderType=GL.OrderType.STP_LMT,
                    action=GL.OrderAction.SELL,
                    quantity=1,
                    price=136,
                    trigger=136,
                    tif=GL.TIF.GTC))
            wait_order_status(GL.OrderStatus.SUBMITTED)

            logger.info("Modify order quantity to 10")
            gw.modify_order(order_ref,
                order=GL.Order(
                    symbol=sym,
                    exchange=GL.Exchange.GLOBEX,
                    contractType=GL.ContractType.Future,
                    orderType=GL.OrderType.STP_LMT,
                    action=GL.OrderAction.SELL,
                    quantity=10,
                    price=136,
                    trigger=136,
                    tif=GL.TIF.GTC))
            wait_order_status(GL.OrderStatus.SUBMITTED)

            logger.info("Modify order limit price to 135")
            gw.modify_order(order_ref,
                order=GL.Order(
                    symbol=sym,
                    exchange=GL.Exchange.GLOBEX,
                    contractType=GL.ContractType.Future,
                    orderType=GL.OrderType.STP_LMT,
                    action=GL.OrderAction.SELL,
                    quantity=10,
                    price=135,
                    trigger=136,
                    tif=GL.TIF.GTC))
            wait_order_status(GL.OrderStatus.SUBMITTED)

            logger.info("Modify order stop price to 135.5")
            gw.modify_order(order_ref,
                order=GL.Order(
                    symbol=sym,
                    exchange=GL.Exchange.GLOBEX,
                    contractType=GL.ContractType.Future,
                    orderType=GL.OrderType.STP_LMT,
                    action=GL.OrderAction.SELL,
                    quantity=10,
                    price=135,
                    trigger=135.5,
                    tif=GL.TIF.GTC))
            wait_order_status(GL.OrderStatus.SUBMITTED)

            time.sleep(10)
            gw.disconnect()


class CancelOrder(TestTemplate):
    def test_cancel_order(self):
        with open_session(url, account_id, user_name, password,
            client_app_id, client_version, name=gateway_label) as gw:
            conn = []
            gw.events.on_connection_update(lambda src, event: conn.append(event.status))
            orders = []
            gw.events.on_order_update(lambda src, event: orders.append(event))
            def wait_order_status(status, timeout=20):
                orders.clear()
                wait_true(orders,
                    until=lambda x: x and x[-1].status == status,
                    timeout=timeout)

            gw.connect()
            wait_true(conn,
                until=lambda x: x and x[-1] == GL.ConnectionStatus.CONNECTED,
                timeout=20)

            sym = 'F.US.ZUC'
            gw.request_contract_details(symbol=sym)

            time.sleep(10)
            order_ref = get_order_reference()
            gw.place_order(order_id=order_ref,
                order=GL.Order(
                    symbol=sym,
                    exchange=GL.Exchange.GLOBEX,
                    contractType=GL.ContractType.Future,
                    orderType=GL.OrderType.LMT,
                    action=GL.OrderAction.BUY,
                    quantity=1,
                    price=6.4,
                    tif=GL.TIF.DAY))
            wait_order_status(GL.OrderStatus.SUBMITTED)

            gw.cancel_order(order_ref)
            wait_order_status(GL.OrderStatus.CANCELLED)

            time.sleep(10)
            gw.disconnect()


class SuccessfulPositionStatusRequest(TestTemplate):
    def test_position_update(self):
        with open_session(url, account_id, user_name, password,
            client_app_id, client_version, name=gateway_label) as gw:
            conn = []
            gw.events.on_connection_update(lambda src, event: conn.append(event.status))
            orders = []
            gw.events.on_order_update(lambda src, event: orders.append(event))
            gw.events.on_position_update(lambda _, evt: logger.info(evt))
            gw.events.on_error(lambda _, evt: logger.error(evt))
            def wait_order_status(status, timeout=20):
                wait_true(orders,
                    until=lambda x: x and x[-1].status == status,
                    timeout=timeout)
                orders.clear()

            gw.connect()
            wait_true(conn,
                until=lambda x: x and x[-1] == GL.ConnectionStatus.CONNECTED,
                timeout=20)

            sym = 'F.US.ZUC'
            gw.request_contract_details(symbol=sym)

            time.sleep(10)
            gw.place_order(order_id=get_order_reference(),
                order=GL.Order(
                    symbol=sym,
                    exchange=GL.Exchange.GLOBEX,
                    contractType=GL.ContractType.Future,
                    orderType=GL.OrderType.MKT,
                    action=GL.OrderAction.BUY,
                    quantity=1,
                    price=None,
                    tif=GL.TIF.DAY))
            wait_order_status(GL.OrderStatus.FILLED)

            time.sleep(10)
            gw.disconnect()


class SuccessfulCollateralStatusRequest(SuccessfulPositionStatusRequest):
    pass

if __name__ == '__main__':
    unittest.main()
    # set_debug_mode()
