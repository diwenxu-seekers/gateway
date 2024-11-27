import pytest
from datetime import datetime
import random

import gateway_lib as GL
from gateway_lib import (Exchange, ContractType, OrderType, OrderAction, TIF)

def create_order_params():
    symbols = ['NQ', 'ES', 'CL', 'GC']
    exchanges = [Exchange.COMEX, Exchange.HKFE, Exchange.NYMEX, Exchange.CME]
    contractTypes = [ContractType.Future]
    orderTypes = [OrderType.STP_LMT, OrderType.STP, OrderType.LMT, OrderType.MKT]
    actions = [OrderAction.BUY, OrderAction.SELL]
    quantities = [1, 5, 11, 50.0, 7.5, None]
    prices = [10.1, 15944.75, 1802.8, 66.44, 4578.75, 22.400, None]
    tifs = [TIF.DAY, TIF.GTC, TIF.GTD]
    for sym in symbols:
        for exch in exchanges:
            for ctype in contractTypes:
                for otype in orderTypes:
                    for act in actions:
                        for qty in quantities:
                            for lmt in prices:
                                for stp in prices:
                                    for tif in tifs:
                                        till = ""
                                        if tif == TIF.GTD:
                                            till = datetime.now().strftime("%Y%m%d %H:%M:%S")
                                            if qty and qty % 5 == 0:
                                                till = till + " ABC"
                                        yield dict(symbol=sym, exchange=exch, contractType=ctype,
                                            orderType=otype, action=act, quantity=qty,
                                            limit_price=lmt, stop_price=stp, tif=tif, goodTillDate=till)
all_cases = list(create_order_params())

normal_cases = [x for x in all_cases if (
    x['quantity'] is not None and (
        (x['orderType'] == OrderType.STP_LMT and x['limit_price'] is not None and x['stop_price'] is not None)
        or (x['orderType'] == OrderType.STP and x['stop_price'] is not None)
        or (x['orderType'] == OrderType.LMT and x['limit_price'] is not None)
    )
)]

normal_stp_or_lmt = [x for x in normal_cases if x['orderType'] != OrderType.MKT]


@pytest.mark.parametrize('args', all_cases)
def test_strcmp_normal_cases(args):
    o = GL.Order(**args)
    expect = str(o)
    assert o.strcmp(expect)


@pytest.mark.parametrize('args', normal_stp_or_lmt)
def test_strcmp_price_offset(args):
    o = GL.Order(**args)
    cmp = str(o)
    noise = 1e-8
    if o.orderType in [OrderType.STP, OrderType.STP_LMT]:
        o.stop_price = o.stop_price + random.choice([1, -1]) * noise
    elif o.orderType in [OrderType.LMT, OrderType.STP_LMT]:
        o.limit_price = o.limit_price + random.choice([1, -1]) * noise
    assert o.strcmp(cmp) == True

""""
# @pytest.mark.parametrize('args', [x for x in create_normal_order_params() if x['orderType'] != OrderType.MKT])
# def test_strcmp_quantity_offset(args):
#     o = GL.Order(**args)
#     cmp = str(o)
#     noise = 1e-9
#     o.quantity = o.quantity + random.choice([1, -1]) * noise
#     assert o.strcmp(cmp) == False
"""

@pytest.mark.parametrize('label,args,cmp,expect', [
    (
        "different order type",
        dict(symbol='CL', exchange=Exchange.GLOBEX, contractType=ContractType.Future,
            orderType=OrderType.STP_LMT, action=OrderAction.BUY, quantity=25,
            limit_price=123.23, stop_price=233.23, tif=TIF.DAY),
        'LMT BUY 1.0 CL @10.23000001 DAY',
        False
    ),
    (
        "different action",
        dict(symbol='CL', exchange=Exchange.GLOBEX, contractType=ContractType.Future,
            orderType=OrderType.LMT, action=OrderAction.BUY, quantity=1,
            limit_price=10.23, stop_price=233.23, tif=TIF.DAY),
        'LMT SELL 1 CL @10.23000001 DAY',
        False
    ),
    (
        "different quantity",
        dict(symbol='CL', exchange=Exchange.GLOBEX, contractType=ContractType.Future,
            orderType=OrderType.STP_LMT, action=OrderAction.BUY, quantity=25,
            limit_price=123.23, stop_price=233.23, tif=TIF.DAY),
        'MKT BUY 3.5000 CL  DAY',
        False
    ),
    (
        "different TIF",
        dict(symbol='CL', exchange=Exchange.GLOBEX, contractType=ContractType.Future,
            orderType=OrderType.LMT, action=OrderAction.BUY, quantity=1,
            limit_price=10.23, stop_price=233.23, tif=TIF.DAY),
        'LMT BUY 1.0 CL @10.23000001 GTC',
        False
    ),
    (
        "different symbol",
        dict(symbol='CL', exchange=Exchange.GLOBEX, contractType=ContractType.Future,
            orderType=OrderType.LMT, action=OrderAction.BUY, quantity=1,
            limit_price=10.23, stop_price=233.23, tif=TIF.DAY),
        'LMT BUY 1.0 NQ @10.23000001 DAY',
        False
    ),
    (
        "invalid quantity",
        dict(symbol='CL', exchange=Exchange.GLOBEX, contractType=ContractType.Future,
            orderType=OrderType.LMT, action=OrderAction.BUY, quantity=1,
            limit_price=10.23, stop_price=233.23, tif=TIF.DAY),
        'LMT BUY abc CL @10.23000001 DAY',
        False
    ),
    (
        "order string is empty",
        dict(symbol='CL', exchange=Exchange.GLOBEX, contractType=ContractType.Future,
            orderType=OrderType.LMT, action=OrderAction.BUY, quantity=1,
            limit_price=10.23, stop_price=233.23, tif=TIF.DAY),
        '',
        False
    ),
    (
        "order string is None",
        dict(symbol='CL', exchange=Exchange.GLOBEX, contractType=ContractType.Future,
            orderType=OrderType.LMT, action=OrderAction.BUY, quantity=1,
            limit_price=10.23, stop_price=233.23, tif=TIF.DAY),
        None,
        False
    ),
    (
        "order string is None",
        dict(symbol='CL', exchange=Exchange.GLOBEX, contractType=ContractType.Future,
            orderType=OrderType.LMT, action=OrderAction.BUY, quantity=1,
            limit_price=10.23, stop_price=233.23, tif=TIF.DAY),
        None,
        False
    ),
    (
        "symbol contains '@' pattern",
        dict(symbol='@CL', exchange=Exchange.GLOBEX, contractType=ContractType.Future,
            orderType=OrderType.LMT, action=OrderAction.BUY, quantity=1,
            limit_price=10.23, stop_price=233.23, tif=TIF.DAY),
        "LMT BUY 1.0 @CL @10.23 DAY",
        True
    ),
    (
        "symbol contains number",
        dict(symbol='CLZ1', exchange=Exchange.GLOBEX, contractType=ContractType.Future,
            orderType=OrderType.LMT, action=OrderAction.BUY, quantity=1,
            limit_price=10.23, stop_price=233.23, tif=TIF.DAY),
        "LMT BUY 1 CLZ1 @10.23 DAY",
        True
    ),
    (
        "GTD",
        dict(symbol='CLZ1', exchange=Exchange.GLOBEX, contractType=ContractType.Future,
            orderType=OrderType.LMT, action=OrderAction.BUY, quantity=1,
            limit_price=10.23, stop_price=233.23, tif=TIF.GTD, goodTillDate="20220101 00:00:00 ABC"),
        "LMT BUY 1 CLZ1 @10.23 GTD 20220101 00:00:00 ABC",
        True
    ),
])
def test_strcmp_corner_cases(label, args, cmp, expect):
    o = GL.Order(**args)
    assert o.strcmp(cmp) == expect, f'case: {label}'

