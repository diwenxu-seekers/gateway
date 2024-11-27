import json
import pathlib
import datetime

def get_settings():
    obj = {}
    current_file = pathlib.Path(__file__)
    res_file = current_file.parent / 'res.json'
    with open(res_file) as fd:
        obj = json.load(fd)
    return obj

def get_front_month(symbol_root: str):
    trade_months = dict(NQ=[3,6,9,12])
    future_map = {i+1: c for i, c in enumerate("FGHJKMNQUVXZ")}
    today = datetime.datetime.now()
    month = today.month
    year = today.year
    for i, v in enumerate(trade_months.get(symbol_root, [])):
        if today.month < v:
            month = trade_months[symbol_root][i]
            break
    month_code = future_map[month]
    year_code = year % 10
    return f'{symbol_root}{month_code}{year_code}'