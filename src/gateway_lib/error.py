from functools import wraps

from .message import (ErrorType, ErrorMessage, OrderError)

class ErrorCode:
    _gw_base = 88000
    GTW_SKIPPED_API_CALL = _gw_base + 1
    GTW_DUPLICATED_ORDER_REF = _gw_base + 2
    GTW_ORDER_REF_NOT_FOUND = _gw_base + 3
    GTW_SYMBOL_RESOLVE_ERROR = _gw_base + 4

## Common error handling

def ensure_api_ready(
    attr_ready: str = 'is_healthy',
    attr_events: str = 'events',
    error_type=ErrorType.GENERAL):

    def wrapper(api_call):
        @wraps(api_call)
        def args(*ar, **kw):
            gateway = ar[0]
            ready = getattr(gateway, attr_ready)
            if not ready:
                events = getattr(gateway, attr_events)
                msg = ErrorMessage(
                    msg=f"Skipped API call '{api_call.__name__}'. Connection with broker is either unhealthy or gateway is not ready.",
                    code=ErrorCode.GTW_SKIPPED_API_CALL,
                    error_type=error_type)
                events.raise_error_event(msg)
            else:
                return api_call(*ar, **kw)
        return args
    return wrapper


def raise_undefined_order_reference(
    check_exists,
    attr_events: str = 'events'):
    def wrapper(api_call):
        @wraps(api_call)
        def args(*ar, **kw):
            gateway = ar[0]
            order_id, exists = check_exists(*ar, **kw)
            if not exists:
                events = getattr(gateway, attr_events)
                msg = OrderError(
                    order_id=order_id,
                    msg=f"Order reference not found. Order reference '{order_id}' is not recognized by gateway.",
                    code=ErrorCode.GTW_ORDER_REF_NOT_FOUND)
                events.raise_error_event(msg)
            else:
                return api_call(*ar, **kw)
        return args
    return wrapper


def raise_duplicated_order_reference(
    check_exists,
    attr_events: str = 'events'):
    def wrapper(api_call):
        @wraps(api_call)
        def args(*ar, **kw):
            gateway = ar[0]
            order_id, exists = check_exists(*ar, **kw)
            if exists:
                events = getattr(gateway, attr_events)
                msg = OrderError(
                    order_id=order_id,
                    msg=f"Duplicated order reference. Order reference '{order_id}' is already used.",
                    code=ErrorCode.GTW_DUPLICATED_ORDER_REF)
                events.raise_error_event(msg)
            else:
                return api_call(*ar, **kw)
        return args
    return wrapper