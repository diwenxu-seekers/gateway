from collections import OrderedDict

from .gateway import AbstractGateway
from .retry_gateway import RetryGateway
from .ib import IBGateway
from .cqg import CQGGateway
from .config import GatewayConfig as cfg

class GatewayFactory:
    @staticmethod
    def create(config: OrderedDict) -> AbstractGateway:
        try:
            if config[cfg.TYPE] == cfg.GATEWAY_IB:
                gw = IBGateway(
                    name=config[cfg.NAME],
                    host=config[cfg.HOST],
                    port=config[cfg.PORT],
                    client_id=config[cfg.CLIENT_ID],
                    state_filepath=config.get(cfg.STATE_FILE_PATH, None))
            elif config[cfg.TYPE] == cfg.GATEWAY_CQG:
                gw = CQGGateway(
                    name=config[cfg.NAME],
                    url=config[cfg.HOST],
                    account_id=config[cfg.ACCOUNT_ID],
                    username=config[cfg.USER],
                    password=config[cfg.PASSWORD],
                    client_app_id=config[cfg.APP_ID],
                    client_version=config[cfg.VERSION],
                    private_label=config.get(cfg.PRIVATE_LABEL, None),
                    state_filepath=config.get(cfg.STATE_FILE_PATH, None))
            else:
                raise Exception("Unrecognized gateway type.")
            reconnect_retry = config.get(cfg.RECONNECT_MAX_RETRY, 0)
            if reconnect_retry != 0:
                rgw = RetryGateway(gw)
                rgw.configure(
                    max_retry=reconnect_retry,
                    retry_interval=config[cfg.RECONNECT_INTERVAL])
                return rgw
            return gw
        except KeyError as e:
            raise Exception(f"Invalid gateway config key: {e}")