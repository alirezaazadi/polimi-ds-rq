from RDQueue.common import address

DEFAULTS = {
    'BROKER_ADDRESSES': [
        # '192.168.0.101:9091',
        '192.168.0.101:9092',
        # '192.168.0.101:9093',
        # '192.168.0.101:9094',
        # '192.168.0.101:9095',
        # '192.168.0.101:9096',
    ],

    'LOAD_BALANCER_ADDRESS': '192.168.0.101:9090',
    'MAX_MESSAGE_SIZE': 4096,

    'REPLICATION_ADDRESS': [
        '192.168.0.101:8081',
        '192.168.0.101:8082',
        '192.168.0.101:8083',
        # '192.168.0.101:8084',
        # '192.168.0.101:8085',
        # '192.168.0.101:8086',
    ]

}


class Settings:

    def __getattr__(self, name):
        if name not in DEFAULTS:
            msg = "'%s' object has no attribute '%s'"
            raise AttributeError(msg % (self.__class__.__name__, name))

        value = self.get_setting(name)

        if isinstance(value, list):
            return [address.address_factory.from_str(addr) for addr in value if address.Address.is_valid_address(addr)]

        if address.Address.is_valid_address(value):
            return address.address_factory.from_str(value)

    def get_setting(self, setting):
        return DEFAULTS[setting]

    def change_setting(self, setting, value, enter, **kwargs):
        # ensure a valid app setting is being overridden
        if setting not in DEFAULTS:
            return

        # if exiting, delete value to repopulate
        if enter:
            self._user_settings[setting] = value
        else:
            self._user_settings.pop(setting, None)


settings = Settings()
