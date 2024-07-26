DEFAULTS = {
    'BROKER_ADDRESSES': [
        'localhost:9091',
        'localhost:9092',
        'localhost:9093',
        'localhost:9094',
        'localhost:9095',
        'localhost:9096',
    ],

    'LOAD_BALANCER_ADDRESS': 'localhost:9090',

    'MAX_MESSAGE_SIZE': 4096

}


class Settings:

    def __getattr__(self, name):
        if name not in DEFAULTS:
            msg = "'%s' object has no attribute '%s'"
            raise AttributeError(msg % (self.__class__.__name__, name))

        return self.get_setting(name)

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
