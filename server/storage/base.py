import dataclasses
from dataclasses import dataclass
from dataclasses import asdict


@dataclass(frozen=True)
class StorageConfig:
    username: str | None = None
    password: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    connection_string: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    def __hash__(self):
        return hash(tuple(sorted(self.to_dict().items())))


class PerConfigSingleton(type):
    _instances = dict()

    def __call__(cls, *args, **kwargs):
        config: StorageConfig = args[0] if args else kwargs.get('config')
        key = hash(config)

        if instance := cls._instances.get(key):
            return instance

        instance = super(PerConfigSingleton, cls).__call__(*args, **kwargs)
        cls._instances[key] = instance
        return instance


class BaseStorage(metaclass=PerConfigSingleton):
    def __init__(self, config: StorageConfig):
        self._config: StorageConfig = config
        self._connection = None

    @property
    def username(self) -> str:
        return self._config.username

    @property
    def password(self) -> str:
        return self._config.password

    @property
    def host(self) -> str:
        return self._config.host

    @property
    def port(self) -> int:
        return self._config.port

    @property
    def database(self) -> str:
        return self._config.database

    @property
    def connection_string(self) -> str:
        return self._config.connection_string

    @property
    def connection(self):
        if self._connection is None:
            self.connect()

        return self._connection

    def connect(self) -> bool:
        raise NotImplementedError

    def disconnect(self) -> bool:
        raise NotImplementedError

    def execute(self, query: str) -> [bool, list]:
        raise NotImplementedError

    def fetch(self, query: str) -> [bool, list]:
        raise NotImplementedError

    def insert(self, query: str) -> bool:
        raise NotImplementedError

    def update(self, query: str) -> bool:
        raise NotImplementedError
