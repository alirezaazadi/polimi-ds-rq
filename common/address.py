from dataclasses import dataclass
import ipaddress


@dataclass
class Address:
    host: ipaddress.IPv4Address | ipaddress.IPv6Address
    port: int

    @property
    def host_str(self):
        return str(self.host)

    @property
    def tuple(self):
        return self.host_str, self.port

    @property
    def connection_str(self):
        return f'{self.host_str}:{self.port}'

    def __str__(self):
        return f'{self.host}:{self.port}'

    def __repr__(self):
        return f'{self.host}:{self.port}'

    def __eq__(self, other):
        return self.host == other.host and self.port == other.port

    def __ge__(self, other):
        return self.host >= other.host and self.port >= other.port

    def __hash__(self):
        return hash((self.host, self.port))


class AddressFactory:
    @staticmethod
    def from_str(addr: str) -> Address:
        host, port = addr.split(':')

        if host == 'localhost':
            host = '127.0.0.1'

        return Address(ipaddress.ip_address(host), int(port))

    @staticmethod
    def from_tuple(*addr) -> Address:
        host = addr[0]

        if host == 'localhost':
            host = '127.0.0.1'

        return Address(ipaddress.ip_address(host), int(addr[1]))


address_factory = AddressFactory()
