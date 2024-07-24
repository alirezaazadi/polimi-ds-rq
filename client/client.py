# client/client.py
import asyncio


class Client:
    def __init__(self, client_id: str, load_balancer_address: tuple[str, int]):
        self.client_id: str = client_id
        self.load_balancer_address: tuple[str, int] = load_balancer_address
        self.reader, self.writer = None, None

    async def setup_connection(self):
        self.reader, self.writer = await asyncio.open_connection(*self.load_balancer_address)

    async def send_request(self, request):
        pass


async def main():
    pass


if __name__ == '__main__':
    asyncio.run(main())
