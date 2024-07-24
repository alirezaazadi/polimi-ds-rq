from broker import Broker
import argparse
import asyncio

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('address', type=str, help='Address to bind the broker to')
    args = parser.parse_args()

    broker = Broker(args.address)
    await broker.start()


if __name__ == '__main__':
    asyncio.run(main())
