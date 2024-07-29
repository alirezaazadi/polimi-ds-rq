import asyncio
import logging
import uuid
from tenacity import retry, wait_fixed, wait_random, retry_if_exception_type

from RDQueue.common.address import Address, address_factory
from RDQueue.common.config import settings
from RDQueue.common.decorator import periodic_task
from RDQueue.common.exceptions import NoBrokerAvailable
from RDQueue.common.message import message_factory
from RDQueue.common.networking import send_message_to_writer, receive_message

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


class DQueue:
    def __init__(self, connection_addr: Address, name: str):
        self._connection_addr: Address = connection_addr
        self._name: str = name
        self.id = str(uuid.uuid4().hex)

        self._remote_queue_id: str | None = None
        self._remote_queue_name: str | None = None
        self._broker_addr: Address | None = None
        self._broker_id: str | None = None
        self._broker_writer: asyncio.StreamWriter | None = None
        self._broker_reader: asyncio.StreamReader | None = None

    async def async_init(self):
        await self.get_broker_information()
        await self.init_broker_writer_reader()
        await self.create_queue()

    @retry(wait=wait_fixed(5) + wait_random(0, 2), retry=retry_if_exception_type(NoBrokerAvailable))
    async def init_broker_writer_reader(self):

        if self.broker_addr is None:
            await self.get_broker_information()

        if self._broker_writer is None or self._broker_reader is None:
            reader, writer = await asyncio.open_connection(
                self.broker_addr.host_str,
                self.broker_addr.port
            )

            self._broker_reader = reader
            self._broker_writer = writer

    @periodic_task(interval=5)
    async def check_broker_connection(self):
        if self.broker_addr is None:
            await self.get_broker_information()

        else:
            try:
                # Enforce a timeout for the connection attempt
                await asyncio.wait_for(
                    asyncio.open_connection(*self.broker_addr.tuple),
                    timeout=1  # Timeout duration in seconds
                )
            except asyncio.TimeoutError:
                self._broker_addr = None
                logger.error(f'Broker {self.broker_addr} is not available (connection timed out).')
                return
            except Exception as e:
                self._broker_addr = None
                logger.error(f'Broker {self.broker_addr} is not available: {e}.')
                return

    async def get_broker_information(self):

        if self.broker_addr is not None:
            return

        reader, writer = await asyncio.open_connection(
            settings.LOAD_BALANCER_ADDRESS.host_str,
            settings.LOAD_BALANCER_ADDRESS.port
        )

        logger.info(
            f'{self.name}({self.connection_addr}) is getting broker information from load balancer: {settings.LOAD_BALANCER_ADDRESS}')

        await send_message_to_writer(writer, message_factory.broker_info_req(
            sender_addr=self.connection_addr.connection_str,
            receiver_addr=settings.LOAD_BALANCER_ADDRESS.connection_str
        ))

        logger.info(
            f'{self.name}({self.connection_addr}) sent broker information request to load balancer: {settings.LOAD_BALANCER_ADDRESS}')
        response = await receive_message(reader)
        logger.info(f'{self.name} received broker information from load balancer: {response.body}')
        self._broker_id = response.body['id']
        self._broker_addr = address_factory.from_str(response.body['address'])

        logger.info(f'{self.name} is connected to broker: {self.broker_addr}')

    @retry(wait=wait_fixed(5) + wait_random(0, 2), retry=retry_if_exception_type((NoBrokerAvailable, AttributeError)))
    async def create_queue(self):

        if self.broker_addr is None:
            await self.get_broker_information()

        reader, writer = await asyncio.open_connection(
            self.broker_addr.host_str,
            self.broker_addr.port
        )

        logger.info(f'Creating queue: {self.name}')

        try:
            await asyncio.wait_for(
                send_message_to_writer(
                    writer=writer,
                    message=message_factory.queue_create_req(
                        sender_addr=self.connection_addr.connection_str,
                        receiver_addr=self.broker_addr.connection_str,
                        sender_id=self.id,
                        body=self.name
                    )
                ), timeout=3)
        except asyncio.TimeoutError:
            logger.error(f'Broker {self.broker_addr} is not available (connection timed out).')
            raise NoBrokerAvailable()

        logger.info(f'Queue creation request sent to broker: {self.broker_addr}')

        try:
            message = await asyncio.wait_for(
                receive_message(reader=reader),
                timeout=3
            )
        except asyncio.TimeoutError:
            logger.error(f'Broker {self.broker_addr} is not available to receive message (connection timed out).')
            raise NoBrokerAvailable()

        logger.info(f'Queue created: {message.body}')

        self._remote_queue_id = message.body['id']
        self._remote_queue_name = message.body['name']

        writer.close()
        await writer.wait_closed()

    @property
    def connection_addr(self):
        return self._connection_addr

    @property
    def name(self):
        return self._name

    @property
    def remote_queue_id(self):
        return self._remote_queue_id

    @property
    def broker_addr(self):
        return self._broker_addr

    @retry(wait=wait_fixed(5) + wait_random(0, 2), retry=retry_if_exception_type(NoBrokerAvailable))
    async def push(self, data):
        if self.broker_addr is None:
            raise NoBrokerAvailable()

        logger.info(f'Pushing data = {data} to queue: {self.name} to broker: {self.broker_addr}')

        reader, writer = await asyncio.open_connection(
            self.broker_addr.host_str,
            self.broker_addr.port
        )

        await send_message_to_writer(
            writer=writer,
            message=message_factory.queue_push_req(
                sender_addr=self.connection_addr.connection_str,
                receiver_addr=self.broker_addr.connection_str,
                sender_id=self.id,
                body={
                    'queue_name': self.name,
                    'message': data
                }
            )
        )

        message = await receive_message(reader)

        logger.info(
            f'Data = {data} pushed to queue: {self.name} to broker: {self.broker_addr} with status: {message.body}')

        writer.close()
        await writer.wait_closed()

    @retry(wait=wait_fixed(5) + wait_random(0, 2), retry=retry_if_exception_type(NoBrokerAvailable))
    async def pop(self):
        if self.broker_addr is None:
            raise NoBrokerAvailable()

        logger.info(f'Popping data from queue: {self.name} from broker: {self.broker_addr}')

        reader, writer = await asyncio.open_connection(
            self.broker_addr.host_str,
            self.broker_addr.port
        )

        await send_message_to_writer(
            writer=writer,
            message=message_factory.queue_pop_req(
                sender_addr=self.connection_addr.connection_str,
                receiver_addr=self.broker_addr.connection_str,
                sender_id=self.id,
                body=self.name
            )
        )

        message = await receive_message(reader)

        logger.info(f'Data = {message.body} popped from queue: {self.name} from broker: {self.broker_addr}')

        writer.close()
        await writer.wait_closed()

        return message.body
