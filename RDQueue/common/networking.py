from RDQueue.common.message import message_factory


async def receive_message(reader):
    message_data = await reader.readuntil(b'\n\r')
    return message_factory.from_bytes(message_data)


async def send_message_to_writer(writer, message):
    writer.write(message.to_bytes() + b'\n\r')
    await writer.drain()



