import asyncio
from contextlib import aclosing

import pytest
from aio_pika import Channel, Message

from taskiq_aio_pika import AioPikaBroker, Queue, QueueType
from taskiq_aio_pika.exchange import Exchange
from tests.conftest import _cleanup_amqp_resources


@pytest.mark.parametrize("queue_count", [1, 2])
@pytest.mark.parametrize("stop", ["close", "throw", "cancel"])
async def test_listener_waits_for_consumer_cleanup(
    amqp_url: str,
    test_channel: Channel,
    queue_name: str,
    dead_queue_name: str,
    exchange_name: str,
    queue_count: int,
    stop: str,
) -> None:
    queue_names = [f"{queue_name}_{index}" for index in range(queue_count)]
    broker = AioPikaBroker(
        url=amqp_url,
        exchange=Exchange(name=exchange_name),
        dead_letter_queue=Queue(name=dead_queue_name, type=QueueType.CLASSIC),
        task_queues=[Queue(name=name, type=QueueType.CLASSIC) for name in queue_names],
    )
    broker.is_worker_process = True
    try:
        await broker.startup()
        queues = [await test_channel.get_queue(name) for name in queue_names]
        async with aclosing(broker.listen()) as listener:
            for queue in queues:
                await test_channel.default_exchange.publish(
                    Message(queue.name.encode()),
                    routing_key=queue.name,
                )
            received = []
            for _ in queues:
                message = await asyncio.wait_for(anext(listener), timeout=3)
                received.append(message.data)
                await message.ack()
            assert sorted(received) == sorted(name.encode() for name in queue_names)
            for queue in queues:
                assert (await queue.declare()).consumer_count == 1

            if stop == "close":
                await listener.aclose()
            elif stop == "throw":
                with pytest.raises(ValueError, match="consumer stopped"):
                    await listener.athrow(ValueError("consumer stopped"))
            else:
                pending = asyncio.create_task(anext(listener))
                await asyncio.sleep(0)
                pending.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await pending

            # A real AMQP declare checks server state as soon as close returns.
            for queue in queues:
                assert (await queue.declare()).consumer_count == 0

        # Closing one listener must leave the shared channel usable.
        async with aclosing(broker.listen()) as listener:
            for queue in queues:
                await test_channel.default_exchange.publish(
                    Message(b"next"),
                    routing_key=queue.name,
                )
            for _ in queues:
                message = await asyncio.wait_for(anext(listener), timeout=3)
                assert message.data == b"next"
                await message.ack()
        for queue in queues:
            assert (await queue.declare()).consumer_count == 0
    finally:
        await broker.shutdown()
        await _cleanup_amqp_resources(
            amqp_url,
            [exchange_name],
            [*queue_names, dead_queue_name],
        )
