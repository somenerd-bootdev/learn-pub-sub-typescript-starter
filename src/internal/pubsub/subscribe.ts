import amqp, { type ConsumeMessage } from "amqplib";
import { declareAndBind, type SimpleQueueType } from "./consume.js";
import { AckType } from "./acktype.js";
import { decode } from "@msgpack/msgpack";

export async function subscribeJSON<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
    return await subscribe<T>(conn, exchange, queueName, key, queueType, handler, (data) => JSON.parse(data.toString()));
}

export async function subscribeMsgPack<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
    return await subscribe<T>(conn, exchange, queueName, key, queueType, handler, (data) => decode(data) as T);
}

export async function subscribe<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    routingKey: string,
    simpleQueueType: SimpleQueueType,
    handler: (data: T) => Promise<AckType> | AckType,
    deserializer: (data: Buffer) => T,
): Promise<void> {
    const [ch, queue] = await declareAndBind(conn, exchange, queueName, routingKey, simpleQueueType);
    console.log(exchange, queueName, routingKey);
    await ch.prefetch(10);
    await ch.consume(queue.queue, onMessage, { noAck: false }); // No nullchecks; we die like men

    async function onMessage(msg: ConsumeMessage | null): Promise<void> {
        if (msg == null) {
            return;
        }
        const parsedContent = deserializer(msg.content);
        const ackType = await handler(parsedContent);
        if (ackType == AckType.Ack) {
            console.log("Acking");
            ch.ack(msg);
        }
        else if (ackType == AckType.NackRequeue) {
            console.log("Nack requeueing");
            ch.nack(msg, false, true);
        }
        else {
            console.log("Nack discarding ")
            ch.nack(msg, false, false);
        }
    }
}