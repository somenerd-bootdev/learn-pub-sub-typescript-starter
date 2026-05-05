import amqp, { type ConsumeMessage } from "amqplib";
import { declareAndBind, type SimpleQueueType } from "./consume.js";

export async function subscribeJSON<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => void,
): Promise<void> {
    const ch = await declareAndBind(conn, exchange, queueName, key, queueType);
    const consumed = await ch[0].consume(ch[1].queue, onMessage); // No nullchecks; we die like men

    function onMessage(msg: ConsumeMessage | null): void {
        if (msg == null) {
            return;
        }
        const parsedContent = JSON.parse(msg.content.toString());
        handler(parsedContent);
        ch[0].ack(msg);
    }
}