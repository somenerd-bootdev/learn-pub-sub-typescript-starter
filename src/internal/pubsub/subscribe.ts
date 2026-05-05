import amqp, { type ConsumeMessage } from "amqplib";
import { declareAndBind, type SimpleQueueType } from "./consume.js";
import { AckType } from "./acktype.js";

export async function subscribeJSON<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => AckType,
): Promise<void> {
    const ch = await declareAndBind(conn, exchange, queueName, key, queueType);
    const consumed = await ch[0].consume(ch[1].queue, onMessage); // No nullchecks; we die like men

    function onMessage(msg: ConsumeMessage | null): void {
        if (msg == null) {
            return;
        }
        const parsedContent = JSON.parse(msg.content.toString());
        const ackType = handler(parsedContent);
        if (ackType == AckType.Ack) {
            console.log("Acking");
            ch[0].ack(msg);
        }
        else if (ackType == AckType.NackRequeue) {
            console.log("Nack requeueing");
            ch[0].nack(msg, false, true);
        }
        else {
            console.log("Nack discarding ")
            ch[0].nack(msg, false, false);
        }
    }
}