import type { ConfirmChannel } from "amqplib";

export async function publishJSON<T>(
    ch: ConfirmChannel,
    exchange: string,
    routingKey: string,
    value: T,
): Promise<void> {
    const serializedValue = Buffer.from(JSON.stringify(value));
    ch.publish(exchange, routingKey, serializedValue, {
        contentType: "application/json"
    });
}