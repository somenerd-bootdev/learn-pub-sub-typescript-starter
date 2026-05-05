import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { SimpleQueueType } from "../internal/pubsub/consume.js";
import { subscribeMsgPack } from "../internal/pubsub/subscribe.js";
import { writeLog, type GameLog } from "../internal/gamelogic/logs.js";
import { AckType } from "../internal/pubsub/acktype.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Peril game server connected to RabbitMQ!");
  printServerHelp();

  subscribeMsgPack(
    conn,
    ExchangePerilTopic,
    GameLogSlug,
    GameLogSlug + ".*",
    SimpleQueueType.Durable,
    async (gameLog: GameLog) => {
      await writeLog(gameLog);
      process.stdout.write("> ");
      return AckType.Ack;
    }
  );

  for (; ;) {
    const input = await getInput("Action: ");
    if (input.length > 0) {
      if (input[0] == "pause") {
        const publishChPau = await conn.createConfirmChannel();
        try {
          await publishJSON(publishChPau, ExchangePerilDirect, PauseKey, {
            isPaused: true,
          });
        } catch (err) {
          console.error("Error publishing message:", err);
        }
      }
      else if (input[0] == "resume") {
        const publishChRes = await conn.createConfirmChannel();
        try {
          await publishJSON(publishChRes, ExchangePerilDirect, PauseKey, {
            isPaused: false,
          });
        } catch (err) {
          console.error("Error publishing message:", err);
        }
      }
      else if (input[0] == "quit") {
        console.log("Exiting...");
        break;
      }
      else {
        console.log(`Did not understand action ${input[0]}`);
      }
    }
  }

  ["SIGINT", "SIGTERM"].forEach((signal) =>
    process.on(signal, async () => {
      try {
        await conn.close();
        console.log("RabbitMQ connection closed.");
      } catch (err) {
        console.error("Error closing RabbitMQ connection:", err);
      } finally {
        process.exit(0);
      }
    }),
  );

}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
