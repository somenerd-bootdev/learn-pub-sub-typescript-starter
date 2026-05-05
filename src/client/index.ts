import amqp, { type ConfirmChannel, type Message } from "amqplib";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ArmyMovesPrefix, ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { subscribeJSON } from "../internal/pubsub/subscribe.js";
import { handlerMove, handlerPause, handlerWar } from "./handlers.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Peril game client connected to RabbitMQ!");

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

  const username = await clientWelcome();

  await declareAndBind(
    conn,
    ExchangePerilDirect,
    `${PauseKey}.${username}`,
    PauseKey,
    SimpleQueueType.Transient,
  );

  const ch = await conn.createConfirmChannel();

  const gameState = new GameState(username);
  const pauseHandler = handlerPause(gameState);
  const moveHandler = handlerMove(gameState, ch);
  const warHandler = handlerWar(gameState, ch);

  await subscribeJSON(conn, ExchangePerilDirect, `${PauseKey}.${username}`, PauseKey, SimpleQueueType.Transient, pauseHandler);
  await subscribeJSON(conn, ExchangePerilTopic, `${ArmyMovesPrefix}.${username}`, `${ArmyMovesPrefix}.*`, SimpleQueueType.Transient, moveHandler);
  await subscribeJSON(conn, ExchangePerilTopic, "war", "war.*", SimpleQueueType.Durable, warHandler);

  for (; ;) {
    const input = await getInput("Action: ");
    if (input.length > 0) {
      const action = input[0];
      try {
        if (action == "spawn") {
          commandSpawn(gameState, input);
        }
        else if (action == "move") {
          const move = commandMove(gameState, input);
          publishJSON(ch, ExchangePerilTopic, `${ArmyMovesPrefix}.*`, move);
        }
        else if (action == "status") {
          commandStatus(gameState);
        }
        else if (action == "help") {
          printClientHelp();
        }
        else if (action == "spam") {
          console.log("No spamming allowed yet!");
        }
        else if (action == "quit") {
          printQuit();
          break;
        }
        else {
          console.log("AN ERROR MESSAGE");
        }
      } catch (err) {
        if (err instanceof Error)
          console.error(err.message);
      }
    }
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});


export async function publishGameLog(ch: ConfirmChannel, username: string, message: string) {
  const gameLog = {
    username: username,
    message: message,
    currentTime: new Date(Date.now())
  };
  await publishMsgPack(ch, ExchangePerilTopic, `${GameLogSlug}.${username}`, gameLog);
}