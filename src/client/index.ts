import amqp from "amqplib";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ArmyMovesPrefix, ExchangePerilDirect, ExchangePerilTopic, PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove, handleMove } from "../internal/gamelogic/move.js";
import { subscribeJSON } from "../internal/pubsub/subscribe.js";
import { handlerMove, handlerPause } from "./handlers.js";
import { publishJSON } from "../internal/pubsub/publish.js";

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

  const gameState = new GameState(username);
  const pauseHandler = handlerPause(gameState);
  const moveHandler = handlerMove(gameState);

  await subscribeJSON(conn, ExchangePerilDirect, `${PauseKey}.${username}`, PauseKey, SimpleQueueType.Transient, pauseHandler);
  await subscribeJSON(conn, ExchangePerilTopic, `${ArmyMovesPrefix}.${username}`, `${ArmyMovesPrefix}.*`, SimpleQueueType.Transient, moveHandler);

  const ch = await conn.createConfirmChannel();
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
