import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";

async function main() {
  console.log("Starting Peril server...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Successfully connected to RabbitMQ");
  const confirmChannel = await conn.createConfirmChannel();
  const playingState = { isPaused: true };
  publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, playingState);
  process.on("SIGINT", () => {
    console.log("Peril is shutting down...")
    conn.close();
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
