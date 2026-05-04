import amqp from "amqplib";

async function main() {
  console.log("Starting Peril server...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Successfully connected to RabbitMQ");
  process.on("SIGINT", () => {
    console.log("Peril is shutting down...")
    conn.close();
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
