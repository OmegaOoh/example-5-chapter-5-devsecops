const express = require("express");
const mongodb = require("mongodb");
const fs = require("fs");
const amqp = require("amqplib");

if (!process.env.PORT) {
  throw new Error(
    "Please specify the port number for the HTTP server with the environment variable PORT.",
  );
}

if (!process.env.RABBIT) {
  throw new Error(
    "Please specify the name of the RabbitMQ host using environment variable RABBIT",
  );
}

if (!process.env.DBHOST) {
  throw new Error(
    "Please specify the database host using environment variable DBHOST.",
  );
}

if (!process.env.DBNAME) {
  throw new Error(
    "Please specify the name of the database using environment variable DBNAME",
  );
}

const PORT = process.env.PORT;
const RABBIT = process.env.RABBIT;
const DBHOST = process.env.DBHOST;
const DBNAME = process.env.DBNAME;

//
// Application entry point.
//
async function main() {
  console.log(`Connecting to RabbitMQ server at ${RABBIT}.`);

  const messagingConnection = await amqp.connect(RABBIT); // Connects to the RabbitMQ server.

  console.log("Connected to RabbitMQ.");

  const messageChannel = await messagingConnection.createChannel(); // Creates a RabbitMQ messaging channel.

  //
  // Connects to the database server.
  //
  const client = await mongodb.MongoClient.connect(DBHOST);

  //
  // Gets the database for this microservice.
  //
  const db = client.db(DBNAME);

  //
  // Gets the collection of videos.
  //
  const videos = db.collection("videos");

  await messageChannel.assertExchange("viewed", "fanout"); // Asserts that we have a "viewed" exchange.

  //
  // Broadcasts the "viewed" message to other microservices.
  //
  function broadcastViewedMessage(messageChannel, videoId) {
    console.log(`Publishing message on "viewed" exchange.`);

    const msg = { videoId: videoId };
    const jsonMsg = JSON.stringify(msg);
    messageChannel.publish("viewed", "", Buffer.from(jsonMsg)); // Publishes message to the "viewed" exchange.
  }

  const app = express();

  app.get("/video", async (req, res) => {
    // Route for streaming video.

    if (!req.query.id) return res.status(400).send("Missing video ID");

    // Get video path from database
    const videoId = req.query.id;
    const video = await videos.findOne({ _id: videoId });
    if (!video) return res.status(404).send("Video not found");

    const videoPath = "./videos/" + video.videoPath;
    const stats = await fs.promises.stat(videoPath);

    res.writeHead(200, {
      "Content-Length": stats.size,
      "Content-Type": "video/mp4",
    });

    // Construct data for stream
    fs.createReadStream(videoPath).pipe(res);
    
    // Make broadcastmessage only when range is present in header. (It is a video stream)
    if (req.headers.range != null)
      broadcastViewedMessage(messageChannel, videoId); // Sends the "viewed" message to indicate this video has been watched.
  });

  app.listen(PORT, () => {
    console.log("Microservice online.");
  });
}

main().catch((err) => {
  console.error("Microservice failed to start.");
  console.error((err && err.stack) || err);
});
