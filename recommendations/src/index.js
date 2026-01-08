const express = require("express");
const mongodb = require("mongodb");
const amqp = require("amqplib");

if (!process.env.PORT) {
    throw new Error("Please specify the port number for the HTTP server with the environment variable PORT.");
}

if (!process.env.DBHOST) {
    throw new Error("Please specify the databse host using environment variable DBHOST.");
}

if (!process.env.DBNAME) {
    throw new Error("Please specify the name of the database using environment variable DBNAME");
}

if (!process.env.RABBIT) {
    throw new Error("Please specify the name of the RabbitMQ host using environment variable RABBIT");
}

const PORT = process.env.PORT;
const DBHOST = process.env.DBHOST;
const DBNAME = process.env.DBNAME;
const RABBIT = process.env.RABBIT;

//
// Application entry point.
//
async function main() {
    const app = express();

    //
    // Enables JSON body parsing for HTTP requests.
    //
    app.use(express.json()); 

    //
    // Connects to the database server.
    //
    const client = await mongodb.MongoClient.connect(DBHOST);

    //
    // Gets the database for this microservice.
    //
    const db  = client.db(DBNAME);

    //
    // Gets the collection for storing video metadata.
    //
    const videosCollection = db.collection("videos");
    
    //
    // Connect to the RabbitMQ server.
    //
    const messagingConnection = await amqp.connect(RABBIT); 

    //
    // Creates a RabbitMQ messaging channel.
    //
    const messageChannel = await messagingConnection.createChannel(); 

    // 
    // Handler for incoming messages.
    //
    async function consumeViewedMessage(msg) {

        const parsedMsg = JSON.parse(msg.content.toString()); // Parse the JSON message.

        const filePath = parsedMsg.videoPath;
        // Strip into filename
        const filename = filePath.split('/').pop();
        // Check for video existence in db
        const video = await videosCollection.findOne({ "video": filename });
        if (video === null) {
          // Create new record.
          await videosCollection.insertOne({ "video": filename, "views": 1 });
        } else {
          // Update views
          await videosCollection.updateOne({ "video": filename }, { $inc: { "views": 1 } });
        }
        
        // Recommend most viewed video
          const mostViewed = await videosCollection.find().sort({ "views": -1 }).limit(2).toArray();
          if (mostViewed.length == 1 || mostViewed.video != filename )
            console.log(`Recommended ${mostViewed[0].video}`);
          else if (mostViewed.length > 1 )
            console.log(`Recommended ${mostViewed[1].video}`);
          else
            console.log(`No recommendations available`);
                
        messageChannel.ack(msg); // If there is no error, acknowledge the message.
    };

    //
    // Asserts that we have a "viewed" exchange.
    //
    await messageChannel.assertExchange("viewed", "fanout"); 

	//
	// Creates an anonyous queue.
	//
	const { queue } = await messageChannel.assertQueue("", { exclusive: true }); 

    console.log(`Created queue ${queue}, binding it to "viewed" exchange.`);
    
    //
    // Binds the queue to the exchange.
    //
    await messageChannel.bindQueue(queue, "viewed", ""); 

    //
    // Start receiving messages from the anonymous queue.
    //
    await messageChannel.consume(queue, consumeViewedMessage);

    //
    // Starts the HTTP server.
    //
    app.listen(PORT, () => {
        console.log("Microservice online.");
    });
}

main()
    .catch(err => {
        console.error("Microservice failed to start.");
        console.error(err && err.stack || err);
    });