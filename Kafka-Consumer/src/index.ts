import Config from "./Config";
import KafkaFactory from "./KafkaFactory";
import MongoFactory from "./MongoFactory";
import readline = require("readline");

const kafkaFactoryObj = new KafkaFactory(Config.kafkaHost, Config.kafkaPort);

let topicNamesArray = new Array();

const consumer = kafkaFactoryObj.getKafkaConsumer(topicNamesArray);

let collectionObj = null;

const unsubscribedTopics = ["LinxupLocationTopic", "googleMap"];

const receivedTopicsSet = new Set();

let lastReceivedTime = null;

let timeDiff = null;

let deviceCount;

let total = 0;
let totalSaved = 0;
const messageStorageArr = new Array();
messageStorageArr.push(new Array());

let tempStorageCount = 0;
let storageIndex = 0;

const readlineInterface = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});

readlineInterface.question("Enter numbder of device for listening:", (numberOfDevice) => {
    deviceCount = parseInt(numberOfDevice, 10);
    if (deviceCount < 100) {
        tempStorageCount = deviceCount;
    } else {
        tempStorageCount = 99;
    }
    readlineInterface.close();
    if (between(deviceCount, 1, 500)) {
        timeDiff = 3000;
    } else if (between(deviceCount, 501, 1000)) {
        timeDiff = 4000;
    } else if (between(deviceCount, 1001, 1500)) {
        timeDiff = 5000;
    } else {
        timeDiff = 7000;
    }
    // console.log("deviceCount:" + deviceCount + ", waitTime" + waitTime);
    startListening();
    /**
     * Check for new topics every 5 seconds
     */
    setInterval(() => {
        checkForNewTopicsAndSubscribe();
    }, 5 * 1000);

});

function between(x, min, max) {
    return x >= min && x <= max;
}

async function startListening() {
    consumer.on("message", (message) => {

        const currentTime = new Date().getTime();
        const diff = currentTime - lastReceivedTime;
        // console.log("diff:", diff);
        if (lastReceivedTime && ((diff) > timeDiff)) {
            console.log("Received messages ----- :", receivedTopicsSet.size);
            receivedTopicsSet.clear();
        }
        lastReceivedTime = currentTime;
        receivedTopicsSet.add(message.topic);

        const receivedData = JSON.parse(message.value);

        // Add new properties for validation
        receivedData.receivedTime = currentTime;
        receivedData.processed = false;

        // console.log("delay:", currentTime - receivedData.publishedTime);
        if (messageStorageArr[storageIndex].length < tempStorageCount) {
            messageStorageArr[storageIndex].push(receivedData);
        } else {
            messageStorageArr[storageIndex].push(receivedData);
            // console.log("storageIndex:", storageIndex);
            // console.log("storage len:", messageStorageArr[storageIndex].length);
            // console.log("messageStorageArr :", messageStorageArr);
            const indexToSave = storageIndex;
            for (let index = 0; index < messageStorageArr.length; index++) {
                // console.log("in:" + index + ", len:" + messageStorageArr[index].length);
                if (messageStorageArr[index].length === 0) {
                    storageIndex = index;
                    break;
                }
            }
            if (indexToSave === storageIndex) {
                messageStorageArr.push(new Array());
                storageIndex = messageStorageArr.length - 1;
            }
            if (collectionObj == null) {
                new MongoFactory().openMongoConnection((deviceCount / 2)).then((mongoClient: any) => {
                    collectionObj = mongoClient.collection(Config.dataCollectionName);
                    saveIntoDB(messageStorageArr[indexToSave], indexToSave);
                });
            } else {
                saveIntoDB(messageStorageArr[indexToSave], indexToSave);
            }
        }
    });
}

async function saveIntoDB(docArray, index) {
    collectionObj.insertMany(docArray, (err, result) => {
        if (err) {
            console.log("insert error:", err);
        } else {
            messageStorageArr[index] = new Array();
            total += docArray.length;
            totalSaved += result.insertedCount;
            console.log("Total Received:", total);
            console.log("Total saved:", totalSaved);
        }
        // console.log("insert result", result);
        // console.log("Inserted document into the collection");
    });
}

consumer.on("error", (err) => {
    console.log("err", err);
});

consumer.on("offsetOutOfRange", (err) => {
    console.log("err", err);
});

const admin = kafkaFactoryObj.getKafkaAdmin();
admin.listTopics((err, res) => {
    if (res && res[1]) {
        topicNamesArray = Object.keys(res[1].metadata);
        for (const topic of unsubscribedTopics) {
            topicNamesArray.splice(topicNamesArray.indexOf(topic), 1);
        }
        consumer.addTopics(topicNamesArray, (addErr, added) => {
            // console.log("Topic added to consumer");
        });
    }
});

async function checkForNewTopicsAndSubscribe() {
    console.log("Checking for new topics...");
    admin.listTopics((err, res) => {
        if (res && res[1]) {
            const topicsArray = Object.keys(res[1].metadata);
            for (const topic of unsubscribedTopics) {
                topicsArray.splice(topicsArray.indexOf(topic), 1);
            }

            const subscribedTopics = new Set(topicNamesArray);
            const newTopics = new Array();
            for (const topic of topicsArray) {
                if (!subscribedTopics.has(topic)) {
                    newTopics.push(topic);
                }
            }
            // console.log("new Topics found:", newTopics);
            if (newTopics.length) {
                topicNamesArray = topicsArray;
                consumer.addTopics(newTopics, (addErr, added) => {
                    console.log("Topic added to consumer:", added);
                    if (addErr) {
                        console.log("Error while adding new topic to consumer:", addErr);
                    }
                });
            }
        }
    });

}

new MongoFactory().openMongoConnection((deviceCount / 2)).then((mongoClient: any) => {
    collectionObj = mongoClient.collection(Config.dataCollectionName);
});
