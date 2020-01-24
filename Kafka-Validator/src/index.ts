import Config from "./Config";
import MongoFactory from "./MongoFactory";

let publishedCollectionObj = null;
let receivedCollectionObj = null;

let fetchLimit = 100;
let timeout = 1000;

const processedObjects = new Set();
const alreadyRead = new Set();
const tempStorageArray = new Array();
tempStorageArray.push(new Array());
let storageIndex = 0;
let maxStorageSize = fetchLimit - 1;

new MongoFactory().openMongoConnection(10).then((mongoClient: any) => {

    publishedCollectionObj = mongoClient.collection(Config.publishedCollectionName);
    receivedCollectionObj = mongoClient.collection(Config.receivedCollectionName);

    startValidaion();
    startDeletion();

    setInterval(() => {
        publishedCollectionObj.estimatedDocumentCount({}, (error, numOfDocs) => {
            console.log("Total documents:" + numOfDocs + ", timeout:" + timeout + ", fetchLimit:" + fetchLimit);
            timeout = 10000;
            if (error) {
                console.log("Count error:", error);
            }
            if (numOfDocs > 50000) {
                timeout = 100;
                fetchLimit = 100;
            } else if (numOfDocs > 10000) {
                timeout = 150;
                fetchLimit = 100;
            } else if (numOfDocs > 7500) {
                timeout = 200;
                fetchLimit = 100;
            } else if (numOfDocs > 5000) {
                timeout = 300;
                fetchLimit = 125;
            } else if (numOfDocs > 1000) {
                timeout = 1000;
                fetchLimit = 200;
            }
            maxStorageSize = fetchLimit - 1;
        });
    }, 10000);

});

async function startValidaion() {
    publishedCollectionObj.find({ uniqueKey: { $nin: Array.from(alreadyRead) } }).limit(fetchLimit)
        .toArray(async (err, docsArray) => {
            if (err) {
                console.log("Find error:", err);
            }
            if (docsArray.length && docsArray.length < fetchLimit) {
                maxStorageSize = docsArray.length - 1;
            }
            if (docsArray.length === 0) {
                alreadyRead.clear();
            }
            let uKey = null;
            for (const doc of docsArray) {
                uKey = doc.uniqueKey;
                alreadyRead.add(uKey);

                if (tempStorageArray[storageIndex].length < maxStorageSize) {
                    tempStorageArray[storageIndex].push(uKey);
                } else {
                    tempStorageArray[storageIndex].push(uKey);
                    const indexToSave = storageIndex;
                    for (let index = 0; index < tempStorageArray.length; index++) {
                        // console.log("in:" + index + ", len:" + messageStorageArr[index].length);
                        if (tempStorageArray[index].length === 0) {
                            storageIndex = index;
                            break;
                        }
                    }
                    if (indexToSave === storageIndex) {
                        tempStorageArray.push(new Array());
                        storageIndex = tempStorageArray.length - 1;
                    }
                    findAndUpdate(tempStorageArray[indexToSave], indexToSave);
                }

            }

            setTimeout(() => {
                startValidaion();
            }, timeout);

        });
}

async function findAndUpdate(uniqueKeys, index) {
    receivedCollectionObj.updateMany(
        { uniqueKey: { $in: uniqueKeys } },
        {
            $set: { processed: true },
        },
        (updateErr, updateResult) => {
            if (updateErr) {
                console.log("updateErr:", updateErr);
            }
            tempStorageArray[index] = new Array();

        });
}

async function startDeletion() {
    const start = new Date().getTime();
    receivedCollectionObj.find({ processed: true, uniqueKey: { $nin: Array.from(processedObjects) } }).limit(fetchLimit)
        .toArray(async (err, docsArray) => {
            if (err) {
                console.log("Received fetch error:", err);
            }
            let key = null;
            const canDelete = new Array();
            for (const doc of docsArray) {
                key = doc.uniqueKey;
                processedObjects.add(key);
                alreadyRead.delete(key);
                canDelete.push(key);
            }
            deleteFromDB(canDelete);
            setTimeout(() => {
                startDeletion();
            }, timeout);
        });
}

async function deleteFromDB(canDelete) {

    publishedCollectionObj.deleteMany({ uniqueKey: { $in: canDelete } },
        (pubDeleteErr, pubResult) => {
            if (pubDeleteErr) {
                console.log("pubDeleteErr:", pubDeleteErr);
            } else {
                console.log("deleted from pub:", pubResult.deletedCount);
                receivedCollectionObj.deleteMany({ uniqueKey: { $in: canDelete } },
                    (subDeleteErr, subResult) => {
                        if (subDeleteErr) {
                            console.log("subDeleteErr:", subDeleteErr);
                        } else {
                            console.log("deleted from received:", subResult.deletedCount);
                            for (const key of canDelete) {
                                processedObjects.delete(key);
                            }
                        }

                    });
            }

        });
}
