const {
    MongoClient,
    ObjectId
} = require('mongodb');
const stream = require('stream');
const async = require('async');
const moment = require('moment');
const dotenv = require('dotenv');
const RedisService = require('./RedisService');
const LockService = require('./LockService');

dotenv.config({});
const retryLimit = 2;

/**
 * Close the given change stream after the given amount of time
 * @param {*} timeInMs The amount of time in ms to monitor listings
 * @param {*} changeStream The open change stream that should be closed
 */
function closeChangeStream(timeInMs = 60000, changeStream) {
    return new Promise((resolve) => {
        setTimeout(() => {
            console.log("Closing the change stream");
            changeStream.close();
            resolve();
        }, timeInMs)
    })
};


/**
 * Monitor listings in the listingsAndReviews collections for changes
 * This function uses the on() function from the EventEmitter class to monitor changes
 * @param {MongoClient} client A MongoClient that is connected to a cluster with the sample_airbnb database
 * @param {Number} timeInMs The amount of time in ms to monitor listings
 * @param {Object} pipeline An aggregation pipeline that determines which change events should be output to the console
 */
async function monitorListingsUsingEventEmitter(client, timeInMs = 60000, pipeline = [], watchOpts) {
    console.log("monitorListingsUsingEventEmitter");
    const collection = client.db("sample_airbnb").collection("listingsAndReviews");

    let changeStream; // Declare changeStream variable outside the scope of functions

    // Function to recreate and restart the change stream
    const restartChangeStream = async () => {
        console.log('Restarting change stream...');
        await listenForNextJob(client, null);
    };

    const watchChanges = () => {

        changeStream = collection.watch(pipeline, watchOpts);

        changeStream.on('change', (next) => {
            handleEvents(client, next);
        });

        changeStream.on('error', (error) => {
            console.error('Change stream error:', error);
            if (error.codeName === 'ChangeStreamHistoryLost' || error.codeName === "ChangeStreamFatalError") {
                // Handle ChangeStreamHistoryLost error by restarting the change stream
                restartChangeStream();

            } else {
                // Handle other errors according to your application's requirements
                console.error('Other error:', error);
            }
        });
    };

    watchChanges();
    // await closeChangeStream(timeInMs, changeStream);
}

const handleEvents = async (client, doc) => {

    const newDoc = JSON.parse(JSON.stringify(doc))
    const nextResumeToken = newDoc._id
    documentId = new ObjectId(newDoc.fullDocument._id)

    const newJobs = async () => {
        await updateListing(client, documentId, {
            status: 'completed',
            completedAt: moment().toDate(),
            resumeToken: nextResumeToken,
        });
        // await listenForNextJob(client, nextResumeToken)
    }

    const pendingJobCounts = async () => {
        const pendingJobs = await client.db("sample_airbnb").collection("listingsAndReviews").aggregate([
            {
                $match: {
                    _id: {
                        $lt: documentId
                    },
                    status: { $in: ['queued', 'failed'], }
                },
            },
            {
                $group: {
                    _id: null,
                    count: { $sum: 1 }
                }
            }
        ]).next();
        console.log("pendingJobs : ", pendingJobs);
        if (pendingJobs?.count) {
            initiateOlderPendingJobs(client, documentId);
        }
    }


    await newJobs();
    await pendingJobCounts();
}



/**
 * Create a new Airbnb listing
 * @param {MongoClient} client A MongoClient that is connected to a cluster with the sample_airbnb database
 * @param {Object} newListing The new listing to be added
 * @returns {String} The id of the new listing
 */
async function createListing(client, newListing) {
    // See http://bit.ly/Node_InsertOne for the insertOne() docs
    const result = await client.db("sample_airbnb").collection("listingsAndReviews").insertOne(newListing);
    console.log(`New listing created with the following id: ${result.insertedId}`);
    return result.insertedId;
}

/**
 * Update an Airbnb listing
 * @param {MongoClient} client A MongoClient that is connected to a cluster with the sample_airbnb database
 * @param {String} listingId The id of the listing you want to update
 * @param {object} updatedListing An object containing all of the properties to be updated for the given listing
 */
// async function updateListing(client, documentId, updatedListing) {
//     // See http://bit.ly/Node_updateOne for the updateOne() docs

//     async.waterfall([
//         // Acquire lock
//         (cbl) => {
//             LockService.acquireLock(`handleEvents:${documentId}`, 1000, (lockErr, queueMsgLock) => {
//                 if (lockErr) {
//                     if (!lockErr.safe) {
//                         console.error(
//                             `Error in acquireLock in handleEvents`,
//                             `documentId: `, documentId,
//                             `lockErr: `, lockErr
//                         );
//                     }
//                     return cbl(lockErr); // Pass error to next function
//                 } else {
//                     console.log(`Lock Acquire in handleEvents documentId: `, documentId);
//                     return cbl(null, queueMsgLock); // Pass lock to next function
//                 }
//             });
//         },
//         (queueMsgLock, cbl) => { // Corrected this line
//             try {
//                 const result = await client.db("sample_airbnb").collection("listingsAndReviews").updateOne({ _id: documentId }, { $set: updatedListing })
//                 console.log(`${result.matchedCount} document(s) matched the query criteria.`);
//                 console.log(`${result.modifiedCount} document(s) was/were updated.`);
//                 return cbl(null, queueMsgLock); // Pass lock to next function
//             } catch (err) {
//                 return cbl(err); // Pass error to final callback
//             }
//         }
//     ], (err, queueMsgLock) => {
//         if (err) {
//             console.error('Error:', err);
//         } else {
//             console.log(`Lock Released in handleEvents documentId: `, documentId);
//             LockService.releaseLock(queueMsgLock);
//         }
//     });
// }


async function updateListing(client, documentId, updatedListing) {
    // See http://bit.ly/Node_updateOne for the updateOne() docs

    LockService.acquireLock(`handleEvents:${documentId}`, 1000, async (lockErr, queueMsgLock) => {
        if (lockErr) {
            if (!lockErr.safe) {
                console.error(
                    `Error in acquireLock in handleEvents`,
                    `documentId: `, documentId,
                    `lockErr: `, lockErr
                );
            }
        } else {
            console.log(`Lock Acquire on handleEvents documentId: ${documentId}\n`);
            const result = await client.db("sample_airbnb").collection("listingsAndReviews").updateOne({ _id: documentId }, { $set: updatedListing })
            console.log(`${result.matchedCount} document(s) matched the query criteria.`);
            console.log(`${result.modifiedCount} document(s) was/were updated.`);
            console.log(`\nLock Released in handleEvents documentId: ${documentId}\n`);
            LockService.releaseLock(queueMsgLock);
        }
    });
}

async function connectToMongoDB() {
    const uri = `mongodb+srv://${process.env.DB_USER}:${process.env.DB_PASS}@cluster0.bhg1dcp.mongodb.net/?retryWrites=true&w=majority`;
    const client = new MongoClient(uri);
    console.log('Attempting to connect to MongoDB...');

    try {
        await client.connect();
        console.log('Connected to MongoDB successfully.');
        return client;
    } catch (error) {
        console.error('Error connecting to MongoDB:', error);
        throw error;
    }
}

async function listenForNextJob(client, resumeToken) {
    let watchOpts = {
        fullDocument: 'updateLookup'
    }

    if (resumeToken) {
        watchOpts.startAfter = resumeToken
    }

    const pipeline = [
        {
            $match: {
                $or: [{
                    'fullDocument.status': 'queued'
                }, {
                    'fullDocument.status': 'failed',
                    'fullDocument.retryCount': {
                        $lt: retryLimit
                    }
                }],
            }
        }
    ];

    await monitorListingsUsingEventEmitter(client, 30000, pipeline, watchOpts);
}


async function main() {
    const client = await connectToMongoDB();

    try {
        const resumeToken = await getResumeToken(client)
        await listenForNextJob(client, resumeToken);

        const italianVilla = await createListing(client, {
            name: "Italian Villa",
            property_type: "Entire home/apt",
            bedrooms: 6,
            status: 'failed',
            retryCount: 0,
            bathrooms: 4,
            address: {
                market: "Cinque Terre",
                country: "Italy"
            }
        });

        const sydneyHarbourHome = await createListing(client, {
            name: "Sydney Harbour Home",
            bedrooms: 4,
            bathrooms: 2.5,
            status: 'failed',
            retryCount: 0,
            address: {
                market: "Sydney",
                country: "Australia"
            }
        });

    } finally {
        // Close the connection to the MongoDB cluster
        // await client.close();
    }
}

const getResumeToken = async (client) => {
    try {
        const job = await client.db("sample_airbnb").collection("listingsAndReviews").aggregate([
            {
                $match: {
                    status: 'completed',
                },
            },
            {
                $sort: {
                    completedAt: -1,
                },
            },
            {
                $limit: 1,
            },
            {
                $project: {
                    resumeToken: 1,
                },
            },
        ]).next();
        console.log("Resume token ", job?.resumeToken);
        return job?.resumeToken;
    } catch (err) {
        console.error(`Error in getResumeToken`, err);
    }
};

// fetch all 'queued' jobs older than current job id and
// initiate reset processes for all of them
const initiateOlderPendingJobs = async (client, currentJobId) => {
    console.log("initiateOlderPendingJobs currentJobId: ", currentJobId);
    const jobs = await client.db("sample_airbnb").collection("listingsAndReviews").aggregate([
        {
            $match: {
                $or: [
                    {
                        _id: {
                            $lt: currentJobId
                        },
                        status: 'queued'
                    },
                    {
                        _id: {
                            $lt: currentJobId
                        },
                        status: 'failed',
                        retryCount: {
                            $lt: retryLimit
                        }
                    }
                ]
            }
        },
        {
            $sort: {
                createdAt: 1,
            }
        }
    ]).toArray(); // Use toArray() to get an array of documents

    console.log(JSON.stringify(jobs))
    async.each(jobs, async (job, cbe) => {
        console.log("job", JSON.stringify(job));
        documentId = new ObjectId(job._id)
        await updateListing(client, documentId, { 'status': 'completed', retrySource: 'initiate_old', completedAt: moment().toDate() });
    })
};

main().catch(console.error);

