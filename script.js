/**
 * This script can be used to create, update, and delete sample data.
 * This script is especially helpful when testing change streams.
 */

const { MongoClient } = require('mongodb');
const dotenv = require('dotenv');
dotenv.config({});

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



async function main() {

    const client = await connectToMongoDB();

    try {

        // const italianVilla = await createListing(client, {
        //     name: "Italian Villa",
        //     property_type: "Entire home/apt",
        //     bedrooms: 6,
        //     status: 'failed',
        //     retryCount: 0,
        //     bathrooms: 4,
        //     address: {
        //         market: "Cinque Terre",
        //         country: "Italy"
        //     }
        // });

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

        // Make the appropriate DB calls
        const operaHouseViews = await createListing(client, {
            name: "Opera House Views",
            summary: "Beautiful apartment with views of the iconic Sydney Opera House",
            property_type: "Apartment",
            bedrooms: 1,
            bathrooms: 1,
            beds: 1,
            status: 'queued',
            address: {
                market: "Sydney",
                country: "Australia"
            }
        });

        const privateRoomInLondon = await createListing(client, {
            name: "Private room in London",
            property_type: "Apartment",
            bedrooms: 1,
            bathroom: 1,
            status: 'queued',
        });

        const beautifulBeachHouse = await createListing(client, {
            name: "Beautiful Beach House",
            summary: "Enjoy relaxed beach living in this house with a private beach",
            bedrooms: 4,
            bathrooms: 2.5,
            status: 'queued',
            beds: 7,
            last_review: new Date()
        });

        await updateListing(client, operaHouseViews, { beds: 2 });

        await updateListing(client, beautifulBeachHouse, {
            address: {
                market: "Sydney",
                status: 'queued',
                country: "Australia"
            }
        });

        const italianVilla = await createListing(client, {
            name: "Italian Villa",
            property_type: "Entire home/apt",
            bedrooms: 6,
            status: 'queued',
            bathrooms: 4,
            address: {
                market: "Cinque Terre",
                country: "Italy"
            }
        });

        const sydneyHarbourHome1 = await createListing(client, {
            name: "Sydney Harbour Home",
            bedrooms: 4,
            bathrooms: 2.5,
            status: 'queued',
            address: {
                market: "Sydney",
                country: "Australia"
            }
        });

        await deleteListing(client, sydneyHarbourHome1);

    } finally {
        // Close the connection to the MongoDB cluster
        await client.close();
    }
}

main().catch(console.error);

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
async function updateListing(client, listingId, updatedListing) {
    // See http://bit.ly/Node_updateOne for the updateOne() docs
    const result = await client.db("sample_airbnb").collection("listingsAndReviews").updateOne({ _id: listingId }, { $set: updatedListing });

    console.log(`${result.matchedCount} document(s) matched the query criteria.`);
    console.log(`${result.modifiedCount} document(s) was/were updated.`);
}

/**
 * Delete an Airbnb listing
 * @param {MongoClient} client A MongoClient that is connected to a cluster with the sample_airbnb database
 * @param {String} listingId The id of the listing you want to delete
 */
async function deleteListing(client, listingId) {
    // See http://bit.ly/Node_deleteOne for the deleteOne() docs
    const result = await client.db("sample_airbnb").collection("listingsAndReviews").deleteOne({ _id: listingId });

    console.log(`${result.deletedCount} document(s) was/were deleted.`);
}
