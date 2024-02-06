
const { MongoClient } = require('mongodb');
const dotenv = require('dotenv');
dotenv.config({});

exports.connectToMongoDB = async () => {
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