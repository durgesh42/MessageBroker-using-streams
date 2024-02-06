const Redis = require("ioredis");
const REDIS_HOSTNAME = process.env.REDIS_HOSTNAME
const REDIS_PORT = process.env.REDIS_PORT;

const redisClients = {
    storageClient: null,
};

Object.keys(redisClients).forEach((clientName) => {
    redisClients[clientName] = new Redis({
        host: REDIS_HOSTNAME,
        port: REDIS_PORT,
    });
});

Object.keys(redisClients).forEach((clientName) => {
    let client = redisClients[clientName];

    client.on("connect", function() {
        console.log(clientName, "Redis connection state: connect");
    });

    client.on("ready", function() {
        console.log(clientName, "Redis connection state: ready");
    });

    client.on("error", function(err) {
        console.error(clientName, "Redis connection state: error", err);
    });

    client.on("close", function() {
        console.error(clientName, "Redis connection state: close");
    });

    client.on("reconnecting", function() {
        console.error(clientName, "Redis connection state: reconnecting");
    });

    client.on("end", function() {
        console.error(clientName, "Redis connection state: end");
    });
});


exports.client = redisClients.storageClient;
exports.clients = redisClients;

exports.get = (key, callback) => {
    redisClients.storageClient.get(key, (err, data) => {
        if (err) {
            callback(err)
        } else {
            if (data) {
                try {
                    data = JSON.parse(data)
                } catch (e) {
                    logger.warn(`Error parsing data in RedisService.get ${data} : ${err}`)
                }
            }
            callback(null, data)
        }
    });
}

exports.del = (key, callback) => {
    redisClients.storageClient.del(key, (err, delCount) => {
        if (err) {
            console.error(`Error deleting key ${key} : ${err}`)
        }
        callback(err, delCount)
    });
}

exports.set = (key, data, callback) => {
    redisClients.storageClient.set(key, JSON.stringify(data), (err, status) => {
        if (err || status != "OK") {
            console.error(`Erorr in RedisService.set with err ${err} and status ${status}`)
            callback(err);
        } else {
            callback(null, data);
        }
    });
}

/*
    set the value of {key} as {data} expiring after {ttl} milliseconds
*/
exports.setEx = (key, data, ttl, callback) => {
    // stringifying data if its not a string
    redisClients.storageClient.set([key, !_.isString(data) ? JSON.stringify(data) : data, "PX", ttl], (err, status) => {
        if (err || status != "OK") {
            console.error(`Erorr in RedisService.set with err ${err} and status ${status}`)
            callback(err);
        } else {
            callback(null, data);
        }
    });
}

/*
    gets the value of {key} in {hash}
*/
exports.hget = (hash, key, callback) => {
    redisClients.storageClient.hget(hash, key, (err, data) => {
        if (err) {
            callback(err)
        } else {
            if (data) {
                try {
                    data = JSON.parse(data)
                } catch (e) {
                    logger.warn(`Error parsing data in RedisService.get ${data} : ${err}`)
                }
            }
            callback(null, data)
        }
    });
}

/*
    gets all values in {hash} as a keyed object
*/
exports.hgetall = (hash, callback) => {
    redisClients.storageClient.hgetall(hash, (err, data) => {
        if (err) {
            callback(err)
        } else {
            if (data && _.isObject(data)) {
                _.keys(data).forEach((key) => {
                    try {
                        data[key] = JSON.parse(data[key])
                    } catch (e) {
                        logger.warn(`Error parsing data in RedisService.hgetall ${data} : ${err}`)
                    }
                })
            }
            callback(null, data)
        }
    });
}

/*
    gets all values in {hash} as an array
*/
exports.hvals = (hash, callback) => {
    redisClients.storageClient.hvals(hash, (err, data) => {
        if (err) {
            callback(err)
        } else {
            if (data && _.isArray(data)) {
                _.each(data, (obj, index) => {
                    try {
                        data[index] = JSON.parse(data[index])
                    } catch (e) {
                        logger.warn(`Error parsing data in RedisService.hvals ${data} : ${err}`)
                    }
                })
            }
            callback(null, data)
        }
    });
}

/*
    deletes the value of {keys} in {hash}
*/
exports.hdel = (hash, keys, callback) => {
    redisClients.storageClient.hdel(hash, keys, (err, delCount) => {
        if (err) {
            console.error(`Error deleting keys: ${keys} from hash: ${hash} : ${err}`)
        }
        callback(err, delCount)
    });
}

/*
    sets the value of {key} in {hash} as "data"
*/
exports.hset = (hash, key, data, callback) => {
    redisClients.storageClient.hset(hash, key, JSON.stringify(data), (err, status) => {
        if (err) {
            console.error(`Erorr in RedisService.set with err ${err} and status ${status}`)
            callback(err);
        } else {
            callback(null, data);
        }
    });
}

/*
    increases the value of {key} in {hash} by 1
*/
exports.hincr = (hash, key, callback) => {
    redisClients.storageClient.hincrby(hash, key, 1, (err, newCount) => {
        if (err) {
            console.error(`Erorr in RedisService.hincr with err ${JSON.stringify(err)} and newCount ${newCount}`)
            return callback(err);
        }
        return callback(null, newCount);
    });
}

exports.expire = (key, ttl, option, callback) => {
    const cb = (err) => {
        if (err) {
            console.error(`Erorr in RedisService.expire with err - ${JSON.stringify(err)}`)
            return callback(err);
        }
        return callback();
    }
    if (option) {
        redisClients.storageClient.expire(key, ttl, option, cb);
    } else {
        redisClients.storageClient.expire(key, ttl, cb);
    }
}