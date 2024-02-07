const moment = require('moment')
const RedisService = require('./RedisService')
const _ = require('underscore')

function lockHelper({
    nameSpace,
    identifier,
    ttl,
    task,
    callback,
    release
}) {

    if (!_.isFunction(callback)) {
        console.error(`callback malformed in lockHelper`);
        return;
    }

    if (_.isEmpty(nameSpace) || _.isEmpty(identifier) || !ttl) {
        console.error(`Invalid arguments for lockHelper ${nameSpace} - ${identifier} - ${ttl}`);
        return callback()
    }

    if (!_.isFunction(task)) {
        console.error(`task malformed in lockHelper`);
        return callback(`task malformed in lockHelper`);
    }

    acquireLock(`${nameSpace}:${identifier}`, ttl || 1000, function(err, lock) {
        if (err) {
            callback(err)
        } else {
            task((err, data) => {
                if (release) {
                    releaseLock(lock)
                }
                callback(err, data)
            })
        }
    });
}


exports.acquireLock = (lockId, ttl, callback) => {
    // lockId is the key being set
    // "true" is the value for the key
    // "NX" indicates to succeed only if the key does not exist
    // "PX" indicates next argument is expiry in milliseconds
    // ttl is the millis till expiry
    const start = moment()
    RedisService.client.set([lockId, "true", "NX", "PX", ttl], (err, data) => {
        // data == "OK" implies that key did not exist and was set successfully

        // subsampling to 1/3rd logs
        if (_.sample([false, true, false]))
            console.log(`Attempting to acquire lock on ${lockId} for ${ttl} took ${moment().diff(start)}`);

        if (!err && data == "OK") {
            callback(null, lockId)
        } else {
            callback(err || {
                safe: true,
                message: "couldn't acquire lock"
            })
        }
    })
}

exports.releaseLock = (lockObj, callback) => {
    RedisService.client.expire(lockObj, 0, (err, data) => {
        // data == 1 implies ttl was set and key exists
        if (err || data != "1") {
            console.warn(`Error releasing lock on ${lockObj},`, err || `${lockObj} does not exist`)
        }
        if (_.isFunction(callback)) {
            callback(err)
        }
    })
}




exports.lockRunAndRelease = (nameSpace, identifier, ttl, task, callback) => {
    lockHelper({
        nameSpace,
        identifier,
        ttl,
        task,
        callback,
        release: true
    });
}

exports.lockAndRun = (nameSpace, identifier, ttl, task, callback) => {
    lockHelper({
        nameSpace,
        identifier,
        ttl,
        task,
        callback
    });
}