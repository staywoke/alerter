'use strict';

var AWS = require('aws-sdk');
var dynamodbstreams = new AWS.DynamoDBStreams({ apiVersion: '2012-08-10' });

module.exports = {
    shardId: null,
    shardIterator: null,
    lastEvaluatedShardId : null,
    
    reset: function () {
        this.lastEvaluatedShardId = null;
        this.shardIterator = null;
        this.shardId = null;
    },

    check: function check() {
        var self = this;    
        if (this.shardIterator) {
            return this.lookForUpdates();
        }

        var params = {
            StreamArn: (process.env.DYNAMO_SYSTEM_STREAM || 'test-dynamodb-uri')
        };
        
        if (this.lastEvaluatedShardId) {
            params.ExclusiveStartShardId = this.lastEvaluatedShardId;
        }
        
        dynamodbstreams.describeStream(params, function (err, data) {
            if (err) {
                console.log('error in describe stream', err, err.stack);
                return self.reset();
            } 
            
            if (data.LastEvaluatedShardId) {
                self.lastEvaluatedShardId = data.LastEvaluatedShardId;
                return check();
            }
            self.shardId = data.StreamDescription.Shards[data.StreamDescription.Shards.length - 1].ShardId;
            self.getIterator();    
        });
    },

    getIterator: function () {
        var self = this;
        var params = {
            ShardId: this.shardId, /* required */
            ShardIteratorType: 'LATEST', /* required */
            StreamArn: (process.env.DYNAMO_SYSTEM_STREAM || 'test-dynamodb-uri')
        };
        dynamodbstreams.getShardIterator(params, function (err, data) {
            if (err) {
                console.log('error getting shard iterator', err, err.stack);
                return self.reset();
            } 

            self.shardIterator = data.ShardIterator;
            self.lookForUpdates(); 
        });    
    },

    lookForUpdates: function () {
        var self = this;
        if (!this.shardIterator) {
            return this.getIterator();
        }
        
        var params = {
            ShardIterator: this.shardIterator
        };
        
        dynamodbstreams.getRecords(params, function (err, data) {
            if (err) {
                console.log('error getting records', err, err.stack);        
                return self.reset();
            }
            
            self.shardIterator = data.NextShardIterator;

            if (data.Records && data.Records.length > 0) {
                data.Records.forEach(function (item) {
                    // // if an alert record for a piece of legislation is created 
                    if (item.eventName !== 'INSERT') {
                        return;
                    }
                    self.updateObject(self.makeObject(item.dynamodb.NewImage));
                });
            } 
            else {
                console.log('No updates');

            }
        });
    },

    makeObject: function (data) {
        var self = this;
        var item = {};
    
        for (var prop in data) {
            if (data.hasOwnProperty(prop)){            
                if (data[prop].S) {
                    item[prop] = data[prop].S;
                } 
                else if (data[prop].N) {
                    item[prop] = +data[prop].N;
                } 
                else if (data[prop].M) {
                    item[prop] = this.makeObject(data[prop].M);
                } 
                else if (data[prop].L) { 
                    item[prop] = [];
                    data[prop].L.forEach(function(ai) {
                        if(ai.M) {
                            item[prop].push(self.makeObject(ai.M));
                        }
                    });
                } 
                else if(data[prop].BOOL) {
                      item[prop] = data[prop].BOOL ? true : false;
                }
            }
        }
    
        return item;
    },
};

