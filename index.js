'use strict';

const AWS = require('aws-sdk');
var axios = require('axios');

const SQS = new AWS.SQS({apiVersion: '2012-11-05'});
const Lambda = new AWS.Lambda({apiVersion: '2015-03-31'});
var docClient = new AWS.DynamoDB.DocumentClient();

var parseString = require('xml2js').parseString;

const QUEUE_URL = "";
const FIFO_QUEUE_URL = "";
const FIFO_MESSAGE_GROUP_ID = "FIFO_MWS_NOTIFICATIONS_GROUP";

const SERVER_HOST = "";
const SERVER_HOOK_ROUTE = "";
const SERVER_HOOK_URL = "";

/**
 * Pass the data to send as `event.data`, and the request options as
 * `event.options`. For more information see the HTTPS module documentation
 * at https://nodejs.org/api/https.html.
 *
 * Will succeed with the response body.
 */

let notifications = [];
let asins = [];
let variable = {};
const TIME_LIMIT = 10 * 1000;
const NUMBER_LIMIT = 40;

function getVarFromDB(type, init_value, callback){
    var params = {
        TableName : "lambda_cache",
        KeyConditionExpression: "#ty = :type",
        ExpressionAttributeNames:{
            "#ty": "type"
        },
        ExpressionAttributeValues: {
            ":type": type
        }
    };


    docClient.query(params, function(err, data) {
        if (err) {
            variable[type] = null;
            callback();
        } else {
            if(data.Items){
                variable[type] = data.Items[0].var_value;
                callback();
            } else {
                var params = {
                    TableName:"lambda_cache",
                    Item:{
                        "type": type,
                        "var_value": init_value
                    }
                };
                docClient.put(params, function(err, data) {
                    if (err) {
                        variable[type] = null;
                    } else {
                        variable[type] = init_value;
                    }
                    callback();
                });
            }
        }
    });
}

function updateVars(type, value, callback){
    var params = {
        TableName:"lambda_cache",
        Key:{
            "type": type
        },
        UpdateExpression: "set var_value = :val",
        ExpressionAttributeValues:{
            ":val":value
        },
        ReturnValues:"UPDATED_NEW"
    };

    docClient.update(params, function(err, data) {
        if (err) {
            console.error("Unable to update item. Error JSON:", JSON.stringify(err, null, 2));
        } else {
            callback();
        }
    });
}

function getNotifications(callback){
    var params = {
        TableName : "mws_notifications",
        KeyConditionExpression: "#ty = :attr",
        ExpressionAttributeNames:{
            "#ty": "attr"
        },
        ExpressionAttributeValues:{
            ":attr" : "notifications"
        }
    };

    docClient.query(params, function(err, data) {
        if (err) {
            notifications = [];
            asins=[];
            console.log(err);
        } else {
            if(data.Items && data.Items.length){
                notifications = JSON.parse(data.Items[0].notification_records);
                asins = notifications.map(notification => notification.asin);
            } else {
                notifications = [];
                asins = [];
            }
        }
        callback();
    });
}


// async function putNotification(asin, notification, callback){
//     var params = {
//         TableName : "mws_notifications",
//         Item:{
//             "asin": asin,
//             "notification": notification,
//             "updated_at": new Date().getTime(),
//             "updated": 1,
//         }
//     };
//
//     await docClient.put(params, function(err, data) {
//         if (err) {
//         } else {
//             callback();
//         }
//     });
// }
//
//
// function updateNotification(asin, notification, callback){
//     var params = {
//         TableName : "mws_notifications",
//         Key:{
//             "asin": asin
//         },
//         UpdateExpression: "set notification=:notification, updated_at=:now updated=:updated",
//         ExpressionAttributeValues:{
//             ":notification":notification,
//             ":now":new Date().getTime(),
//             ":updated":1
//         },
//         ReturnValues:"UPDATED_NEW"
//     };
//
//     console.log('UpdateNotification ', variable['time_counter'], new Date().getTime());
//
//     docClient.update(params, function(err, data) {
//         if (err) {
//         } else {
//             callback();
//         }
//     });
// }

function updateNotifications(records, callback){
    var params = {
        TableName : "mws_notifications",
        Key:{
            "attr": "notifications"
        },
        UpdateExpression: "set notification_records=:notifications",
        ExpressionAttributeValues:{
            ":notifications":JSON.stringify(records),
        }
    };

    console.log('UpdateNotifications ', records.length );

    docClient.update(params, function(err, data) {
        if (err) {
            console.log('UpdateNotifications Error', err);
        } else {
            callback();
        }
    });
}

// function putOrUpdateNotification(asin, notification, callback){
//     var params = {
//         TableName : "mws_notifications",
//         KeyConditionExpression: "#asin = :asin",
//         ExpressionAttributeNames:{
//             "#asin": "asin"
//         },
//         ExpressionAttributeValues: {
//             ":asin": asin
//         }
//     };
//
//     docClient.query(params, function(err, data) {
//         if(err){
//             console.log(err);
//         } else {
//             if(data.Items){
//                 console.log('updateNotification :', asin);
//                 updateNotification(asin, notification, callback);
//             } else {
//                 console.log('putNotification :', asin);
//                 putNotification(asin, notification, callback);
//             }
//         }
//     });
// }

function sendNotification(notification) {
    const data = JSON.stringify(notification);
    let host = process.env.SERVER_HOST?process.env.SERVER_HOST:SERVER_HOST;
    let path = process.env.SERVER_HOOK_ROUTE?process.env.SERVER_HOOK_ROUTE:SERVER_HOOK_ROUTE;
    console.log('host', host);
    const options = {
        "method": "post",
        "host": host,
        "path": path,
        "header": {
            "Content-Type": "application/json",
            "Content-Length": data.length
        }
    };

    const req = https.request(options, (res) => {
        if (res.statusCode == 200) {
            console.log('Send Notification Success :', data);
        }
    });
    req.on('error', (error) => {
        console.log('Send Notification Error :', error);
    });
    req.write(data);
    req.end();
}

async function sendNotificationWithAxios(datas) {
    const data = JSON.stringify(datas);
    let hook_url = (process.env && process.env.SERVER_HOOK_URL)?process.env.SERVER_HOOK_URL:SERVER_HOOK_URL;
    var config = {
        method: 'post',
        url: hook_url,
        headers: {
            'Content-Type': 'application/json',
        },
        data: data
    };
    console.log('hook_url', hook_url);
    await axios(config)
        .then(function (response) {
            console.log('Send Notification Success :', data);
        })
        .catch(function (error) {
            console.log('Send Notification Error :', error);
        });
}

async function process(message, index, message_count, callback) {
    // TODO process message
    const notification = JSON.parse(message.body);

    if(!asins.includes(notification.asin)){
        notifications.push({asin: notification.asin, notification});
        asins.push(notification.asin);
    }

    if((index + 1) === message_count){
        let now_time = new Date().getTime();
        if((now_time - variable["time_counter"]) > TIME_LIMIT  || asins.length > NUMBER_LIMIT){
            let send_data = notifications.map(notification => notification.notification);
            //sendNotification(send_data);
            await sendNotificationWithAxios(send_data);
            console.log(`Send Notifications Success =>  All: ${variable["all_message_count"]}, Sent: ${notifications.length}`, send_data);
            variable["time_counter"] = now_time;
            updateVars("time_counter", new Date().getTime(), ()=>{});
            variable["all_message_count"] = 0;
            updateVars("all_message_count", 0, ()=>{});
            updateNotifications([], () => { });
        } else {
            variable["all_message_count"] += message_count;
            console.log(`Send Notification Next =>  All Messages Count :${variable["all_message_count"]}`);
            updateVars("all_message_count", variable["all_message_count"], ()=>{});
            updateNotifications(notifications, () => { });
        }
    }
    // delete message
    // const params = {
    //     QueueUrl: QUEUE_URL,
    //     ReceiptHandle: message.receiptHandle,
    // };
    // SQS.deleteMessage(params, (err) => callback(err, message));
 }
 
exports.handler = (event, context, callback) => {
        getVarFromDB("time_counter", new Date().getTime(), function () {
            getVarFromDB("all_message_count", 0, function () {
                getNotifications(function () {
                    console.log(`Send Notifications Start =>  From: ${variable["time_counter"]}, All Messages Count: ${variable["all_message_count"]}, Messages Length: ${notifications.length}`);
                    let messageCount = event.Records.length;
                    event.Records.map(async (record, index) => {
                        await process(record, index, messageCount, callback);
                    })
                });
            });
        });

};
