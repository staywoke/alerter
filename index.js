'use strict';

var http = require('http');
var dbStream = require('./dynamostream');


 
var processEvent = function(){};


/*
    receives webhook events
  
*/
var server = http.createServer(function (req, res) {
    if (req.method === 'POST') {
        var body = '';

        req.on('data', function(chunk) {
            body += chunk;
        });

        req.on('end', function() {
            if (req.url === '/') {         
                var data = JSON.parse(body);

                console.log('received messages ', data.Records.length);

                processEvent((data.Records || []).slice(0), null, function(err) {
                    res.writeHead(200, 'OK', {'Content-Type': 'text/plain'});                        
                    return res.end();
                });
            } 
            else {
                res.writeHead(200, 'OK', {'Content-Type': 'text/plain'});                        
                return res.end();
            }
        });
    } 
    else {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end();
    }
});


// loads initial state
var init = function(){};



init(function() {
    dbStream.check();
    setInterval(function () { dbStream.check(); }, 30000);
    server.listen(process.env.PORT || 3000);
});




