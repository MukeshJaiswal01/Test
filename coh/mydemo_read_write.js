const axios = require('axios');
const WebSocket = require('ws');
const config = require('./config.js');

if (!config.apiKey || !config.myDemoContractAddress || !config.auditContractAddress){
	console.error('Fill up all values in config.js');
	process.exit(0);
}

var wsSessionID, gotEvent;

const myDemoInstance = axios.create({
	baseURL: config.apiPrefix + config.myDemoContractAddress,
	timeout: 5000,
	headers: {'X-API-KEY': config.apiKey}
});

const auditInstance = axios.create({
	baseURL: config.apiPrefix + config.auditContractAddress,
	timeout: 5000,
	headers: {'X-API-KEY': config.apiKey}
});

const ws = new WebSocket(config.wsUrl);

ws.on('open', function open() {
	ws.send(JSON.stringify({
		'command': 'register',
		'key': config.apiKey
	}));
});

ws.on('message', function incoming(data) {
	data = JSON.parse(data);
	//console.log('Got WS data', data);
	if (data.command == 'register:nack'){
		console.error('Bad apiKey set!');
	}
	if (data.command == 'register:ack'){
		wsSessionID = data.sessionID;
		console.log('Authenticated with WS');
		console.log('Writing to demo contract...');
		myDemoInstance.post('/setContractInformation', {
			incrValue: 1,
			_note: 'Test '+new Date()
		})
		.then(function (response) {
			console.log(response.data);
			if (!response.data.success){
				process.exit(0);
			}
		})
		.catch(function (error) {
			if (error.response.data){
				console.log(error.response.data);
				if (error.response.data.error == 'unknown contract'){
					console.error('You filled in the wrong contract address!');
				}
			} else {
				console.log(error.response);
			}
			process.exit(0);
		});
		/*
			Our service expects a hearbeat every 30 seconds to keep WS connections. Since this is a one time call, we don't need to remain connected.
		*/
		//setTimeout(heartbeat, 30000);
	}
	if (data.type == 'event' && data.event_name == 'ContractIncremented'){
		gotEvent = true;
		console.log('Received setContractInformation event confirmation', data);
		console.log('Writing to audit log contract...');
		auditInstance.post('/addAuditLog', {
			_newNote: data.event_data.newNote,
			_changedBy: data.event_data.incrementedBy,
			_incrementValue: data.event_data.incrementedValue,
			_timestamp: data.ctime

		})
		.then(function (response) {
			console.log(response.data);
			if (response.data.success){
				console.log('We are done here! Exiting...');
			}
			process.exit(0);
		})
		.catch(function (error) {
			if (error.response.data){
				console.log(error.response.data);
				if (error.response.data.error == 'unknown contract'){
					console.error('You filled in the wrong contract address!');
				}
				if (error.response.data.error == 'Transaction execution will fail with supplied arguments'){
					console.error('You forgot to add the sender to whitelist!');
				}
			} else {
				console.log(error.response);
			}
			process.exit(0);
		});
	}
});

ws.on('close', function close() {
	//Websocket will auto disconnect if you do not send heartbeat.
	if (gotEvent){
		console.log('WS disconnected');
	} else {
		console.error('WS disconnected before we could receive an event - this should not have happened! Reach out to hello@blockvigil.com.');
	}
});

function heartbeat() {
	ws.send(JSON.stringify({
		command: "heartbeat",
		sessionID: wsSessionID
	}));
	setTimeout(heartbeat, 30000);
}
