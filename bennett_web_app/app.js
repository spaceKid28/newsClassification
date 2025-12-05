'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = new URL(process.argv[3]);
const hbase = require('hbase');
// require('dotenv').config()
const port = Number(process.argv[2]);

var hclient = hbase({
	host: url.hostname,
	path: url.pathname ?? "/",
	port: url.port, // http or https defaults
	protocol: url.protocol.slice(0, -1), // Don't want the colon
	encoding: 'latin1',
	auth: process.env.HBASE_AUTH
});

function counterToNumber(c) {
	return Number(Buffer.from(c).readBigInt64BE());
}
function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = counterToNumber(item['$'])
	});
	return stats;
}

hclient.table('weather_delays_by_route').row('ORDAUS').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})


hclient.table('weather_delays_by_route').row('ORDAUS').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})


app.use(express.static('public'));
app.get('/delays.html',function (req, res) {
    const route=req.query['origin'] + req.query['dest'];
    console.log(route);
	hclient.table('weather_delays_by_route').row(route).get(function (err, cells) {
		const weatherInfo = rowToMap(cells);
		console.log(weatherInfo)
		function weather_delay(weather) {
			var flights = weatherInfo["delay:" + weather + "_flights"];
			var delays = weatherInfo["delay:" + weather + "_delays"];
			if(flights == 0)
				return " - ";
			return (delays/flights).toFixed(1); /* One decimal place */
		}

		var template = filesystem.readFileSync("result.mustache").toString();
		var html = mustache.render(template,  {
			origin : req.query['origin'],
			dest : req.query['dest'],
			clear_dly : weather_delay("clear"),
			fog_dly : weather_delay("fog"),
			rain_dly : weather_delay("rain"),
			snow_dly : weather_delay("snow"),
			hail_dly : weather_delay("hail"),
			thunder_dly : weather_delay("thunder"),
			tornado_dly : weather_delay("tornado")
		});
		res.send(html);
	});
});



const { Kafka } = require('kafkajs');

const kafkajsClient = new Kafka({
	clientId: 'test-client',
	brokers: ['boot-public-byg.mpcs53014kafka.2siu49.c2.kafka.us-east-1.amazonaws.com:9196'],
	ssl: true,
	sasl: {
		mechanism: 'scram-sha-512',
		username: 'mpcs53014-2025',
		password: 'A3v4rd4@ujjw'
	},
	connectionTimeout: 10000,
	requestTimeout: 30000
});

const testConnection = async () => {
	try {
		const admin = kafkajsClient.admin();
		await admin.connect();
		console.log('✅ KafkaJS connection successful!');

		const topics = await admin.listTopics();
		console.log('Available topics:', topics);

		await admin.disconnect();
	} catch (error) {
		console.error('❌ KafkaJS connection error:', error);
	}
};
const producer= kafkajsClient.producer()
producer.connect()
testConnection()

app.get('/weather.html',function (req, res) {
	var station_val = req.query['station'];
	var fog_val = (req.query['fog']) ? true : false;
	var rain_val = (req.query['rain']) ? true : false;
	var snow_val = (req.query['snow']) ? true : false;
	var hail_val = (req.query['hail']) ? true : false;
	var thunder_val = (req.query['thunder']) ? true : false;
	var tornado_val = (req.query['tornado']) ? true : false;
	var report = {
		station : station_val,
		clear : !fog_val && !rain_val && !snow_val && !hail_val && !thunder_val && !tornado_val,
		fog : fog_val,
		rain : rain_val,
		snow : snow_val,
		hail : hail_val,
		thunder : thunder_val,
		tornado : tornado_val
	};

	producer.send({
		topic: 'weather-reports',
		messages: [{ value: JSON.stringify(report)}]
	}).then(_ => res.redirect('submit-weather.html'))
		.catch(e => {
			console.error(`[example/producer] ${e.message}`, e);
			res.redirect('submit-weather.html');
		})
});

app.listen(port);
