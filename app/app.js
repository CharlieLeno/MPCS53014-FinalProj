'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
require('dotenv').config()
const port = Number(process.argv[2]);
const hbase = require('hbase')

const url = new URL(process.argv[3]);

var hclient = hbase({
	host: url.hostname,
	path: url.pathname ?? "/",
	port: url.port ?? 'http' ? 8090 : 443, // http or https defaults
	protocol: url.protocol.slice(0, -1), // Don't want the colon
	encoding: 'latin1',
	auth: process.env.HBASE_AUTH
});

const tableName = 'price_hbase_qileichen';
const speedLayerTableName = 'latest_price_hbase_qileichen';
const itemName = "Boytone - 2500W 2.1-Ch. Home Theater System - Black Diamond";

hclient
	.table(tableName)
	.scan({
		filter: {
			type: 'PrefixFilter',         // Apply a PrefixFilter
			value: itemName,                // The prefix to match
		},
		maxVersions: 1,                    // Optional: limit versions of each cell
	}, (error, rows) => {
		if (error) {
			console.error('Error scanning table:', error);
		} else {
			console.info('Rows:', rows);
		}
	});

hclient.table(tableName).row('Boytone - 2500W 2.1-Ch. Home Theater System - Black DiamondBestbuy.com').get((error, value) => {
	console.info(value)
})

function counterToNumber(c) {
	return parseFloat(Number(Buffer.from(c, 'latin1').readFloatBE()).toFixed(2));
}

function rowToMap(row) {
	var stats = {}
	if (row === null) {
		return stats;
	}
	row.forEach(function (item) {
		stats[item['column']] = counterToNumber(item['$'])
	});
	return stats;
}

app.use(express.static('public'));
app.get('/prices.html',function (req, res) {
	const product_name = req.query['name'];
	console.log(product_name);
	// History price
	hclient
		.table(tableName)
		.scan({
			filter: {
				type: 'PrefixFilter',
				value: product_name,
			},
			maxVersions: 1,
		}, (error, rows) => {
			if (error) {
				console.error('Error scanning table:', error);
			} else {
				console.info('Rows:', rows);
				const groupedData = rows.reduce((acc, row) => {
					const { key, column, $ } = row;
					const value = Buffer.from($, 'latin1').readFloatBE(0).toFixed(2); // Decode value as float
					const strippedKey = key.startsWith(product_name) ? key.substring(product_name.length).trim() : key;

					if (!acc[strippedKey]) {
						acc[strippedKey] = {
							merchant: strippedKey,
							avg_price: '-',
							min_price: '-',
							max_price: '-'
						};
					}

					if (column === 'price:avg_price') acc[strippedKey].avg_price = value;
					else if (column === 'price:min_price') acc[strippedKey].min_price = value;
					else if (column === 'price:max_price') acc[strippedKey].max_price = value;

					return acc;
				}, {});

				const dataArray = Object.values(groupedData);

				// Get Latest Lowest Price Info
				hclient.table(speedLayerTableName).row(product_name).get((error, row) => {
					const parsedRow = row.reduce((acc, cell) => {
						const column = cell.column; // E.g., 'name:merchant'
						if (column === 'name:merchant') {
							acc.merchant = Buffer.from(cell.$, 'latin1').toString('utf8'); // Decode as string
						} else if (column === 'name:price') {
							acc.price = Buffer.from(cell.$, 'latin1').readFloatBE(0); // Decode as float
						}

						return acc;
					}, {});

					// Log the parsed values
					console.log('Parsed Row:', parsedRow);
					const { merchant, price } = parsedRow;

					console.log(`Merchant: ${merchant}, Price: ${price}`);

					// Generate HTML Item
					var template = filesystem.readFileSync("result.mustache").toString();
					var html = mustache.render(template,  {
						name: product_name,
						rows: dataArray,
						latest_merchant : merchant,
						latest_price : price.toFixed(2)
					});
					res.send(html);
				})

				// var template = filesystem.readFileSync("result.mustache").toString();
				// var html = mustache.render(template, {
				// 	name: product_name,
				// 	rows: dataArray
				// });
				//
				//
				// res.send(html);
			}
		});

	// Latest Lowest Price
	// hclient.table(speedLayerTableName).row(product_name).get((error, row) => {
	// 	var latestPrice = rowToMap(row);
	// 	console.log(latestPrice);
	// 	var merchant = latestPrice["name:merchant"];
	// 	var price = latestPrice["name:price"];
	//
	// 	var template = filesystem.readFileSync("latest.mustache").toString();
	// 	var html = mustache.render(template,  {
	// 		merchant : merchant,
	// 		price : price
	// 	});
	// 	res.send(html);
	// })
});

/* Send simulated weather to kafka */
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[4]});
var kafkaProducer = new Producer(kafkaClient);

app.get('/realtimePrice.html',function (req, res) {
	var name_val = req.query['name'];
	var merchant_val = req.query['merchant'];
	var curr_price_val = req.query['price']

	var report = {
		name : name_val,
		merchant : merchant_val,
		price : curr_price_val
	};

	kafkaProducer.send([{ topic: 'price_qileichen', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log(err);
			console.log(report);
			res.redirect('submit-price.html');
		});
});

app.listen(port);
