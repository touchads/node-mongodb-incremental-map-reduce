var mongodb = require('mongodb');
require('./index');

var Db = mongodb.Db,
	Connection = mongodb.Connection,
	Server = mongodb.Server;


var host = process.env['MONGO_NODE_DRIVER_HOST'] != null ? process.env['MONGO_NODE_DRIVER_HOST'] : 'localhost';
var port = process.env['MONGO_NODE_DRIVER_PORT'] != null ? process.env['MONGO_NODE_DRIVER_PORT'] : Connection.DEFAULT_PORT;


var db = new Db('node-mongo-examples', new Server(host, port, {}), {native_parser:false});

console.log('opening', host, port);
db.open(function(err, db) {
	if (err) return console.error('open:', err.trace.stack);
	
	db.collection('test', function(err, collection) {
		if (err) return console.error('collection test:', err.stack);
		
		collection.insert([
			{
				username: "jones",
				likes: 20,
				text: "Hello world!"
			},
			{
				username: "jimmy",
				likes: 13,
				text: "The James"
			},
			{
				username: "juju",
				likes: 0,
				text: "beans"
			}
		]);
		
		
		var map = function map() {
			emit(this.username, { count: 1, likes: this.likes });
		};
		
		var reduce = function reduce() {
			
		};
		
		var options = {
			out: {
				incremental: 'testResults'
			}
		};
		
		
		
		collection.mapReduce(map, reduce, options, function(err, results) {
			if (err) return console.error('mapreduce:', err.stack);
			
			console.log('done!');
			console.log(results);
		});
	});
	
	db.collection('log101011', function(err, collection) {
		if (err) return console.error('collection log101011:', err.stack);
		
		collection.insert([
			{ tags: ['one', 'two', 'three'] },
			{ tags: ['one', 'four', 'five'] },
			{ tags: ['two', 'six', 'seven'] }
		]);
		
		db.collection('log101111', function(err, collection) {
			if (err) return console.error('collection log101111:', err.stack);
			
			collection.insert([
				{ tags: ['one', 'four', 'eight'] },
				{ tags: ['three', 'four', 'five'] },
				{ tags: ['one', 'two', 'four'] }
			]);
			
			db.collection('log101211', function(err, collection) {
				if (err) return console.error('collection log 101211:', err.stack);
				
				
				collection.insert([
					{ tags: ['one', 'two', 'three'] },
					{ tags: ['one', 'four', 'five'] },
					{ tags: ['two', 'six', 'seven'] }
				]);
				
				
				
				
				var map = function map() {
					this.tags.forEach(function(tag) {
						emit(tag, 1);
					});
				};
				
				var reduce = function reduce(values) {
					return values.reduce(function(a, b) {
						return a + b;
					});
				};
				
				var options = {
					out: {
						incremental: 'logs',
						interval: 10000
					}
				};
				
				
				
				collection.mapReduce(map, reduce, options, function(err, results) {
					if (err) return console.error('mapreduce-last:', err.stack);
					
					console.log('done!');
					console.log(results);
				});
				
				
			});
		});
	});
});
