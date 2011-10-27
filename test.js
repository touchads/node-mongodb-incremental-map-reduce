var mongodb = require('mongodb');
var incremental = require('./index');

var Db = mongodb.Db,
	Connection = mongodb.Connection,
	Server = mongodb.Server;


var host = process.env['MONGO_NODE_DRIVER_HOST'] != null ? process.env['MONGO_NODE_DRIVER_HOST'] : 'localhost';
var port = process.env['MONGO_NODE_DRIVER_PORT'] != null ? process.env['MONGO_NODE_DRIVER_PORT'] : Connection.DEFAULT_PORT;


var db = new Db('node-mongo-examples', new Server(host, port, {}), {});

console.log('opening', host, port);
db.open(function(err, db) {
	
//	incremental.mapReduce({ db: db, collections: 'log*' });
//	
//	return;
	if (err) return console.error('open:', err);
	
	db.dropCollection('test', function(err, result) {
		console.log("dropped test: ", result);
	});
	db.dropCollection('testResults', function(err, result) {
		console.log("dropped testResults: ", result);
	});
	db.dropCollection('log101011', function(err, result) {
		console.log("dropped log101011: ", result);
	});
	db.dropCollection('log101111', function(err, result) {
		console.log("dropped log101111: ", result);
	});
	db.dropCollection('log101211', function(err, result) {
		console.log("dropped log101211: ", result);
	});
	db.dropCollection('logs', function(err, result) {
		console.log("dropped logs: ", result);
	});
	db.dropCollection('incmapreduce', function(err, result) {
		console.log("dropped incmapreduce: ", result);
	});
	
	
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
				username: "jones",
				likes: 5,
				text: "Second Comment"
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
		
		var reduce = function reduce(key, values) {
			var totals = { count: 0, likes: 0 };
			values.forEach(function(current) {
				totals.count += current.count;
				totals.likes += current.likes;
			});
			return totals;
		};
		
		var options = {
			out: {
				incremental: 'testResults'
			}
		};
		
		
		console.log('starting map reduce');
		collection.mapReduce(map, reduce, options, function(err, results) {
			if (err) return console.error('mapreduce:', err);
			
			console.log('done with comments!');
		});
	});
	
	
	db.collection('log101011', function(err, collection) {
		if (err) return console.error('collection log101011:', err);
		
		var map = function map() {
			this.tags.forEach(function(tag) {
				emit(tag, 1);
			});
		};
		
		var reduce = function reduce(key, values) {
			var sum = 0;
			values.forEach(function(value) {
				sum += value;
			});
			return sum;
		};
		
		var options = {
			out: {
				incremental: 'logs'
			}
		};
		
		collection.insert([
			{ tags: ['one', 'two', 'three'] },
			{ tags: ['one', 'four', 'five'] },
			{ tags: ['two', 'six', 'seven'] }
		]);
		
		db.collection('log101111', function(err, collection) {
			if (err) return console.error('collection log101111:', err);
			
			collection.insert([
				{ tags: ['one', 'four', 'eight'] },
				{ tags: ['three', 'four', 'five'] },
				{ tags: ['one', 'two', 'four'] }
			]);
			
			
			incremental.mapReduce({ collections: 'log*', db: db }, map, reduce, options, function(err, results) {
				if (err) return console.error('mapreduce-last:', err);
				
				console.log('done with 2 logs!');
				
				db.collection('log101211', function(err, collection) {
					if (err) return console.error('collection log 101211:', err);
					
					
					collection.insert([
						{ tags: ['one', 'two', 'three'] },
						{ tags: ['one', 'four', 'five'] },
						{ tags: ['two', 'six', 'seven'] }
					]);
					
					
					incremental.mapReduce({ collections: 'log*', db: db }, map, reduce, options, function(err, results) {
						if (err) return console.error('mapreduce-last:', err);
						
						console.log('done with logs!');
						db.close();
					});
					
					
				});
			});
		});
	});
});
