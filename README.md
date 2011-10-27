mongodb-incremental-mapreduce
=============================

[MongoDB](http://www.mongodb.org/) allows [incremental map/reduce](http://www.mongodb.org/display/DOCS/MapReduce#MapReduce-IncrementalMapreduce), but you must build a solution to do it. This library provides that solution for you, out of the box.

Usage
-----

### Installation

Install node.js (http://nodejs.org) first, then npm (http://npmjs.org). mongodb-incremental-mapreduce relies on the [mongodb](https://github.com/christkv/node-mongodb-native) npm module. This has been tested with mongodb@0.9.4. 

```bash
npm install mongodb
npm install mongodb-incremental-mapreduce
```

### Usage

MongoDB Incremental Map/Reduce (IMR for short) extends the existing functionality of the [mongodb](https://github.com/christkv/node-mongodb-native) node.js library by adding an additional "out" option.

```javascript
var mongodb = require('mongodb');
var incremental = require('mongodb-incremental-mapreduce'); // must be required, even if not using the package object

// ... get access to a 'comments' collection in the regular way ...
// Here we want to reduce comment counts and likes per username

var map = function() {
	emit(this.username, { count: 1, likes: this.likes });
}

var reduce = function() {
	var totals = { count: 0, likes: 0 };
	values.forEach(function(current) {
		totals.count += current.count;
		totals.likes += current.likes;
	});
	return totals;
}

collection.mapReduce(map, reduce, { out: { incremental: 'commentCounts' } }, function(err, results) {
	console.log('IMR done!');
});
```

IMR will only map/reduce new docs that have been inserted since the last time you called collection.mapReduce for a given collection-destination combo (in the example above that would be comments and commentCounts).

You can specify an interval in seconds at which the map/reduce should run. If you do this you will get the intervalId back from the mapReduce call which you may then use to clear the interval with later.

```javascript
// run every minute
var intervalId = collection.mapReduce(map, reduce, { out: { incremental: 'commentCounts', interval: 60 } }, function(err, results) {
	if (err) clearInterval(intervalId);
	else console.log('IMR done!', 'Again!');
});
```

Sometimes it is desirable to map/reduce across many collections, such as rotating log collections. This can be done by using a slightly modified mapReduce method in the mongodb-incremental-mapreduce package.

```javascript
var incremental = require('mongodb-incremental-mapreduce');

var collections = { collections: 'log*', db: db };
var options = { out: { incremental: 'logs' } };
incremental.mapReduce(collections, map, reduce, options, callback);
```

You can use a either a string with an asterisk (e.g. "logs*" will match "logs-10" and "logster") or a regular expression object. Note that even if your string or expression matches the "out" collection IMR will not include it, so you are safe taking "logs*" and reducing into "logs". You can even use this method for other types of mapReduce methods on multiple collections, even if it isn't incremental. And you may add interval to any type this way.

### How it works

Under the hood IMR stores the _id of the last doc it reduced in a metadata table called "incmapreduce". The next time mapReduce is called it will load the last _id from the metadata collection and go from there.

### Additional work

There is a possibility of docs being double counted in a map/reduce if you run it again before the former map/reduce is finished. This is likely to occur when your map/reduce is taking longer than the interval at which you want it to be updated. Currently it is on the user of the library to ensure this doesn't happen, but it would be nice to have it built in so that it will skip subsequent map/reduces if one is still running.

There are probably optimizations that can be done also.