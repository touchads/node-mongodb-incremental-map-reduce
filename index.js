var Collection = require('mongodb').Collection;


var origMapReduce = Collection.prototype.mapReduce;


Collection.prototype.mapReduce = function mapReduce(map, reduce, options, callback) {
	if ('function' === typeof options) callback = options, options = {};

	var out = options.out;
	if (!out || !out.incremental) {
		return origMapReduce(map, reduce, options, callback);
	}
	
	// fix out
	out.reduce = out.incremental;
	
	var interval = out.interval || 10000;
	options.$orderBy = out.sort || {_id: 1};
	
	delete out.incremental;
	delete out.interval;
	delete out.sort;
	
	// pull the metadata if it exists for this collection
	var collection = this.db.collection('__incmapreduce__');
	var metaId = this.collectionName + ':' + out.reduce;
	
	collection.find({ _id: metaId }, function(err, result) {
		if (result) {
			options.$query = {};
			for (var i in options.$orderBy) {
				// TODO determine this result structure and fix the line accordingly
				options.$query[i] = { $gt: result[i] || 0 };
			}
		}
		
		
		// run the mapreduce at a regular interval
		setInterval(function() {
			// get the max id at this point in time so we can reliably mapreduce
			// TODO fix this orderby and select fields
			var find = { $query:options.$query, $orderBy: reverse.$orderBy, $limit: 1 }, { /* only orderBy fields */};
			this.find(find, function(err, results) {
				// TODO results needs to be what we want
				// TODO add the $lte to the $query 
				origMapReduce(map, reduce, options, function(err, results) {
					var lastProcessed = results;
					
					if (callback) callback(err, results);
					
					if (err) {
						return;
					}
					
					// TODO find the last reduced items and store their sort field's value here
					collection.save({ _id: metaId, lastProcessed: lastProcessed });
				});
			});
		}, interval);
	});
};
