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
	options.$orderBy = { _id: 1 };
	
	delete out.incremental;
	delete out.interval;
	
	
	runMapReduce.call(this, options);
	
	// run the mapreduce at a regular interval
	setInterval(function() {
		
	}, interval);
};

function runMapReduce(options) {
	// pull the metadata if it exists for this collection
	var collection = this.db.collection('__incmapreduce__');
	var metaId = this.collectionName + ':' + out.reduce;
	
	collection.find({ _id: metaId }, function(err, results) {
		if (results.length) {
			options.$query = { _id: { $gt: results[0].lastId }};
		}
		
		// get the max id at this point in time so we can reliably store the last id of the batch
		var find = { $orderBy: { _id: -1 }, $limit: 1 };
		
		this.find(find, { _id: 1 }, function(err, results) {
			if (!results.length) return;
			
			var lastId = results[0]._id;
			var query = options.$query || (options.$query = {});
			query.$lte = lastId;
			
			origMapReduce(map, reduce, options, function(err, results) {
				if (callback) callback(err, results);
				
				if (err) {
					return;
				}
				
				collection.save({ _id: metaId, lastId: lastId });
			});
		});
	});
}