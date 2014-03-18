var mongodb = require('mongodb');
var Collection = mongodb.Collection;


var origMapReduce = Collection.prototype.mapReduce;


exports.mapReduce = function incMapReduce(collection, map, reduce, options, callback) {
	if ('function' === typeof options) callback = options, options = {};
	var db = collection.db;
	var isMatch = collection.collections;
	if (!options) options = {};
	else options = clone(options);
	if (!(isMatch instanceof RegExp))
		isMatch = new RegExp('^' + RegExp.escape(isMatch).replace(/\\\*/g, '.*') + '$');

	if (options.out) {
		var interval = options.out.interval;
		delete options.out.interval;
		var outCollection = options.out.interval || options.out.reduce || options.out.replace || options.out.merge;
	}

	db.collectionNames(function(err, collectionNames) {
		if (err) return callback(err);

		collectionNames = collectionNames.map(getName).filter(function(name) {
			return (name != outCollection && isMatch.test(name));
		});

		var count = 0;
		collectionNames.forEach(function(collectionName) {
			count++;
			db.collection(collectionName, function(err, collection) {
				//				console.log('handling collection:', collectionName);
				collection.mapReduce(map, reduce, options, function(err, results) {
					//					console.log('done with:', collectionName);
					if (!--count) {
						if (callback) callback(null, results);
					}
				});
			});
		});
	});

	if (interval) {
		return setInterval(function() {
			exports.mapReduce(collection, map, reduce, options, callback);
		}, interval * 1000);
	}
};


Collection.prototype.mapReduce = function incMapReduce(map, reduce, options, callback) {
	if ('function' === typeof options) callback = options, options = {};
	else options = clone(options);

	var collection = this;
	var out = options.out;
	if (!out || !out.incremental) {
		return origMapReduce.call(collection, map, reduce, options, callback);
	}

	// fix out
	out.reduce = out.incremental;

	var interval = out.interval;
	delete out.incremental;
	delete out.interval;


	runMapReduce(collection, map, reduce, options, callback);

	// run the mapreduce at a regular interval
	if (interval) {
		return setInterval(function() {
			runMapReduce(collection, map, reduce, options, callback);
		}, interval * 1000);
	}
};

function runMapReduce(collection, map, reduce, options, callback) {

	// pull the metadata if it exists for this collection
	collection.db.collection('incmapreduce', function(err, metaCollection) {

		var metaId = collection.collectionName + ':' + options.out.reduce;
		//		console.log('metaId:', metaId);

		metaCollection.findOne({
			_id: metaId
		}, function(err, meta) {

			var query = {};

			if (meta) {
				//				console.log('Found META:', meta);
				options.query = query = {
					_id: {
						$gt: meta.lastId
					}
				};
			}

			// get the max id at this point in time so we can reliably store the last id of the batch
			var cursor = collection.find(query, {
				_id: 1
			});
			cursor.sort({
				_id: -1
			});

			cursor.nextObject(function(err, doc) {
				if (!doc) {
					//					console.log('NO MORE TO ADD AT THIS TIME');
					if (callback) callback(null, null);
					return;
				}

				var lastId = doc._id;
				var query = options.$query || (options.$query = {});
				query.$lte = lastId;

				origMapReduce.call(collection, map, reduce, options, function(err, results) {

					if (err) {
						if (callback) callback(err);
						return;
					}
					saveMeta(metaCollection, metaId, lastId, callback);
				});
			});
		});
	});
}


function saveMeta(metaCollection, metaId, lastId, callback) {
        console.log(lastId);
        metaCollection.update({
                _id: metaId
        }, {
                $set: {
                        lastId: lastId
                }
        }, {
                safe: true,
                upsert: true
        }, function(err) {
                if (err) {
                        console.warn(err.message);
                        saveMeta(metaCollection, metaId, lastId, callback);
                } else {
                        if (callback) callback(err);
                }
        });
}

function getName(collection) {
	return collection.name.split('.').slice(1).join('.');
}

function clone(obj) {
	var result = {};
	for (var i in obj) {
		var value = obj[i];
		if (value && typeof value === 'object') result[i] = clone(value);
		else result[i] = value;
	}
	return result;
}

RegExp.escape = function(text) {
	return text.replace(/[-[\]{}()*+?.,\\^$|#\s]/g, "\\$&");
};