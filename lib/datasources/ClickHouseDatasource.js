let Datasource = require('./Datasource');
let LRU = require('lru-cache');
let ClickHouse = require('@apla/clickhouse');

// Creates a new ClickHouseDatasource
function ClickHouseDatasource(options) {
  if (!(this instanceof ClickHouseDatasource))
    return new ClickHouseDatasource(options);
  Datasource.call(this, options);

  // cache
  this._countCache = new LRU({ max: 1000, maxAge: 1000 * 60 * 60 * 3 });
  this._queryCache = new LRU({ max: 1000, maxAge: 1000 * 60 * 60 * 3 });

  this._options = options = options || {};
}
Datasource.extend(ClickHouseDatasource, ['triplePattern', 'limit', 'offset', 'totalCount']);

// Prepares the datasource for querying
ClickHouseDatasource.prototype._initialize = function (done) {
  this._dbConfig = {
    host: this._options.host,
    port: this._options.port,
    queryOptions: {
      database: this._options.database,
    },
  };

  this._dbConfig.auth = this._options.username || 'default';
  if (this._options.password) this._dbConfig.auth += `:${this._options.password}`;

  this.table = this._options.table || 'triples';

  this._db = new ClickHouse(this._dbConfig);

  // check connection
  this._db.query(`SELECT * from ${this.table} LIMIT 1`, (err, res) => {
    if (err) done(new Error('ClickHouse triplestore authentication error ' + err));
    else done();
  });
};

// Writes the results of the query to the given triple stream
ClickHouseDatasource.prototype._executeQuery = async function (query, destination) {
  let condition = {
    subject: query.subject,
    predicate: query.predicate,
    object: query.object,
  };
  let where = '';
  let clauses = [];
  for (let key in condition) {
    let val = condition[key];
    if (val) clauses.push(`${key}='${val}'`);
  }
  if (clauses.length !== 0) where = 'WHERE ' + clauses.join(' AND ');

  let lim = [];
  if (query.limit && query.offset) lim.push(query.offset);
  if (query.limit) lim.push(query.limit);
  let limit = (lim.length) ? 'LIMIT ' + lim.join(', ') : '';

  let q = `SELECT subject, predicate, object FROM ${this.table} ${where} ${limit}`;
  let countQuery = `SELECT count(*) FROM ${this.table} ${where}`;

  let results = this._queryCache.get(q);
  if (results) {
    let l = results.length;
    for (let i = 0; i < l; i++) destination._push(results[i]);
    // destination._buffer = results; //??????? asynciterator str 620

    this._getPatternCount(countQuery, (totalCount, err) => {
      if (err) destination.emit('error', new Error(err));

      destination.setProperty('metadata', {
        totalCount: totalCount,
        hasExactCount: l,
      });
      destination.close();
    });
  }

  else {
    results = [];

    let stream = this._db.query(q, { dataObjects: true });
    let resultsCount = 0;
    stream.on('data', (row) => {
      resultsCount++;
      destination._push(row);
      results.push(row); // cache
    });

    stream.on('end', () => {
      this._getPatternCount(countQuery, (totalCount, err) => {
        if (err) destination.emit('error', new Error(err));

        destination.setProperty('metadata', {
          totalCount: totalCount,
          hasExactCount: resultsCount,
        });
        this._queryCache.set(q, results); // cache
        destination.close();
      });
    });

    stream.on('error', function (err) {
      destination.emit('error', new Error(err));
    });
  }
};

ClickHouseDatasource.prototype._getPatternCount = function (query, cb) {
  let count = this._countCache.get(query);
  if (count) cb(count);
  else {
    let stream = this._db.query(query);
    stream.on('data', (row) => {
      count = row[0];
      this._countCache.set(query, count);
      cb(count, null);
    });

    stream.on('error', function (err) {
      cb(null, err);
    });
  }
};

module.exports = ClickHouseDatasource;
