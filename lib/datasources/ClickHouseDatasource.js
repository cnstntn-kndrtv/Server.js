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
  this.tableLex = this.table + '_lex';
  this.tableLex2 = this.table + '_lex2';

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

  let results = this._queryCache.get(q);
  if (results) {
    let l = results.length;
    for (let i = 0; i < l; i++) destination._push(results[i]);
    // destination._buffer = results; //??????? asynciterator str 620

    this._getPatternCount(condition, (totalCount, err) => {
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
      this._getPatternCount(condition, (totalCount, err) => {
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

ClickHouseDatasource.prototype._getPatternCount = function (condition, cb) {
  let table;
  let sum;
  let where;
  let query;
  let condLength = 0;
  for (let key in condition) if (condition[key]) condLength++;

  switch (condLength) {
  case 1:
    table = this.tableLex;
    // S
    if (condition.subject) {
      sum = 'sc';
      where = `s1='${condition.subject}'`;
    }
    if (condition.predicate) {
      sum = 'pc';
      where = `s1='${condition.predicate}'`;
    }
    if (condition.object) {
      sum = 'oc';
      where = `s1='${condition.object}'`;
    }
    query = `SELECT sum(${sum}) from ${table} WHERE ${where} GROUP BY s1`;
    break;

  case 2:
    table = this.tableLex2;
    // SP
    if (condition.subject && condition.predicate) {
      sum = 'spc';
      where = `s1='${condition.subject}' AND s2='${condition.predicate}'`;
    }
    // SO
    if (condition.subject && condition.object) {
      sum = 'soc';
      where = `s1='${condition.subject}' AND s2='${condition.object}'`;
    }
    // PO
    if (condition.predicate && condition.object) {
      sum = 'poc';
      where = `s1='${condition.predicate}' AND s2='${condition.object}'`;
    }
    query = `SELECT sum(${sum}) from ${table} WHERE ${where} GROUP BY s1`;
    break;

  default:
    query = `SELECT count(*) from ${this.table}`;
    break;
  }

  let count = this._countCache.get(query);
  if (count) cb(count);
  else {
    // console.log(query);
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
