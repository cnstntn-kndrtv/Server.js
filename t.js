let ClickHouse = require('@apla/clickhouse');
let params = {
    "host": "172.17.4.101",
    "port": 8123,
    "auth": "default",
    queryOptions: {
        database: "ldf",
    },
    table: 'triples'
}

let ch = new ClickHouse(params);
let query = {
    // subject: 'http://ldf.kloud.one/ontorugrammaform#345755_test:form1_test',
    // predicate: 'http://www.w3.org/ns/lemon/ontolex#writtenRep',
    // predicate: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
    // object: '"тест"@ru',
    object: 'http://www.w3.org/ns/lemon/ontolex#Form',
    // object: '"буратино"@ru',
    limit: 100,
}
let condition = {
    subject: query.subject,
    predicate: query.predicate,
    object: query.object
}
let where = '';
let clauses = [];
for (let key in condition) {
    let val = condition[key]
    if (val) clauses.push(`${key}='${val}'`);
}
if (clauses.length != 0) where = 'PREWHERE ' + clauses.join(' AND ');

let lim = [];
if (query.limit && query.offset) lim.push(query.offset);
if (query.limit) lim.push(query.limit);
let limit = (lim.length) ? 'LIMIT ' + lim.join(', ') : '';

let q = `SELECT * FROM ${params.table} ${where} ${limit}`;
// let q = `SELECT count(*) FROM ${params.table} ${where}`;

// let q = `SELECT * FROM triples2 WHERE predicate='http://www.w3.org/ns/lemon/ontolex#writtenRep' AND object='"буратино"@ru' LIMIT 100`

let stream = ch.query(q);

console.time('q')

// stream.on('metadata', res => console.log(res))

stream.on('data', (row) => {
    console.log(row);
})

stream.on('end', () => console.timeEnd('q'))