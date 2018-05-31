let ClickHouse = require('@apla/clickhouse');
let params = {
    "host": "172.17.4.101",
    "port": 8123,
    "auth": "default",
    queryOptions: {
        database: "ldf",
    },
    // table: 'triples'
}

let ch = new ClickHouse(params);
let query = {
    subject: 'http://ldf.kloud.one/ontorugrammaform#a-chto-esli_207360',
    // predicate: 'http://www.w3.org/ns/lemon/ontolex#writtenRep',
    // predicate: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
    // object: '"тест"@ru',
    // object: 'http://www.w3.org/ns/lemon/ontolex#Form',
    object: '"а что если"@ru',
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
if (clauses.length !== 0) where = 'PREWHERE ' + clauses.join(' AND ');

let lim = [];
if (query.limit && query.offset) lim.push(query.offset);
if (query.limit) lim.push(query.limit);
let limit = (lim.length) ? 'LIMIT ' + lim.join(', ') : '';

// let q = `SELECT * FROM ${params.table} ${where} ${limit}`;
// let q = `SELECT count(*) FROM ${params.table} ${where}`;

// sp so po == s1 s2
// q = `SELECT sum(oc) from ${params.table} WHERE s1='${condition.object}' GROUP BY s1`;
// q = `SELECT sum(sc) from ${params.table} WHERE s1='${condition.subject}' GROUP BY s1`;
// q = `SELECT * from triples LIMIT 20`;
// q = `SELECT sum(soc) from lexicon2 WHERE s1='${condition.subject}' AND s2='${condition.object}' GROUP BY s1`;

let tableLex = 'triples' + '_lex';
let tableLex2 = 'triples' + '_lex2';

function count() {
    let table;
    let sum;
    let where;
    let condLength = 0;
    for (let key in condition) if (condition[key]) condLength++;
    console.log(condLength);
    
    switch (condLength) {
        case 1:
            table = tableLex;
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
            break;
        
        case 2:
            table = tableLex2;
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
            break;
        
        default:
            break;
    }

    let q = `SELECT sum(${sum}) from ${table} WHERE ${where} GROUP BY s1`;
    console.log(q);
    
    let stream = ch.query(q);
    
    console.time('q');
    stream.on('data', (row) => {
        console.log(row);
    })
    stream.on('end', () => console.timeEnd('q'))
}

count();

// let q = `SELECT * FROM triples2 WHERE predicate='http://www.w3.org/ns/lemon/ontolex#writtenRep' AND object='"буратино"@ru' LIMIT 100`

// let stream = ch.query(q);

// console.time('q')

// // stream.on('metadata', res => console.log(res))

// stream.on('data', (row) => {
//     console.log(row);
// })

// stream.on('end', () => console.timeEnd('q'))