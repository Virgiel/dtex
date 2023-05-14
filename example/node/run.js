const pl = require('nodejs-polars');
const { ex } = require('dtex');

const path = '../../data/nfl.csv';
const df = pl.readCSV(path).sort('sec', true);
console.log(df)
ex(path)
