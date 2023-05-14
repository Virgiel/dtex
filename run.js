import pl from 'npm:nodejs-polars';

pl.scanCSV('data/postcode.csv', {
  inferSchemaLength: 1024,
})
  .groupBy('code_commune_insee')
  .agg(pl.count('nom_de_la_commune'))
  .sort('nom_de_la_commune', true)
  .collectSync()
  .writeCSV('tmp.csv');
