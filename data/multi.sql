CREATE TABLE postcode AS
SELECT
    *
FROM
    'data/postcode.csv';

SELECT
    code_commune_insee,
    count(*) as nb_commune
FROM
    postcode
GROUP BY
    code_commune_insee
ORDER BY
    nb_commune DESC;