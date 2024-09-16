-- csv_municipio.sql
SELECT
    *
FROM
    read_csv("../data/1-csv_sources/*.MUNICCSV",
             header = false,
             columns = {'codigo': 'USMALLINT',
                        'descricao': 'VARCHAR'},
             decimal_separator = ',',
             delim = ';')
