-- csv_natureza_juridica.sql
SELECT
    *
FROM
    read_csv("../data/1-csv_sources/*.NATJUCSV",
             header = false,
             columns = {'codigo': 'USMALLINT',
                        'descricao': 'VARCHAR'},
             decimal_separator = ',',
             delim = ';')
