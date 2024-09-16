-- csv_pais.sql
SELECT
    *
FROM
    read_csv("../data/1-csv_sources/*.PAISCSV",
             header = false,
             columns = {'codigo': 'USMALLINT',
                        'descricao': 'VARCHAR'},
             decimal_separator = ',',
             delim = ';')
