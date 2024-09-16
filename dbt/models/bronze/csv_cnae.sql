-- csv_cnae.sql
SELECT
    *
FROM
    read_csv("../data/1-csv_sources/*.CNAECSV",
             header = false,
             columns = {'codigo': 'UINTEGER',
                        'descricao': 'VARCHAR'},
             decimal_separator = ',',
             delim = ';')