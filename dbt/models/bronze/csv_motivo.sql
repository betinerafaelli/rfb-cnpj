-- csv_motivo.sql
SELECT
    *
FROM
    read_csv("../data/1-csv_sources/*.MOTICSV",
             header = false,
             columns = {'codigo': 'USMALLINT',
                        'descricao': 'VARCHAR'},
             decimal_separator = ',',
             delim = ';')
