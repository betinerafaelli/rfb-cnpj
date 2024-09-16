-- csv_simples.sql
SELECT
    COALESCE(cnpj_base, 0) cnpj_base,
    CAST(opcao_simples = 'S' AS BOOLEAN) AS opcao_simples,
    data_opcao_simples,
    data_exclusao_simples,
    CAST(opcao_mei = 'S' AS BOOLEAN) AS opcao_mei,
    data_opcao_mei,
    data_exclusao_mei
FROM
    read_csv("../data/1-csv_sources/*.SIMPLES.CSV.*",
             header = false,
             columns = {'cnpj_base': 'UINTEGER',
                        'opcao_simples': 'VARCHAR',
                        'data_opcao_simples': 'DATE',
                        'data_exclusao_simples': 'DATE',
                        'opcao_mei': 'VARCHAR',
                        'data_opcao_mei': 'DATE',
                        'data_exclusao_mei': 'DATE'},
             dateformat = '%Y%M%d',
             nullstr = ['', '00000000'],
             decimal_separator = ',',
             delim = ';')
