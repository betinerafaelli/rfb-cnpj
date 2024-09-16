-- csv_regime_tributacao.sql
SELECT *
FROM
    read_csv("../data/1-csv_sources/Imunes e isentas.csv",
                header = false,
                columns = {'ano': 'USMALLINT', 
                           'cnpj': 'VARCHAR', 
                           'cnpj_scp': 'VARCHAR', 
                           'tributacao': 'VARCHAR', 
                           'qtd': 'UTINYINT'},
                nullstr = ['', '0'],
                decimal_separator = ',',
                delim = ';')

UNION ALL

SELECT *
FROM
    read_csv("../data/1-csv_sources/Lucro Arbitrado.csv",
                header = true,
                columns = {'ano': 'USMALLINT', 
                           'cnpj': 'VARCHAR', 
                           'cnpj_scp': 'VARCHAR', 
                           'tributacao': 'VARCHAR', 
                           'qtd': 'UTINYINT'},
                nullstr = ['', '0'],
                decimal_separator = '.',
                delim = ',')

UNION ALL

SELECT *
FROM
    read_csv("../data/1-csv_sources/Lucro Presumido *.csv",
                header = false,
                columns = {'ano': 'USMALLINT', 
                           'cnpj': 'VARCHAR', 
                           'cnpj_scp': 'VARCHAR', 
                           'tributacao': 'VARCHAR', 
                           'qtd': 'UTINYINT'},
                nullstr = ['', '0'],
                decimal_separator = ',',
                delim = ';')
UNION ALL

SELECT *
FROM
    read_csv("../data/1-csv_sources/Lucro Real.csv",
                header = true,
                columns = {'ano': 'USMALLINT', 
                           'cnpj': 'VARCHAR', 
                           'cnpj_scp': 'VARCHAR', 
                           'tributacao': 'VARCHAR', 
                           'qtd': 'UTINYINT'},
                nullstr = ['', '0'],
                decimal_separator = '.',
                delim = ',')
