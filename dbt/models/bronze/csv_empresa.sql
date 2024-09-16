-- csv_empresa.sql
SELECT
    cnpj_base,
    LTRIM(LTRIM(CAST(razao_social AS VARCHAR), '01234567890.')) AS razao_social,
    natureza_juridica,
    qualificacao_responsavel,
    NULLIF(capital_social, 0) AS capital_social,
    porte_empresa,
    ente_federativo_responsavel
FROM
    read_csv("../data/1-csv_sources/*.EMPRECSV",
             header = false,
             columns = {'cnpj_base': 'UINTEGER',
                        'razao_social': 'VARCHAR',
                        'natureza_juridica': 'USMALLINT',
                        'qualificacao_responsavel': 'UTINYINT',
                        'capital_social': 'NUMERIC',
                        'porte_empresa': 'UTINYINT',
                        'ente_federativo_responsavel': 'VARCHAR'},
             decimal_separator = ',',
             delim = ';')
