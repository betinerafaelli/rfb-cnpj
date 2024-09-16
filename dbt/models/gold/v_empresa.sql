-- v_empresa.sql
SELECT
    em.cnpj_base,
    em.razao_social,
    em.natureza_juridica,
    em.qualificacao_responsavel,
    em.capital_social,
    em.porte_empresa,
    CASE em.porte_empresa
        WHEN 1 THEN 'Não informado'
        WHEN 2 THEN 'Micro empresa'
        WHEN 4 THEN 'Empresa de pequeno porte'
        WHEN 5 THEN 'Demais'
    ELSE
        '?'
    END AS porte_empresa_str,
    em.ente_federativo_responsavel,
    s.opcao_simples,
    s.data_opcao_simples,
    s.data_exclusao_simples,
    s.opcao_mei,
    s.data_opcao_mei,
    s.data_exclusao_mei
FROM
    {{ ref('empresa') }} AS em
    LEFT JOIN {{ ref('simples') }} AS s
            ON s.cnpj_base = em.cnpj_base
