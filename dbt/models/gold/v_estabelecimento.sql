-- v_estabelecimento.sql
SELECT
    es.cnpj_base,
    es.cnpj_ordem,
    es.cnpj_dv,
    es.matriz,
    es.nome_fantasia,
    es.situacao_cadastral,
    es.data_situacao_cadastral,
    es.motivo_situacao_cadastral,
    CASE es.situacao_cadastral
        WHEN 1 THEN 'Nula'
        WHEN 2 THEN 'Ativa'
        WHEN 3 THEN 'Suspensa'
        WHEN 4 THEN 'Inapta'
        WHEN 8 THEN 'Baixada'
    ELSE
        '?'
    END	AS situacao_cadastral_str,
    es.nome_cidade_exterior,
    es.pais,
    es.data_inicio_atividades,
    es.cnae,
    es.cnae_secundario,
    es.tipo_logradouro,
    es.logradouro,
    es.numero,
    es.complemento,
    es.bairro,
    es.cep,
    es.uf,
    es.municipio,
    LTRIM(es.ddd1, '0') || LPAD(es.telefone1, 8, '?') telefone1,
    LTRIM(es.ddd2, '0') || LPAD(es.telefone2, 8, '?') telefone2,
    LTRIM(es.ddd_fax, '0') || LPAD(es.fax, 8, '?') fax,
    es.ddd1,
    es.telefone1,
    es.ddd2,
    es.telefone2,
    es.ddd_fax,
    es.fax,
    es.correio_eletronico,
    es.situacao_especial,
    es.data_situacao_especial,
    es.cnpj,
    (
        -- Retorna o regime de tributacao do maior ano para cada cnpj.
        SELECT REPLACE(REPLACE(arg_max(rt.tributacao, rt.ano), 'LUCRO ', ''), ' DO IRPJ', '')
        FROM {{ ref('regime_tributacao') }} AS rt
        WHERE rt.cnpj = es.cnpj
        GROUP BY rt.cnpj
    ) AS regime_tributacao
FROM
    {{ ref('estabelecimento') }} AS es
