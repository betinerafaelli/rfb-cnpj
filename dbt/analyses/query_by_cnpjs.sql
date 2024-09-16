@set CNPJS_BASE = '40117016,41157123,42133170,43137249,44303619,45015393,46263150,47111520,48306247,49857691,50422625'

-- Todos estabelecimentos com determinados CNPJs base
SELECT 
    es.cnpj, 
    em.razao_social, 
    es.nome_fantasia, 
    es.matriz,
    
    -- Contato
    es.telefone1, 
    es.telefone2, 
    es.fax, 
    correio_eletronico email,
    mun.descricao municipio, 
    es.uf, 

    -- Cadastro
    em.capital_social,
    em.porte_empresa_str AS porte_empresa,  
    es.cnae, 
    es.cnae_secundario,
    es.situacao_cadastral_str AS situacao_cadastral,
    es.data_situacao_cadastral,
    es.motivo_situacao_cadastral,
    es.data_inicio_atividades,
    es.regime_tributacao,
    
    -- SIMPLES
    em.opcao_simples,
    em.data_opcao_simples,
    em.data_exclusao_simples,
    
    -- MEI
    em.opcao_mei,
    em.data_opcao_mei,
    em.data_exclusao_mei
FROM 
    v_estabelecimento es
    JOIN v_empresa em ON em.cnpj_base = es.cnpj_base
    JOIN municipio mun ON mun.codigo = es.municipio      
WHERE 
    LIST_CONTAINS(STRING_SPLIT(:CNPJS_BASE, ','), CAST(es.cnpj_base AS VARCHAR))
    
ORDER BY 1
