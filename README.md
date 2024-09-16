# rfb-cnpj

Ferramentas para consulta da base nacional de CNPJ da RFB.

Disponível em https://dados.rfb.gov.br/CNPJ.



# Requisitos

- Python 3.11
- Poetry: https://python-poetry.org
- 40 GB de espaço livre em disco
- 32 GB de RAM (recomendável)



# Inicialização

A partir da pasta raiz do projeto, execute:

```bash
# Inicializa ambiente python
poetry install
poetry shell

# Faz o download dos dados inicais.
build

# Gera o banco de dados DuckDB, os arquivos .parquet e as views.
cd ./dbt
dbt run
```

O processo inteiro deve levar em torno de 20 minutos (variando de acordo com seu computador e conexão de rede).

As funções do script de build são idempotentes. Etapas já executadas não serão realizadas novamente.



# Visão geral

Todos os dados necessários (baixados ou gerados) ficam na pasta [`data/`](./data/). 

Os arquivos de dados estão estruturados em _camadas_, cada qual dependente da anterior:

- Camada 0: Arquivos `.zip` disponibilizados pela RFB.
- Camada 1: Arquivos `.csv` extraídos dos arquivos `.zip` e convertidos para `utf-8` (_bronze_).
- Camada 2: Arquivos `.parquet` gerados a partir dos arquivos `.csv` (_silver_).
- Camada 3: _Views_ e consultas SQL sobre os arquivos `.parquet` (_gold_).

O script de build incialmente baixa os arquivos `.zip` da RFB e descompacta-os na pasta da camada [_bronze_](./data/0-zip_sources/).

Após isso, o [dbt](https://www.getdbt.com/) cria um banco de dados [DuckDB](https://duckdb.org/) que irá acessar os dados `.csv` e gerar os arquivos e views das demais camadas.

Ao final do processo, pode-se rodar [consultas SQL](./dbt/analyses/) diretamente sobre o banco DuckDB.



# Consultas

Para consultar o banco de dados DuckDB uma boa opção é o [DBeaver](https://dbeaver.io/). 

> Consulte as [instruções de configuração](https://duckdb.org/docs/guides/sql_editors/dbeaver.html) na documentação do DuckDB.

Alguns exemplos de consultas estão disponíveis na pasta [`./dbt/analyses`](./dbt/analyses/).



### Pasta raiz do projeto
Após conectar no banco de dados, execute o seguinte SQL:

```sql
-- Deve apontar para a pasta raiz do projeto dbt (local do arquivo `dbt_project.yml`).
SET file_search_path = '/tmp/rfb-cnpj/data/'
```

As _views_ dos banco de dados utilizam caminhos relativos a essa pasta. Para mais informações consulte [essa discussão](https://github.com/dbeaver/dbeaver/issues/21671#issuecomment-2147389720).



# FAQ

P. Ao tentar executar uma consulta estou recebendo o erro:

> `IO Error: No files found that match the pattern "../data/2-parquet_sources/empresa.parquet"`.

R. Você não definiu a [pasta raiz do projeto](#pasta-raiz-do-projeto) na variável `file_search_path`.
