# Mini Pipeline Python para Extração, Validação, Transformação e Carga de Dados

## Objetivo

Este projeto implementa um mini-pipeline de dados em Python para praticar fundamentos de pipelines, consumo de APIs, validação, transformação, SQL, banco de dados, Docker e preparação para Apache Airflow.

O pipeline extrai dados de uma API pública, salva os dados brutos em JSON, valida a estrutura mínima, transforma os dados para formato tabular, gera um CSV processado e carrega os dados em uma tabela SQLite.

## Stack utilizada

- Python 3.13
- Pandas
- Requests
- Python Dotenv
- SQLite
- Logging
- Docker
- Conda

## Fluxo do pipeline

1. Extrair dados da API JSONPlaceholder.
2. Salvar os dados brutos em `data/raw`.
3. Validar campos obrigatórios.
4. Transformar dados aninhados em estrutura tabular.
5. Salvar dados processados em `data/processed`.
6. Criar tabela SQLite.
7. Carregar os dados na tabela `users`.
8. Validar a quantidade de registros carregados.
9. Registrar logs de execução em `logs`.

## Arquitetura lógica

```text
API pública
   |
   v
Extração com requests
   |
   v
Dados brutos JSON
   |
   v
Validação estrutural
   |
   v
Transformação tabular com pandas
   |
   v
CSV processado
   |
   v
Carga em SQLite
   |
   v
Tabela users
```

## Estrutura do projeto

```text
ena-mini_pipeline_python/
|
├── data/
|   ├── raw/
|   ├── processed/
|   └── database/
|
├── logs/
|
├── src/
|   ├── __init__.py
|   ├── config.py
|   ├── database.py
|   ├── extract.py
|   ├── validate.py
|   ├── transform.py
|   ├── load.py
|   └── logger_config.py
|
├── .env.example
├── .gitignore
├── .dockerignore
├── Dockerfile
├── environment.yml
├── requirements.txt
├── main.py
└── README.md
```

## Como configurar o ambiente com Conda

Crie o ambiente:

```bash
conda env create -f environment.yml
```

Ative o ambiente:

```bash
conda activate mini_pipeline_python
```

## Configuração da API

Crie um arquivo `.env` na raiz do projeto com base no arquivo `.env.example`.

Exemplo:

```env
API_URL=https://jsonplaceholder.typicode.com/users
API_TIMEOUT_SECONDS=10
```

## Como executar localmente

Na raiz do projeto, execute:

```bash
python main.py
```

## Como executar com Docker

Construa a imagem:

```bash
docker build -t mini-pipeline-python .
```

Execute o container usando o arquivo `.env`:

```bash
docker run --rm --env-file .env mini-pipeline-python
```

Para persistir os arquivos gerados no Windows, execute com volumes:

```bash
docker run --rm --env-file .env -v "%cd%\data:/app/data" -v "%cd%\logs:/app/logs" mini-pipeline-python
```

## Saídas geradas

O pipeline gera:

- Arquivo JSON bruto em `data/raw`.
- Arquivo CSV processado em `data/processed`.
- Banco SQLite em `data/database`.
- Arquivo de log em `logs`.

Esses arquivos são ignorados pelo Git porque são artefatos gerados em tempo de execução.

## Tabela final

A tabela `users` contém os seguintes campos:

```text
user_id
name
email
city
zipcode
latitude
longitude
company_name
processed_at
```

## Conceitos praticados

- Funções Python
- Listas e dicionários
- Consumo de API
- Tratamento de erros HTTP
- Manipulação de JSON
- Validação de dados
- Transformação tabular com Pandas
- Escrita de CSV
- Carga em SQLite
- Consultas SQL básicas
- Logging
- Variáveis de ambiente
- Modularização de projeto
- Docker
- Volumes Docker

## Relação com Apache Airflow

Este projeto foi estruturado para facilitar uma futura migração para Apache Airflow.

Cada etapa pode ser convertida em uma task:

```text
extract_users
save_raw_data
validate_users
transform_users
save_processed_data
create_users_table
load_users_to_database
count_users_in_database
```

## Próximos passos

- Criar uma DAG no Apache Airflow com TaskFlow API.
- Orquestrar as etapas do pipeline como tasks.
- Adicionar testes de dados.
- Evoluir de SQLite para PostgreSQL com Docker Compose.
- Integrar o pipeline com dbt em uma etapa posterior.
