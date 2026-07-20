# Mini Pipeline Python para Extração, Validação, Transformação e Carga de Dados

## Objetivo

Este projeto implementa um mini-pipeline de dados em Python para praticar fundamentos de pipelines, consumo de APIs, validação, transformação, SQL, banco de dados, Docker, testes unitários e preparação para orquestração com Apache Airflow.

O pipeline extrai dados de uma API pública, salva os dados brutos em JSON, valida a estrutura mínima, transforma os dados para formato tabular, gera um CSV processado e carrega os dados em uma tabela SQLite.

## Stack utilizada

- Python 3.13
- Pandas
- Requests
- Python Dotenv
- SQLite
- Pytest
- Logging
- Docker
- Docker Compose
- Conda
- Apache Airflow 3.3.0
- PostgreSQL como banco de metadados do Airflow

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
mini-python-data-pipeline/
|
├── airflow/
|   ├── config/
|   ├── dags/
|   ├── logs/
|   ├── plugins/
|   ├── .env.example
|   └── docker-compose.yaml
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
├── tests/
|   └── test_pipeline_functions.py
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

Os arquivos gerados dentro das pastas de dados e logs são ignorados pelo Git porque são artefatos produzidos em tempo de execução.

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

No Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

No Linux, macOS ou Git Bash:

```bash
cp .env.example .env
```

Exemplo de conteúdo:

```env
API_URL=https://jsonplaceholder.typicode.com/users
API_TIMEOUT_SECONDS=10
```

O arquivo `.env` contém configurações locais e não deve ser versionado.

## Como executar localmente

Na raiz do projeto, execute:

```bash
python main.py
```

## Como executar os testes

Os testes unitários validam regras importantes das etapas de validação e transformação do pipeline.

Na raiz do projeto, execute:

```bash
python -m pytest -q
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

Para persistir os arquivos gerados no Windows PowerShell:

```powershell
docker run --rm --env-file .env `
  --mount type=bind,source="${PWD}\data",target=/app/data `
  --mount type=bind,source="${PWD}\logs",target=/app/logs `
  mini-pipeline-python
```

No Prompt de Comando do Windows:

```cmd
docker run --rm --env-file .env ^
  --mount type=bind,source="%cd%\data",target=/app/data ^
  --mount type=bind,source="%cd%\logs",target=/app/logs ^
  mini-pipeline-python
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

A carga utiliza uma estratégia de full refresh. Antes de inserir os novos registros, os registros existentes na tabela são removidos, evitando duplicações em reexecuções do pipeline.

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
- Full refresh e idempotência
- Logging
- Variáveis de ambiente
- Modularização de projeto
- Testes unitários com Pytest
- Docker
- Volumes Docker
- Docker Compose
- Fundamentos de Apache Airflow

## Relação com Apache Airflow

O pipeline foi modularizado para permitir que suas funções sejam orquestradas pelo Apache Airflow.

O ambiente local do Airflow já está configurado com Docker Compose, LocalExecutor e PostgreSQL como banco de metadados. A próxima etapa será criar uma DAG para orquestrar as funções existentes nos módulos de `src`.

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

A lógica de extração, validação, transformação e carga permanece nos módulos de `src`. A DAG será responsável por definir a ordem das tasks, suas dependências, o agendamento e as políticas operacionais.

## Apache Airflow

O projeto possui um ambiente local de orquestração baseado em:

- Apache Airflow 3.3.0
- Docker Compose
- LocalExecutor
- PostgreSQL como banco de metadados

### Serviços

- PostgreSQL
- Airflow API Server
- Airflow Scheduler
- Airflow DAG Processor
- Airflow Triggerer

O PostgreSQL é utilizado exclusivamente como banco de metadados operacionais do Airflow.

O destino atual dos dados processados pelo pipeline continua sendo o SQLite. Portanto:

```text
SQLite = destino dos dados do pipeline
PostgreSQL = banco de metadados do Airflow
```

### Inicialização

Entre na pasta do Airflow:

```bash
cd airflow
```

Crie o arquivo local de configuração.

No Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

No Linux, macOS ou Git Bash:

```bash
cp .env.example .env
```

Revise as variáveis do arquivo `.env`, especialmente o usuário e a senha do administrador.

Inicialize o banco de metadados e crie o usuário administrador:

```bash
docker compose up airflow-init
```

Inicie os serviços em segundo plano:

```bash
docker compose up -d
```

Verifique o estado dos serviços:

```bash
docker compose ps -a
```

O serviço `airflow-init` deve aparecer como finalizado com código `0`, enquanto os demais serviços devem permanecer ativos.

A interface web estará disponível em:

```text
http://localhost:8080
```

### Verificações básicas

Confirmar a versão instalada:

```bash
docker compose exec airflow-scheduler airflow version
```

Confirmar o executor:

```bash
docker compose exec airflow-scheduler airflow config get-value core executor
```

Listar as DAGs reconhecidas:

```bash
docker compose exec airflow-scheduler airflow dags list
```

Verificar erros de importação:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Validar a conexão com o banco de metadados:

```bash
docker compose exec airflow-scheduler airflow db check
```

### Parada e reinicialização

Parar os serviços sem remover os containers:

```bash
docker compose stop
```

Reiniciar os mesmos containers:

```bash
docker compose restart
```

Remover os containers preservando o volume do PostgreSQL:

```bash
docker compose down
```

Recriar os containers:

```bash
docker compose up -d
```

Não utilize o comando abaixo caso queira preservar os metadados do Airflow:

```bash
docker compose down --volumes
```

A opção `--volumes` também remove o volume persistente do PostgreSQL.

## Próximos passos

- Criar uma DAG mínima no Apache Airflow.
- Orquestrar as etapas do pipeline como tasks usando TaskFlow API.
- Definir dependências entre as tasks.
- Configurar retries e timeouts.
- Adicionar testes de qualidade dos dados.
- Migrar futuramente o banco de destino do pipeline de SQLite para PostgreSQL.
- Integrar o pipeline com dbt em uma etapa posterior.