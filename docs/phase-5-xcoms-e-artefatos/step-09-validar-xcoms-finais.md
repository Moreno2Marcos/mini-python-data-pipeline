# Fase 5 — Passo 9: validar os XComs finais

## Objetivo

Comprovar que os XComs da DAG transportam somente referências e
metadados pequenos, mantendo os datasets completos nos artefatos JSON,
CSV e SQLite.

## Fluxo anterior

```mermaid
flowchart TD

    subgraph OLD1["Transporte inadequado pelo XCom"]
        direction TB

        A[Extração da API]
        B[Lista completa de usuários]
        C[XCom com dataset raw]
        D[Transformação]
        E[Lista completa de registros]
        F[XCom com dataset processado]

        A --> B
        B --> C
        C --> D
        D --> E
        E --> F
    end

    classDef unchanged fill:#E8EEF7,stroke:#4A6280,color:#000;
    classDef oldChange fill:#FFF1F1,stroke:#C62828,color:#000;

    class A,D unchanged;
    class B,C,E,F oldChange;

    style OLD1 fill:#FFF8F8,stroke:#C62828,stroke-width:2px,stroke-dasharray:6 6
```

## Fluxo atual

```mermaid
flowchart TD

    A[Extração da API]

    subgraph RAW["Dataset raw fora do XCom"]
        direction TB
        B[JSON raw]
    end

    C[RawMetadata no XCom]
    D[Transformação em memória]

    subgraph PROCESSED["Dataset processado fora do XCom"]
        direction TB
        E[CSV processado]
    end

    F[ProcessedMetadata no XCom]

    subgraph DATABASE["Dataset carregado fora do XCom"]
        direction TB
        G[SQLite]
    end

    H[LoadMetadata no XCom]
    I[Metadados da validação final]

    A --> B
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    G --> H
    H --> I

    classDef artifact fill:#E8EEF7,stroke:#4A6280,color:#000;
    classDef metadata fill:#E8F5E9,stroke:#2E7D32,color:#000;

    class A,D artifact;
    class B,E,G artifact;
    class C,F,H,I metadata;

    style RAW fill:#F5F8FC,stroke:#4A6280,stroke-width:2px
    style PROCESSED fill:#F5F8FC,stroke:#4A6280,stroke-width:2px
    style DATABASE fill:#F5F8FC,stroke:#4A6280,stroke-width:2px
```

## Conceitos de Engenharia de Dados

### Plano de controle e plano de dados

Os XComs pertencem ao plano de controle e transportam caminhos,
contagens, nomes e status.

JSON, CSV e SQLite pertencem ao plano de dados e armazenam os datasets
completos.

### Pointer/reference pattern

As tasks downstream recebem referências para os artefatos e
reconstroem os dados somente quando necessário.

### Fronteira de serialização

Os objetos que atravessam tasks precisam ser serializáveis, pequenos e
independentes do processo que os criou.

### Observabilidade dos contratos

Os valores reais dos XComs foram inspecionados na interface do Airflow
após uma DAG Run bem-sucedida.

## XComs validados

- RawMetadata.
- ValidationMetadata.
- ProcessedMetadata.
- TableMetadata.
- LoadMetadata.
- Metadados da validação final.

## Critérios de aprovação

- Nenhum XCom contém lista completa de usuários.
- Nenhum XCom contém DataFrame.
- Nenhum XCom contém registros transformados.
- Nenhum XCom contém conteúdo integral de JSON ou CSV.
- Os XComs contêm apenas referências e metadados.
- Cada retorno utiliza a chave return_value.
- A DAG conclui as seis tasks com sucesso.
- Os artefatos permanecem persistidos fora do banco de metadados do
  Airflow.

## Resultado final

A comunicação entre tasks utiliza somente metadados pequenos, enquanto
os datasets completos permanecem nas camadas raw, processed e
database.

A arquitetura reduz o acoplamento entre workers e evita utilizar o
banco de metadados do Airflow como armazenamento de datasets.

## Próximo passo

Passo 10 — executar a regressão final da Fase 5 e consolidar a
documentação da arquitetura.
