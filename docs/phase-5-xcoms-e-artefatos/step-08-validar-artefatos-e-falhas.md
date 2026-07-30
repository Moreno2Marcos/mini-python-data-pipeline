# Fase 5 — Passo 8: validar artefatos e falhas controladas

## Objetivo do passo

Comprovar que o pipeline rejeita arquivos ausentes, vazios,
corrompidos ou inconsistentes, apresentando mensagens diagnosticáveis
e interrompendo o processamento antes que dados inválidos avancem.

## Fluxo

```mermaid
flowchart TB

    subgraph HEAD[" "]
        direction LR
        H1["ANTES — cobertura parcial"]
        ESP[" "]
        H2["DEPOIS — cobertura fortalecida"]
    end

    subgraph ROW1[" "]
        direction LR
        A1[Artefato recebido]
        REL1["mesmo estágio"]
        A2[Artefato recebido]
        A1 -.-> REL1 -.-> A2
    end

    subgraph ROW2[" "]
        direction LR
        B1[Validações já existentes]
        REL2["fortalecido por"]
        B2[Validação física,<br/>sintática e estrutural]
        B1 -.-> REL2 -.-> B2
    end

    subgraph ROW3[" "]
        direction LR
        C1[Erros técnicos<br/>parcialmente contextualizados]
        REL3["fortalecido por"]
        C2[Erros técnicos<br/>contextualizados]
        C1 -.-> REL3 -.-> C2
    end

    subgraph ROW4[" "]
        direction LR
        D1[Testes negativos<br/>incompletos]
        REL4["substituído por"]
        D2[Cenários inválidos<br/>automatizados]
        D1 -.-> REL4 -.-> D2
    end

    subgraph ROW5[" "]
        direction LR
        E1[Carga no SQLite]
        REL5["mesmo estágio"]
        E2[Carga no SQLite]
        E1 -.-> REL5 -.-> E2
    end

    subgraph ROW6[" "]
        direction LR
        F1[Falha de escrita<br/>não simulada]
        REL6["substituído por"]
        F2[Falha de escrita simulada<br/>e propagação comprovada]
        F1 -.-> REL6 -.-> F2
    end

    A1 --> B1 --> C1 --> D1 --> E1 --> F1
    A2 --> B2 --> C2 --> D2 --> E2 --> F2

    classDef unchanged fill:#E8EEF7,stroke:#4A6280,color:#000;
    classDef oldChange fill:#FFF1F1,stroke:#C62828,color:#000;
    classDef newChange fill:#E8F5E9,stroke:#2E7D32,color:#000;
    classDef relation fill:#F5F5F5,stroke:#757575,color:#000;
    classDef oldTitle fill:#FFF8F8,stroke:#C62828,color:#000,stroke-dasharray:5 5;
    classDef newTitle fill:#F6FFF7,stroke:#2E7D32,color:#000;

    class A1,A2,E1,E2 unchanged;
    class B1,C1,D1,F1 oldChange;
    class B2,C2,D2,F2 newChange;
    class REL1,REL2,REL3,REL4,REL5,REL6 relation;
    class H1 oldTitle;
    class H2 newTitle;

    style HEAD fill:none,stroke:none
    style ROW1 fill:none,stroke:none
    style ROW2 fill:none,stroke:none
    style ROW3 fill:none,stroke:none
    style ROW4 fill:none,stroke:none
    style ROW5 fill:none,stroke:none
    style ROW6 fill:none,stroke:none
    style ESP fill:none,stroke:none,color:transparent
```

## Conceitos de Engenharia de Dados aplicados

### Negative testing

Entradas inválidas são criadas deliberadamente para comprovar o
comportamento do pipeline fora do cenário ideal.

### Fail-fast

O processamento é interrompido assim que uma inconsistência é
identificada.

### Observabilidade de falhas

Erros técnicos são convertidos em mensagens associadas ao arquivo ou
à operação que falhou.

### Failure injection

Uma falha de escrita no SQLite é simulada por meio de monkeypatch,
sem comprometer o ambiente real.

### Teste de regressão

Todos os testes anteriores são executados novamente para garantir que
os novos tratamentos de erro não afetaram os cenários válidos.

## Decisões arquiteturais e justificativas

As validações permanecem nas funções reutilizáveis de src, enquanto o
Airflow continua responsável pela orquestração e propagação dos
estados das tasks.

Os artefatos inválidos são testados em diretórios temporários
fornecidos pelo pytest.

Não são corrompidos manualmente os arquivos reais do projeto.

A falha de escrita é simulada em teste para manter o ambiente isolado
e reproduzível.

## Arquivos alterados

- src/load.py
- tests/test_raw_validation.py
- tests/test_processed_artifact.py
- tests/test_database_load.py
- docs/phase-5-xcoms-e-artefatos/step-08-validar-artefatos-e-falhas.md

## Testes e critérios de aprovação

- Extensão raw incorreta rejeitada.
- JSON sintaticamente inválido rejeitado.
- Objeto JSON incompatível rejeitado.
- Lista raw vazia rejeitada.
- CSV inexistente rejeitado.
- Caminho que aponta para diretório rejeitado.
- CSV com zero bytes rejeitado.
- CSV sem registros rejeitado.
- Contagem divergente rejeitada.
- Coluna obrigatória ausente rejeitada.
- user_id nulo ou duplicado rejeitado.
- Falha simulada de escrita propagada.
- Suíte completa aprovada.
- DAG Run válida concluída com sucesso.

## Resultado final

O pipeline passa a possuir cobertura explícita dos principais cenários
de falha dos artefatos raw, processados e da carga no SQLite.

As falhas são interrompidas no ponto de origem e não permitem que
tasks downstream processem dados inválidos.

## Próximo passo

Passo 9 — validar os XComs finais e comprovar que contêm somente
referências e metadados pequenos.
