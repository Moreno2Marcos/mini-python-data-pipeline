# Fase 5 — Passo 8: validar artefatos e falhas controladas

## Objetivo do passo

Comprovar que o pipeline rejeita arquivos ausentes, vazios,
corrompidos ou inconsistentes, apresentando mensagens diagnosticáveis
e interrompendo o processamento antes que dados inválidos avancem.

## Fluxo antes

```mermaid
flowchart TD

    subgraph ANTES["ANTES — cobertura parcial"]
        direction TB

        A[Artefato recebido]
        B[Validações já existentes]
        C[Erros técnicos<br/>parcialmente contextualizados]
        D[Testes negativos<br/>incompletos]
        E[Carga no SQLite]
        F[Falha de escrita<br/>não simulada]

        A --> B
        B --> C
        C --> D
        D --> E
        E --> F
    end

    classDef unchanged fill:#E8EEF7,stroke:#4A6280,color:#000;
    classDef oldChange fill:#FFF1F1,stroke:#C62828,color:#000;

    class A,E unchanged;
    class B,C,D,F oldChange;

    style ANTES fill:#FFF8F8,stroke:#C62828,stroke-width:2px,stroke-dasharray:6 6
```

## Fluxo depois

```mermaid
flowchart TD

    A[Artefato recebido]

    N1["Substitui os blocos antigos:<br/>validações já existentes,<br/>erros parcialmente contextualizados<br/>e testes negativos incompletos"]

    subgraph R1[" "]
        direction TB

        B[Validação física,<br/>sintática e estrutural]
        C[Erros técnicos<br/>contextualizados]
        D[Cenários inválidos<br/>automatizados]

        B --> C
        C --> D
    end

    E[Carga no SQLite]

    N2["Substitui bloco antigo:<br/>falha de escrita<br/>não simulada"]

    subgraph R2[" "]
        direction TB

        F[Falha de escrita simulada<br/>e propagação comprovada]
    end

    A --> B
    D --> E
    E --> F

    N1 -.-> B
    N2 -.-> F

    classDef unchanged fill:#E8EEF7,stroke:#4A6280,color:#000;
    classDef newChange fill:#E8F5E9,stroke:#2E7D32,color:#000;
    classDef noteOld fill:#FFF1F1,stroke:#C62828,color:#000,stroke-dasharray:6 6;

    class A,E unchanged;
    class B,C,D,F newChange;
    class N1,N2 noteOld;

    style R1 fill:#FFF8F8,stroke:#C62828,stroke-width:2px,stroke-dasharray:6 6
    style R2 fill:#FFF8F8,stroke:#C62828,stroke-width:2px,stroke-dasharray:6 6
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
