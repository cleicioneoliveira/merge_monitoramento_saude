# merge_monitoramento_saude

Pacote para combinar o monitoramento corrigido com a timeline reconstruída de saúde animal.

Este repositório é o elo entre:

```text
environment_correction
status_timeline_reconstructor
        ↓
merge_monitoramento_saude
        ↓
carga-termica-bovinos
```

O objetivo é gerar um dataset final enxuto, padronizado e estável para ser consumido pelo pipeline de carga térmica.

---

## Entrada

### Monitoramento corrigido

Arquivo produzido por `environment_correction`:

```text
dataset/processado/monitoramento_corrigido.csv
```

Colunas mínimas esperadas:

```text
brinco
data_hora
```

Colunas comportamentais e ambientais esperadas para a saída final:

```text
ruminacao_hora
atividade_hora
ocio_hora
ofegacao_hora
ruminacao_acumulado
atividade_acumulado
ocio_acumulado
ofegacao_acumulado
temperatura_compost_1
humidade_compost_1
thi_compost1
temperatura_compost_2
humidade_compost_2
thi_compost2
```

### Timeline de saúde

Arquivo produzido por `status_timeline_reconstructor`:

```text
dataset/processado/saude_timeline_final.parquet
```

Colunas mínimas esperadas:

```text
brinco
data_hora
```

Coluna de status recomendada:

```text
status_vigente
```

Durante o merge, `status_vigente` é renomeado para:

```text
status_saude
```

quando `status_saude` ainda não existe.

---

## Saída

Por padrão, o pacote gera:

```text
dataset/processado/monitoramento_saude_unificado.parquet
dataset/processado/monitoramento_saude_unificado.csv
```

O arquivo `.parquet` é o recomendado para o `carga-termica-bovinos`.

A saída é estritamente limitada a estas colunas, nesta ordem:

```text
brinco
data_hora
status_saude
ruminacao_hora
atividade_hora
ocio_hora
ofegacao_hora
ruminacao_acumulado
atividade_acumulado
ocio_acumulado
ofegacao_acumulado
temperatura_compost_1
humidade_compost_1
thi_compost1
temperatura_compost_2
humidade_compost_2
thi_compost2
```

Colunas auxiliares da timeline de saúde, como `episode_number`, `proxima_mudanca`, `status_inicio_vigencia` e similares, não são exportadas no dataset final.

Se alguma coluna final obrigatória estiver ausente após o merge, o pipeline interrompe com erro claro.

---

## Comando principal

```bash
python -m merge_monitoramento_saude.cli \
  --monitoramento dataset/processado/monitoramento_corrigido.csv \
  --saude dataset/processado/saude_timeline_final.parquet \
  --output-dir dataset/processado/
```

Também é possível executar o pacote diretamente:

```bash
python -m merge_monitoramento_saude \
  --monitoramento dataset/processado/monitoramento_corrigido.csv \
  --saude dataset/processado/saude_timeline_final.parquet \
  --output-dir dataset/processado/
```

---

## Estratégia de merge

O padrão é `left`, preservando todas as linhas do monitoramento:

```bash
--how left
```

Outras opções:

```bash
--how inner
--how outer
```

---

## Padronização de colunas

O pacote possui um contrato explícito em:

```text
merge_monitoramento_saude/columns.py
```

Principais enums:

```text
KeyColumn
MonitoringColumn
HealthColumn
FinalColumn
```

A chave oficial é sempre:

```text
brinco + data_hora
```

Aliases conhecidos são normalizados automaticamente, por exemplo:

```text
animal_id      -> brinco
timestamp      -> data_hora
status_vigente -> status_saude
thi_compost_1  -> thi_compost1
umidade_compost_1 -> humidade_compost_1
```

---

## Instalação

```bash
pip install -r requirements.txt
```

Ou em modo editável:

```bash
pip install -e .
```

---

## API Python

```python
from merge_monitoramento_saude import run

outputs = run(
    monitoramento_path="dataset/processado/monitoramento_corrigido.csv",
    saude_path="dataset/processado/saude_timeline_final.parquet",
    output_dir="dataset/processado",
)

print(outputs["parquet"])
print(outputs["csv"])
```
