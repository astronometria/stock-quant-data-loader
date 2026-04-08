# 01 — Bootstrap / installation

## Setup minimal

```bash
cd ~/stock-quant-data-loader
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip setuptools wheel
python3 -m pip install -e .
mkdir -p logs
```

## Exécution robuste des jobs

```bash
cd ~/stock-quant-data-loader
source .venv/bin/activate
PYTHONPATH=src python3 -m stock_quant_data.jobs.nom_du_job
```

## Vérifications utiles

```bash
cd ~/stock-quant-data-loader && \
source .venv/bin/activate && \
python3 - <<'PY'
import sys
import stock_quant_data
print("python", sys.version)
print("package_file", stock_quant_data.__file__)
PY
```
