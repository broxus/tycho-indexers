# Generates a Grafana panel JSON.
gen_dashboard:
    #!/usr/bin/env bash
    if ! [ -d ".venv" ]; then
        ./scripts/install-python-deps.sh
    fi
    source .venv/bin/activate
    /usr/bin/env python3 ./scripts/gen-dashboard.py > dashboard.json
