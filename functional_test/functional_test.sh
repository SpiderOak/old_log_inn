#!/bin/bash

set -e 
set -x 

export PYTHONPATH="${HOME}/old_log_inn"
python3.2 "${HOME}/old_log_inn/functional_test/functional_test_main.py" \
    --log-path="/tmp/functional_test.log" \
    --config-path="${HOME}/old_log_inn/functional_test/functional_test.json" \
    --duration="900" \
    --old-log-inn-path="${HOME}/old_log_inn"
