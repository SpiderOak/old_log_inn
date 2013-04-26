#!/bin/bash

set -e 
set -x

SOURCE_ROOT="${HOME}/git" 

export PYTHONPATH="${SOURCE_ROOT}/old_log_inn"
export NIMBUSIO_CONNECTION_TIMEOUT=360.0
export NIMBUS_IO_SERVICE_PORT="9000"
export NIMBUS_IO_SERVICE_HOST="dev.nimbus.io"
export NIMBUS_IO_SERVICE_DOMAIN="dev.nimbus.io"
export NIMBUS_IO_SERVICE_SSL="0"
python3.2 "${SOURCE_ROOT}/old_log_inn/functional_test/functional_test_main.py" \
    --log-path="/tmp/functional_test.log" \
    --config-path="${SOURCE_ROOT}/old_log_inn/functional_test/functional_test.json" \
    --duration="900" \
    --old-log-inn-path="${SOURCE_ROOT}/old_log_inn" \
    --directory-wormhole-path="${SOURCE_ROOT}/directory-wormhole"
