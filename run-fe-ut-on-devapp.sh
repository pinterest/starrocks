#!/usr/bin/env bash
# Run StarRocks FE unit tests on devapp EC2 node
# Usage: ./run-fe-ut-on-devapp.sh [OPTIONS]
#
# Options:
#   -h, --host HOST       devapp hostname or IP (default: devapp)
#   -u, --user USER       SSH user (default: ec2-user)
#   -j, --jobs N          parallel test jobs (default: 4)
#   -t, --test TEST       run specific test class
#   -f, --filter FILTER   skip specific tests
#   --coverage            run with coverage
#   --dry-run             validate compilation only
#   --copy-results PATH   copy results back to local PATH
#   --help                show this help

set -e

# Defaults
DEVAPP_HOST="${DEVAPP_HOST:-devapp}"
DEVAPP_USER="${DEVAPP_USER:-ec2-user}"
JOBS=4
TEST_CLASS=""
FILTER_TEST=""
COVERAGE=0
DRY_RUN=0
COPY_RESULTS=""
STARROCKS_REMOTE_PATH="${STARROCKS_REMOTE_PATH:-~/code/starrocks}"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--host)
            DEVAPP_HOST="$2"
            shift 2
            ;;
        -u|--user)
            DEVAPP_USER="$2"
            shift 2
            ;;
        -j|--jobs)
            JOBS="$2"
            shift 2
            ;;
        -t|--test)
            TEST_CLASS="$2"
            shift 2
            ;;
        -f|--filter)
            FILTER_TEST="$2"
            shift 2
            ;;
        --coverage)
            COVERAGE=1
            shift
            ;;
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        --copy-results)
            COPY_RESULTS="$2"
            shift 2
            ;;
        --help)
            sed -n '1,/^$/p' "$0" | sed 's/^# //'
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

DEVAPP_ADDR="${DEVAPP_USER}@${DEVAPP_HOST}"
REMOTE_LOG="/tmp/fe-ut-run-$(date +%Y%m%d-%H%M%S).log"

echo "=========================================="
echo "StarRocks FE UT on devapp"
echo "=========================================="
echo "Host:          $DEVAPP_ADDR"
echo "Repo path:     $STARROCKS_REMOTE_PATH"
echo "Parallel jobs: $JOBS"
[[ -n "$TEST_CLASS" ]] && echo "Test class:    $TEST_CLASS"
[[ -n "$FILTER_TEST" ]] && echo "Filter:        $FILTER_TEST"
[[ $COVERAGE -eq 1 ]] && echo "Coverage:      enabled"
[[ $DRY_RUN -eq 1 ]] && echo "Mode:          dry-run"
echo "Log:           $REMOTE_LOG"
echo "=========================================="

# Build the test command
TEST_CMD="cd ${STARROCKS_REMOTE_PATH} && export STARROCKS_HOME=\$PWD && . ./env.sh"

if [[ $DRY_RUN -eq 1 ]]; then
    TEST_CMD="${TEST_CMD} && ./run-fe-ut.sh --dry-run -j${JOBS}"
elif [[ $COVERAGE -eq 1 ]]; then
    TEST_CMD="${TEST_CMD} && ./run-fe-ut.sh --coverage -j${JOBS}"
elif [[ -n "$TEST_CLASS" ]]; then
    TEST_CMD="${TEST_CMD} && ./run-fe-ut.sh --test ${TEST_CLASS}"
    if [[ -n "$FILTER_TEST" ]]; then
        TEST_CMD="${TEST_CMD} --filter ${FILTER_TEST}"
    fi
else
    TEST_CMD="${TEST_CMD} && ./run-fe-ut.sh -j${JOBS}"
    if [[ -n "$FILTER_TEST" ]]; then
        TEST_CMD="${TEST_CMD} --filter ${FILTER_TEST}"
    fi
fi

# Add logging and send to remote
TEST_CMD="${TEST_CMD} 2>&1 | tee ${REMOTE_LOG}"

echo ""
echo "Executing on devapp..."
echo ""

# Execute on devapp
ssh "${DEVAPP_ADDR}" << EOFCMD
${TEST_CMD}
EOFCMD

TEST_EXIT=$?

if [[ $TEST_EXIT -eq 0 ]]; then
    echo ""
    echo "✓ Tests completed successfully"
else
    echo ""
    echo "✗ Tests failed with exit code $TEST_EXIT"
fi

# Optionally copy results back
if [[ -n "$COPY_RESULTS" ]]; then
    echo ""
    echo "Copying results to ${COPY_RESULTS}..."
    mkdir -p "${COPY_RESULTS}"

    scp -r "${DEVAPP_ADDR}:${STARROCKS_REMOTE_PATH}/fe/fe-core/target/surefire-reports" \
        "${COPY_RESULTS}/surefire-reports-$(date +%Y%m%d-%H%M%S)" 2>/dev/null || true

    scp "${DEVAPP_ADDR}:${REMOTE_LOG}" \
        "${COPY_RESULTS}/fe-ut-run.log" 2>/dev/null || true

    echo "✓ Results copied"
fi

echo ""
echo "Log file on devapp: ${REMOTE_LOG}"
echo "To retrieve: scp ${DEVAPP_ADDR}:${REMOTE_LOG} ./"
echo ""

exit $TEST_EXIT
