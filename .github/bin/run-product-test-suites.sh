#!/usr/bin/env bash

set -eo pipefail

mkdir -p logs

# Suites load checked-in configuration from paths relative to the repository root.
product_test_distribution=testing/trino-product-tests/target/product-test-distribution
test_jar=("${product_test_distribution}"/lib/trino-product-tests-*-tests.jar)
product_test_classpath=$(< "${product_test_distribution}/classpath.txt")
product_test_classpath="${product_test_distribution}/${product_test_classpath//:/:${product_test_distribution}/}"

declare -a suite_results=()
declare -a suite_totals=()
echo "## pt ${MATRIX_SUITE}" >> "$GITHUB_STEP_SUMMARY"
echo "" >> "$GITHUB_STEP_SUMMARY"

failed=0
failed_suites=()

for suite in ${MATRIX_SUITE}; do
  log_file="logs/${suite}.log"
  echo "### ${suite}" >> "$GITHUB_STEP_SUMMARY"
  echo "" >> "$GITHUB_STEP_SUMMARY"

  set +e
  java \
    -Xmx512M \
    -XX:+ExitOnOutOfMemoryError \
    -Dtrino.product-tests.image="${TRINO_PRODUCT_TESTS_IMAGE}" \
    -cp "${test_jar[0]}:${product_test_classpath}" \
    io.trino.tests.product.suite.${suite} \
    | tee "${log_file}"
  rc=${PIPESTATUS[0]}
  set -e

  total_line=$(grep '^TOTAL' "${log_file}" | tail -1 || true)
  if [ -n "${total_line}" ]; then
    suite_totals+=("${suite}:${total_line}")
  else
    suite_totals+=("${suite}:TOTAL summary not found")
  fi

  if [ "${rc}" -ne 0 ]; then
    failed=1
    failed_suites+=("${suite}")
    suite_results+=("${suite}:FAILED")
    echo "- Result: FAILED" >> "$GITHUB_STEP_SUMMARY"
  else
    suite_results+=("${suite}:PASSED")
    echo "- Result: PASSED" >> "$GITHUB_STEP_SUMMARY"
  fi

  if grep -Eq '^\[FAILURE\]|^\[ERROR\]|^    at io\.trino\.tests\.product\.' "${log_file}"; then
    echo "- Failure details:" >> "$GITHUB_STEP_SUMMARY"
    grep -E '^\[FAILURE\]|^\[ERROR\]|^    at io\.trino\.tests\.product\.' "${log_file}" >> "$GITHUB_STEP_SUMMARY"
  fi
  echo "" >> "$GITHUB_STEP_SUMMARY"
done

echo ""
echo "========================================"
echo "         PT-JUNIT SUITE SUMMARY         "
echo "========================================"
echo "Suite: ${MATRIX_SUITE}"
echo ""
for entry in "${suite_results[@]}"; do
  suite_name=${entry%%:*}
  suite_result=${entry#*:}
  suite_total=""
  for total_entry in "${suite_totals[@]}"; do
    if [[ "${total_entry}" == "${suite_name}:"* ]]; then
      suite_total=${total_entry#*:}
      break
    fi
  done
  printf '%-32s | %-7s | %s\n' "${suite_name}" "${suite_result}" "${suite_total}"
done
echo "========================================"
echo ""

if [ "${failed}" -ne 0 ]; then
  echo "FAILED SUITES:"
  for suite in "${failed_suites[@]}"; do
    echo "- ${suite}"
  done
  echo ""
  echo "FAILURE DETAILS:"
  echo "----------------"
  for suite in "${failed_suites[@]}"; do
    log_file="logs/${suite}.log"
    echo ""
    echo "${suite}:"
    if grep -q '^FAILURE DETAILS:' "${log_file}"; then
      sed -n '/^FAILURE DETAILS:/,$p' "${log_file}"
    elif grep -Eq '^\[FAILURE\]|^\[ERROR\]|^    at io\.trino\.tests\.product\.' "${log_file}"; then
      grep -E '^\[FAILURE\]|^\[ERROR\]|^    at io\.trino\.tests\.product\.' "${log_file}"
    else
      echo "No structured failure details found in ${log_file}"
    fi
  done
  echo ""
  echo "## Failed suites" >> "$GITHUB_STEP_SUMMARY"
  for suite in "${failed_suites[@]}"; do
    echo "- ${suite}" >> "$GITHUB_STEP_SUMMARY"
  done
  exit 1
fi
