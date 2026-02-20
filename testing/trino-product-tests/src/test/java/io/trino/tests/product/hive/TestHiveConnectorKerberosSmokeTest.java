/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.product.hive;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Kerberos smoke tests for the Hive connector.
 * <p>
 * Ported from the Tempto-based TestHiveConnectorKerberosSmokeTest.
 * <p>
 * This test verifies that Kerberos ticket expiry/renewal works correctly
 * during long-running queries.
 */
@ProductTest
@RequiresEnvironment(HiveKerberosEnvironment.class)
@TestGroup.HiveKerberos
class TestHiveConnectorKerberosSmokeTest
{
    @Test
    @Timeout(value = 120_000, unit = MILLISECONDS)
    void kerberosTicketExpiryTest(HiveKerberosEnvironment env)
            throws Exception
    {
        ExecutorService executor = newSingleThreadExecutor();
        String tableName = "hive.default.orders";
        try {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableName);

            // Session properties to reduce memory usage and also make the query run longer
            env.executeTrinoInSession(session -> {
                session.executeUpdate("SET SESSION scale_writers = false");
                session.executeUpdate("SET SESSION task_scale_writers_enabled = false");
            });

            String sql = "CREATE TABLE %s AS SELECT * FROM tpch.sf1000.orders".formatted(tableName);
            Future<?> queryExecution = executor.submit(() -> env.executeTrino(sql));

            // 2x of ticket_lifetime as configured in hadoop-kerberos krb5.conf, sufficient to cause at-least 1 ticket expiry
            SECONDS.sleep(60L);
            cancelQueryIfRunning(env, sql);

            try {
                queryExecution.get(30, SECONDS);
                fail("Expected query to have failed");
            }
            catch (TimeoutException e) {
                queryExecution.cancel(true);
                throw e;
            }
            catch (ExecutionException expected) {
                assertThat(expected)
                        .rootCause()
                        .hasMessageContaining("explicitly cancelled for test without failure");
            }
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableName);
            executor.shutdownNow();
        }
    }

    private void cancelQueryIfRunning(HiveKerberosEnvironment env, String sql)
    {
        QueryResult queryResult = env.executeTrino("SELECT query_id FROM system.runtime.queries WHERE query = '%s' AND state = 'RUNNING' LIMIT 2".formatted(sql));
        checkState(queryResult.getRowsCount() < 2, "Found multiple queries");
        if (queryResult.getRowsCount() == 1) {
            String queryId = (String) queryResult.getOnlyValue();
            env.executeTrinoUpdate("CALL system.runtime.kill_query(query_id => '%s', message => 'explicitly cancelled for test without failure')".formatted(queryId));
        }
    }
}
