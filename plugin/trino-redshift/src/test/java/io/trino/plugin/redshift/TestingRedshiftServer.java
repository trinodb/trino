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
package io.trino.plugin.redshift;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestView;
import org.jdbi.v3.core.HandleCallback;
import org.jdbi.v3.core.HandleConsumer;
import org.jdbi.v3.core.Jdbi;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.time.Duration;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Throwables.getCausalChain;
import static io.trino.plugin.redshift.RedshiftQueryRunner.IAM_ROLE;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;

public final class TestingRedshiftServer
{
    private TestingRedshiftServer() {}

    public static final String JDBC_ENDPOINT = requiredNonEmptySystemProperty("test.redshift.jdbc.endpoint");
    public static final String JDBC_USER = requiredNonEmptySystemProperty("test.redshift.jdbc.user");
    public static final String JDBC_PASSWORD = requiredNonEmptySystemProperty("test.redshift.jdbc.password");

    public static final String TEST_DATABASE = "testdb";
    public static final String TEST_SCHEMA = "test_schema";

    public static final String JDBC_URL = "jdbc:redshift://" + JDBC_ENDPOINT + TEST_DATABASE + "?connectTimeout=0";

    @GuardedBy("TestingRedshiftServer.class")
    private static boolean sleepFunctionCreated;

    public static void executeInRedshiftWithRetry(String sql, Object... parameters)
    {
        executeInRedshiftWithRetry(handle -> handle.execute(sql, parameters));
    }

    public static <E extends Exception> void executeInRedshiftWithRetry(HandleConsumer<E> consumer)
            throws E
    {
        executeWithRedshiftWithRetry(consumer.asCallback());
    }

    public static <T, E extends Exception> T executeWithRedshiftWithRetry(HandleCallback<T, E> callback)
            throws E
    {
        return Failsafe.with(retryPolicy())
                .get(() -> executeWithRedshift(callback));
    }

    public static void executeInRedshift(String sql, Object... parameters)
    {
        executeInRedshift(handle -> handle.execute(sql, parameters));
    }

    public static <E extends Exception> void executeInRedshift(HandleConsumer<E> consumer)
            throws E
    {
        executeWithRedshift(consumer.asCallback());
    }

    public static <T, E extends Exception> T executeWithRedshift(HandleCallback<T, E> callback)
            throws E
    {
        return Jdbi.create(JDBC_URL, JDBC_USER, JDBC_PASSWORD).withHandle(callback);
    }

    public static SqlExecutor onRemoteDatabaseWithSchema(String schema)
    {
        return sql -> executeInRedshift("SET search_path TO %s; %s".formatted(schema, sql));
    }

    /**
     * Creates a view in {@link #TEST_SCHEMA} whose scan blocks for at least {@code secondsToSleep} seconds,
     * for tests that need a long-running remote query.
     */
    public static TestView createSleepingView(long secondsToSleep)
    {
        ensureSleepFunctionExists();
        // Select from a real table so the query runs on the compute nodes, where Lambda UDFs are evaluated.
        // A query without a table reference runs only on the leader node, and Redshift UNLOAD completes it immediately without sleeping.
        // Filter to a single row as the Lambda sleeps once per input row.
        return new TestView(
                onRemoteDatabaseWithSchema(TEST_SCHEMA),
                "test_sleeping_view",
                "SELECT janky_sleep(%d) AS value FROM %s.nation WHERE nationkey = 0".formatted(secondsToSleep, TEST_SCHEMA));
    }

    // Created once per JVM under a lock: concurrent CREATE OR REPLACE of the same function fails in Redshift
    // with "could not complete because of conflict with concurrent transaction"
    private static synchronized void ensureSleepFunctionExists()
    {
        if (sleepFunctionCreated) {
            return;
        }
        // pg_sleep unsupported: https://docs.aws.amazon.com/redshift/latest/dg/c_unsupported-postgresql-functions.html,
        // Using a predefined AWS lambda replacement
        executeInRedshiftWithRetry(
                """
                SET search_path TO %s;
                CREATE OR REPLACE EXTERNAL FUNCTION\s
                        janky_sleep(x int) returns int
                        lambda 'trino-redshift-ci-sleep' IAM_ROLE '%s'
                STABLE
                """.formatted(TEST_SCHEMA, IAM_ROLE));
        sleepFunctionCreated = true;
    }

    public static boolean isExceptionRecoverable(Throwable exception)
    {
        if (exception == null) {
            return false;
        }

        String message = nullToEmpty(exception.getMessage());
        return message.matches(".* concurrent transaction.*")
                || message.matches(".*deadlock detected.*")
                || message.matches(".*could not open relation with OID.*")
                || message.matches(".*The connection attempt failed.*")
                || message.matches(".*Connection to .* refused.*")
                || getCausalChain(exception).stream()
                .anyMatch(e -> e instanceof ConnectException || e instanceof SocketTimeoutException);
    }

    private static RetryPolicy<Object> retryPolicy()
    {
        return RetryPolicy.builder()
                .handleIf(TestingRedshiftServer::isExceptionRecoverable)
                .withDelay(Duration.ofSeconds(10))
                .withMaxRetries(3)
                .build();
    }
}
