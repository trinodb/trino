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
package io.trino.tests.product.deltalake;

import com.google.common.base.Throwables;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.tests.product.deltalake.util.DatabricksVersion;
import org.intellij.lang.annotations.Language;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;

import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;

public final class DeltaLakeDatabricksUtilsJunit
{
    private static final Logger log = Logger.get(DeltaLakeDatabricksUtilsJunit.class);

    public static final String DATABRICKS_COMMUNICATION_FAILURE_ISSUE = "https://github.com/trinodb/trino/issues/14391";
    @Language("RegExp")
    public static final String DATABRICKS_COMMUNICATION_FAILURE_MATCH =
            "\\Q[Databricks][\\E(DatabricksJDBCDriver|JDBCDriver)\\Q](500593) Communication link failure. Failed to connect to server. Reason: \\E" +
                    "(" +
                    "(HTTP retry after response received with no Retry-After header, error: HTTP Response code: 503|HTTP Response code: 504), Error message: Unknown." +
                    "|java.net.SocketTimeoutException: Read timed out." +
                    ")";

    private static final RetryPolicy<QueryResult> DATABRICKS_COMMUNICATION_FAILURE_RETRY_POLICY = RetryPolicy.<QueryResult>builder()
            .handleIf(throwable -> Throwables.getRootCause(throwable) instanceof SQLException)
            .handleIf(throwable -> {
                Throwable rootCause = Throwables.getRootCause(throwable);
                String message = rootCause.getMessage();
                return message != null && Pattern.compile(DATABRICKS_COMMUNICATION_FAILURE_MATCH).matcher(message).find();
            })
            .withBackoff(1, 10, ChronoUnit.SECONDS)
            .withMaxRetries(3)
            .onRetry(event -> log.warn(event.getLastException(), "Query failed on attempt %d, will retry.", event.getAttemptCount()))
            .build();

    private static final RetryPolicy<QueryResult> CONCURRENT_MODIFICATION_EXCEPTION_RETRY_POLICY = RetryPolicy.<QueryResult>builder()
            .handleIf(throwable -> Throwables.getRootCause(throwable) instanceof ConcurrentModificationException)
            .handleIf(throwable -> throwable.getMessage() != null && throwable.getMessage().contains("Table being modified concurrently"))
            .withBackoff(1, 10, ChronoUnit.SECONDS)
            .withMaxRetries(3)
            .onRetry(event -> log.warn(event.getLastException(), "Query failed on attempt %d, will retry.", event.getAttemptCount()))
            .build();

    private DeltaLakeDatabricksUtilsJunit() {}

    public static Optional<DatabricksVersion> getDatabricksRuntimeVersion(DeltaLakeDatabricksEnvironment env)
    {
        String version = (String) Failsafe.with(DATABRICKS_COMMUNICATION_FAILURE_RETRY_POLICY)
                .get(() -> env.executeDatabricksSql("SELECT java_method('java.lang.System', 'getenv', 'DATABRICKS_RUNTIME_VERSION')"))
                .getOnlyValue();

        if ("null".equals(version)) {
            return Optional.empty();
        }
        return Optional.of(DatabricksVersion.parse(version));
    }

    public static List<String> getColumnNamesOnDatabricks(DeltaLakeDatabricksEnvironment env, String schemaName, String tableName)
    {
        QueryResult result = Failsafe.with(DATABRICKS_COMMUNICATION_FAILURE_RETRY_POLICY)
                .get(() -> env.executeDatabricksSql("SHOW COLUMNS IN " + schemaName + "." + tableName));
        return result.column(1).stream()
                .map(String.class::cast)
                .toList();
    }

    public static String getColumnCommentOnTrino(DeltaLakeDatabricksEnvironment env, String schemaName, String tableName, String columnName)
    {
        return (String) env.executeTrinoSql("SELECT comment FROM delta.information_schema.columns WHERE table_schema = '" + schemaName + "' AND table_name = '" + tableName + "' AND column_name = '" + columnName + "'")
                .getOnlyValue();
    }

    public static String getColumnCommentOnDatabricks(DeltaLakeDatabricksEnvironment env, String schemaName, String tableName, String columnName)
    {
        QueryResult result = Failsafe.with(DATABRICKS_COMMUNICATION_FAILURE_RETRY_POLICY)
                .get(() -> env.executeDatabricksSql("DESCRIBE " + schemaName + "." + tableName + " " + columnName));
        return (String) result.rows().get(2).get(1);
    }

    public static String getTableCommentOnTrino(DeltaLakeDatabricksEnvironment env, String schemaName, String tableName)
    {
        return (String) env.executeTrinoSql("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'delta' AND schema_name = '" + schemaName + "' AND table_name = '" + tableName + "'")
                .getOnlyValue();
    }

    public static String getTableCommentOnDatabricks(DeltaLakeDatabricksEnvironment env, String schemaName, String tableName)
    {
        QueryResult result = Failsafe.with(DATABRICKS_COMMUNICATION_FAILURE_RETRY_POLICY)
                .get(() -> env.executeDatabricksSql("DESCRIBE EXTENDED " + schemaName + "." + tableName));
        return (String) result.rows().stream()
                .filter(row -> "Comment".equals(row.get(0)))
                .map(row -> row.get(1))
                .collect(onlyElement());
    }

    public static Map<String, String> getTablePropertiesOnDatabricks(DeltaLakeDatabricksEnvironment env, String schemaName, String tableName)
    {
        QueryResult result = Failsafe.with(DATABRICKS_COMMUNICATION_FAILURE_RETRY_POLICY)
                .get(() -> env.executeDatabricksSql("SHOW TBLPROPERTIES " + schemaName + "." + tableName));
        return result.rows().stream()
                .map(column -> Map.entry((String) column.get(0), (String) column.get(1)))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static String getTablePropertyOnDatabricks(DeltaLakeDatabricksEnvironment env, String schemaName, String tableName, String propertyName)
    {
        QueryResult result = Failsafe.with(DATABRICKS_COMMUNICATION_FAILURE_RETRY_POLICY)
                .get(() -> env.executeDatabricksSql("SHOW TBLPROPERTIES " + schemaName + "." + tableName + "(" + propertyName + ")"));
        return (String) getOnlyElement(result.rows()).get(1);
    }

    public static QueryResult dropDeltaTableWithRetry(DeltaLakeDatabricksEnvironment env, String tableName)
    {
        return Failsafe.with(CONCURRENT_MODIFICATION_EXCEPTION_RETRY_POLICY)
                .get(() -> env.executeDatabricksSql("DROP TABLE IF EXISTS " + tableName));
    }
}
