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
package io.trino.tests.product.deltalake.util;

import io.airlift.log.Logger;
import io.trino.tempto.query.QueryResult;
import io.trino.tests.product.utils.QueryExecutors;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.function.CheckedSupplier;
import org.intellij.lang.annotations.Language;

import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public final class DeltaLakeTestUtils
{
    private static final Logger log = Logger.get(DeltaLakeTestUtils.class);

    public static final String DATABRICKS_COMMUNICATION_FAILURE_ISSUE = "https://github.com/trinodb/trino/issues/14391";
    @Language("RegExp")
    public static final String DATABRICKS_COMMUNICATION_FAILURE_MATCH =
            "\\Q[Databricks][DatabricksJDBCDriver](500593) Communication link failure. Failed to connect to server. Reason: HTTP retry after response received with no Retry-After header, error: HTTP Response code: 503, Error message: Unknown.";

    public static final RetryPolicy<QueryResult> ERROR_TABLE_MODIFIED_CONCURRENTLY_RETRY_POLICY = new RetryPolicy<QueryResult>()
            .handleIf(e -> e.getMessage().contains("Table being modified concurrently"))
            .withBackoff(1, 10, ChronoUnit.SECONDS)
            .withMaxRetries(10)
            .onRetry(event -> log.warn(event.getLastFailure(), "Query failed on attempt %d, will retry.", event.getAttemptCount()));

    private DeltaLakeTestUtils() {}

    public static Optional<DatabricksVersion> getDatabricksRuntimeVersion()
    {
        String version = (String) onDelta().executeQuery("SELECT java_method('java.lang.System', 'getenv', 'DATABRICKS_RUNTIME_VERSION')").getOnlyValue();
        // OSS Spark returns null
        if (version.equals("null")) {
            return Optional.empty();
        }
        return Optional.of(DatabricksVersion.parse(version));
    }

    public static String getColumnCommentOnTrino(String schemaName, String tableName, String columnName)
    {
        return (String) onTrino()
                .executeQuery("SELECT comment FROM information_schema.columns WHERE table_schema = '" + schemaName + "' AND table_name = '" + tableName + "' AND column_name = '" + columnName + "'")
                .getOnlyValue();
    }

    public static String getColumnCommentOnDelta(String schemaName, String tableName, String columnName)
    {
        QueryResult result = onDelta().executeQuery(format("DESCRIBE %s.%s %s", schemaName, tableName, columnName));
        return (String) result.row(2).get(1);
    }

    public static String getTableCommentOnDelta(String schemaName, String tableName)
    {
        QueryResult result = onDelta().executeQuery(format("DESCRIBE EXTENDED %s.%s", schemaName, tableName));
        return (String) result.rows().stream()
                .filter(row -> row.get(0).equals("Comment"))
                .map(row -> row.get(1))
                .findFirst().orElseThrow();
    }

    /**
     * Workaround method to avoid <a href="https://github.com/trinodb/trino/issues/13199">Table being modified concurrently error in Glue</a>.
     * This method should be used only for dropping objects using {@link QueryExecutors#onDelta()} method.
     */
    @Deprecated
    public static void retryOnModifiedConcurrentlyFailure(CheckedSupplier<QueryResult> supplier)
    {
        Failsafe.with(ERROR_TABLE_MODIFIED_CONCURRENTLY_RETRY_POLICY)
                .get(supplier);
    }
}
