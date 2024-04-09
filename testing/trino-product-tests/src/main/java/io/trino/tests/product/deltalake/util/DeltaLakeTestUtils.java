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

import com.amazonaws.services.glue.model.ConcurrentModificationException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Throwables;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.tempto.query.QueryExecutionException;
import io.trino.tempto.query.QueryResult;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public final class DeltaLakeTestUtils
{
    private static final Logger log = Logger.get(DeltaLakeTestUtils.class);

    public static final String DATABRICKS_COMMUNICATION_FAILURE_ISSUE = "https://github.com/trinodb/trino/issues/14391";
    @Language("RegExp")
    public static final String DATABRICKS_COMMUNICATION_FAILURE_MATCH =
            "\\Q[Databricks][\\E(DatabricksJDBCDriver|JDBCDriver)\\Q](500593) Communication link failure. Failed to connect to server. Reason: \\E" +
            "(" +
            "(HTTP retry after response received with no Retry-After header, error: HTTP Response code: 503|HTTP Response code: 504), Error message: Unknown." +
            "|java.net.SocketTimeoutException: Read timed out." +
            ")";
    private static final RetryPolicy<QueryResult> CONCURRENT_MODIFICATION_EXCEPTION_RETRY_POLICY = RetryPolicy.<QueryResult>builder()
            .handleIf(throwable -> Throwables.getRootCause(throwable) instanceof ConcurrentModificationException)
            .handleIf(throwable -> throwable.getMessage() != null && throwable.getMessage().contains("Table being modified concurrently"))
            .withBackoff(1, 10, ChronoUnit.SECONDS)
            .withMaxRetries(3)
            .onRetry(event -> log.warn(event.getLastException(), "Query failed on attempt %d, will retry.", event.getAttemptCount()))
            .build();

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

    public static void skipTestUnlessUnsupportedWriterVersionExists()
    {
        // TODO: This method should be called only once per environment. Consider using a cache or creating a new module like HiveVersionProvider.
        String tableName = "test_dl_unsupported_writer_version_" + randomNameSuffix();

        try {
            onDelta().executeQuery("CREATE TABLE default." + tableName + "(col int) USING DELTA TBLPROPERTIES ('delta.minWriterVersion'='8')");
            dropDeltaTableWithRetry("default." + tableName);
        }
        catch (QueryExecutionException e) {
            assertThat(e).hasMessageMatching("(?s).* delta.minWriterVersion needs to be (an integer between \\[1, 7]|one of 1, 2, 3, 4, 5(, 6)?, 7).*");
            throw new SkipException("Cannot test unsupported writer version");
        }
    }

    public static List<String> getColumnNamesOnDelta(String schemaName, String tableName)
    {
        QueryResult result = onDelta().executeQuery("SHOW COLUMNS IN " + schemaName + "." + tableName);
        return result.column(1);
    }

    public static String getColumnCommentOnTrino(String schemaName, String tableName, String columnName)
    {
        return (String) onTrino()
                .executeQuery("SELECT comment FROM delta.information_schema.columns WHERE table_schema = '" + schemaName + "' AND table_name = '" + tableName + "' AND column_name = '" + columnName + "'")
                .getOnlyValue();
    }

    public static String getColumnCommentOnDelta(String schemaName, String tableName, String columnName)
    {
        QueryResult result = onDelta().executeQuery(format("DESCRIBE %s.%s %s", schemaName, tableName, columnName));
        return (String) result.row(2).get(1);
    }

    public static String getTableCommentOnTrino(String schemaName, String tableName)
    {
        return (String) onTrino().executeQuery("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'delta' AND schema_name = '" + schemaName + "' AND table_name = '" + tableName + "'")
                .getOnlyValue();
    }

    public static String getTableCommentOnDelta(String schemaName, String tableName)
    {
        QueryResult result = onDelta().executeQuery(format("DESCRIBE EXTENDED %s.%s", schemaName, tableName));
        return (String) result.rows().stream()
                .filter(row -> row.get(0).equals("Comment"))
                .map(row -> row.get(1))
                .collect(onlyElement());
    }

    public static Map<String, String> getTablePropertiesOnDelta(String schemaName, String tableName)
    {
        QueryResult result = onDelta().executeQuery("SHOW TBLPROPERTIES %s.%s".formatted(schemaName, tableName));
        return result.rows().stream()
                .map(column -> Map.entry((String) column.get(0), (String) column.get(1)))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static String getTablePropertyOnDelta(String schemaName, String tableName, String propertyName)
    {
        QueryResult result = onDelta().executeQuery("SHOW TBLPROPERTIES %s.%s(%s)".formatted(schemaName, tableName, propertyName));
        return (String) getOnlyElement(result.rows()).get(1);
    }

    /**
     * Workaround method to avoid <a href="https://github.com/trinodb/trino/issues/13199">Table being modified concurrently error in Glue</a>.
     */
    public static QueryResult dropDeltaTableWithRetry(String tableName)
    {
        return Failsafe.with(CONCURRENT_MODIFICATION_EXCEPTION_RETRY_POLICY)
                .get(() -> onDelta().executeQuery("DROP TABLE IF EXISTS " + tableName));
    }

    public static void removeS3Directory(AmazonS3 s3, String bucketName, String directoryPrefix)
    {
        ObjectListing listing = s3.listObjects(bucketName, directoryPrefix);
        do {
            List<String> objectKeys = listing.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(toImmutableList());
            DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucketName).withKeys(objectKeys.toArray(new String[0]));
            log.info("Deleting keys: %s", objectKeys);
            s3.deleteObjects(deleteObjectsRequest);
            listing = s3.listNextBatchOfObjects(listing);
        }
        while (listing.isTruncated());
    }
}
