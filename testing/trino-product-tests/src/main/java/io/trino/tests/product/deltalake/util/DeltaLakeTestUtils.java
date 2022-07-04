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

import io.trino.tempto.query.QueryResult;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public final class DeltaLakeTestUtils
{
    public static final String DATABRICKS_104_RUNTIME_VERSION = "10.4";

    private DeltaLakeTestUtils() {}

    public static String getDatabricksRuntimeVersion()
    {
        QueryResult result = onDelta().executeQuery("SELECT java_method('java.lang.System', 'getenv', 'DATABRICKS_RUNTIME_VERSION')");
        return firstNonNull((String) result.row(0).get(0), "unknown");
    }

    public static String getColumnCommentOnTrino(String schemaName, String tableName, String columnName)
    {
        QueryResult result = onTrino().executeQuery("SELECT comment FROM information_schema.columns WHERE table_schema = '" + schemaName + "' AND table_name = '" + tableName + "' AND column_name = '" + columnName + "'");
        return (String) result.row(0).get(0);
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
}
