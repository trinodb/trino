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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_VERSION_NAME;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_VIEW_COMMENT;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveMetadata.TRINO_CREATED_BY;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.hive.ViewReaderUtil.isHiveOrPrestoView;
import static io.trino.plugin.hive.ViewReaderUtil.isPrestoView;

public final class TrinoViewUtil
{
    private TrinoViewUtil() {}

    public static Optional<ConnectorViewDefinition> getView(
            SchemaTableName viewName,
            Optional<String> viewOriginalText,
            String tableType,
            Map<String, String> tableParameters,
            Optional<String> tableOwner)
    {
        if (!isView(tableType, tableParameters)) {
            // Filter out Tables and Materialized Views
            return Optional.empty();
        }

        if (!isPrestoView(tableParameters)) {
            // Hive views are not compatible
            throw new HiveViewNotSupportedException(viewName);
        }

        checkArgument(viewOriginalText.isPresent(), "viewOriginalText must be present");
        ConnectorViewDefinition definition = ViewReaderUtil.PrestoViewReader.decodeViewData(viewOriginalText.get());
        // use owner from table metadata if it exists
        if (tableOwner.isPresent() && !definition.isRunAsInvoker()) {
            definition = new ConnectorViewDefinition(
                    definition.getOriginalSql(),
                    definition.getCatalog(),
                    definition.getSchema(),
                    definition.getColumns(),
                    definition.getComment(),
                    tableOwner,
                    false);
        }
        return Optional.of(definition);
    }

    private static boolean isView(String tableType, Map<String, String> tableParameters)
    {
        return isHiveOrPrestoView(tableType) && PRESTO_VIEW_COMMENT.equals(tableParameters.get(TABLE_COMMENT));
    }

    public static Map<String, String> createViewProperties(ConnectorSession session, String trinoVersion, String connectorName)
    {
        return ImmutableMap.<String, String>builder()
                .put(PRESTO_VIEW_FLAG, "true")
                .put(TRINO_CREATED_BY, connectorName)
                .put(PRESTO_VERSION_NAME, trinoVersion)
                .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                .put(TABLE_COMMENT, PRESTO_VIEW_COMMENT)
                .buildOrThrow();
    }
}
