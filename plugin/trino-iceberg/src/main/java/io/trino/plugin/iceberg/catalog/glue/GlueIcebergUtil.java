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
package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.services.glue.model.TableInput;
import jakarta.annotation.Nullable;
import org.apache.iceberg.TableMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.HiveMetadata.PRESTO_VIEW_EXPANDED_TEXT_MARKER;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.plugin.hive.ViewReaderUtil.ICEBERG_MATERIALIZED_VIEW_COMMENT;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

public final class GlueIcebergUtil
{
    private GlueIcebergUtil() {}

    public static TableInput getTableInput(String tableName, Optional<String> owner, TableMetadata metadata, String newMetadataLocation, Map<String, String> parameters)
    {
        requireNonNull(metadata, "metadata is null"); // suppress unused

        parameters = new HashMap<>(parameters);
        parameters.putIfAbsent(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(ENGLISH));
        parameters.put(METADATA_LOCATION_PROP, newMetadataLocation);

        return new TableInput()
                .withName(tableName)
                .withOwner(owner.orElse(null))
                .withParameters(parameters)
                // Iceberg does not distinguish managed and external tables, all tables are treated the same and marked as EXTERNAL
                .withTableType(EXTERNAL_TABLE.name());
    }

    public static TableInput getViewTableInput(String viewName, String viewOriginalText, @Nullable String owner, Map<String, String> parameters)
    {
        return new TableInput()
                .withName(viewName)
                .withTableType(VIRTUAL_VIEW.name())
                .withViewOriginalText(viewOriginalText)
                .withViewExpandedText(PRESTO_VIEW_EXPANDED_TEXT_MARKER)
                .withOwner(owner)
                .withParameters(parameters);
    }

    public static TableInput getMaterializedViewTableInput(String viewName, String viewOriginalText, String owner, Map<String, String> parameters)
    {
        return new TableInput()
                .withName(viewName)
                .withTableType(VIRTUAL_VIEW.name())
                .withViewOriginalText(viewOriginalText)
                .withViewExpandedText(ICEBERG_MATERIALIZED_VIEW_COMMENT)
                .withOwner(owner)
                .withParameters(parameters);
    }
}
