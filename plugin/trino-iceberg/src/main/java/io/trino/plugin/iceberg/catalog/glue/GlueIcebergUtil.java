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

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.HiveMetadata.PRESTO_VIEW_EXPANDED_TEXT_MARKER;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;

public final class GlueIcebergUtil
{
    private GlueIcebergUtil() {}

    public static TableInput getTableInput(String tableName, Optional<String> owner, Map<String, String> parameters)
    {
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
}
