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

import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.type.TypeId;

import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveToTrinoTranslator.translateHiveViewToTrino;

public class LegacyHiveViewReader
        implements ViewReaderUtil.ViewReader
{
    private final boolean hiveViewsRunAsInvoker;

    public LegacyHiveViewReader(boolean hiveViewsRunAsInvoker)
    {
        this.hiveViewsRunAsInvoker = hiveViewsRunAsInvoker;
    }

    @Override
    public ConnectorViewDefinition decodeViewData(String viewData, Table table, CatalogName catalogName)
    {
        String viewText = table.getViewExpandedText()
                .orElseThrow(() -> new TrinoException(HIVE_INVALID_METADATA, "No view expanded text: " + table.getSchemaTableName()));
        return new ConnectorViewDefinition(
                translateHiveViewToTrino(viewText),
                Optional.of(catalogName.toString()),
                Optional.ofNullable(table.getDatabaseName()),
                Stream.concat(table.getDataColumns().stream(), table.getPartitionColumns().stream())
                        .map(column -> new ConnectorViewDefinition.ViewColumn(column.getName(), TypeId.of(column.getType().getTypeSignature().toString()), column.getComment()))
                        .collect(toImmutableList()),
                Optional.ofNullable(table.getParameters().get(TABLE_COMMENT)),
                Optional.empty(), // will be filled in later by HiveMetadata
                hiveViewsRunAsInvoker);
    }
}
