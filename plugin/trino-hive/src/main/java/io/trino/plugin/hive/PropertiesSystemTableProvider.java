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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.SystemTableHandler.PROPERTIES;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.SystemTables.createSystemTable;
import static java.lang.String.format;

public class PropertiesSystemTableProvider
        implements SystemTableProvider
{
    @Override
    public Optional<SchemaTableName> getSourceTableName(SchemaTableName tableName)
    {
        if (!PROPERTIES.matches(tableName)) {
            return Optional.empty();
        }

        return Optional.of(PROPERTIES.getSourceTableName(tableName));
    }

    @Override
    public Optional<SystemTable> getSystemTable(HiveMetadata metadata, ConnectorSession session, SchemaTableName tableName)
    {
        if (!PROPERTIES.matches(tableName)) {
            return Optional.empty();
        }

        SchemaTableName sourceTableName = PROPERTIES.getSourceTableName(tableName);
        Table table = metadata.getMetastore()
                .getTable(new HiveIdentity(session), sourceTableName.getSchemaName(), sourceTableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        if (isDeltaLakeTable(table)) {
            throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, format("Cannot query Delta Lake table '%s'", sourceTableName));
        }
        Map<String, String> sortedTableParameters = ImmutableSortedMap.copyOf(table.getParameters());
        List<ColumnMetadata> columns = sortedTableParameters.keySet().stream()
                .map(key -> new ColumnMetadata(key, VarcharType.VARCHAR))
                .collect(toImmutableList());
        List<Type> types = columns.stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        Iterable<List<Object>> propertyValues = ImmutableList.of(ImmutableList.copyOf(sortedTableParameters.values()));

        return Optional.of(createSystemTable(new ConnectorTableMetadata(sourceTableName, columns), constraint -> new InMemoryRecordSet(types, propertyValues).cursor()));
    }
}
