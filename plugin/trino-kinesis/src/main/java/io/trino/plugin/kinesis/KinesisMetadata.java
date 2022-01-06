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
package io.trino.plugin.kinesis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.decoder.dummy.DummyRowDecoder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class KinesisMetadata
        implements ConnectorMetadata
{
    private final Supplier<Map<SchemaTableName, KinesisStreamDescription>> tableDescriptionSupplier;
    private final Set<KinesisInternalFieldDescription> internalFieldDescriptions;
    private final boolean isHideInternalColumns;

    @Inject
    public KinesisMetadata(
            KinesisConfig kinesisConfig,
            Supplier<Map<SchemaTableName, KinesisStreamDescription>> tableDescriptionSupplier,
            Set<KinesisInternalFieldDescription> internalFieldDescriptions)
    {
        requireNonNull(kinesisConfig, "kinesisConfig is null");
        isHideInternalColumns = kinesisConfig.isHideInternalColumns();
        this.tableDescriptionSupplier = requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null");
        this.internalFieldDescriptions = requireNonNull(internalFieldDescriptions, "internalFieldDescriptions is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return tableDescriptionSupplier.get().keySet().stream()
                .map(SchemaTableName::getSchemaName)
                .collect(toImmutableList());
    }

    @Override
    public KinesisTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        KinesisStreamDescription table = tableDescriptionSupplier.get().get(schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        return new KinesisTableHandle(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                table.getStreamName(),
                getDataFormat(table.getMessage()),
                table.getMessage().getCompressionCodec());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession connectorSession, ConnectorTableHandle tableHandle)
    {
        return getTableMetadata(((KinesisTableHandle) tableHandle).toSchemaTableName());
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (SchemaTableName tableName : tableDescriptionSupplier.get().keySet()) {
            if ((schemaName.isEmpty()) || tableName.getSchemaName().equals(schemaName.get())) {
                builder.add(tableName);
            }
        }

        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession connectorSession, ConnectorTableHandle tableHandle)
    {
        KinesisTableHandle kinesisTableHandle = (KinesisTableHandle) tableHandle;

        KinesisStreamDescription kinesisStreamDescription = tableDescriptionSupplier.get().get(kinesisTableHandle.toSchemaTableName());
        if (kinesisStreamDescription == null) {
            throw new TableNotFoundException(kinesisTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

        int index = 0;
        // Note: partition key and related fields are handled by internalFieldDescriptions below
        KinesisStreamFieldGroup message = kinesisStreamDescription.getMessage();
        if (message != null) {
            List<KinesisStreamFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (KinesisStreamFieldDescription kinesisStreamFieldDescription : fields) {
                    columnHandles.put(kinesisStreamFieldDescription.getName(), kinesisStreamFieldDescription.getColumnHandle(index++));
                }
            }
        }

        for (KinesisInternalFieldDescription kinesisInternalFieldDescription : internalFieldDescriptions) {
            columnHandles.put(kinesisInternalFieldDescription.getColumnName(), kinesisInternalFieldDescription.getColumnHandle(index++, isHideInternalColumns));
        }

        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession connectorSession, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        KinesisColumnHandle kinesisColumnHandle = (KinesisColumnHandle) columnHandle;
        return kinesisColumnHandle.getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        // NOTE: prefix.getTableName or prefix.getSchemaName can be null
        List<SchemaTableName> tableNames;
        if (prefix.getSchema().isPresent() && prefix.getTable().isPresent()) {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchema().get(), prefix.getTable().get()));
        }
        else {
            tableNames = listTables(session, Optional.empty());
        }

        for (SchemaTableName tableName : tableNames) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private static String getDataFormat(KinesisStreamFieldGroup fieldGroup)
    {
        return (fieldGroup == null) ? DummyRowDecoder.NAME : fieldGroup.getDataFormat();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        KinesisStreamDescription kinesisStreamDescription = tableDescriptionSupplier.get().get(schemaTableName);
        if (kinesisStreamDescription == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        KinesisStreamFieldGroup message = kinesisStreamDescription.getMessage();
        if (message != null) {
            List<KinesisStreamFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (KinesisStreamFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata());
                }
            }
        }

        for (KinesisInternalFieldDescription fieldDescription : internalFieldDescriptions) {
            builder.add(fieldDescription.getColumnMetadata(isHideInternalColumns));
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }
}
