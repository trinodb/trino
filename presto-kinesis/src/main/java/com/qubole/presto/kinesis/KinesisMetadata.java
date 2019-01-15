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
package com.qubole.presto.kinesis;

import com.facebook.presto.decoder.dummy.DummyRowDecoder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class KinesisMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(KinesisMetadata.class);

    private final String connectorId;
    private final KinesisConnectorConfig kinesisConnectorConfig;
    private final KinesisHandleResolver handleResolver;

    private final Supplier<Map<SchemaTableName, KinesisStreamDescription>> kinesisTableDescriptionSupplier;
    private final Set<KinesisInternalFieldDescription> internalFieldDescriptions;

    @Inject
    KinesisMetadata(@Named("connectorId") String connectorId,
            KinesisConnectorConfig kinesisConnectorConfig,
            KinesisHandleResolver handleResolver,
            Supplier<Map<SchemaTableName, KinesisStreamDescription>> kinesisTableDescriptionSupplier,
            Set<KinesisInternalFieldDescription> internalFieldDescriptions)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.kinesisConnectorConfig = requireNonNull(kinesisConnectorConfig, "kinesisConfig is null");
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");

        log.debug("Loading kinesis table definitions from %s", kinesisConnectorConfig.getTableDescriptionDir());

        this.kinesisTableDescriptionSupplier = kinesisTableDescriptionSupplier;
        this.internalFieldDescriptions = requireNonNull(internalFieldDescriptions, "internalFieldDescriptions is null");
    }

    /**
     * Expose configuration to related internal classes that may need it.
     */
    public KinesisConnectorConfig getConnectorConfig()
    {
        return this.kinesisConnectorConfig;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (SchemaTableName tableName : getDefinedTables().keySet()) {
            builder.add(tableName.getSchemaName());
        }
        return ImmutableList.copyOf(builder.build());
    }

    @Override
    public KinesisTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        KinesisStreamDescription table = getDefinedTables().get(schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        return new KinesisTableHandle(connectorId,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                table.getStreamName(),
                getDataFormat(table.getMessage()));
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession connectorSession, ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> optional)
    {
        KinesisTableHandle tblHandle = handleResolver.convertTableHandle(table);
        ConnectorTableLayout layout = new ConnectorTableLayout(new KinesisTableLayoutHandle(connectorId, tblHandle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession connectorSession, ConnectorTableLayoutHandle connectorTableLayoutHandle)
    {
        return new ConnectorTableLayout(connectorTableLayoutHandle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession connectorSession, ConnectorTableHandle tableHandle)
    {
        KinesisTableHandle kinesisTableHandle = handleResolver.convertTableHandle(tableHandle);
        log.debug("Called getTableMetadata on %s.%s", kinesisTableHandle.getSchemaName(), kinesisTableHandle.getTableName());
        return getTableMetadata(kinesisTableHandle.toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (SchemaTableName tableName : getDefinedTables().keySet()) {
            if (schemaNameOrNull == null || tableName.getSchemaName().equals(schemaNameOrNull)) {
                builder.add(tableName);
            }
        }

        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession connectorSession, ConnectorTableHandle tableHandle)
    {
        KinesisTableHandle kinesisTableHandle = handleResolver.convertTableHandle(tableHandle);

        KinesisStreamDescription kinesisStreamDescription = getDefinedTables().get(kinesisTableHandle.toSchemaTableName());
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
                    columnHandles.put(kinesisStreamFieldDescription.getName(), kinesisStreamFieldDescription.getColumnHandle(connectorId, index++));
                }
            }
        }

        for (KinesisInternalFieldDescription kinesisInternalFieldDescription : internalFieldDescriptions) {
            columnHandles.put(kinesisInternalFieldDescription.getColumnName(), kinesisInternalFieldDescription.getColumnHandle(connectorId, index++, kinesisConnectorConfig.isHideInternalColumns()));
        }

        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession connectorSession, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        handleResolver.convertTableHandle(tableHandle);
        KinesisColumnHandle kinesisColumnHandle = handleResolver.convertColumnHandle(columnHandle);

        return kinesisColumnHandle.getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        log.debug("Called listTableColumns on %s.%s", prefix.getSchemaName(), prefix.getTableName());

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        // NOTE: prefix.getTableName or prefix.getSchemaName can be null
        List<SchemaTableName> tableNames;
        if (prefix.getSchemaName() != null && prefix.getTableName() != null) {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            tableNames = listTables(session, (String) null);
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

    @VisibleForTesting
    Map<SchemaTableName, KinesisStreamDescription> getDefinedTables()
    {
        return kinesisTableDescriptionSupplier.get();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        KinesisStreamDescription table = getDefinedTables().get(schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        KinesisStreamFieldGroup message = table.getMessage();
        if (message != null) {
            List<KinesisStreamFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (KinesisStreamFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata());
                }
            }
        }

        for (KinesisInternalFieldDescription fieldDescription : internalFieldDescriptions) {
            builder.add(fieldDescription.getColumnMetadata(kinesisConnectorConfig.isHideInternalColumns()));
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }
}
