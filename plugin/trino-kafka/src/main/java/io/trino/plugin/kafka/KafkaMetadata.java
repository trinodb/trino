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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.decoder.dummy.DummyRowDecoder;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ComputedStatistics;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.DUPLICATE_COLUMN_NAME;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

/**
 * Manages the Kafka connector specific metadata information. The Connector provides an additional set of columns
 * for each table that are created as hidden columns. See {@link KafkaInternalFieldManager} for a list
 * of per-topic additional columns.
 */
public class KafkaMetadata
        implements ConnectorMetadata
{
    private final boolean hideInternalColumns;
    private final TableDescriptionSupplier tableDescriptionSupplier;
    private final KafkaInternalFieldManager kafkaInternalFieldManager;

    @Inject
    public KafkaMetadata(
            KafkaConfig kafkaConfig,
            TableDescriptionSupplier tableDescriptionSupplier,
            KafkaInternalFieldManager kafkaInternalFieldManager)
    {
        this.hideInternalColumns = kafkaConfig.isHideInternalColumns();
        this.tableDescriptionSupplier = requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null");
        this.kafkaInternalFieldManager = requireNonNull(kafkaInternalFieldManager, "kafkaInternalFieldManager is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return tableDescriptionSupplier.listTables().stream()
                .map(SchemaTableName::getSchemaName)
                .collect(toImmutableList());
    }

    @Override
    public KafkaTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        return getTopicDescription(session, schemaTableName)
                .map(kafkaTopicDescription -> new KafkaTableHandle(
                        schemaTableName.getSchemaName(),
                        schemaTableName.getTableName(),
                        kafkaTopicDescription.topicName(),
                        getDataFormat(kafkaTopicDescription.key()),
                        getDataFormat(kafkaTopicDescription.message()),
                        kafkaTopicDescription.key().flatMap(KafkaTopicFieldGroup::dataSchema),
                        kafkaTopicDescription.message().flatMap(KafkaTopicFieldGroup::dataSchema),
                        kafkaTopicDescription.key().flatMap(KafkaTopicFieldGroup::subject),
                        kafkaTopicDescription.message().flatMap(KafkaTopicFieldGroup::subject),
                        getColumnHandles(session, schemaTableName).values().stream()
                                .map(KafkaColumnHandle.class::cast)
                                .collect(toImmutableList()),
                        TupleDomain.all()))
                .orElse(null);
    }

    private static String getDataFormat(Optional<KafkaTopicFieldGroup> fieldGroup)
    {
        return fieldGroup.map(KafkaTopicFieldGroup::dataFormat).orElse(DummyRowDecoder.NAME);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getTableMetadata(session, ((KafkaTableHandle) tableHandle).schemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return tableDescriptionSupplier.listTables().stream()
                .filter(tableName -> schemaName.map(tableName.getSchemaName()::equals).orElse(true))
                .collect(toImmutableList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getColumnHandles(session, ((KafkaTableHandle) tableHandle).schemaTableName());
    }

    private Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, SchemaTableName schemaTableName)
    {
        KafkaTopicDescription kafkaTopicDescription = getRequiredTopicDescription(session, schemaTableName);

        Stream<KafkaColumnHandle> keyColumnHandles = kafkaTopicDescription.key().stream()
                .map(KafkaTopicFieldGroup::fields)
                .flatMap(Collection::stream)
                .map(kafkaTopicFieldDescription -> kafkaTopicFieldDescription.columnHandle(true));

        Stream<KafkaColumnHandle> messageColumnHandles = kafkaTopicDescription.message().stream()
                .map(KafkaTopicFieldGroup::fields)
                .flatMap(Collection::stream)
                .map(kafkaTopicFieldDescription -> kafkaTopicFieldDescription.columnHandle(false));

        List<KafkaColumnHandle> topicColumnHandles = concat(keyColumnHandles, messageColumnHandles)
                .collect(toImmutableList());

        List<KafkaColumnHandle> internalColumnHandles = kafkaInternalFieldManager.getInternalFields().stream()
                .map(kafkaInternalField -> kafkaInternalField.getColumnHandle(hideInternalColumns))
                .collect(toImmutableList());

        Set<String> conflictingColumns = topicColumnHandles.stream().map(KafkaColumnHandle::getName).collect(toSet());
        conflictingColumns.retainAll(internalColumnHandles.stream().map(KafkaColumnHandle::getName).collect(toSet()));
        if (!conflictingColumns.isEmpty()) {
            throw new TrinoException(DUPLICATE_COLUMN_NAME, "Internal Kafka column names conflict with column names from the table. "
                    + "Consider changing kafka.internal-column-prefix configuration property. "
                    + "topic=" + schemaTableName
                    + ", Conflicting names=" + conflictingColumns);
        }

        return concat(topicColumnHandles.stream(), internalColumnHandles.stream())
                .collect(toImmutableMap(KafkaColumnHandle::getName, identity()));
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        SchemaTablePrefix prefix = schemaName.map(SchemaTablePrefix::new)
                .orElseGet(SchemaTablePrefix::new);

        List<SchemaTableName> tableNames;
        if (prefix.getTable().isEmpty()) {
            tableNames = listTables(session, prefix.getSchema());
        }
        else {
            tableNames = ImmutableList.of(prefix.toSchemaTableName());
        }

        Map<SchemaTableName, RelationColumnsMetadata> relationColumns = new HashMap<>();
        for (SchemaTableName tableName : tableNames) {
            try {
                relationColumns.put(tableName, RelationColumnsMetadata.forTable(tableName, getTableMetadata(session, tableName).getColumns()));
            }
            catch (TableNotFoundException e) {
                // information_schema table or a system table
            }
        }
        return relationFilter.apply(relationColumns.keySet()).stream()
                .map(relationColumns::get)
                .iterator();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((KafkaColumnHandle) columnHandle).getColumnMetadata();
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName schemaTableName)
    {
        KafkaTopicDescription table = getRequiredTopicDescription(session, schemaTableName);

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        table.key().ifPresent(key -> {
            List<KafkaTopicFieldDescription> fields = key.fields();
            if (fields != null) {
                for (KafkaTopicFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.columnMetadata());
                }
            }
        });

        table.message().ifPresent(message -> {
            List<KafkaTopicFieldDescription> fields = message.fields();
            if (fields != null) {
                for (KafkaTopicFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.columnMetadata());
                }
            }
        });

        for (KafkaInternalFieldManager.InternalField fieldDescription : kafkaInternalFieldManager.getInternalFields()) {
            builder.add(fieldDescription.getColumnMetadata(hideInternalColumns));
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        KafkaTableHandle handle = (KafkaTableHandle) table;
        TupleDomain<ColumnHandle> oldDomain = handle.constraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new KafkaTableHandle(
                handle.schemaName(),
                handle.tableName(),
                handle.topicName(),
                handle.keyDataFormat(),
                handle.messageDataFormat(),
                handle.keyDataSchemaLocation(),
                handle.messageDataSchemaLocation(),
                handle.keySubject(),
                handle.messageSubject(),
                handle.columns(),
                newDomain);

        return Optional.of(new ConstraintApplicationResult<>(handle, constraint.getSummary(), constraint.getExpression(), false));
    }

    private KafkaTopicDescription getRequiredTopicDescription(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return getTopicDescription(session, schemaTableName).orElseThrow(() -> new TableNotFoundException(schemaTableName));
    }

    private Optional<KafkaTopicDescription> getTopicDescription(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return tableDescriptionSupplier.getTopicDescription(session, schemaTableName);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        // TODO: support transactional inserts https://github.com/trinodb/trino/issues/4303
        KafkaTableHandle table = (KafkaTableHandle) tableHandle;
        List<KafkaColumnHandle> actualColumns = table.columns().stream()
                .filter(columnHandle -> !columnHandle.isInternal() && !columnHandle.isHidden())
                .collect(toImmutableList());

        checkArgument(columns.equals(actualColumns), "Unexpected columns!\nexpected: %s\ngot: %s", actualColumns, columns);

        return new KafkaTableHandle(
                table.schemaName(),
                table.tableName(),
                table.topicName(),
                table.keyDataFormat(),
                table.messageDataFormat(),
                table.keyDataSchemaLocation(),
                table.messageDataSchemaLocation(),
                table.keySubject(),
                table.messageSubject(),
                actualColumns,
                TupleDomain.none());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        // TODO: support transactional inserts https://github.com/trinodb/trino/issues/4303
        return Optional.empty();
    }
}
