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
package io.trino.connector.system;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.security.AccessControl;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.MaterializedViewNotFoundException;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.LongTimestampWithTimeZone;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.connector.system.jdbc.FilterUtil.isImpossibleObjectName;
import static io.trino.connector.system.jdbc.FilterUtil.tablePrefix;
import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataListing.getMaterializedViews;
import static io.trino.metadata.MetadataListing.listCatalogNames;
import static io.trino.metadata.MetadataListing.listMaterializedViews;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class MaterializedViewSystemTable
        implements SystemTable
{
    private static final ConnectorTableMetadata TABLE_DEFINITION = tableMetadataBuilder(
            new SchemaTableName("metadata", "materialized_views"))
            .column("catalog_name", createUnboundedVarcharType())
            .column("schema_name", createUnboundedVarcharType())
            .column("name", createUnboundedVarcharType())
            .column("storage_catalog", createUnboundedVarcharType())
            .column("storage_schema", createUnboundedVarcharType())
            .column("storage_table", createUnboundedVarcharType())
            .column("freshness", createUnboundedVarcharType())
            .column("last_fresh_time", createTimestampWithTimeZoneType(9)) // point in time
            .column("comment", createUnboundedVarcharType())
            .column("definition", createUnboundedVarcharType())
            .build();

    private static final Set<String> LISTING_ONLY_COLUMNS = ImmutableSet.of("catalog_name", "schema_name", "name");

    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public MaterializedViewSystemTable(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return TABLE_DEFINITION;
    }

    @Override
    public RecordCursor cursor(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession connectorSession,
            TupleDomain<Integer> constraint,
            Set<Integer> requiredColumns,
            ConnectorSplit split)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();
        Set<String> requiredColumnNames = requiredColumns.stream()
                .map(TABLE_DEFINITION.getColumns()::get)
                .map(ColumnMetadata::getName)
                .collect(toImmutableSet());

        InMemoryRecordSet.Builder displayTable = InMemoryRecordSet.builder(getTableMetadata());

        Domain catalogDomain = constraint.getDomain(0, VARCHAR);
        Domain schemaDomain = constraint.getDomain(1, VARCHAR);
        Domain tableDomain = constraint.getDomain(2, VARCHAR);

        if (isImpossibleObjectName(catalogDomain) || isImpossibleObjectName(schemaDomain) || isImpossibleObjectName(tableDomain)) {
            return displayTable.build().cursor();
        }

        Optional<String> tableFilter = tryGetSingleVarcharValue(tableDomain);
        boolean needNameOnly = LISTING_ONLY_COLUMNS.containsAll(requiredColumnNames);
        boolean needFreshness = requiredColumnNames.contains("freshness") || requiredColumnNames.contains("last_fresh_time");

        listCatalogNames(session, metadata, accessControl, catalogDomain).forEach(catalogName -> {
            // TODO A connector may be able to pull information from multiple schemas at once, so pass the schema filter to the connector instead.
            // TODO Support LIKE predicates on schema name (or any other functional predicates), so pass the schema filter as Constraint-like to the connector.
            if (schemaDomain.isNullableDiscreteSet()) {
                for (Object slice : schemaDomain.getNullableDiscreteSet().getNonNullValues()) {
                    String schemaName = ((Slice) slice).toStringUtf8();
                    if (isImpossibleObjectName(schemaName)) {
                        continue;
                    }
                    addMaterializedViewForCatalog(session, displayTable, tablePrefix(catalogName, Optional.of(schemaName), tableFilter), needNameOnly, needFreshness);
                }
            }
            else {
                addMaterializedViewForCatalog(session, displayTable, tablePrefix(catalogName, Optional.empty(), tableFilter), needNameOnly, needFreshness);
            }
        });

        return displayTable.build().cursor();
    }

    private void addMaterializedViewForCatalog(Session session, InMemoryRecordSet.Builder displayTable, QualifiedTablePrefix tablePrefix, boolean needNameOnly, boolean needFreshness)
    {
        if (needNameOnly) {
            listMaterializedViews(session, metadata, accessControl, tablePrefix).forEach(name ->
                    displayTable.addRow(createMaterializedViewRow(
                            tablePrefix.getCatalogName(),
                            name.getSchemaName(),
                            name.getTableName(),
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null)));
            return;
        }

        getMaterializedViews(session, metadata, accessControl, tablePrefix).forEach((tableName, definition) -> {
            QualifiedObjectName name = new QualifiedObjectName(tablePrefix.getCatalogName(), tableName.getSchemaName(), tableName.getTableName());
            Optional<MaterializedViewFreshness> freshness = Optional.empty();

            if (needFreshness) {
                try {
                    freshness = Optional.of(metadata.getMaterializedViewFreshness(session, name));
                }
                catch (MaterializedViewNotFoundException e) {
                    // Ignore materialized view that was dropped during query execution (race condition)
                    return;
                }
            }

            displayTable.addRow(createMaterializedViewRow(
                    name.catalogName(),
                    name.schemaName(),
                    name.objectName(),
                    definition.getStorageTable()
                            .map(CatalogSchemaTableName::getCatalogName)
                            .orElse(""),
                    definition.getStorageTable()
                            .map(storageTable -> storageTable.getSchemaTableName().getSchemaName())
                            .orElse(""),
                    definition.getStorageTable()
                            .map(storageTable -> storageTable.getSchemaTableName().getTableName())
                            .orElse(""),
                    // freshness
                    freshness.map(MaterializedViewFreshness::getFreshness)
                            .map(Enum::name)
                            .orElse(null),
                    // last_fresh_time
                    freshness.flatMap(MaterializedViewFreshness::getLastFreshTime)
                            .map(instant -> LongTimestampWithTimeZone.fromEpochSecondsAndFraction(
                                    instant.getEpochSecond(),
                                    (long) instant.getNano() * PICOSECONDS_PER_NANOSECOND,
                                    UTC_KEY))
                            .orElse(null),
                    definition.getComment().orElse(""),
                    definition.getOriginalSql()));
        });
    }

    // Table schema as a Java method signature
    private static Object[] createMaterializedViewRow(
            String catalogName,
            String schemaName,
            String name,
            String storageCatalog,
            String storageSchema,
            String storageTable,
            String freshness,
            LongTimestampWithTimeZone lastFreshTime,
            String comment,
            String definition)
    {
        return new Object[] {
                catalogName,
                schemaName,
                name,
                storageCatalog,
                storageSchema,
                storageTable,
                freshness,
                lastFreshTime,
                comment,
                definition,
        };
    }
}
