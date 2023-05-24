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

import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.ViewInfo;
import io.trino.security.AccessControl;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.MaterializedViewNotFoundException;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.LongTimestampWithTimeZone;

import javax.inject.Inject;

import java.util.Optional;

import static io.trino.connector.system.jdbc.FilterUtil.tablePrefix;
import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataListing.getMaterializedViews;
import static io.trino.metadata.MetadataListing.listCatalogNames;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
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
            TupleDomain<Integer> constraint)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();
        InMemoryRecordSet.Builder displayTable = InMemoryRecordSet.builder(getTableMetadata());

        Optional<String> catalogFilter = tryGetSingleVarcharValue(constraint, 0);
        Optional<String> schemaFilter = tryGetSingleVarcharValue(constraint, 1);
        Optional<String> tableFilter = tryGetSingleVarcharValue(constraint, 2);

        listCatalogNames(session, metadata, accessControl, catalogFilter).forEach(catalogName -> {
            QualifiedTablePrefix tablePrefix = tablePrefix(catalogName, schemaFilter, tableFilter);

            getMaterializedViews(session, metadata, accessControl, tablePrefix).forEach((tableName, definition) -> {
                QualifiedObjectName name = new QualifiedObjectName(tablePrefix.getCatalogName(), tableName.getSchemaName(), tableName.getTableName());
                MaterializedViewFreshness freshness;

                try {
                    freshness = metadata.getMaterializedViewFreshness(session, name);
                }
                catch (MaterializedViewNotFoundException e) {
                    // Ignore materialized view that was dropped during query execution (race condition)
                    return;
                }

                Object[] materializedViewRow = createMaterializedViewRow(name, freshness, definition);
                displayTable.addRow(materializedViewRow);
            });
        });

        return displayTable.build().cursor();
    }

    private static Object[] createMaterializedViewRow(
            QualifiedObjectName name,
            MaterializedViewFreshness freshness,
            ViewInfo definition)
    {
        return new Object[] {
                name.getCatalogName(),
                name.getSchemaName(),
                name.getObjectName(),
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
                freshness.getFreshness().name(),
                // last_fresh_time
                freshness.getLastFreshTime()
                        .map(instant -> LongTimestampWithTimeZone.fromEpochSecondsAndFraction(
                                instant.getEpochSecond(),
                                (long) instant.getNano() * PICOSECONDS_PER_NANOSECOND,
                                UTC_KEY))
                        .orElse(null),
                definition.getComment().orElse(""),
                definition.getOriginalSql()
        };
    }
}
