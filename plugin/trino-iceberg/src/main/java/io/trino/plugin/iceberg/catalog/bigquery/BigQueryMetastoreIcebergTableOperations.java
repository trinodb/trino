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
package io.trino.plugin.iceberg.catalog.bigquery;

import com.google.api.pathtemplate.ValidationException;
import com.google.api.services.bigquery.model.ExternalCatalogTableOptions;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.common.collect.Maps;
import io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.gcp.bigquery.BigQueryMetastoreClientImpl;
import org.apache.iceberg.io.FileIO;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

public class BigQueryMetastoreIcebergTableOperations
        extends AbstractIcebergTableOperations
{
    private static final String TABLE_PROPERTIES_BQ_CONNECTION = "bq_connection";
    private final BigQueryMetastoreClientImpl client;
    private final String projectId;
    private final String datasetId;
    private final String tableId;
    private final TableReference tableReference;
    private final String tableName;

    public BigQueryMetastoreIcebergTableOperations(
            BigQueryMetastoreClientImpl bqmsClient,
            FileIO fileIO,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location,
            String projectId)
    {
        super(fileIO, session, database, table, owner, location);
        this.client = requireNonNull(bqmsClient, "BigQueryClient is null");
        this.projectId = requireNonNull(projectId, "projectId is null");
        this.datasetId = requireNonNull(database, "database is null");
        this.tableId = requireNonNull(table, "table is null");
        this.tableReference = new TableReference()
                .setProjectId(projectId)
                .setDatasetId(datasetId)
                .setTableId(tableId);
        this.tableName = String.format("%s.%s.%s", this.projectId, this.datasetId, this.tableId);
    }

    //ok
    private String loadMetadataLocationOrThrow(ExternalCatalogTableOptions tableOptions)
    {
        if (tableOptions == null || !tableOptions.getParameters().containsKey(METADATA_LOCATION_PROP)) {
            throw new ValidationException(
                    "Table %s is not a valid BigQuery Metastore Iceberg table, metadata location not found",
                    this.tableName);
        }

        return tableOptions.getParameters().get(METADATA_LOCATION_PROP);
    }

    //ok
    @Override
    protected String getRefreshedLocation(boolean invalidateCaches)
    {
        try {
            return loadMetadataLocationOrThrow(client.load(tableReference).getExternalCatalogTableOptions());
        }
        catch (NoSuchTableException e) {
            throw new TableNotFoundException(getSchemaTableName());
        }
    }

    /** Adds Hive-style basic statistics from snapshot metadata if it exists. */
    private static void updateParametersWithSnapshotMetadata(
            TableMetadata metadata, Map<String, String> parameters)
    {
        if (metadata.currentSnapshot() == null) {
            return;
        }

        Map<String, String> summary = metadata.currentSnapshot().summary();
        if (summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP) != null) {
            parameters.put("numFiles", summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
        }

        if (summary.get(SnapshotSummary.TOTAL_RECORDS_PROP) != null) {
            parameters.put("numRows", summary.get(SnapshotSummary.TOTAL_RECORDS_PROP));
        }

        if (summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP) != null) {
            parameters.put("totalSize", summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP));
        }
    }

    private Map<String, String> buildTableParameters(
            String metadataFileLocation, TableMetadata metadata)
    {
        Map<String, String> parameters = Maps.newHashMap(metadata.properties());
        if (metadata.uuid() != null) {
            parameters.put(TableProperties.UUID, metadata.uuid());
        }
        if (metadata.metadataFileLocation() != null && !metadata.metadataFileLocation().isEmpty()) {
            parameters.put(PREVIOUS_METADATA_LOCATION_PROP, metadata.metadataFileLocation());
        }
        parameters.put(METADATA_LOCATION_PROP, metadataFileLocation);
        parameters.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE);
        parameters.put("EXTERNAL", "TRUE");

        updateParametersWithSnapshotMetadata(metadata, parameters);
        return parameters;
    }

    private com.google.api.services.bigquery.model.Table makeNewTable(TableMetadata metadata, String metadataFileLocation)
    {
        return new com.google.api.services.bigquery.model.Table()
                .setExternalCatalogTableOptions(
                        BigQueryMetastoreIcebergUtil.createExternalCatalogTableOptions(
                                metadata.location(), buildTableParameters(metadataFileLocation, metadata)));
    }

    private void addConnectionIfProvided(Table tableBuilder, Map<String, String> metadataProperties)
    {
        if (metadataProperties.containsKey(TABLE_PROPERTIES_BQ_CONNECTION)) {
            tableBuilder
                    .getExternalCatalogTableOptions()
                    .setConnectionId(metadataProperties.get(TABLE_PROPERTIES_BQ_CONNECTION));
        }
    }

    private void createTable(String newMetadataLocation, TableMetadata metadata)
    {
        com.google.api.services.bigquery.model.Table tableBuilder = makeNewTable(metadata, newMetadataLocation);
        tableBuilder.setTableReference(this.tableReference);
        addConnectionIfProvided(tableBuilder, metadata.properties());
        client.create(tableBuilder);
    }

    /** Update table properties with concurrent update detection using etag. */
    private void updateTable(
            String oldMetadataLocation, String newMetadataLocation, TableMetadata metadata)
    {
        Table table = client.load(tableReference);
        if (table.getEtag().isEmpty()) {
            throw new ValidationException(
                    "Etag of legacy table %s is empty, manually update the table via the BigQuery API or"
                            + " recreate and retry",
                    this.tableName);
        }
        ExternalCatalogTableOptions options = table.getExternalCatalogTableOptions();
        addConnectionIfProvided(table, metadata.properties());

        String metadataLocationFromMetastore =
                options.getParameters().getOrDefault(METADATA_LOCATION_PROP, "");
        if (!metadataLocationFromMetastore.isEmpty()
                && !metadataLocationFromMetastore.equals(oldMetadataLocation)) {
            throw new CommitFailedException(
                    "Cannot commit base metadata location '%s' is not same as the current table metadata location '%s' for"
                            + " %s.%s",
                    oldMetadataLocation,
                    metadataLocationFromMetastore,
                    tableReference.getDatasetId(),
                    tableReference.getTableId());
        }

        options.setParameters(buildTableParameters(newMetadataLocation, metadata));
        try {
            client.update(tableReference, table);
        }
        catch (ValidationException e) {
            if (e.getMessage().toLowerCase(Locale.ENGLISH).contains("etag mismatch")) {
                throw new CommitFailedException(
                        "Updating table failed due to conflict updates (etag mismatch). Retry the update");
            }
            throw e;
        }
    }

    @Override
    protected void commitNewTable(TableMetadata metadata)
    {
        verify(version.isEmpty(), "commitNewTable called on a table which already exists");
        String newMetadataLocation = writeNewMetadata(metadata, 0);
        boolean isCommitted = false;
        try {
            createTable(newMetadataLocation, metadata);
            isCommitted = true;
        }
        catch (Exception e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Cannot commit table creation", e);
        }
        finally {
            try {
                if (!isCommitted) {
                    io().deleteFile(newMetadataLocation);
                }
            }
            catch (RuntimeException e) {
                throw new TrinoException(ICEBERG_INVALID_METADATA, format("Failed to cleanup metadata file at [%s] property: %s", newMetadataLocation, getSchemaTableName()));
            }
        }
        shouldRefresh = true;
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        String newMetadataLocation = writeNewMetadata(metadata, version.orElseThrow() + 1);
        try {
            updateTable(base.metadataFileLocation(), newMetadataLocation, metadata);
        }
        catch (Exception e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Cannot update table", e);
        }
        shouldRefresh = true;
    }

    @Override
    protected void commitMaterializedViewRefresh(TableMetadata base, TableMetadata metadata)
    {
        throw new UnsupportedOperationException("Committing views through BigQuery Metastore is not yet implemented");
    }
}
