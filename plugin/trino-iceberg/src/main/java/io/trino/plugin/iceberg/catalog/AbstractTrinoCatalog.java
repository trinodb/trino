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
package io.trino.plugin.iceberg.catalog;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveMetadata;
import io.trino.plugin.hive.HiveViewNotSupportedException;
import io.trino.plugin.hive.ViewReaderUtil;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.hive.ViewReaderUtil.isHiveOrPrestoView;
import static io.trino.plugin.hive.ViewReaderUtil.isPrestoView;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.Transactions.createTableTransaction;

public abstract class AbstractTrinoCatalog
        implements TrinoCatalog
{
    // Be compatible with views defined by the Hive connector, which can be useful under certain conditions.
    protected static final String TRINO_CREATED_BY = HiveMetadata.TRINO_CREATED_BY;
    protected static final String TRINO_CREATED_BY_VALUE = "Trino Iceberg connector";
    protected static final String PRESTO_VIEW_COMMENT = HiveMetadata.PRESTO_VIEW_COMMENT;
    protected static final String PRESTO_VERSION_NAME = HiveMetadata.PRESTO_VERSION_NAME;
    protected static final String PRESTO_QUERY_ID_NAME = HiveMetadata.PRESTO_QUERY_ID_NAME;
    protected static final String PRESTO_VIEW_EXPANDED_TEXT_MARKER = HiveMetadata.PRESTO_VIEW_EXPANDED_TEXT_MARKER;

    protected final IcebergTableOperationsProvider tableOperationsProvider;
    private final String trinoVersion;
    private final boolean useUniqueTableLocation;

    protected AbstractTrinoCatalog(
            IcebergTableOperationsProvider tableOperationsProvider,
            String trinoVersion,
            boolean useUniqueTableLocation)
    {
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        this.useUniqueTableLocation = useUniqueTableLocation;
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        Table icebergTable = loadTable(session, schemaTableName);
        if (comment.isEmpty()) {
            icebergTable.updateProperties().remove(TABLE_COMMENT).commit();
        }
        else {
            icebergTable.updateProperties().set(TABLE_COMMENT, comment.get()).commit();
        }
    }

    @Override
    public void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment)
    {
        Table icebergTable = loadTable(session, schemaTableName);
        icebergTable.updateSchema().updateColumnDoc(columnIdentity.getName(), comment.orElse(null)).commit();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        for (SchemaTableName name : listViews(session, namespace)) {
            try {
                getView(session, name).ifPresent(view -> views.put(name, view));
            }
            catch (TrinoException e) {
                if (e.getErrorCode().equals(TABLE_NOT_FOUND.toErrorCode())) {
                    // Ignore view that was dropped during query execution (race condition)
                }
                else {
                    throw e;
                }
            }
        }
        return views.buildOrThrow();
    }

    protected Transaction newCreateTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            String location,
            Map<String, String> properties,
            Optional<String> owner)
    {
        TableMetadata metadata = newTableMetadata(schema, partitionSpec, location, properties);
        TableOperations ops = tableOperationsProvider.createTableOperations(
                this,
                session,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                owner,
                Optional.of(location));
        return createTableTransaction(schemaTableName.toString(), ops, metadata);
    }

    protected String createNewTableName(String baseTableName)
    {
        String tableName = baseTableName;
        if (useUniqueTableLocation) {
            tableName += "-" + randomUUID().toString().replace("-", "");
        }
        return tableName;
    }

    protected void deleteTableDirectory(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            HdfsEnvironment hdfsEnvironment,
            Path tableLocation)
    {
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session), tableLocation);
            fileSystem.delete(tableLocation, true);
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, format("Failed to delete directory %s of the table %s", tableLocation, schemaTableName), e);
        }
    }

    protected Optional<ConnectorViewDefinition> getView(
            SchemaTableName viewName,
            Optional<String> viewOriginalText,
            String tableType,
            Map<String, String> tableParameters,
            Optional<String> tableOwner)
    {
        if (!isView(tableType, tableParameters)) {
            // Filter out Tables and Materialized Views
            return Optional.empty();
        }

        if (!isPrestoView(tableParameters)) {
            // Hive views are not compatible
            throw new HiveViewNotSupportedException(viewName);
        }

        checkArgument(viewOriginalText.isPresent(), "viewOriginalText must be present");
        ConnectorViewDefinition definition = ViewReaderUtil.PrestoViewReader.decodeViewData(viewOriginalText.get());
        // use owner from table metadata if it exists
        if (tableOwner.isPresent() && !definition.isRunAsInvoker()) {
            definition = new ConnectorViewDefinition(
                    definition.getOriginalSql(),
                    definition.getCatalog(),
                    definition.getSchema(),
                    definition.getColumns(),
                    definition.getComment(),
                    tableOwner,
                    false);
        }
        return Optional.of(definition);
    }

    private static boolean isView(String tableType, Map<String, String> tableParameters)

    {
        return isHiveOrPrestoView(tableType) && PRESTO_VIEW_COMMENT.equals(tableParameters.get(TABLE_COMMENT));
    }

    protected Map<String, String> createViewProperties(ConnectorSession session)
    {
        return ImmutableMap.<String, String>builder()
                .put(PRESTO_VIEW_FLAG, "true") // Ensures compatibility with views created by the Hive connector
                .put(TRINO_CREATED_BY, TRINO_CREATED_BY_VALUE)
                .put(PRESTO_VERSION_NAME, trinoVersion)
                .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                .put(TABLE_COMMENT, PRESTO_VIEW_COMMENT)
                .buildOrThrow();
    }
}
