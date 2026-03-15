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
package io.trino.plugin.paimon.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.paimon.CatalogType;
import io.trino.plugin.paimon.PaimonConnector;
import io.trino.plugin.paimon.PaimonMetadata;
import io.trino.plugin.paimon.PaimonMetadataFactory;
import io.trino.plugin.paimon.catalog.TrinoCatalog;
import io.trino.plugin.paimon.fileio.PaimonFileIO;
import io.trino.plugin.tpch.TpchMetadata;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorSession;
import io.trino.tpch.TpchTable;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.intellij.lang.annotations.Language;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.paimon.testing.PaimonTestingUtils.createPaimonTable;
import static java.lang.String.format;

public class PaimonTablesInitializer
{
    private static final CatalogSchemaName TPCH_TINY = new CatalogSchemaName("tpch", "tiny");
    private static final Logger log = Logger.get(PaimonTablesInitializer.class);

    private final List<TpchTable<?>> tpchTables;

    public PaimonTablesInitializer(List<TpchTable<?>> tpchTables)
    {
        this.tpchTables = ImmutableList.copyOf(tpchTables);
    }

    private static void createTableWithData(Catalog catalog, String databaseName, TpchTable<?> tpchTable, QueryRunner queryRunner)
            throws Exception
    {
        @Language("SQL") String sql = generateScanSql(TPCH_TINY, tpchTable);
        log.info("Executing %s", sql);
        MaterializedResult result = queryRunner.execute(sql);

        org.apache.paimon.table.Table paimonTable = catalog.getTable(Identifier.create(databaseName, tpchTable.getTableName()));
        BatchWriteBuilder builder = paimonTable.newBatchWriteBuilder();

        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (MaterializedRow row : result.getMaterializedRows()) {
                write.write(toPaimonRow(row));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private static InternalRow toPaimonRow(MaterializedRow row)
    {
        GenericRow genericRow = new GenericRow(row.getFields().size());
        for (int i = 0; i < row.getFields().size(); i++) {
            Object field = row.getField(i);
            switch (field) {
                case String string -> genericRow.setField(i, BinaryString.fromString(string));
                case LocalDate date -> genericRow.setField(i, (int) date.toEpochDay());
                default -> genericRow.setField(i, field);
            }
        }
        return genericRow;
    }

    private static String generateScanSql(CatalogSchemaName catalogSchemaName, TpchTable<?> table)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        String columnList = table.getColumns().stream()
                .map(column -> quote(column.getSimplifiedColumnName()))
                .collect(Collectors.joining(", "));
        builder.append(columnList);
        String tableName = format("%s.%s", catalogSchemaName.toString(), table.getTableName());
        builder.append(" FROM ").append(tableName);
        return builder.toString();
    }

    private static String quote(String name)
    {
        return "\"" + name + "\"";
    }

    public void initializeTables(Session session, QueryRunner queryRunner, String schemaName)
            throws Exception
    {
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog(TPCH_TINY.getCatalogName(), "tpch", ImmutableMap.of());
        ConnectorIdentity connectorIdentity = ConnectorIdentity.ofUser(session.getUser());
        ConnectorSession connectorSession = TestingConnectorSession.SESSION;

        PaimonMetadata paimonMetadata = ((PaimonConnector) queryRunner.getCoordinator().getConnector("paimon")).getInjector()
                .getInstance(PaimonMetadataFactory.class)
                .create(connectorIdentity);

        TrinoFileSystem fileSystem = ((PaimonConnector) queryRunner.getCoordinator().getConnector("paimon")).getInjector()
                .getInstance(TrinoFileSystemFactory.class)
                .create(connectorIdentity);

        PaimonFileIO paimonFileIO = new PaimonFileIO(fileSystem, new Path(paimonMetadata.catalog().warehouse()));

        TrinoCatalog paimonTrinoCatalog = paimonMetadata.catalog();

        Options options = new Options();
        options.set(CatalogOptions.WAREHOUSE, paimonTrinoCatalog.warehouse());
        CatalogContext context = CatalogContext.create(options, new FileIOLoader()
        {
            @Override
            public String getScheme()
            {
                return "s3";
            }

            @Override
            public FileIO load(Path path)
            {
                return paimonFileIO;
            }
        });

        Catalog paimonCatalog = new FileSystemCatalog(paimonFileIO, new Path(paimonTrinoCatalog.warehouse()), context.options())
        {
            @Override
            public Database getDatabaseImpl(String name)
            {
                return Database.of(name);
            }
        };

        if (paimonMetadata.catalog().config().getCatalogType() == CatalogType.HIVE) {
            HiveMetastoreFactory hiveMetastoreFactory = ((PaimonConnector) queryRunner.getCoordinator().getConnector("paimon")).getInjector()
                    .getInstance(HiveMetastoreFactory.class);
            HiveMetastore hiveMetaStore = hiveMetastoreFactory.createMetastore(Optional.of(connectorIdentity));
            paimonCatalog = new TrinoHiveCatalog(hiveMetaStore, paimonCatalog);
        }

        paimonCatalog.createDatabase(schemaName, true);
        TpchMetadata tpchMetadata = (TpchMetadata) queryRunner.getCoordinator().getConnector("tpch").getMetadata(null, null);

        for (TpchTable<?> tpchTable : tpchTables) {
            TpchTableHandle tpchTableHandle = tpchMetadata.getTableHandle(connectorSession, SchemaTableName.schemaTableName(TPCH_TINY.getSchemaName(), tpchTable.getTableName()), Optional.empty(), Optional.empty());
            ConnectorTableMetadata metadata = tpchMetadata.getTableMetadata(connectorSession, tpchTableHandle);
            ConnectorTableMetadata paimonTableMeta = new ConnectorTableMetadata(
                    new SchemaTableName(schemaName, metadata.getTable().getTableName()),
                    metadata.getColumns().stream().filter(c -> !c.isHidden()).collect(Collectors.toList()),
                    metadata.getProperties(),
                    metadata.getComment());
            createPaimonTable(paimonCatalog, paimonTableMeta);
            createTableWithData(paimonCatalog, session.getSchema().orElse(schemaName), tpchTable, queryRunner);
        }
    }
}
