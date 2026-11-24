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
package io.trino.plugin.hive.metastore.lakehouse;

import com.google.cloud.biglake.hive.v1.CatalogName;
import com.google.cloud.biglake.hive.v1.FieldSchema;
import com.google.cloud.biglake.hive.v1.HiveDatabase;
import com.google.cloud.biglake.hive.v1.HiveMetastoreServiceClient;
import com.google.cloud.biglake.hive.v1.HiveTable;
import com.google.cloud.biglake.hive.v1.NamespaceName;
import com.google.cloud.biglake.hive.v1.SerdeInfo;
import com.google.cloud.biglake.hive.v1.StorageDescriptor;
import com.google.cloud.biglake.hive.v1.TableName;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.Table;
import io.trino.plugin.hive.HiveConnector;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static java.util.Objects.requireNonNull;

/**
 * Creates TPC-H tables in a Lakehouse runtime catalog, which Trino reads but cannot write.
 *
 * <p>A file metastore catalog writes the table data to object storage, and the Lakehouse client
 * registers the written tables in the Lakehouse runtime catalog.
 */
public class TpchLakehouseTablesInitializer
{
    private static final Logger log = Logger.get(TpchLakehouseTablesInitializer.class);

    private static final String WRITER_CATALOG = "hive_writer";

    private final List<TpchTable<?>> tpchTables;

    public TpchLakehouseTablesInitializer(List<TpchTable<?>> tpchTables)
    {
        this.tpchTables = requireNonNull(tpchTables, "tpchTables is null");
    }

    public void initializeTables(
            QueryRunner queryRunner,
            Map<String, String> fileSystemProperties,
            HiveMetastoreServiceClient client,
            CatalogName catalogName,
            Location externalLocation,
            String schemaName)
    {
        queryRunner.createCatalog(WRITER_CATALOG, "hive", ImmutableMap.<String, String>builder()
                .putAll(fileSystemProperties)
                .put("hive.metastore.catalog.dir", externalLocation.toString())
                .buildOrThrow());
        queryRunner.execute("CREATE SCHEMA %s.%s".formatted(WRITER_CATALOG, schemaName));
        HiveMetastore writerMetastore = ((HiveConnector) queryRunner.getCoordinator().getConnector(WRITER_CATALOG)).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        client.createHiveDatabase(catalogName, HiveDatabase.newBuilder().setLocationUri(externalLocation.appendPath(schemaName).toString()).build(), schemaName);
        NamespaceName namespaceName = NamespaceName.of(catalogName.getProject(), catalogName.getCatalog(), schemaName);
        for (TpchTable<?> tpchTable : tpchTables) {
            String tableName = tpchTable.getTableName();
            queryRunner.execute("CREATE TABLE %s.%s.%s AS SELECT * FROM tpch.%s.%s".formatted(WRITER_CATALOG, schemaName, tableName, TINY_SCHEMA_NAME, tableName));
            Table table = writerMetastore.getTable(schemaName, tableName).orElseThrow();
            client.createHiveTable(namespaceName, toLakehouseTable(table), tableName);
            log.info("Registered %s.%s in the Lakehouse runtime catalog", schemaName, tableName);
        }
    }

    public void dropTables(QueryRunner queryRunner, HiveMetastoreServiceClient client, CatalogName catalogName, String schemaName)
    {
        for (TpchTable<?> tpchTable : tpchTables) {
            String tableName = tpchTable.getTableName();
            client.deleteHiveTable(TableName.of(catalogName.getProject(), catalogName.getCatalog(), schemaName, tableName));
            // Dropping the table in the file metastore catalog deletes the table data
            queryRunner.execute("DROP TABLE %s.%s.%s".formatted(WRITER_CATALOG, schemaName, tableName));
        }
        client.deleteHiveDatabase(NamespaceName.of(catalogName.getProject(), catalogName.getCatalog(), schemaName));
        queryRunner.execute("DROP SCHEMA %s.%s".formatted(WRITER_CATALOG, schemaName));
    }

    private static HiveTable toLakehouseTable(Table table)
    {
        StorageDescriptor.Builder storageDescriptor = StorageDescriptor.newBuilder()
                .setLocationUri(table.getStorage().getLocation())
                .setInputFormat(table.getStorage().getStorageFormat().getInputFormat())
                .setOutputFormat(table.getStorage().getStorageFormat().getOutputFormat())
                .setSerdeInfo(SerdeInfo.newBuilder()
                        .setSerializationLib(table.getStorage().getStorageFormat().getSerde())
                        .putAllParameters(table.getStorage().getSerdeParameters()));
        for (Column column : table.getDataColumns()) {
            FieldSchema.Builder field = FieldSchema.newBuilder()
                    .setName(column.getName())
                    .setType(column.getType().toString());
            column.getComment().ifPresent(field::setComment);
            storageDescriptor.addColumns(field);
        }
        return HiveTable.newBuilder()
                .setTableType("EXTERNAL_TABLE")
                .putAllParameters(table.getParameters())
                .putParameters("EXTERNAL", "TRUE")
                .setStorageDescriptor(storageDescriptor)
                .build();
    }
}
