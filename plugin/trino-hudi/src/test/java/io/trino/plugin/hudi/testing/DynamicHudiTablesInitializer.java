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
package io.trino.plugin.hudi.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.hdfs.HdfsContext;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.plugin.hudi.HudiConnector;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.QueryRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.bootstrap.index.NoOpBootstrapIndex;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.nio.file.Files.createTempDirectory;

/**
 * Initializes Hudi test tables by dynamically creating them using Hudi's Java client,
 * rather than unzipping pre-existing table artifacts.
 * <p>
 * This approach provides several benefits:
 * <ul>
 *   <li>No binary artifacts (zip files) stored in the repository</li>
 *   <li>Table definitions as readable code</li>
 *   <li>Deterministic and reproducible test data</li>
 *   <li>Easy to modify table schemas and data</li>
 * </ul>
 */
public class DynamicHudiTablesInitializer
        implements HudiTablesInitializer
{
    private static final Logger log = Logger.get(DynamicHudiTablesInitializer.class);
    private static final HdfsContext HDFS_CONTEXT = new HdfsContext(SESSION);

    private final List<HudiTableDefinition> tableDefinitions;

    public DynamicHudiTablesInitializer()
    {
        this.tableDefinitions =
                ImmutableList.of(
                        new HudiNonPartCowTableDefinition(),
                        new HudiCowPtTblTableDefinition(),
                        new StockTicksCowTableDefinition(),
                        new StockTicksMorTableDefinition());
    }

    @Override
    public void initializeTables(QueryRunner queryRunner, Location externalLocation, String schemaName)
            throws Exception
    {
        TrinoFileSystem fileSystem = getFileSystem(queryRunner);
        HiveMetastore metastore = getMetastore(queryRunner);
        Location baseLocation = externalLocation.appendSuffix(schemaName);

        java.nio.file.Path tempDir = createTempDirectory("hudi-script-based-tables");
        try {
            for (HudiTableDefinition tableDefinition : tableDefinitions) {
                log.info("Creating Hudi table: %s", tableDefinition.getTableName());
                createTable(tableDefinition, tempDir, fileSystem, baseLocation, metastore, schemaName);
            }
        }
        finally {
            deleteRecursively(tempDir, ALLOW_INSECURE);
        }
    }

    /**
     * Creates a single Hudi table based on the provided definition.
     */
    private void createTable(
            HudiTableDefinition tableDefinition,
            java.nio.file.Path tempDir,
            TrinoFileSystem fileSystem,
            Location baseLocation,
            HiveMetastore metastore,
            String schemaName)
            throws Exception
    {
        String tableName = tableDefinition.getTableName();
        java.nio.file.Path tableDir = tempDir.resolve(tableName);

        // Create Hudi write client
        HoodieWriteConfig config = tableDefinition.createWriteConfig(tableDir.toString());
        try (HoodieJavaWriteClient<HoodieAvroPayload> client = createWriteClient(config, tableDir)) {
            // Execute all commits defined for this table
            tableDefinition.executeCommits(client);
        }

        // Copy table data to target location
        Location tableLocation = baseLocation.appendPath(tableName);
        ResourceHudiTablesInitializer.copyDir(tableDir, fileSystem, tableLocation);

        // Register table with metastore
        registerTableWithMetastore(
                metastore,
                schemaName,
                tableName,
                tableLocation,
                tableDefinition.getDataColumns(),
                tableDefinition.getPartitionColumns(),
                tableDefinition.getPartitions());

        log.info("Successfully created Hudi table: %s at %s", tableName, tableLocation);
    }

    /**
     * Creates a HoodieJavaWriteClient for writing data to a Hudi table.
     */
    private HoodieJavaWriteClient<HoodieAvroPayload> createWriteClient(
            HoodieWriteConfig config,
            java.nio.file.Path tableDir)
    {
        StorageConfiguration<?> storageConfig = new HadoopStorageConfiguration(
                HDFS_ENVIRONMENT.getConfiguration(HDFS_CONTEXT, new Path(tableDir.toUri())));

        // Initialize Hudi table metadata
        try {
            HoodieTableMetaClient.newTableBuilder()
                    .fromProperties(config.getProps())
                    .setTableType(config.getTableType())
                    .setTableName(config.getTableName())
                    .setTableVersion(HoodieTableVersion.SIX.versionCode())
                    .setBootstrapIndexClass(NoOpBootstrapIndex.class.getName())
                    .setPayloadClassName(HoodieAvroPayload.class.getName())
                    .initTable(storageConfig, config.getBasePath());
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to initialize Hudi table: " + config.getTableName(), e);
        }

        return new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(storageConfig), config);
    }

    /**
     * Registers a Hudi table with the Hive metastore.
     */
    private void registerTableWithMetastore(
            HiveMetastore metastore,
            String schemaName,
            String tableName,
            Location tableLocation,
            List<Column> dataColumns,
            List<Column> partitionColumns,
            Map<String, String> partitions)
    {
        StorageFormat storageFormat = StorageFormat.create(
                PARQUET_HIVE_SERDE_CLASS,
                HUDI_PARQUET_INPUT_FORMAT,
                MAPRED_PARQUET_OUTPUT_FORMAT_CLASS);

        Table table = Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setTableType(EXTERNAL_TABLE.name())
                .setOwner(Optional.of("public"))
                .setDataColumns(dataColumns)
                .setPartitionColumns(partitionColumns)
                .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                .withStorage(storageBuilder -> storageBuilder
                        .setStorageFormat(storageFormat)
                        .setLocation(tableLocation.toString()))
                .build();

        metastore.createTable(table, PrincipalPrivileges.NO_PRIVILEGES);

        // Register partitions if this is a partitioned table
        if (!partitions.isEmpty()) {
            List<PartitionWithStatistics> partitionsToAdd = new ArrayList<>();
            partitions.forEach((partitionName, partitionPath) -> {
                Partition partition = Partition.builder()
                        .setDatabaseName(schemaName)
                        .setTableName(tableName)
                        .setValues(extractPartitionValues(partitionName))
                        .withStorage(storageBuilder -> storageBuilder
                                .setStorageFormat(storageFormat)
                                .setLocation(tableLocation.appendPath(partitionPath).toString()))
                        .setColumns(dataColumns)
                        .build();
                partitionsToAdd.add(new PartitionWithStatistics(partition, partitionName, PartitionStatistics.empty()));
            });
            metastore.addPartitions(schemaName, tableName, partitionsToAdd);
        }
    }

    private TrinoFileSystem getFileSystem(QueryRunner queryRunner)
    {
        return ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));
    }

    private HiveMetastore getMetastore(QueryRunner queryRunner)
    {
        return ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
    }
}
