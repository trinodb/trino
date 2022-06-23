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
package io.trino.plugin.hive.metastore.dlf;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.security.PrincipalType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class TestingAlibabaDlfUtils
{
    private static final String WAREHOUSE_BASE_PATH = "file://" + System.getProperty("user.dir") + "/metastore-ut/";
    private static final String WAREHOUSE_PATH = WAREHOUSE_BASE_PATH + "hive/warehouse/";
    public static final String TEST_DB = "metastore_ut_test_db";
    public static final String TEST_TABLE = "metastore_ut_test_tb";
    private static final String CONFIG_FILE = "alibaba-dlf.properties";
    private static final String DEFAULT_TABLE_TYPE = "MANAGED_TABLE";
    private static final String DEFAULT_OWNER = "Mr.EMR";
    private static final PrincipalType DEFAULT_OWNER_TYPE = PrincipalType.USER;
    private static final Map<String, String> DEFAULT_PARAMETERS = ImmutableMap.of("key", "value");

    private TestingAlibabaDlfUtils() {}

    static HiveMetastore getDlfClient() throws IOException
    {
        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration =
                new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment =
                new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());

        Properties properties = new Properties();
        InputStream is = TestingAlibabaDlfUtils.class.getClassLoader().getResourceAsStream(CONFIG_FILE);
        if (is != null) {
            properties.load(is);
        }
        else {
            throw new IOException("Cannot load config file from classpath: " + CONFIG_FILE);
        }

        AlibabaDlfMetaStoreConfig dlfConfig = new AlibabaDlfMetaStoreConfig()
                .setRegionId("cn-hangzhou")
                .setAkMode("MANUAL")
                .setProxyMode("DLF_ONLY")
                .setAccessKeyId(properties.getProperty("alibaba-dlf.catalog.accessKeyId"))
                .setAccessKeySecret(properties.getProperty("alibaba-dlf.catalog.accessKeySecret"))
                .setEndpoint(properties.getProperty("alibaba-dlf.catalog.endpoint"));
        return new ProxyTrinoClient(null, new AlibabaDlfMetaStoreClient(hdfsEnvironment, dlfConfig), dlfConfig);
    }

    static HiveMetastore getDlfClient(AlibabaDlfMetaStoreConfig dlfConfig) throws IOException
    {
        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration =
                new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment =
                new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());

        Properties properties = new Properties();
        InputStream is = TestingAlibabaDlfUtils.class.getClassLoader().getResourceAsStream(CONFIG_FILE);
        if (is != null) {
            properties.load(is);
        }
        else {
            throw new IOException("Cannot load config file from classpath: " + CONFIG_FILE);
        }

        dlfConfig.setAccessKeyId(properties.getProperty("alibaba-dlf.catalog.accessKeyId"))
                .setAccessKeySecret(properties.getProperty("alibaba-dlf.catalog.accessKeySecret"))
                .setEndpoint(properties.getProperty("alibaba-dlf.catalog.endpoint"))
                .setRegionId("cn-hangzhou")
                .setAkMode("MANUAL")
                .setProxyMode("DLF_ONLY")
                .setDbCacheEnable(true)
                .setDbCacheSize(100)
                .setDbCacheTTLMins(2)
                .setTbCacheEnable(true)
                .setTbCacheSize(150)
                .setTbCacheTTLMins(1);
        return new ProxyTrinoClient(null, new AlibabaDlfMetaStoreClient(hdfsEnvironment, dlfConfig), dlfConfig);
    }

    /**
     * Return the default database path.
     */
    static Path getDefaultDatabasePath(String databaseName)
    {
        String path = WAREHOUSE_PATH + "/" + databaseName + ".db";
        return new Path(path);
    }

    /**
     * Return the default table path.
     */
    static Path getDefaultTablePath(String databaseName, String tableName)
    {
        return new Path(getDefaultDatabasePath(databaseName), tableName);
    }

    static Database getDatabase(String databaseName, String location)
    {
        requireNonNull(databaseName);
        requireNonNull(location);

        return Database.builder()
                .setDatabaseName(databaseName)
                .setLocation(Optional.of(location))
                .setOwnerName(Optional.of(DEFAULT_OWNER))
                .setOwnerType(Optional.of(DEFAULT_OWNER_TYPE))
                .setParameters(DEFAULT_PARAMETERS)
                .setComment(Optional.empty())
                .build();
    }

    static Table getTable(
            String databaseName,
            String tableName,
            Map<String, String> dataColumns,
            Map<String, String> partitionColumns)
    {
        requireNonNull(databaseName);
        requireNonNull(tableName);
        requireNonNull(dataColumns);

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(databaseName)
                .setTableName(tableName)
                .setOwner(Optional.of(DEFAULT_OWNER))
                .setTableType(DEFAULT_TABLE_TYPE)
                .setDataColumns(dataColumns.entrySet().stream()
                        .map(kv -> new Column(kv.getKey(), HiveType.valueOf(kv.getValue()), Optional.empty()))
                        .collect(Collectors.toList()))
                .setParameters(DEFAULT_PARAMETERS)
                .setViewOriginalText(Optional.empty())
                .setViewExpandedText(Optional.empty());
        if (partitionColumns != null) {
            if (partitionColumns.size() > 0) {
                tableBuilder.setPartitionColumns(partitionColumns.entrySet().stream()
                        .map(kv -> new Column(kv.getKey(), HiveType.valueOf(kv.getValue()), Optional.empty()))
                        .collect(Collectors.toList()));
            }
            else {
                tableBuilder.setPartitionColumns(new ArrayList<Column>());
            }
        }
        tableBuilder.getStorageBuilder()
                .setLocation(getDefaultTablePath(databaseName, tableName).toString())
                .setStorageFormat(StorageFormat.fromHiveStorageFormat(HiveStorageFormat.ORC))
                .setBucketProperty(HiveBucketProperty.fromStorageDescriptor(null, new StorageDescriptor(), null))
                .setSerdeParameters(ImmutableMap.of());
        return tableBuilder.build();
    }

    public static Table getTable(
            String databaseName,
            String tableName,
            Map<String, String> dataColumns,
            Map<String, String> partitionColumns,
            String tableType)
    {
        requireNonNull(databaseName);
        requireNonNull(tableName);
        requireNonNull(dataColumns);

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(databaseName)
                .setTableName(tableName)
                .setOwner(Optional.of(DEFAULT_OWNER))
                .setTableType(tableType)
                .setDataColumns(dataColumns.entrySet().stream()
                        .map(kv -> new Column(kv.getKey(), HiveType.valueOf(kv.getValue()), Optional.empty()))
                        .collect(Collectors.toList()))
                .setParameters(DEFAULT_PARAMETERS)
                .setViewOriginalText(Optional.empty())
                .setViewExpandedText(Optional.empty());
        if (partitionColumns != null) {
            if (partitionColumns.size() > 0) {
                tableBuilder.setPartitionColumns(partitionColumns.entrySet().stream()
                        .map(kv -> new Column(kv.getKey(), HiveType.valueOf(kv.getValue()), Optional.empty()))
                        .collect(Collectors.toList()));
            }
            else {
                tableBuilder.setPartitionColumns(new ArrayList<Column>());
            }
        }
        tableBuilder.getStorageBuilder()
                .setLocation(getDefaultTablePath(databaseName, tableName).toString())
                .setStorageFormat(StorageFormat.fromHiveStorageFormat(HiveStorageFormat.ORC))
                .setBucketProperty(HiveBucketProperty.fromStorageDescriptor(null, new StorageDescriptor(), null))
                .setSerdeParameters(ImmutableMap.of());
        return tableBuilder.build();
    }

    static Partition getPartition(
            String databaseName,
            String tableName,
            Map<String, String> partitionColumns,
            List<String> partitionValues)
    {
        requireNonNull(databaseName);
        requireNonNull(tableName);
        requireNonNull(partitionColumns);
        requireNonNull(partitionValues);

        return Partition.builder()
                .setDatabaseName(databaseName)
                .setTableName(tableName)
                .setParameters(DEFAULT_PARAMETERS)
                .setColumns(partitionColumns.entrySet().stream()
                        .map(kv -> new Column(kv.getKey(), HiveType.valueOf(kv.getValue()), Optional.empty()))
                        .collect(Collectors.toList()))
                .setValues(partitionValues)
                .build();
    }
}
