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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.azure.HiveAzureConfig;
import io.trino.hdfs.azure.TrinoAzureConfigurationInitializer;
import io.trino.plugin.hive.AbstractTestHive.Transaction;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import org.apache.hadoop.fs.Path;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.EXTERNAL_LOCATION_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.SKIP_FOOTER_LINE_COUNT;
import static io.trino.plugin.hive.HiveTableProperties.SKIP_HEADER_LINE_COUNT;
import static io.trino.plugin.hive.HiveTableProperties.SORTED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static org.testng.util.Strings.isNullOrEmpty;

public abstract class AbstractTestHiveFileSystemAbfs
        extends AbstractTestHiveFileSystem
{
    protected String account;
    protected String container;
    protected String testDirectory;

    protected static String checkParameter(String value, String name)
    {
        checkArgument(!isNullOrEmpty(value), "expected non-empty %s", name);
        return value;
    }

    protected void setup(String host, int port, String databaseName, String container, String account, String testDirectory)
    {
        this.container = checkParameter(container, "container");
        this.account = checkParameter(account, "account");
        this.testDirectory = checkParameter(testDirectory, "test directory");
        super.setup(
                checkParameter(host, "host"),
                port,
                checkParameter(databaseName, "database name"),
                false,
                createHdfsConfiguration());
    }

    @Override
    protected void onSetupComplete()
    {
        ensureTableExists(table, "trino_test_external_fs", ImmutableMap.of());
        ensureTableExists(tableWithHeader, "trino_test_external_fs_with_header", ImmutableMap.of(SKIP_HEADER_LINE_COUNT, 1));
        ensureTableExists(tableWithHeaderAndFooter, "trino_test_external_fs_with_header_and_footer", ImmutableMap.of(SKIP_HEADER_LINE_COUNT, 2, SKIP_FOOTER_LINE_COUNT, 2));
    }

    private void ensureTableExists(SchemaTableName table, String tableDirectoryName, Map<String, Object> tableProperties)
    {
        try (Transaction transaction = newTransaction()) {
            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                    table,
                    ImmutableList.of(new ColumnMetadata("t_bigint", BIGINT)),
                    ImmutableMap.<String, Object>builder()
                            .putAll(tableProperties)
                            .put(STORAGE_FORMAT_PROPERTY, HiveStorageFormat.TEXTFILE)
                            .put(EXTERNAL_LOCATION_PROPERTY, getBasePath().toString() + "/" + tableDirectoryName)
                            .put(BUCKET_COUNT_PROPERTY, 0)
                            .put(BUCKETED_BY_PROPERTY, ImmutableList.of())
                            .put(SORTED_BY_PROPERTY, ImmutableList.of())
                            .buildOrThrow());
            if (!transaction.getMetadata().listTables(newSession(), Optional.of(table.getSchemaName())).contains(table)) {
                transaction.getMetadata().createTable(newSession(), tableMetadata, false);
            }
            transaction.commit();
        }
    }

    protected abstract HiveAzureConfig getConfig();

    private HdfsConfiguration createHdfsConfiguration()
    {
        ConfigurationInitializer initializer = new TrinoAzureConfigurationInitializer(getConfig());
        return new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig(), ImmutableSet.of(initializer)), ImmutableSet.of());
    }

    @Override
    protected Path getBasePath()
    {
        return new Path(format("abfs://%s@%s.dfs.core.windows.net/%s/", container, account, testDirectory));
    }
}
