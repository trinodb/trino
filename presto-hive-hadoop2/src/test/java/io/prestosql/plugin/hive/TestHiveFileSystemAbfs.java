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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.AbstractTestHive.Transaction;
import io.prestosql.plugin.hive.azure.HiveAzureConfig;
import io.prestosql.plugin.hive.azure.PrestoAzureConfigurationInitializer;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static io.prestosql.plugin.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static io.prestosql.plugin.hive.HiveTableProperties.EXTERNAL_LOCATION_PROPERTY;
import static io.prestosql.plugin.hive.HiveTableProperties.SKIP_FOOTER_LINE_COUNT;
import static io.prestosql.plugin.hive.HiveTableProperties.SKIP_HEADER_LINE_COUNT;
import static io.prestosql.plugin.hive.HiveTableProperties.SORTED_BY_PROPERTY;
import static io.prestosql.plugin.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static org.testng.util.Strings.isNullOrEmpty;

public class TestHiveFileSystemAbfs
        extends AbstractTestHiveFileSystem
{
    private String container;
    private String account;
    private String accessKey;
    private String testDirectory;

    @Parameters({
            "hive.hadoop2.metastoreHost",
            "hive.hadoop2.metastorePort",
            "hive.hadoop2.databaseName",
            "hive.hadoop2.abfs.container",
            "hive.hadoop2.abfs.account",
            "hive.hadoop2.abfs.accessKey",
            "hive.hadoop2.abfs.testDirectory",
    })
    @BeforeClass
    public void setup(String host, int port, String databaseName, String container, String account, String accessKey, String testDirectory)
    {
        checkArgument(!isNullOrEmpty(host), "expected non empty host");
        checkArgument(!isNullOrEmpty(databaseName), "Expected non empty databaseName");
        checkArgument(!isNullOrEmpty(container), "expected non empty container");
        checkArgument(!isNullOrEmpty(account), "expected non empty account");
        checkArgument(!isNullOrEmpty(accessKey), "expected non empty accessKey");
        checkArgument(!isNullOrEmpty(testDirectory), "expected non empty testDirectory");

        this.container = container;
        this.account = account;
        this.accessKey = accessKey;
        this.testDirectory = testDirectory;

        super.setup(host, port, databaseName, false, createHdfsConfiguration());
    }

    @Override
    protected void onSetupComplete()
    {
        ensureTableExists(table, "presto_test_external_fs", ImmutableMap.of());

        ensureTableExists(tableWithHeader, "presto_test_external_fs_with_header", ImmutableMap.of(SKIP_HEADER_LINE_COUNT, 1));
        ensureTableExists(tableWithHeaderAndFooter, "presto_test_external_fs_with_header_and_footer", ImmutableMap.of(SKIP_HEADER_LINE_COUNT, 2, SKIP_FOOTER_LINE_COUNT, 2));
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
                            .build());
            if (!transaction.getMetadata().listTables(newSession(), Optional.of(table.getSchemaName())).contains(table)) {
                transaction.getMetadata().createTable(newSession(), tableMetadata, false);
            }
            transaction.commit();
        }
    }

    private HdfsConfiguration createHdfsConfiguration()
    {
        ConfigurationInitializer azureConfig = new PrestoAzureConfigurationInitializer(new HiveAzureConfig()
                .setAbfsAccessKey(accessKey)
                .setAbfsStorageAccount(account));
        return new HiveHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig(), ImmutableSet.of(azureConfig)), ImmutableSet.of());
    }

    @Override
    protected Path getBasePath()
    {
        return new Path(format("abfs://%s@%s.dfs.core.windows.net/%s/", container, account, testDirectory));
    }
}
