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
package io.trino.plugin.hive.metastore.thrift;

import io.opentelemetry.api.OpenTelemetry;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.TableInfo;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.TestingNodeManager;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.metastore.thrift.ThriftHttpMetastoreConfig.AuthenticationMode.BEARER;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static org.assertj.core.api.Assertions.assertThat;

final class TestUnityMetastore
{
    @Test
    void test()
            throws Exception
    {
        String databricksHost = requireEnv("DATABRICKS_HOST");
        String databricksToken = requireEnv("DATABRICKS_TOKEN");
        String databricksCatalogName = requireEnv("DATABRICKS_UNITY_CATALOG_NAME");
        URI metastoreUri = URI.create("https://%s:443/api/2.0/unity-hms-proxy/metadata" .formatted(databricksHost));

        ThriftHttpMetastoreConfig config = new ThriftHttpMetastoreConfig()
                .setAuthenticationMode(BEARER)
                .setHttpBearerToken(databricksToken)
                .setAdditionalHeaders("X-Databricks-Catalog-Name:" + databricksCatalogName);
        ThriftMetastoreClient client = ((ThriftMetastoreClientFactory) new HttpThriftMetastoreClientFactory(config, new TestingNodeManager(), OpenTelemetry.noop()))
                .create(metastoreUri, Optional.empty());

        try (var closer = AutoCloseableCloser.create()) {
            HiveMetastore metastore = new BridgingHiveMetastore(testingThriftHiveMetastoreBuilder()
                    .metastoreClient(client)
                    .thriftMetastoreConfig(new ThriftMetastoreConfig().setDeleteFilesOnDrop(true))
                    .build(closer::register));

            List<TableInfo> tables = metastore.getAllDatabases().stream()
                    .map(metastore::getTables)
                    .flatMap(List::stream)
                    .toList();
            assertThat(tables).isNotEmpty();

            SchemaTableName schemaTableName = tables.getFirst().tableName();
            assertThat(metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName())).isPresent();
        }
    }
}
