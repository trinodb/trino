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
package io.trino.plugin.deltalake;

import io.trino.Session;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestDeltaLakeTableWithCustomLocationUsingHiveMetastore
        extends BaseDeltaLakeTableWithCustomLocation
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema(SCHEMA)
                .build();

        DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(session);
        QueryRunner queryRunner = builder.build();

        Map<String, String> connectorProperties = new HashMap<>();
        metastoreDir = Files.createTempDirectory("test_delta_lake").toFile();
        connectorProperties.putIfAbsent("delta.unique-table-location", "true");
        connectorProperties.putIfAbsent("hive.metastore", "file");
        connectorProperties.putIfAbsent("hive.metastore.catalog.dir", metastoreDir.getPath());

        queryRunner.installPlugin(new TestingDeltaLakePlugin(metastoreDir.toPath()));
        queryRunner.createCatalog(DELTA_CATALOG, CONNECTOR_NAME, connectorProperties);

        metastore = TestingDeltaLakeUtils.getConnectorService(queryRunner, HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        queryRunner.execute("CREATE SCHEMA " + SCHEMA);

        return queryRunner;
    }
}
