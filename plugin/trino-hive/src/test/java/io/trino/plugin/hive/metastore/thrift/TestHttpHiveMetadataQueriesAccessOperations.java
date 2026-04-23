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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.containers.Hive4Metastore;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;

import static io.airlift.units.Duration.nanosSince;
import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestHttpHiveMetadataQueriesAccessOperations
        extends TestHiveMetastoreMetadataQueriesAccessOperations
{
    private Hive4Metastore hiveMetastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        hiveMetastore = Hive4Metastore.builder()
                .withFilesToMount(ImmutableMap.of("/opt/hive/conf/hive-site.xml", getPathFromClassPathResource("containers/hive_hadoop/hive-site-http.xml")))
                .withEnvVars(Map.of("SERVICE_NAME", "metastore"))
                .build();
        hiveMetastore.start();

        QueryRunner queryRunner = HiveQueryRunner.builder(SESSION)
                // metadata queries do not use workers
                .setWorkerCount(0)
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .addHiveProperty("hive.metastore", "thrift")
                .addHiveProperty("hive.metastore.uri", hiveMetastore.getHiveHttpMetastoreEndpoint().toString() + "/metastore")
                .addHiveProperty("hive.metastore.http.client.authentication.type", "BEARER")
                .addHiveProperty("hive.metastore.http.client.additional-headers", "x-actor-username:hive")
                .addHiveProperty("hive.hive-views.enabled", "true").setCreateTpchSchemas(false).build();

        try {
            long start = System.nanoTime();
            createTestingTables(queryRunner);
            log.info("Created testing tables in %s", nanosSince(start));
        }
        catch (RuntimeException e) {
            queryRunner.close();
            throw e;
        }

        return queryRunner;
    }

    @Override
    @AfterAll
    void afterAll()
    {
        hiveMetastore.stop();
    }
}
