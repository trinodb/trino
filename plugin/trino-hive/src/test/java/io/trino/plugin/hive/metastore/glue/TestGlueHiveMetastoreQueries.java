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
package io.trino.plugin.hive.metastore.glue;

import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestGlueHiveMetastoreQueries
        extends AbstractTestQueryFramework
{
    private final String testSchema = "test_schema_" + randomNameSuffix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = HiveQueryRunner.builder(testSessionBuilder()
                        .setCatalog("hive")
                        .setSchema(testSchema)
                        .build())
                .addHiveProperty("hive.metastore", "glue")
                .addHiveProperty("hive.metastore.glue.default-warehouse-dir", "local:///glue")
                .addHiveProperty("hive.security", "allow-all")
                .setCreateTpchSchemas(false)
                .build();
        queryRunner.execute("CREATE SCHEMA " + testSchema);
        return queryRunner;
    }

    @AfterAll
    public void cleanUpSchema()
    {
        getQueryRunner().execute("DROP SCHEMA " + testSchema + " CASCADE");
    }

    @Test
    public void testFlushMetadataDisabled()
    {
        // Flushing metadata cache does not fail even if cache is disabled
        assertQuerySucceeds("CALL system.flush_metadata_cache()");
    }
}
