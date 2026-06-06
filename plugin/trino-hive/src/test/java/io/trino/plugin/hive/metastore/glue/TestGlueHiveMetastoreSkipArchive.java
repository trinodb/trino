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

import io.trino.plugin.hive.FlociS3AndGlue;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.TableVersion;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGlueHiveMetastoreSkipArchive
        extends AbstractTestQueryFramework
{
    private final String testSchema = "test_schema_" + randomNameSuffix();
    private GlueClient glueClient;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        FlociS3AndGlue floci = closeAfterClass(new FlociS3AndGlue());
        String bucketName = "test-glue-hive-skip-archive-" + randomNameSuffix();
        floci.createBucket(bucketName);
        glueClient = closeAfterClass(floci.createGlueClient());

        DistributedQueryRunner queryRunner = HiveQueryRunner.builder(testSessionBuilder()
                        .setCatalog("hive")
                        .setSchema(testSchema)
                        .build())
                .addHiveProperty("hive.metastore", "glue")
                .addHiveProperty("hive.metastore.glue.default-warehouse-dir", "s3://%s/".formatted(bucketName))
                .addHiveProperty("hive.security", "allow-all")
                .addHiveProperty("hive.metastore.glue.skip-archive", "true")
                .addHiveProperty("fs.s3.enabled", "true")
                .addHiveProperties(floci.s3AndGlueProperties())
                .setCreateTpchSchemas(false)
                .build();
        queryRunner.execute("CREATE SCHEMA " + testSchema + " WITH (location = 's3://%s/%s')".formatted(bucketName, testSchema));
        return queryRunner;
    }

    @Test
    void testSkipArchive()
    {
        try (TestTable table = newTrinoTable("test_skip_archive", "(col int)")) {
            List<TableVersion> tableVersionsBeforeInsert = getTableVersions(testSchema, table.getName());
            assertThat(tableVersionsBeforeInsert).hasSize(1);
            String versionIdBeforeInsert = getOnlyElement(tableVersionsBeforeInsert).versionId();

            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);

            // Verify count of table versions isn't increased, but version id is changed
            List<TableVersion> tableVersionsAfterInsert = getTableVersions(testSchema, table.getName());
            assertThat(tableVersionsAfterInsert).hasSize(1);
            String versionIdAfterInsert = getOnlyElement(tableVersionsAfterInsert).versionId();
            assertThat(versionIdBeforeInsert).isNotEqualTo(versionIdAfterInsert);
        }
    }

    private List<TableVersion> getTableVersions(String databaseName, String tableName)
    {
        return glueClient.getTableVersions(builder -> builder.databaseName(databaseName).tableName(tableName)).tableVersions();
    }
}
