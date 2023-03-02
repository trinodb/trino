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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/*
 * TestHiveGlueMetastoreCompatibility currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
public class TestHiveGlueMetastoreCompatibility
        extends AbstractTestQueryFramework
{
    protected static final String SCHEMA = "test_hive_glue_" + randomNameSuffix();

    private String dataDirectory;
    private GlueHiveMetastore glueMetastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("hive")
                .setSchema(SCHEMA)
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toString();
        glueMetastore = createTestingGlueHiveMetastore(dataDirectory);

        queryRunner.installPlugin(new TestingHivePlugin(glueMetastore));

        Map<String, String> connectorProperties = ImmutableMap.<String, String>builder()
                .put("hive.security", "allow-all")
                .buildOrThrow();

        queryRunner.createCatalog("hive", "hive", connectorProperties);
        queryRunner.execute("CREATE SCHEMA " + SCHEMA);

        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (glueMetastore != null) {
            glueMetastore.dropDatabase(SCHEMA, false);
            deleteRecursively(Path.of(dataDirectory), ALLOW_INSECURE);
        }
    }

    @Test
    public void testSetColumnType()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_column_type_", "AS SELECT CAST(123 AS integer) AS col")) {
            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col SET DATA TYPE bigint");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES BIGINT '123'");
        }
    }
}
