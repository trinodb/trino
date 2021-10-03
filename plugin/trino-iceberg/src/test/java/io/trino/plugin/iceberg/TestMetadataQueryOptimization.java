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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.trino.Session;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.security.PrincipalType;
import io.trino.sql.planner.assertions.BasePushdownPlanTest;
import io.trino.sql.tree.LongLiteral;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestMetadataQueryOptimization
        extends BasePushdownPlanTest
{
    private static final String ICEBERG_CATALOG = "iceberg";
    private static final String SCHEMA_NAME = "test_schema";
    private File baseDir;

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(SCHEMA_NAME)
                .build();

        baseDir = Files.createTempDir();
        HiveMetastore metastore = createTestingFileHiveMetastore(baseDir);
        LocalQueryRunner queryRunner = LocalQueryRunner.create(session);

        queryRunner.createCatalog(
                ICEBERG_CATALOG,
                new TestingIcebergConnectorFactory(Optional.of(metastore), Optional.empty()),
                ImmutableMap.of());

        Database database = Database.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
        metastore.createDatabase(database);

        return queryRunner;
    }

    @Test
    public void testOptimization()
    {
        String testTable = "test_metadata_optimization";

        getQueryRunner().execute(format(
                "CREATE TABLE %s (a, b, c) WITH (PARTITIONING = ARRAY['b', 'c']) AS VALUES (5, 6, 7), (8, 9, 10)",
                testTable));

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty("optimize_metadata_queries", "true")
                .build();

        assertPlan(
                format("SELECT DISTINCT b, c FROM %s ORDER BY b", testTable),
                session,
                anyTree(values(
                        ImmutableList.of("b", "c"),
                        ImmutableList.of(
                                ImmutableList.of(new LongLiteral("6"), new LongLiteral("7")),
                                ImmutableList.of(new LongLiteral("9"), new LongLiteral("10"))))));

        assertPlan(
                format("SELECT DISTINCT b, c FROM %s WHERE b > 7", testTable),
                session,
                anyTree(values(
                        ImmutableList.of("b", "c"),
                        ImmutableList.of(ImmutableList.of(new LongLiteral("9"), new LongLiteral("10"))))));

        assertPlan(
                format("SELECT DISTINCT b, c FROM %s WHERE b > 7 AND c < 8", testTable),
                session,
                anyTree(
                        values(ImmutableList.of("b", "c"), ImmutableList.of())));
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws Exception
    {
        if (baseDir != null) {
            deleteRecursively(baseDir.toPath(), ALLOW_INSECURE);
        }
    }
}
