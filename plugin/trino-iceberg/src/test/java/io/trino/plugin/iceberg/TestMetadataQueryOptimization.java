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
import io.trino.Session;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.spi.security.PrincipalType;
import io.trino.sql.ir.Constant;
import io.trino.sql.planner.assertions.BasePushdownPlanTest;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.SystemSessionProperties.TASK_MAX_WRITER_COUNT;
import static io.trino.spi.type.IntegerType.INTEGER;
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
    protected PlanTester createPlanTester()
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(SCHEMA_NAME)
                // optimize_metadata_queries doesn't work when files are written by different writers
                .setSystemProperty(TASK_MAX_WRITER_COUNT, "1")
                .build();

        try {
            baseDir = Files.createTempDirectory(null).toFile();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        PlanTester planTester = PlanTester.create(session);
        planTester.installPlugin(new TestingIcebergPlugin(baseDir.toPath()));
        planTester.createCatalog(ICEBERG_CATALOG, "iceberg", ImmutableMap.of());

        HiveMetastore metastore = ((IcebergConnector) planTester.getConnector(ICEBERG_CATALOG)).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        Database database = Database.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
        metastore.createDatabase(database);

        return planTester;
    }

    @Test
    public void testOptimization()
    {
        String testTable = "test_metadata_optimization";

        getPlanTester().executeStatement(format(
                "CREATE TABLE %s (a, b, c) WITH (PARTITIONING = ARRAY['b', 'c']) AS VALUES (5, 6, 7), (8, 9, 10)",
                testTable));

        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty("optimize_metadata_queries", "true")
                .build();

        assertPlan(
                format("SELECT DISTINCT b, c FROM %s ORDER BY b", testTable),
                session,
                anyTree(values(
                        ImmutableList.of("b", "c"),
                        ImmutableList.of(
                                ImmutableList.of(new Constant(INTEGER, 6L), new Constant(INTEGER, 7L)),
                                ImmutableList.of(new Constant(INTEGER, 9L), new Constant(INTEGER, 10L))))));

        assertPlan(
                format("SELECT DISTINCT b, c FROM %s WHERE b > 7", testTable),
                session,
                anyTree(values(
                        ImmutableList.of("b", "c"),
                        ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 9L), new Constant(INTEGER, 10L))))));

        assertPlan(
                format("SELECT DISTINCT b, c FROM %s WHERE b > 7 AND c < 8", testTable),
                session,
                anyTree(
                        values(ImmutableList.of("b", "c"), ImmutableList.of())));
    }

    @AfterAll
    public void cleanup()
            throws Exception
    {
        if (baseDir != null) {
            deleteRecursively(baseDir.toPath(), ALLOW_INSECURE);
        }
    }
}
