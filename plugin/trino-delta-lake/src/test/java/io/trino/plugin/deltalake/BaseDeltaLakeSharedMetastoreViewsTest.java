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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.deltalake.metastore.TestingDeltaLakeMetastoreModule;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Tests querying views on a schema which has a mix of Hive and Delta Lake tables.
 */
@TestInstance(PER_CLASS)
public abstract class BaseDeltaLakeSharedMetastoreViewsTest
        extends AbstractTestQueryFramework
{
    protected static final String DELTA_CATALOG_NAME = "delta_lake";
    protected static final String HIVE_CATALOG_NAME = "hive";
    protected static final String SCHEMA = "test_shared_schema_views_" + randomNameSuffix();

    private Path dataDirectory;
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(DELTA_CATALOG_NAME)
                .setSchema(SCHEMA)
                .build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        this.dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("shared_data");
        this.metastore = createTestMetastore(dataDirectory);

        queryRunner.installPlugin(new TestingDeltaLakePlugin(dataDirectory, () -> Optional.of(new TestingDeltaLakeMetastoreModule(metastore))));
        queryRunner.createCatalog(DELTA_CATALOG_NAME, "delta_lake", ImmutableMap.of("fs.hadoop.enabled", "true"));

        queryRunner.installPlugin(new TestingHivePlugin(dataDirectory, metastore));

        queryRunner.createCatalog(HIVE_CATALOG_NAME, "hive", ImmutableMap.of("fs.hadoop.enabled", "true"));
        queryRunner.execute("CREATE SCHEMA " + SCHEMA);

        return queryRunner;
    }

    protected abstract HiveMetastore createTestMetastore(Path dataDirectory);

    @Test
    public void testViewWithLiteralColumnCreatedInDeltaLakeIsReadableInHive()
    {
        String deltaViewName = "delta_view_" + randomNameSuffix();
        String deltaView = "%s.%s.%s".formatted(DELTA_CATALOG_NAME, SCHEMA, deltaViewName);
        String deltaViewOnHiveCatalog = "%s.%s.%s".formatted(HIVE_CATALOG_NAME, SCHEMA, deltaViewName);
        try {
            assertUpdate("CREATE VIEW %s AS SELECT 1 bee".formatted(deltaView));
            assertQuery("SELECT * FROM %s".formatted(deltaView), "VALUES 1");
            assertQuery("SELECT * FROM %s".formatted(deltaViewOnHiveCatalog), "VALUES 1");
            assertQuery("SELECT table_type FROM %s.information_schema.tables WHERE table_name = '%s' AND table_schema='%s'".formatted(HIVE_CATALOG_NAME, deltaViewName, SCHEMA), "VALUES 'VIEW'");
        }
        finally {
            assertUpdate("DROP VIEW IF EXISTS %s".formatted(deltaView));
        }
    }

    @Test
    public void testViewOnDeltaLakeTableCreatedInDeltaLakeIsReadableInHive()
    {
        String deltaTableName = "delta_table_" + randomNameSuffix();
        String deltaTable = "%s.%s.%s".formatted(DELTA_CATALOG_NAME, SCHEMA, deltaTableName);
        String deltaViewName = "delta_view_" + randomNameSuffix();
        String deltaView = "%s.%s.%s".formatted(DELTA_CATALOG_NAME, SCHEMA, deltaViewName);
        String deltaViewOnHiveCatalog = "%s.%s.%s".formatted(HIVE_CATALOG_NAME, SCHEMA, deltaViewName);
        try {
            assertUpdate("CREATE TABLE %s AS SELECT 1 bee".formatted(deltaTable), 1);
            assertUpdate("CREATE VIEW %s AS SELECT * from %s".formatted(deltaView, deltaTable));
            assertQuery("SELECT * FROM %s".formatted(deltaView), "VALUES 1");
            assertQuery("SELECT * FROM %s".formatted(deltaViewOnHiveCatalog), "VALUES 1");
            assertQuery("SELECT table_type FROM %s.information_schema.tables WHERE table_name = '%s' AND table_schema='%s'".formatted(HIVE_CATALOG_NAME, deltaViewName, SCHEMA), "VALUES 'VIEW'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS %s".formatted(deltaTable));
            assertUpdate("DROP VIEW IF EXISTS %s".formatted(deltaView));
        }
    }

    @Test
    public void testViewWithLiteralColumnCreatedInHiveIsReadableInDeltaLake()
    {
        String trinoViewOnHiveName = "trino_view_on_hive_" + randomNameSuffix();
        String trinoViewOnHive = "%s.%s.%s".formatted(HIVE_CATALOG_NAME, SCHEMA, trinoViewOnHiveName);
        String trinoViewOnHiveOnDeltaCatalog = "%s.%s.%s".formatted(DELTA_CATALOG_NAME, SCHEMA, trinoViewOnHiveName);
        try {
            assertUpdate("CREATE VIEW %s AS SELECT 1 bee".formatted(trinoViewOnHive));
            assertQuery("SELECT * FROM %s".formatted(trinoViewOnHive), "VALUES 1");
            assertQuery("SELECT * FROM %s".formatted(trinoViewOnHiveOnDeltaCatalog), "VALUES 1");
            assertQuery("SELECT table_type FROM %s.information_schema.tables WHERE table_name = '%s' AND table_schema='%s'".formatted(HIVE_CATALOG_NAME, trinoViewOnHiveName, SCHEMA), "VALUES 'VIEW'");
        }
        finally {
            assertUpdate("DROP VIEW IF EXISTS %s".formatted(trinoViewOnHive));
        }
    }

    @Test
    public void testViewOnHiveTableCreatedInHiveIsReadableInDeltaLake()
    {
        String hiveTableName = "hive_table_" + randomNameSuffix();
        String hiveTable = "%s.%s.%s".formatted(HIVE_CATALOG_NAME, SCHEMA, hiveTableName);
        String trinoViewOnHiveName = "trino_view_on_hive_" + randomNameSuffix();
        String trinoViewOnHive = "%s.%s.%s".formatted(HIVE_CATALOG_NAME, SCHEMA, trinoViewOnHiveName);
        String trinoViewOnHiveOnDeltaCatalog = "%s.%s.%s".formatted(DELTA_CATALOG_NAME, SCHEMA, trinoViewOnHiveName);
        try {
            assertUpdate("CREATE TABLE %s AS SELECT 1 bee".formatted(hiveTable), 1);
            assertUpdate("CREATE VIEW %s AS SELECT 1 bee".formatted(trinoViewOnHive));
            assertQuery("SELECT * FROM %s".formatted(trinoViewOnHive), "VALUES 1");
            assertQuery("SELECT * FROM %s".formatted(trinoViewOnHiveOnDeltaCatalog), "VALUES 1");
            assertQuery("SELECT table_type FROM %s.information_schema.tables WHERE table_name = '%s' AND table_schema='%s'".formatted(DELTA_CATALOG_NAME, trinoViewOnHiveName, SCHEMA), "VALUES 'VIEW'");
            assertQuery(
                    "SELECT table_name FROM %s.information_schema.columns WHERE table_name = '%s' AND table_schema='%s'".formatted(DELTA_CATALOG_NAME, trinoViewOnHiveName, SCHEMA),
                    "VALUES '" + trinoViewOnHiveName + "'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS %s".formatted(hiveTable));
            assertUpdate("DROP VIEW IF EXISTS %s".formatted(trinoViewOnHive));
        }
    }

    @Test
    public void testNonDeltaTablesCannotBeAccessed()
    {
        String schemaName = "test_schema" + randomNameSuffix();
        String tableName = "hive_table";

        assertUpdate("CREATE SCHEMA %s.%s".formatted(HIVE_CATALOG_NAME, schemaName));
        try {
            assertUpdate("CREATE TABLE %s.%s.%s(id BIGINT)".formatted(HIVE_CATALOG_NAME, schemaName, tableName));
            assertThat(computeScalar("SHOW TABLES FROM %s LIKE '%s'".formatted(schemaName, tableName))).isEqualTo(tableName);
            assertQueryFails("DESCRIBE " + schemaName + "." + tableName, ".* is not a Delta Lake table");
        }
        finally {
            assertUpdate("DROP SCHEMA %s.%s CASCADE".formatted(HIVE_CATALOG_NAME, schemaName));
        }
    }

    @AfterAll
    public void cleanup()
            throws IOException
    {
        if (metastore != null) {
            metastore.dropDatabase(SCHEMA, false);
            if (metastore instanceof GlueHiveMetastore glueMetastore) {
                glueMetastore.shutdown();
            }
            deleteRecursively(dataDirectory, ALLOW_INSECURE);
        }
    }
}
