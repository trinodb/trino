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
import io.trino.plugin.deltalake.metastore.TestingDeltaLakeMetastoreModule;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

/**
 * Tests querying views on a schema which has a mix of Hive and Delta Lake tables.
 */
public abstract class BaseDeltaLakeSharedMetastoreViewsTest
        extends AbstractTestQueryFramework
{
    protected static final String DELTA_CATALOG_NAME = "delta_lake";
    protected static final String HIVE_CATALOG_NAME = "hive";
    protected static final String SCHEMA = "test_shared_schema_views_" + randomNameSuffix();

    private String dataDirectory;
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(DELTA_CATALOG_NAME)
                .setSchema(SCHEMA)
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        this.dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data").toString();
        this.metastore = createTestMetastore(dataDirectory);

        queryRunner.installPlugin(new TestingDeltaLakePlugin(Optional.of(new TestingDeltaLakeMetastoreModule(metastore)), EMPTY_MODULE));
        queryRunner.createCatalog(DELTA_CATALOG_NAME, "delta_lake");

        queryRunner.installPlugin(new TestingHivePlugin(metastore));

        ImmutableMap<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                .put("hive.allow-drop-table", "true")
                .buildOrThrow();

        queryRunner.createCatalog(HIVE_CATALOG_NAME, "hive", hiveProperties);
        queryRunner.execute("CREATE SCHEMA " + SCHEMA);

        return queryRunner;
    }

    protected abstract HiveMetastore createTestMetastore(String dataDirectory);

    @Test
    public void testViewWithLiteralColumnCreatedInDeltaLakeIsReadableInHive()
    {
        String deltaViewName = "delta_view_" + randomNameSuffix();
        String deltaView = format("%s.%s.%s", DELTA_CATALOG_NAME, SCHEMA, deltaViewName);
        String deltaViewOnHiveCatalog = format("%s.%s.%s", HIVE_CATALOG_NAME, SCHEMA, deltaViewName);
        try {
            assertUpdate(format("CREATE VIEW %s AS SELECT 1 bee", deltaView));
            assertQuery(format("SELECT * FROM %s", deltaView), "VALUES 1");
            assertQuery(format("SELECT * FROM %s", deltaViewOnHiveCatalog), "VALUES 1");
            assertQuery(format("SELECT table_type FROM %s.information_schema.tables WHERE table_name = '%s' AND table_schema='%s'", HIVE_CATALOG_NAME, deltaViewName, SCHEMA), "VALUES 'VIEW'");
        }
        finally {
            assertUpdate(format("DROP VIEW IF EXISTS %s", deltaView));
        }
    }

    @Test
    public void testViewOnDeltaLakeTableCreatedInDeltaLakeIsReadableInHive()
    {
        String deltaTableName = "delta_table_" + randomNameSuffix();
        String deltaTable = format("%s.%s.%s", DELTA_CATALOG_NAME, SCHEMA, deltaTableName);
        String deltaViewName = "delta_view_" + randomNameSuffix();
        String deltaView = format("%s.%s.%s", DELTA_CATALOG_NAME, SCHEMA, deltaViewName);
        String deltaViewOnHiveCatalog = format("%s.%s.%s", HIVE_CATALOG_NAME, SCHEMA, deltaViewName);
        try {
            assertUpdate(format("CREATE TABLE %s AS SELECT 1 bee", deltaTable), 1);
            assertUpdate(format("CREATE VIEW %s AS SELECT * from %s", deltaView, deltaTable));
            assertQuery(format("SELECT * FROM %s", deltaView), "VALUES 1");
            assertQuery(format("SELECT * FROM %s", deltaViewOnHiveCatalog), "VALUES 1");
            assertQuery(format("SELECT table_type FROM %s.information_schema.tables WHERE table_name = '%s' AND table_schema='%s'", HIVE_CATALOG_NAME, deltaViewName, SCHEMA), "VALUES 'VIEW'");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", deltaTable));
            assertUpdate(format("DROP VIEW IF EXISTS %s", deltaView));
        }
    }

    @Test
    public void testViewWithLiteralColumnCreatedInHiveIsReadableInDeltaLake()
    {
        String trinoViewOnHiveName = "trino_view_on_hive_" + randomNameSuffix();
        String trinoViewOnHive = format("%s.%s.%s", HIVE_CATALOG_NAME, SCHEMA, trinoViewOnHiveName);
        String trinoViewOnHiveOnDeltaCatalog = format("%s.%s.%s", DELTA_CATALOG_NAME, SCHEMA, trinoViewOnHiveName);
        try {
            assertUpdate(format("CREATE VIEW %s AS SELECT 1 bee", trinoViewOnHive));
            assertQuery(format("SELECT * FROM %s", trinoViewOnHive), "VALUES 1");
            assertQuery(format("SELECT * FROM %s", trinoViewOnHiveOnDeltaCatalog), "VALUES 1");
            assertQuery(format("SELECT table_type FROM %s.information_schema.tables WHERE table_name = '%s' AND table_schema='%s'", HIVE_CATALOG_NAME, trinoViewOnHiveName, SCHEMA), "VALUES 'VIEW'");
        }
        finally {
            assertUpdate(format("DROP VIEW IF EXISTS %s", trinoViewOnHive));
        }
    }

    @Test
    public void testViewOnHiveTableCreatedInHiveIsReadableInDeltaLake()
    {
        String hiveTableName = "hive_table_" + randomNameSuffix();
        String hiveTable = format("%s.%s.%s", HIVE_CATALOG_NAME, SCHEMA, hiveTableName);
        String trinoViewOnHiveName = "trino_view_on_hive_" + randomNameSuffix();
        String trinoViewOnHive = format("%s.%s.%s", HIVE_CATALOG_NAME, SCHEMA, trinoViewOnHiveName);
        String trinoViewOnHiveOnDeltaCatalog = format("%s.%s.%s", DELTA_CATALOG_NAME, SCHEMA, trinoViewOnHiveName);
        try {
            assertUpdate(format("CREATE TABLE %s AS SELECT 1 bee", hiveTable), 1);
            assertUpdate(format("CREATE VIEW %s AS SELECT 1 bee", trinoViewOnHive));
            assertQuery(format("SELECT * FROM %s", trinoViewOnHive), "VALUES 1");
            assertQuery(format("SELECT * FROM %s", trinoViewOnHiveOnDeltaCatalog), "VALUES 1");
            assertQuery(format("SELECT table_type FROM %s.information_schema.tables WHERE table_name = '%s' AND table_schema='%s'", DELTA_CATALOG_NAME, trinoViewOnHiveName, SCHEMA), "VALUES 'VIEW'");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", hiveTable));
            assertUpdate(format("DROP VIEW IF EXISTS %s", trinoViewOnHive));
        }
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        if (metastore != null) {
            metastore.dropDatabase(SCHEMA, false);
            deleteRecursively(Path.of(dataDirectory), ALLOW_INSECURE);
        }
    }
}
