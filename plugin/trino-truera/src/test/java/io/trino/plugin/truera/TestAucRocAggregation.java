package io.trino.plugin.truera;

import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import io.trino.plugin.hive.HivePlugin;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(singleThreaded = true)
public class TestAucRocAggregation extends AbstractTestQueryFramework {
    private ImmutableMap.Builder<String, String> icebergProperties = ImmutableMap.builder();
    private Optional<File> metastoreDirectory = Optional.empty();
    public static final String ICEBERG_CATALOG = "iceberg";
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRole("hive", new SelectedRole(ROLE, Optional.of("admin")))
                        .build())
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        queryRunner.installPlugin(new IcebergPlugin());
        Map<String, String> icebergProperties = new HashMap<>(this.icebergProperties.buildOrThrow());
        String catalogType = icebergProperties.get("iceberg.catalog.type");
        Path dataDir = metastoreDirectory.map(File::toPath).orElseGet(() -> queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data"));
        if (catalogType == null) {
            icebergProperties.put("iceberg.catalog.type", "TESTING_FILE_METASTORE");
            icebergProperties.put("hive.metastore.catalog.dir", dataDir.toString());
        }

        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", icebergProperties);
        queryRunner.installPlugin(new HivePlugin());
        queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", dataDir.toString())
                .put("hive.security", "sql-standard")
                .buildOrThrow());
        queryRunner.installPlugin(new TrueraTrinoPlugin());

        return queryRunner;
    }

    @BeforeClass
    public void setUp()
    {
        assertQuerySucceeds("CREATE SCHEMA hive.test_schema");
        assertQuerySucceeds("CREATE TABLE iceberg.test_schema.iceberg_probits_table (__id__ VARCHAR, __preds__ double)");
        assertQuerySucceeds("CREATE TABLE iceberg.test_schema.iceberg_labels_table (__id__ VARCHAR, __label__ double)");
        assertQuerySucceeds("INSERT INTO iceberg.test_schema.iceberg_probits_table VALUES ('A', 0.5), ('B', 0.2), ('C', 0.9)");
        assertQuerySucceeds("INSERT INTO iceberg.test_schema.iceberg_labels_table VALUES ('A', 1), ('B', 0), ('C', 1)");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS iceberg.test_schema.iceberg_labels_table");
        assertQuerySucceeds("DROP TABLE IF EXISTS iceberg.test_schema.iceberg_probits_table");
        assertQuerySucceeds("DROP SCHEMA IF EXISTS hive.test_schema");
    }

    @Test
    public void testAucRoc()
    {
        assertQuery("SELECT auc_roc(__label__, __preds__) " +
                "FROM iceberg.test_schema.iceberg_probits_table as pred " +
                "JOIN iceberg.test_schema.iceberg_labels_table AS label " +
                "ON pred.__id__ = label.__id__",
                "VALUES (1.0)");
    }
}
