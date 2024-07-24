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
import io.trino.Session;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveMetastoreCatalogs
        extends AbstractTestQueryFramework
{
    private static final String TRINO_HIVE_CATALOG = "hive_catalog";
    private static final String TRINO_HIVE_CUSTOM_CATALOG = "hive_custom_catalog";
    private static final String HIVE_CUSTOM_CATALOG = "custom";

    private String bucketName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.bucketName = "test-hive-metastore-catalogs-" + randomNameSuffix();
        HiveMinioDataLake hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName, HiveHadoop.HIVE3_IMAGE));
        hiveMinioDataLake.start();

        QueryRunner queryRunner = HiveQueryRunner.builder()
                .setHiveProperties(buildHiveProperties(hiveMinioDataLake))
                .setCreateTpchSchemas(false)
                .build();

        hiveMinioDataLake.getHiveHadoop().runOnMetastore("INSERT INTO CTLGS VALUES (2, '%s', 'Custom catalog', 's3://%s/custom')".formatted(HIVE_CUSTOM_CATALOG, bucketName));

        queryRunner.createCatalog(TRINO_HIVE_CUSTOM_CATALOG, "hive", ImmutableMap.<String, String>builder()
                .put("hive.metastore.thrift.catalog-name", HIVE_CUSTOM_CATALOG)
                .putAll(buildHiveProperties(hiveMinioDataLake))
                .buildOrThrow());

        queryRunner.createCatalog(TRINO_HIVE_CATALOG, "hive", ImmutableMap.<String, String>builder()
                .put("hive.metastore.thrift.catalog-name", "hive") // HiveMetastore uses "hive" as the default catalog name
                .putAll(buildHiveProperties(hiveMinioDataLake))
                .buildOrThrow());

        return queryRunner;
    }

    private static Map<String, String> buildHiveProperties(HiveMinioDataLake hiveMinioDataLake)
    {
        return ImmutableMap.<String, String>builder()
                .put("hive.metastore", "thrift")
                .put("hive.metastore.uri", hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint().toString())
                .put("fs.hadoop.enabled", "false")
                .put("fs.native-s3.enabled", "true")
                .put("s3.path-style-access", "true")
                .put("s3.region", MINIO_REGION)
                .put("s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                .buildOrThrow();
    }

    @Test
    public void testShowTables()
    {
        assertThat(query("SHOW SCHEMAS")).matches("VALUES VARCHAR 'default', VARCHAR 'information_schema'");

        HiveMetastore metastore = getConnectorService(getQueryRunner(), HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        String defaultCatalogSchema = "default_catalog_schema";
        metastore.createDatabase(createDatabaseMetastoreObject(defaultCatalogSchema, Optional.of("s3://%s/%s".formatted(bucketName, defaultCatalogSchema))));

        Session defaultCatalogSession = testSessionBuilder()
                .setCatalog("hive")
                .setSchema(defaultCatalogSchema)
                .build();
        Session hiveCatalogSession = testSessionBuilder()
                .setCatalog(TRINO_HIVE_CATALOG)
                .setSchema(defaultCatalogSchema)
                .build();
        assertUpdate(defaultCatalogSession, "CREATE TABLE tabledefault (data integer)");
        assertUpdate(defaultCatalogSession, "INSERT INTO tabledefault VALUES (1),(2),(3),(4)", 4);

        String customCatalogSchema = "custom_catalog_schema";
        assertUpdate(
                testSessionBuilder()
                        .setCatalog(TRINO_HIVE_CUSTOM_CATALOG)
                        .build(),
                "CREATE SCHEMA " + customCatalogSchema);
        Session customCatalogSession = testSessionBuilder()
                .setCatalog(TRINO_HIVE_CUSTOM_CATALOG)
                .setSchema(customCatalogSchema)
                .build();
        assertUpdate(customCatalogSession, "CREATE TABLE tablecustom (data integer)");
        assertUpdate(customCatalogSession, "INSERT INTO tablecustom VALUES (4),(5),(6),(7)", 4);

        // schemas from the default Hive catalog are not visible in the custom Hive catalog and vice versa
        assertThat(computeActual(defaultCatalogSession, "SHOW SCHEMAS").getOnlyColumn())
                .containsOnly("default", "default_catalog_schema", "information_schema");
        assertThat(computeActual(hiveCatalogSession, "SHOW SCHEMAS").getOnlyColumn())
                .containsOnly("default", "default_catalog_schema", "information_schema");
        assertThat(computeActual(customCatalogSession, "SHOW SCHEMAS").getOnlyColumn())
                .containsOnly(customCatalogSchema, "information_schema");

        // tables from the default Hive catalog are not visible in the custom Hive catalog and vice versa
        assertThat(computeActual(defaultCatalogSession, "SHOW TABLES IN " + defaultCatalogSchema).getOnlyColumn())
                .containsOnly("tabledefault");
        assertThat(computeActual(hiveCatalogSession, "SHOW TABLES IN " + defaultCatalogSchema).getOnlyColumn())
                .containsOnly("tabledefault");
        assertThat(computeActual(customCatalogSession, "SHOW TABLES IN " + customCatalogSchema).getOnlyColumn())
                .containsOnly("tablecustom");
        assertThat((String) computeScalar(customCatalogSession, format("SHOW CREATE TABLE %s.tablecustom", customCatalogSchema)))
                .isEqualTo("""
                        CREATE TABLE hive_custom_catalog.custom_catalog_schema.tablecustom (
                           data integer
                        )
                        WITH (
                           format = 'ORC'
                        )""");

        // select query : join hive's and custom catalog's table
        assertQuery("SELECT a.data from hive.default_catalog_schema.tabledefault a, hive_custom_catalog.custom_catalog_schema.tablecustom b WHERE a.data = b.data", "SELECT 4");

        assertUpdate(defaultCatalogSession, "DROP SCHEMA " + defaultCatalogSchema + " CASCADE");
        assertUpdate(customCatalogSession, "DROP SCHEMA " + customCatalogSchema + " CASCADE");
    }

    private static Database createDatabaseMetastoreObject(String name, Optional<String> locationBase)
    {
        return Database.builder()
                .setLocation(locationBase.map(base -> base + "/" + name))
                .setDatabaseName(name)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
    }
}
