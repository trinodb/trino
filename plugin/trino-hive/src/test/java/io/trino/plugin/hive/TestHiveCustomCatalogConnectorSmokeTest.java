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
package io.trino.plugin.hive;

import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveCustomCatalogConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    private static final String HIVE_CUSTOM_CATALOG = "custom";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String bucketName = "test-hive-metastore-catalog-smoke-test-" + randomNameSuffix();
        HiveMinioDataLake hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName, HiveHadoop.HIVE3_IMAGE));
        hiveMinioDataLake.start();

        // Inserting into metastore's database directly because the Hive does not expose a way to create a custom catalog
        hiveMinioDataLake.getHiveHadoop().runOnMetastore("INSERT INTO CTLGS VALUES (2, '%s', 'Custom catalog', 's3://%s/custom')".formatted(HIVE_CUSTOM_CATALOG, bucketName));

        QueryRunner queryRunner = HiveQueryRunner.builder()
                .addHiveProperty("hive.metastore", "thrift")
                .addHiveProperty("hive.metastore.uri", hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint().toString())
                .addHiveProperty("hive.metastore.thrift.catalog-name", HIVE_CUSTOM_CATALOG)
                .addHiveProperty("fs.hadoop.enabled", "false")
                .addHiveProperty("fs.native-s3.enabled", "true")
                .addHiveProperty("s3.path-style-access", "true")
                .addHiveProperty("s3.region", MINIO_REGION)
                .addHiveProperty("s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                .addHiveProperty("s3.aws-access-key", MINIO_ACCESS_KEY)
                .addHiveProperty("s3.aws-secret-key", MINIO_SECRET_KEY)
                .setCreateTpchSchemas(false) // Create the required tpch tables after the initialisation of the query runner
                .build();

        HiveMetastore metastore = getConnectorService(queryRunner, HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
        metastore.createDatabase(createDatabaseMetastoreObject(TPCH_SCHEMA, Optional.of("s3://%s/%s".formatted(bucketName, TPCH_SCHEMA))));
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, REQUIRED_TPCH_TABLES);

        return queryRunner;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_MULTI_STATEMENT_WRITES -> true;
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_TRUNCATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        assertThatThrownBy(super::testRowLevelDelete)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testRowLevelUpdate()
    {
        assertThatThrownBy(super::testRowLevelUpdate)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testUpdate()
    {
        assertThatThrownBy(super::testUpdate)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMerge()
    {
        assertThatThrownBy(super::testMerge)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo("""
                        CREATE TABLE hive.tpch.region (
                           regionkey bigint,
                           name varchar(25),
                           comment varchar(152)
                        )
                        WITH (
                           format = 'ORC'
                        )""");
    }

    @Test
    @Override
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        // Override because HivePrincipal's username is case-sensitive unlike TrinoPrincipal
        assertThatThrownBy(super::testCreateSchemaWithNonLowercaseOwnerName)
                .hasMessageContaining("Access Denied: Cannot create schema")
                .hasStackTraceContaining("CREATE SCHEMA");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertQueryFails(
                format("ALTER SCHEMA %s RENAME TO %s", schemaName, schemaName + randomNameSuffix()),
                "Hive metastore does not support renaming schemas");
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
