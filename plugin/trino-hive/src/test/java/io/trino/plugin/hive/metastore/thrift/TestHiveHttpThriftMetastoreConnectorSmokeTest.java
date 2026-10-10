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
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.containers.Hive4HttpMetastoreFlociDataLake;
import io.trino.plugin.hive.containers.Hive4HttpMetastoreFlociDataLake.Transport;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs the connector smoke tests against a Hive 4 metastore that serves Thrift over HTTP.
 */
public class TestHiveHttpThriftMetastoreConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    protected Hive4HttpMetastoreFlociDataLake dataLake;

    protected Transport transport()
    {
        return Transport.HTTP;
    }

    /**
     * Catalog properties specific to the transport, in addition to the metastore URI.
     */
    protected Map<String, String> transportProperties()
    {
        return ImmutableMap.of();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        dataLake = closeAfterClass(new Hive4HttpMetastoreFlociDataLake(transport()));
        dataLake.start();

        QueryRunner queryRunner = HiveQueryRunner.builder()
                .addHiveProperties(catalogProperties())
                // The metastore runs without JWT authentication, so it identifies the caller from this header
                .addHiveProperty("hive.metastore.http.client.additional-headers", "x-actor-username:hive")
                .setCreateTpchSchemas(false)
                .build();

        HiveMetastore metastore = getConnectorService(queryRunner, HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
        metastore.createDatabase(Database.builder()
                .setDatabaseName(TPCH_SCHEMA)
                .setLocation(Optional.of("s3://%s/%s".formatted(dataLake.getBucketName(), TPCH_SCHEMA)))
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build());
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, REQUIRED_TPCH_TABLES);

        return queryRunner;
    }

    /**
     * Catalog properties for the metastore and S3, without the user header.
     */
    protected Map<String, String> catalogProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("hive.metastore", "thrift")
                .put("hive.metastore.uri", dataLake.getHiveMetastoreEndpoint().toString())
                .putAll(transportProperties())
                .put("fs.s3.enabled", "true")
                .put("s3.path-style-access", "true")
                .put("s3.endpoint", dataLake.floci().endpoint().toString())
                .put("s3.region", FLOCI_REGION)
                .put("s3.aws-access-key", FLOCI_ACCESS_KEY)
                .put("s3.aws-secret-key", FLOCI_SECRET_KEY)
                .buildOrThrow();
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
                .isEqualTo(
                        """
                        CREATE TABLE hive.tpch.region (
                           regionkey bigint,
                           name varchar(25),
                           comment varchar(152)
                        )
                        WITH (
                           format = 'PARQUET'
                        )\
                        """);
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
                "ALTER SCHEMA %s RENAME TO %s".formatted(schemaName, schemaName + randomNameSuffix()),
                "Hive metastore does not support renaming schemas");
    }

    @Test
    public void testMissingUserHeader()
    {
        // Without JWT, the metastore rejects requests that do not carry the x-actor-username header
        String catalog = "hive_without_user_header_" + randomNameSuffix();
        getQueryRunner().createCatalog(catalog, "hive", ImmutableMap.<String, String>builder()
                .putAll(catalogProperties())
                .put("hive.metastore.thrift.client.max-retries", "0")
                .buildOrThrow());

        assertThatThrownBy(() -> computeActual("SHOW SCHEMAS FROM " + catalog))
                .hasStackTraceContaining("HTTP Response code: 401");
    }
}
