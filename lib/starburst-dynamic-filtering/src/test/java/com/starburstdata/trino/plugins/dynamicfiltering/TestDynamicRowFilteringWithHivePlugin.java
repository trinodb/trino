/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamicfiltering;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestDynamicRowFilteringWithHivePlugin
        extends AbstractTestDynamicRowFiltering
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(getDefaultSession()).build();

        File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toFile();
        HiveMetastore metastore = createTestingFileHiveMetastore(baseDir);
        metastore.createDatabase(
                Database.builder()
                        .setDatabaseName("default")
                        .setOwnerName(Optional.of("public"))
                        .setOwnerType(Optional.of(PrincipalType.ROLE))
                        .build());

        queryRunner.installPlugin(new TestingHivePlugin(Optional.of(metastore), new TestingHiveDynamicRowFilteringExtension(), Optional.empty()));
        queryRunner.createCatalog("test", "hive");
        queryRunner.execute("CREATE SCHEMA test.tpch");
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());
        for (TpchTable<?> table : REQUIRED_TPCH_TABLES) {
            queryRunner.execute(
                    format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.%s",
                            table.getTableName(),
                            table.getTableName()));
        }
        return queryRunner;
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        super.init();
        assertUpdate(
                "CREATE TABLE orders AS SELECT " +
                        "CAST(clerk AS CHAR(15)) clerk, " +
                        "CAST(orderstatus AS CHAR(5)) orderstatus, " +
                        "custkey FROM tpch.tiny.orders",
                15000);
    }

    @Test(timeOut = 30_000, dataProvider = "joinDistributionTypes")
    public void testRowFilteringWithCharStrings(JoinDistributionType joinDistributionType)
    {
        assertRowFiltering(
                "SELECT o1.clerk, o1.custkey, CAST(o1.orderstatus AS VARCHAR(1)) FROM orders o1, orders o2 WHERE o1.clerk = o2.clerk AND o2.custkey < 10",
                joinDistributionType,
                "orders");

        assertNoRowFiltering(
                "SELECT COUNT(*) FROM orders o1, orders o2 WHERE o1.orderstatus = o2.orderstatus AND o2.custkey < 20",
                joinDistributionType,
                "orders");
    }

    @Override
    protected SchemaTableName getSchemaTableName(ConnectorTableHandle connectorHandle)
    {
        return ((HiveTableHandle) connectorHandle).getSchemaTableName();
    }

    private Session getDefaultSession()
    {
        return testSessionBuilder()
                .setCatalog("test")
                .setSchema("tpch")
                .build();
    }
}
