/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import io.trino.Session;
import io.trino.plugin.sqlserver.BaseSqlServerConnectorTest;
import io.trino.plugin.sqlserver.DataCompression;
import io.trino.testing.AbstractTestDistributedQueries;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.createSynapseQueryRunner;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSynapseConnectorTest
        extends BaseSqlServerConnectorTest
{
    private SynapseServer synapseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        synapseServer = new SynapseServer();
        return createSynapseQueryRunner(
                synapseServer,
                "sqlserver",
                true,
                Function.identity(),
                Map.of(),
                TpchTable.getTables());
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return synapseServer::execute;
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Test
    @Override
    public void testInsertUnicode()
    {
        // TODO(https://starburstdata.atlassian.net/browse/SEP-5085) Test varchars with unicode with data setup done by synapse
    }

    @Test
    @Override
    public void testSelectInformationSchemaTables()
    {
        // TODO(https://starburstdata.atlassian.net/browse/SEP-5089)
        throw new SkipException("long execution");
    }

    @Test
    @Override
    public void testSelectInformationSchemaColumns()
    {
        // TODO(https://starburstdata.atlassian.net/browse/SEP-5089)
        throw new SkipException("long execution");
    }

    @Test
    @Override
    public void testInsertInPresenceOfNotSupportedColumn()
    {
        throw new SkipException("sql_variant not supported in Synapse");
    }

    @Test
    @Override // Needs an override because the SQL Server override is different from the base version of the test
    public void testColumnComment()
    {
        throw new SkipException("Synapse does not support column comments");
    }

    @Test
    public void testDecimalPredicatePushdown()
            throws Exception
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_decimal_pushdown", "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))")) {
            onRemoteDatabase().execute(format("INSERT INTO %s VALUES (123.321, 123456789.987654321)", table.getName()));

            assertThat(query(String.format("SELECT * FROM %s WHERE short_decimal <= 124", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE short_decimal <= 124", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE long_decimal <= 123456790", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE short_decimal <= 123.321", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE long_decimal <= 123456789.987654321", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE short_decimal = 123.321", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE long_decimal = 123456789.987654321", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
        }
    }

    @Test
    @Override // synapse doesn't support data_compression, so reverse SQL Server's override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar(79)\n" +
                        ")");
    }

    @Test(dataProvider = "dataCompression")
    @Override
    public void testCreateWithDataCompression(DataCompression compression)
    {
        // TODO(https://starburstdata.atlassian.net/browse/SEP-5073) overwrite with test of table option (WITH)
        throw new SkipException("data_compression not supported in Synapse");
    }

    @Test
    @Override
    public void testShowCreateForPartitionedTablesWithDataCompression()
    {
        throw new SkipException("CREATE PARTITION FUNCTION and data_compression not supported in Synapse");
    }

    @Test
    @Override
    public void testShowCreateForIndexedAndCompressedTable()
    {
        throw new SkipException("data_compression not supported in Synapse");
    }

    @Test
    @Override
    public void testShowCreateForUniqueConstraintCompressedTable()
    {
        throw new SkipException("data_compression not supported in Synapse");
    }

    @Override
    protected Optional<AbstractTestDistributedQueries.DataMappingTestSetup> filterDataMappingSmokeTestData(AbstractTestDistributedQueries.DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time") || typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.equals("varbinary")) {
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        return super.filterDataMappingSmokeTestData(dataMappingTestSetup);
    }

    @Override
    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                // strategy is AUTOMATIC by default and would not work for certain test cases (even if statistics are collected)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
    }
}
