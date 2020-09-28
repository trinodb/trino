/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.TestTable;
import io.prestosql.tpch.TpchTable;
import org.testng.annotations.AfterClass;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.starburstdata.presto.plugin.saphana.SapHanaQueryRunner.createSapHanaQueryRunner;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSapHanaDistributedQueries
        extends AbstractTestDistributedQueries
{
    private TestingSapHanaServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = new TestingSapHanaServer();
        return createSapHanaQueryRunner(
                server,
                ImmutableMap.of(),
                ImmutableMap.of(),
                TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        server.close();
    }

    // Now unused, but added here to provoke conflict when upgrading to https://github.com/prestosql/presto/pull/5326
    // TODO remove testDelete override
    protected boolean supportsDelete()
    {
        return false;
    }

    @Override
    public void testDelete()
    {
        assertThatThrownBy(super::testDelete)
                .hasStackTraceContaining("This connector does not support updates or deletes");
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsArrays()
    {
        return false;
    }

    // Now unused, but added here to provoke conflict when upgrading to https://github.com/prestosql/presto/pull/5326
    // TODO remove testCommentTable override
    protected boolean supportsCommentOnTable()
    {
        return false;
    }

    @Override
    public void testCommentTable()
    {
        assertThatThrownBy(super::testCommentTable)
                .hasStackTraceContaining("io.prestosql.spi.PrestoException: This connector does not support setting table comments");
    }

    @Override
    protected boolean supportsCommentOnColumn()
    {
        return false;
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                server::execute,
                "tpch.table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        switch (dataMappingTestSetup.getPrestoTypeName()) {
            case "time":
                verify(dataMappingTestSetup.getHighValueLiteral().equals("TIME '23:59:59.999'"), "super has changed high value for TIME");
                return Optional.of(
                        new DataMappingTestSetup(
                                dataMappingTestSetup.getPrestoTypeName(),
                                dataMappingTestSetup.getSampleValueLiteral(),
                                "TIME '23:59:59.000'")); // SAP HANA does not store second fraction, so 23:59:59.999 would became 00:00:00

            case "timestamp(3) with time zone":
                return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }
}
