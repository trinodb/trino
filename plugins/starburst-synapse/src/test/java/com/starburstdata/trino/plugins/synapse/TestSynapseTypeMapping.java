/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.synapse;

import io.trino.plugin.sqlserver.BaseSqlServerTypeMapping;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testng.services.ManageTestResources;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.starburstdata.trino.plugins.synapse.SynapseQueryRunner.createSynapseQueryRunner;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSynapseTypeMapping
        extends BaseSqlServerTypeMapping
{
    @ManageTestResources.Suppress(because = "Mock to remote server")
    protected final SynapseServer synapseServer = new SynapseServer();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        SynapseServer synapseServer = new SynapseServer();
        return createSynapseQueryRunner(
                synapseServer,
                // Synapse tests are slow. Cache metadata to speed them up.
                Map.of("metadata.cache-ttl", "60m"),
                List.of());
    }

    @Override
    @Test
    public void testTrinoLongChar()
    {
        assertThatThrownBy(super::testTrinoLongChar).hasRootCauseMessage("Unsupported column type: char(4001)");
        throw new SkipException("Synapse does not support char > 4000");
    }

    @Override
    @Test
    public void testTrinoLongVarchar()
    {
        assertThatThrownBy(super::testTrinoLongVarchar).hasRootCauseMessage("Unsupported column type: varchar(4001)");
        throw new SkipException("Synapse does not support varchar > 4000");
    }

    @Override
    @Test
    public void testSqlServerLongVarchar()
    {
        assertThatThrownBy(super::testSqlServerLongVarchar).hasRootCauseMessage("Cannot find data type 'text'.");
        throw new SkipException("Synapse does not support text and ntext data types");
    }

    @Override
    @Test
    public void testTrinoUnboundedVarchar()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("varchar", "'text_a'", createVarcharType(4000), "CAST('text_a' AS varchar(4000))")
                .addRoundTrip("varchar", "'text_b'", createVarcharType(4000), "CAST('text_b' AS varchar(4000))")
                .addRoundTrip("varchar", "'text_d'", createVarcharType(4000), "CAST('text_d' AS varchar(4000))")
                .addRoundTrip("varchar", "CAST('攻殻機動隊' AS varchar)", createVarcharType(4000), "CAST('攻殻機動隊' AS varchar(4000))")
                .addRoundTrip("varchar", "CAST('攻殻機動隊' AS varchar)", createVarcharType(4000), "CAST('攻殻機動隊' AS varchar(4000))")
                .addRoundTrip("varchar", "CAST('攻殻機動隊' AS varchar)", createVarcharType(4000), "CAST('攻殻機動隊' AS varchar(4000))")
                .addRoundTrip("varchar", "CAST('😂' AS varchar)", createVarcharType(4000), "CAST('😂' AS varchar(4000))")
                .addRoundTrip("varchar", "CAST('Ну, погоди!' AS varchar)", createVarcharType(4000), "CAST('Ну, погоди!' AS varchar(4000))")
                .addRoundTrip("varchar", "'text_f'", createVarcharType(4000), "CAST('text_f' AS varchar(4000))")
                .execute(getQueryRunner(), trinoCreateAndInsert(getSession(), "test_varchar_unbounded"))
                .execute(getQueryRunner(), trinoCreateAsSelect("test_varchar_unbounded"));
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return synapseServer::execute;
    }
}
