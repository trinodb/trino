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

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestIntegrationSmokeTest;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.List;

import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.createSynapseQueryRunner;
import static com.starburstdata.presto.plugin.synapse.SynapseServer.JDBC_URL;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSynapseActiveDirectoryPasswordAuth
        extends AbstractTestIntegrationSmokeTest
{
    private static final String ACTIVE_DIRECTORY_USERNAME = requireNonNull(System.getProperty("test.synapse.jdbc.active-directory-user"), "test.synapse.jdbc.active-directory-user is not set");
    private static final String ACTIVE_DIRECTORY_PASSWORD = requireNonNull(System.getProperty("test.synapse.jdbc.active-directory-password"), "test.synapse.jdbc.active-directory-password is not set");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        SynapseServer synapseServer = new SynapseServer();
        return createSynapseQueryRunner(
                synapseServer,
                true,
                ImmutableMap.<String, String>builder()
                    .put("connection-url", JDBC_URL)
                    .put("connection-user", ACTIVE_DIRECTORY_USERNAME)
                    .put("connection-password", ACTIVE_DIRECTORY_PASSWORD)
                    .put("synapse.authentication.type", "ACTIVE_DIRECTORY_PASSWORD")
                    .build(),
                List.of());
    }

    @Test
    public void testActiveDirectoryPasswordAuthentication()
    {
        assertQuery("SELECT count(*) FROM nation", "SELECT 25");
    }

    @Test
    @Override // default test execution too long
    public void testSelectInformationSchemaTables()
    {
        assertThat(query(format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", getSession().getSchema().orElseThrow())))
                .skippingTypesCheck()
                .containsAll("VALUES 'nation', 'region'");
    }

    @Test
    @Override // default test execution too long
    public void testSelectInformationSchemaColumns()
    {
        assertThat(query(format("SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = 'region'", getSession().getSchema().orElseThrow())))
                .skippingTypesCheck()
                .matches("VALUES 'regionkey', 'name', 'comment'");
    }
}
