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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.List;

import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.createSynapseQueryRunner;
import static com.starburstdata.presto.plugin.synapse.SynapseServer.JDBC_URL;
import static io.trino.testing.assertions.Assert.assertEquals;
import static java.util.Objects.requireNonNull;

public class TestSynapseActiveDirectoryPasswordAuth
        extends AbstractTestQueryFramework
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
                ImmutableMap.<String, String>builder()
                        .put("connection-url", JDBC_URL)
                        .put("connection-user", ACTIVE_DIRECTORY_USERNAME)
                        .put("connection-password", ACTIVE_DIRECTORY_PASSWORD)
                        .put("synapse.authentication.type", "ACTIVE_DIRECTORY_PASSWORD")
                        .buildOrThrow(),
                List.of());
    }

    @Test
    public void testActiveDirectoryPasswordAuthentication()
    {
        assertQuery("SELECT count(*) FROM nation");
        assertQuery("SELECT * FROM nation");
        assertEquals(computeScalar("SELECT session_user_column FROM user_context"), ACTIVE_DIRECTORY_USERNAME);
    }
}
