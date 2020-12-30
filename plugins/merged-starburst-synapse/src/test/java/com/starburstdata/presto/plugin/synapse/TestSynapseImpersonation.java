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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.ALICE_USER;
import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.BOB_USER;
import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.CHARLIE_USER;
import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.UNKNOWN_USER;
import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.createSession;
import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.createSynapseQueryRunner;

public class TestSynapseImpersonation
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        SynapseServer synapseServer = new SynapseServer();
        return createSynapseQueryRunner(
                synapseServer,
                true,
                session -> createSession(ALICE_USER),
                ImmutableMap.of("synapse.impersonation.enabled", "true"),
                ImmutableList.of());
    }

    @Test
    public void testUserImpersonation()
    {
        assertQuery(createSession(ALICE_USER), "SELECT * FROM user_context", "SELECT 'alice', 'alice'");
        assertQuery(createSession(BOB_USER), "SELECT * FROM user_context", "SELECT 'bob', 'bob'");
        assertQueryFails(createSession(CHARLIE_USER), "SELECT * FROM user_context", "line 1:15: Table 'synapse.dbo.user_context' does not exist");
        assertQueryFails(
                createSession(UNKNOWN_USER),
                "SELECT count(*) FROM orders",
                "Cannot execute as the database principal because the principal \"non_existing_user\" does not exist, " +
                        "this type of principal cannot be impersonated, or you do not have permission.");
    }
}
