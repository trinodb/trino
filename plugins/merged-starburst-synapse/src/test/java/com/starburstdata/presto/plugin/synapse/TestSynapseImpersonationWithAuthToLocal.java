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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.google.common.io.Resources.getResource;
import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.ALICE_USER;
import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.BOB_USER;
import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.CHARLIE_USER;
import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.UNKNOWN_USER;
import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.createSession;
import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.createSynapseQueryRunner;
import static java.util.function.Function.identity;

public class TestSynapseImpersonationWithAuthToLocal
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        SynapseServer synapseServer = new SynapseServer();
        return createSynapseQueryRunner(
                synapseServer,
                identity(),
                ImmutableMap.of(
                        "synapse.impersonation.enabled", "true",
                        "auth-to-local.config-file", getResource("auth-to-local.json").getPath()),
                ImmutableList.of());
    }

    @Test
    public void testUserImpersonation()
    {
        assertQuery(
                createSession(ALICE_USER + "/admin@company.com"),
                "SELECT * FROM user_context",
                "SELECT 'alice', 'alice'");
        assertQuery(
                createSession(BOB_USER + "/user@company.com"),
                "SELECT * FROM user_context",
                "SELECT 'bob', 'bob'");
        assertQueryFails(
                createSession(CHARLIE_USER + "/hr@company.com"),
                "SELECT * FROM user_context",
                "line 1:15: Table 'synapse.dbo.user_context' does not exist");
        assertQueryFails(
                createSession(UNKNOWN_USER + "/marketing@company.com"),
                "SELECT * FROM user_context",
                ".*database principal .* does not exist.*");
        assertQueryFails(
                createSession(UNKNOWN_USER + "/marketing@other.com"),
                "SELECT * FROM user_context",
                "No auth-to-local rule was found for user \\[non_existing_user/marketing@other.com] and principal \\[non_existing_user/marketing@other.com]");
        assertQueryFails(
                createSession(BOB_USER),
                "SELECT * FROM user_context",
                "No auth-to-local rule was found for user \\[bob] and principal \\[bob]");
    }
}
