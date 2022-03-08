/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Map;

import static com.google.common.io.Resources.getResource;
import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createSession;
import static java.util.Objects.requireNonNull;

@Test
public abstract class BaseOracleImpersonationWithAuthToLocalTest
        extends BaseOracleImpersonationWithAuthToLocal
{
    private final Map<String, String> additionalProperties;

    protected BaseOracleImpersonationWithAuthToLocalTest(Map<String, String> additionalProperties)
    {
        this.additionalProperties = ImmutableMap.copyOf(requireNonNull(additionalProperties, "additionalProperties is null"));
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .putAll(TestingStarburstOracleServer.connectionProperties())
                .put("oracle.impersonation.enabled", "true")
                .put("oracle.synonyms.enabled", "true")
                .put("auth-to-local.config-file", getResource("auth-to-local.json").getPath())
                .put("auth-to-local.refresh-period", "1s")
                .putAll(additionalProperties)
                .buildOrThrow();

        return OracleQueryRunner.builder()
                .withUnlockEnterpriseFeatures(true)
                .withConnectorProperties(properties)
                .withSessionModifier(session -> createSession(session.getIdentity().getUser() + "/admin@company.com"))
                .build();
    }

    @Override
    protected String getProxyUser()
    {
        return "presto_test_user";
    }
}
