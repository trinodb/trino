/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.Session;
import io.trino.plugin.base.security.FileBasedSystemAccessControl;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.stargate.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunner;
import static com.starburstdata.trino.plugin.stargate.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunnerWithMemory;
import static com.starburstdata.trino.plugin.stargate.StarburstRemoteQueryRunner.starburstRemoteConnectionUrl;
import static io.trino.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStarburstRemoteUserImpersonation
        extends AbstractTestQueryFramework
{
    private static final String TEST_USER = "test_user";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String authToLocalConfig = Resources.getResource("test-user-impersonation.auth-to-local.json").getPath();
        String accessControlRules = Resources.getResource("test-user-impersonation.system-access-rules.json").getPath();
        DistributedQueryRunner remoteStarburst = closeAfterClass(createStarburstRemoteQueryRunnerWithMemory(
                Map.of(),
                ImmutableList.of(NATION),
                Optional.of(new FileBasedSystemAccessControl.Factory().create(ImmutableMap.of(SECURITY_CONFIG_FILE, accessControlRules)))));

        return createStarburstRemoteQueryRunner(
                true,
                Map.of(),
                Map.of(
                        "connection-url", starburstRemoteConnectionUrl(remoteStarburst, "memory"),
                        "starburst.impersonation.enabled", "true",
                        "auth-to-local.config-file", authToLocalConfig));
    }

    @Test
    public void testUserIsImpersonated()
    {
        assertThatThrownBy(() -> computeActual("SELECT count(*) FROM nation"))
                .hasMessageContaining("No auth-to-local rule was found for user [user] and principal [user]");

        Session sessionWithReadOnlyUser = Session.builder(getSession())
                .setIdentity(Identity.forUser(TEST_USER).build())
                .build();
        assertQuery(sessionWithReadOnlyUser, "SELECT count(*) FROM nation");

        assertThatThrownBy(() -> computeActual(sessionWithReadOnlyUser, "CREATE TABLE nation_copy AS SELECT * FROM nation"))
                .hasMessageContaining("Access Denied");
    }
}
