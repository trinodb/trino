/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerConfig.SqlServerAuthenticationType.PASSWORD;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerConfig.SqlServerAuthenticationType.PASSWORD_PASS_THROUGH;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStarburstSqlServerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StarburstSqlServerConfig.class)
                .setImpersonationEnabled(false)
                .setOverrideCatalogEnabled(false)
                .setOverrideCatalogName(null)
                .setAuthenticationType(PASSWORD)
                .setBulkCopyForWriteLockDestinationTable(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("sqlserver.impersonation.enabled", "true")
                .put("sqlserver.override-catalog.enabled", "true")
                .put("sqlserver.override-catalog.name", "catalog")
                .put("sqlserver.authentication.type", "PASSWORD_PASS_THROUGH")
                .put("sqlserver.bulk-copy-for-write.lock-destination-table", "true")
                .build();

        StarburstSqlServerConfig expected = new StarburstSqlServerConfig()
                .setImpersonationEnabled(true)
                .setOverrideCatalogEnabled(true)
                .setOverrideCatalogName("catalog")
                .setAuthenticationType(PASSWORD_PASS_THROUGH)
                .setBulkCopyForWriteLockDestinationTable(true);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testDisableOverrideCatalog()
    {
        assertThatThrownBy(() -> new StarburstSqlServerConfig()
                .setOverrideCatalogEnabled(false)
                .setOverrideCatalogName("ignore")
                .validate())
                .hasMessageContaining("sqlserver.override-catalog.enabled needs to be set in order to use sqlserver.override-catalog.name parameter");
    }
}
