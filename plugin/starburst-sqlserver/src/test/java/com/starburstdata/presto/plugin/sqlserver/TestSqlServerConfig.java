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

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSqlServerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SqlServerConfig.class)
                .setImpersonationEnabled(false)
                .setOverrideCatalogEnabled(false)
                .setOverrideCatalogName(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("sqlserver.impersonation.enabled", "true")
                .put("sqlserver.override-catalog.enabled", "true")
                .put("sqlserver.override-catalog.name", "catalog")
                .build();

        SqlServerConfig expected = new SqlServerConfig()
                .setImpersonationEnabled(true)
                .setOverrideCatalogEnabled(true)
                .setOverrideCatalogName("catalog");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testDisableOverrideCatalog()
    {
        assertThatThrownBy(() -> new SqlServerConfig()
                .setOverrideCatalogEnabled(false)
                .setOverrideCatalogName("ignore")
                .validate())
                .hasMessageContaining("sqlserver.override-catalog.enabled needs to be set in order to use sqlserver.override-catalog.name parameter");
    }
}
