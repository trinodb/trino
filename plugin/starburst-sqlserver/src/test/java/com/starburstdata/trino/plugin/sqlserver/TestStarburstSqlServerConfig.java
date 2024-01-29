/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.sqlserver;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

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
                .setOverrideCatalogEnabled(false)
                .setOverrideCatalogName(null)
                .setDatabasePrefixForSchemaEnabled(false)
                .setConnectionsCount(1));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("sqlserver.override-catalog.enabled", "true")
                .put("sqlserver.override-catalog.name", "catalog")
                .put("sqlserver.database-prefix-for-schema.enabled", "true")
                .put("sqlserver.parallel.connections-count", "2")
                .buildOrThrow();

        StarburstSqlServerConfig expected = new StarburstSqlServerConfig()
                .setOverrideCatalogEnabled(true)
                .setOverrideCatalogName("catalog")
                .setConnectionsCount(2)
                .setDatabasePrefixForSchemaEnabled(true);
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
