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

public class TestStarburstCommonSqlServerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StarburstCommonSqlServerConfig.class)
                .setBulkCopyForWrite(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("sqlserver.bulk-copy-for-write.enabled", "true")
                .buildOrThrow();

        StarburstCommonSqlServerConfig expected = new StarburstCommonSqlServerConfig()
                .setBulkCopyForWrite(true);

        assertFullMapping(properties, expected);
    }
}
