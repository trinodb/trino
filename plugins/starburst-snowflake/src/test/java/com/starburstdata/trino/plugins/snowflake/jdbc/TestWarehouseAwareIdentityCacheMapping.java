/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.jdbc;

import com.starburstdata.trino.plugins.snowflake.SnowflakeConfig;
import com.starburstdata.trino.plugins.snowflake.jdbc.WarehouseAwareIdentityCacheMapping.Key;
import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.plugin.jdbc.IdentityCacheMapping.IdentityCacheKey;
import io.trino.plugin.jdbc.SingletonIdentityCacheMapping;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestWarehouseAwareIdentityCacheMapping
{
    @Test(dataProvider = "testKeyDataProvider")
    public void testWarehouseAwareIdentityCacheKey(SnowflakeConfig snowflakeConfig)
    {
        IdentityCacheMapping delegate = new SingletonIdentityCacheMapping();
        WarehouseAwareIdentityCacheMapping cacheMapping = new WarehouseAwareIdentityCacheMapping(delegate);

        IdentityCacheKey cacheKey = cacheMapping.getRemoteUserCacheKey(TestingConnectorSession.builder()
                .setPropertyMetadata(new SnowflakeJdbcSessionProperties(snowflakeConfig).getSessionProperties())
                .build());

        assertThat(cacheKey)
                .isInstanceOf(Key.class)
                .isEqualTo(new Key(
                        delegate.getRemoteUserCacheKey(TestingConnectorSession.builder().build()),
                        snowflakeConfig.getWarehouse()));
    }

    @DataProvider
    public static Object[][] testKeyDataProvider()
    {
        return new Object[][] {
                {new SnowflakeConfig()},
                {new SnowflakeConfig().setWarehouse("test warehouse")},
        };
    }
}
