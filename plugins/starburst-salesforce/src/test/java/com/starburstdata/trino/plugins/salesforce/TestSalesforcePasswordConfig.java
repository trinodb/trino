/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.salesforce;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.starburstdata.trino.plugins.salesforce.SalesforceConnectionFactory.CDATA_OEM_KEY;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestSalesforcePasswordConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SalesforcePasswordConfig.class)
                .setUser(null)
                .setPassword(null)
                .setSecurityToken(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("salesforce.user", "user")
                .put("salesforce.password", "password")
                .put("salesforce.security-token", "foobar")
                .buildOrThrow();

        SalesforcePasswordConfig expected = new SalesforcePasswordConfig()
                .setUser("user")
                .setPassword("password")
                .setSecurityToken("foobar");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testSalesforcePasswordAuthenticationProvider()
    {
        SalesforceConfig config = new SalesforceConfig();

        SalesforcePasswordConfig passwordConfig = new SalesforcePasswordConfig()
                .setUser("user")
                .setPassword("password")
                .setSecurityToken("foobar");

        SalesforceModule.PasswordConnectionUrlProvider provider = new SalesforceModule.PasswordConnectionUrlProvider(config, passwordConfig);
        assertEquals(provider.get(), format("jdbc:salesforce:User=\"user\";Password=\"password\";UseSandbox=\"false\";OEMKey=\"%s\";SecurityToken=\"foobar\";", CDATA_OEM_KEY));
    }
}
