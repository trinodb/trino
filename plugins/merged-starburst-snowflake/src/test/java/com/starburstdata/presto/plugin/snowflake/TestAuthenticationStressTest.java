/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.starburstdata.presto.plugin.snowflake;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.plugin.snowflake.auth.CachingSnowflakeOauthService;
import com.starburstdata.presto.plugin.snowflake.auth.DefaultSnowflakeOauthService;
import com.starburstdata.presto.plugin.snowflake.auth.NativeOktaAuthClient;
import com.starburstdata.presto.plugin.snowflake.auth.NativeSnowflakeAuthClient;
import com.starburstdata.presto.plugin.snowflake.auth.OauthCredential;
import com.starburstdata.presto.plugin.snowflake.auth.OktaConfig;
import com.starburstdata.presto.plugin.snowflake.auth.SnowflakeOauthConfig;
import com.starburstdata.presto.plugin.snowflake.auth.SnowflakeOauthService;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.ACCOUNT_NAME;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.ACCOUNT_URL;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.CLIENT_ID;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.CLIENT_SECRET;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.OKTA_PASSWORD;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.OKTA_URL;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.OKTA_USER;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.testng.Assert.assertEquals;

public class TestAuthenticationStressTest
{
    private static final Logger log = Logger.get(TestAuthenticationStressTest.class);

    private NativeOktaAuthClient oktaAuthClient;
    private NativeSnowflakeAuthClient snowflakeAuthClient;
    private CredentialProvider credentialProvider;
    private Duration ttl = new Duration(24, HOURS);

    @BeforeTest
    public void setUp()
    {
        OktaConfig oktaConfig = new OktaConfig().setAccountUrl(OKTA_URL);
        oktaAuthClient = new NativeOktaAuthClient(oktaConfig);
        SnowflakeOauthConfig snowflakeOauthConfig = new SnowflakeOauthConfig()
                .setAccountName(ACCOUNT_NAME)
                .setAccountUrl(ACCOUNT_URL)
                .setClientId(CLIENT_ID)
                .setClientSecret(CLIENT_SECRET)
                .setCredentialTtl(ttl);
        snowflakeAuthClient = new NativeSnowflakeAuthClient(snowflakeOauthConfig);
        credentialProvider = new DummyCredentialProvider();
    }

    @DataProvider
    public Object[][] caching()
    {
        return new Object[][] {
                new Object[] {Boolean.TRUE}, new Object[] {Boolean.FALSE}
        };
    }

    @Test(enabled = false, dataProvider = "caching")
    public void test(boolean caching)
    {
        SnowflakeOauthService nonCachingService = new DefaultSnowflakeOauthService(snowflakeAuthClient, oktaAuthClient, credentialProvider);
        SnowflakeOauthService service = caching ?
                new CachingSnowflakeOauthService(nonCachingService, credentialProvider, ttl, 1) :
                nonCachingService;
        log.info("Calling getCredential()");
        for (int i = 0; i < 1000; i++) {
            OauthCredential credential = service.getCredential(new JdbcIdentity(OKTA_USER, Optional.empty(), ImmutableMap.of(), Optional.empty()));
            assertEquals(credential.getSnowflakeUsername(), "OKTA_TEST");
            if (i % 50 == 49) {
                log.info("Made %s calls to getCredential()", i + 1);
            }
        }
    }

    public static class DummyCredentialProvider
            implements CredentialProvider
    {
        @Override
        public Optional<String> getConnectionUser(Optional<JdbcIdentity> jdbcIdentity)
        {
            return Optional.of(OKTA_USER);
        }

        @Override
        public Optional<String> getConnectionPassword(Optional<JdbcIdentity> jdbcIdentity)
        {
            return Optional.of(OKTA_PASSWORD);
        }
    }
}
