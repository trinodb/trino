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
package com.starburstdata.presto.plugin.snowflake.jdbc;

import com.starburstdata.presto.plugin.snowflake.auth.OauthCredential;
import com.starburstdata.presto.plugin.snowflake.auth.SnowflakeOauthService;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.credential.CredentialPropertiesProvider;

import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class SnowflakeOauthPropertiesProvider
        implements CredentialPropertiesProvider
{
    private final SnowflakeOauthService snowflakeOauthService;

    public SnowflakeOauthPropertiesProvider(SnowflakeOauthService snowflakeOauthService)
    {
        this.snowflakeOauthService = requireNonNull(snowflakeOauthService, "snowflakeOauthService is null");
    }

    @Override
    public Properties getCredentialProperties(JdbcIdentity identity)
    {
        OauthCredential cred = snowflakeOauthService.getCredential(identity);

        Properties props = new Properties();
        props.put("authenticator", "oauth");
        props.put("token", cred.getAccessToken());

        return props;
    }
}
