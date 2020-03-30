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

import com.starburstdata.presto.plugin.snowflake.auth.SnowflakeOauthService;
import io.prestosql.plugin.jdbc.AuthToLocal;
import io.prestosql.plugin.jdbc.JdbcIdentity;

import static java.util.Objects.requireNonNull;

/**
 * Users authenticate with their Okta user names (usually their email address), which has to match
 * Snowflake's <pre>login_name</pre>.  However, that is typically not the Snowflake user name (which
 * is what's used for example in <pre>DESCRIBE USER</pre>). When an OAuth token is granted, the user
 * name for which it was issued is returned in the response and we use that information to perform
 * the mapping from Okta to Snowflake user name.
 */
public class OauthToLocal
        implements AuthToLocal
{
    private final SnowflakeOauthService snowflakeOauthService;

    public OauthToLocal(SnowflakeOauthService snowflakeOauthService)
    {
        this.snowflakeOauthService = requireNonNull(snowflakeOauthService, "snowflakeOauthService is null");
    }

    @Override
    public String translate(JdbcIdentity identity)
    {
        return snowflakeOauthService.getCredential(identity).getSnowflakeUsername();
    }
}
