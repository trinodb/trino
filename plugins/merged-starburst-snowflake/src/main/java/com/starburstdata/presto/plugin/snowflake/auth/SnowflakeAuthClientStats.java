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
package com.starburstdata.presto.plugin.snowflake.auth;

import io.prestosql.plugin.base.jmx.InvocationStats;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class SnowflakeAuthClientStats
{
    private final InvocationStats requestOauthToken = new InvocationStats();
    private final InvocationStats refreshCredential = new InvocationStats();
    private final InvocationStats generateSamlRequest = new InvocationStats();

    @Managed
    @Nested
    public InvocationStats getRequestOauthToken()
    {
        return requestOauthToken;
    }

    @Managed
    @Nested
    public InvocationStats getRefreshCredential()
    {
        return refreshCredential;
    }

    @Managed
    @Nested
    public InvocationStats getGenerateSamlRequest()
    {
        return generateSamlRequest;
    }
}
