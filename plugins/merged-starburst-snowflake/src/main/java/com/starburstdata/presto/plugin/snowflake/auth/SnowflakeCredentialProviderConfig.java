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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class SnowflakeCredentialProviderConfig
{
    private boolean useExtraCredentials;

    public boolean isUseExtraCredentials()
    {
        return useExtraCredentials;
    }

    @Config("snowflake.credential.use-extra-credentials")
    @ConfigDescription("Enables the use of extra credentials")
    public void setUseExtraCredentials(boolean useExtraCredentials)
    {
        this.useExtraCredentials = useExtraCredentials;
    }
}
