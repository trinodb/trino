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
package io.trino.plugin.databend;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.trino.plugin.jdbc.BaseJdbcConfig;

import java.util.concurrent.TimeUnit;

public class DatabendConfig
        extends BaseJdbcConfig
{
    private Duration connectionTimeout = new Duration(60, TimeUnit.SECONDS);
    private Boolean presignedUrlDisabled = false;

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("databend.connection-timeout")
    public DatabendConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public Boolean getPresignedUrlDisabled()
    {
        return presignedUrlDisabled;
    }

    @Config("databend.presigned-url-disabled")
    public DatabendConfig setPresignedUrlDisabled(Boolean presignedUrlDisabled)
    {
        this.presignedUrlDisabled = presignedUrlDisabled;
        return this;
    }
}
