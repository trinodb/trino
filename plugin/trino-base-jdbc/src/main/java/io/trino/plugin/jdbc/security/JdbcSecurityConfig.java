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
package io.trino.plugin.jdbc.security;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

import static io.trino.plugin.jdbc.security.JdbcSecurityConfig.SecuritySystem.ALLOW_ALL;
import static java.util.Locale.ENGLISH;

public class JdbcSecurityConfig
{
    public enum SecuritySystem
    {
        ALLOW_ALL,
        READ_ONLY,
        FILE;

        public static SecuritySystem fromString(String value)
        {
            return valueOf(value.toUpperCase(ENGLISH).replace("-", "_"));
        }
    }

    private SecuritySystem securitySystem = ALLOW_ALL;

    @NotNull
    public SecuritySystem getSecuritySystem()
    {
        return securitySystem;
    }

    @Config("security")
    @ConfigDescription("Security system to use: allow-all, read-only, file")
    public JdbcSecurityConfig setSecuritySystem(SecuritySystem securitySystem)
    {
        this.securitySystem = securitySystem;
        return this;
    }
}
