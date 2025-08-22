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

package io.trino.plugin.teradata;

import java.util.Map;

public class DatabaseConfig
{
    private final String jdbcUrl;
    private final String hostName;
    private final String databaseName;
    private final boolean useClearScape;
    private final LogonMechanism logMech;
    private final AuthenticationConfig authConfig;
    private final String clearScapeEnvName;
    private final Map<String, String> jdbcProperties;

    private DatabaseConfig(Builder builder)
    {
        this.jdbcUrl = builder.jdbcUrl;
        this.hostName = builder.hostName;
        this.databaseName = builder.databaseName;
        this.useClearScape = builder.useClearScape;
        this.logMech = builder.logMech;
        this.authConfig = builder.authConfig;
        this.clearScapeEnvName = builder.clearScapeEnvName;
        this.jdbcProperties = builder.jdbcProperties;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public Builder toBuilder()
    {
        return builder()
                .jdbcUrl(this.jdbcUrl)
                .hostName(this.hostName)
                .databaseName(this.databaseName)
                .useClearScape(this.useClearScape)
                .logMech(this.logMech)
                .authConfig(this.authConfig)
                .clearScapeEnvName(this.clearScapeEnvName)
                .jdbcProperties(this.jdbcProperties);
    }

    // Getters
    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public boolean isUseClearScape()
    {
        return useClearScape;
    }

    public LogonMechanism getLogMech()
    {
        return logMech;
    }

    public AuthenticationConfig getAuthConfig()
    {
        return authConfig;
    }

    public String getClearScapeEnvName()
    {
        return clearScapeEnvName;
    }

    public Map<String, String> getJdbcProperties()
    {
        return jdbcProperties;
    }

    public String getHostName()
    {
        return hostName;
    }

    public String getTMode()
    {
        if (jdbcProperties != null && jdbcProperties.containsKey("TMODE")) {
            return jdbcProperties.get("TMODE");
        }
        return "ANSI";
    }

    public static class Builder
    {
        private String jdbcUrl;
        private String hostName;
        private String databaseName = "trino";
        private boolean useClearScape;
        private LogonMechanism logMech = LogonMechanism.TD2;
        private AuthenticationConfig authConfig = new AuthenticationConfig();
        private String clearScapeEnvName;
        private Map<String, String> jdbcProperties;

        public Builder jdbcUrl(String jdbcUrl)
        {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder databaseName(String databaseName)
        {
            this.databaseName = databaseName;
            return this;
        }

        public Builder useClearScape(boolean useClearScape)
        {
            this.useClearScape = useClearScape;
            return this;
        }

        public Builder logMech(LogonMechanism logMech)
        {
            this.logMech = logMech;
            return this;
        }

        public Builder authConfig(AuthenticationConfig authConfig)
        {
            this.authConfig = authConfig;
            return this;
        }

        public Builder clearScapeEnvName(String clearScapeEnvName)
        {
            this.clearScapeEnvName = clearScapeEnvName;
            return this;
        }

        public Builder jdbcProperties(Map<String, String> jdbcProperties)
        {
            this.jdbcProperties = jdbcProperties;
            return this;
        }

        public Builder hostName(String hostName)
        {
            this.hostName = hostName;
            return this;
        }

        public DatabaseConfig build()
        {
            return new DatabaseConfig(this);
        }
    }
}
