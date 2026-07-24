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
package io.trino.plugin.teradata.integration;

import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.SystemEnvironmentUtils.isEnvSet;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;

public final class DatabaseConfigFactory
{
    private DatabaseConfigFactory() {}

    public static DatabaseConfig create(String envName)
    {
        String userName;
        String password;
        String hostName = null;

        if (isEnvSet("CLEARSCAPE_TOKEN")) {
            userName = TeradataTestConstants.CLEARSCAPE_USERNAME;
            password = requireEnv("CLEARSCAPE_PASSWORD");
        }
        else {
            userName = requireEnv("TERADATA_USERNAME");
            password = requireEnv("TERADATA_PASSWORD");
            hostName = requireEnv("TERADATA_HOSTNAME");
        }

        String databaseName = envName.replace("-", "_");

        AuthenticationConfig authConfig = createAuthConfig(userName, password);
        Map<String, String> jdbcProperties = new HashMap<>();
        jdbcProperties.put("TMODE", "ANSI");
        jdbcProperties.put("CHARSET", "UTF8");

        return DatabaseConfig.builder()
                .hostName(hostName)
                .databaseName(databaseName)
                .useClearScape(hostName == null)
                .authConfig(authConfig)
                .clearScapeEnvName(envName)
                .jdbcProperties(jdbcProperties)
                .build();
    }

    private static AuthenticationConfig createAuthConfig(String username, String password)
    {
        return new AuthenticationConfig(username, password);
    }
}
