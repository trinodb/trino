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

import io.trino.plugin.teradata.LogonMechanism;

import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.SystemEnvironmentUtils.isEnvSet;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;

public class DatabaseConfigFactory
{
    private static final String DEFAULT_LOG_MECH = "TD2";

    private DatabaseConfigFactory() {}

    public static DatabaseConfig create(String envName)
    {
        String userName = null;
        String password = null;
        String hostName = null;

        if (!isEnvSet("CLEARSCAPE_TOKEN")) {
            hostName = requireEnv("hostname");
        }

        String logMech = DEFAULT_LOG_MECH;
        if (isEnvSet("logMech")) {
            logMech = requireEnv("logMech");
        }
        if (DEFAULT_LOG_MECH.equals(logMech)) {
            if (isEnvSet("CLEARSCAPE_TOKEN")) {
                userName = TeradataTestConstants.ENV_CLEARSCAPE_USERNAME;
                password = requireEnv("CLEARSCAPE_PASSWORD");
            }
            else {
                userName = requireEnv("username");
                password = requireEnv("password");
            }
        }
        LogonMechanism logonMechanism = LogonMechanism.fromString(logMech);
        String databaseName = envName.replace("-", "_");

        AuthenticationConfig authConfig = createAuthConfig(userName, password);
        return DatabaseConfig.builder()
                .hostName(hostName)
                .databaseName(databaseName)
                .useClearScape(isEnvSet("CLEARSCAPE_TOKEN"))
                .logMech(logonMechanism)
                .authConfig(authConfig)
                .clearScapeEnvName(envName)
                .jdbcProperties(getJdbcProperties())
                .build();
    }

    public static Map<String, String> getJdbcProperties()
    {
        Map<String, String> propsMap = new HashMap<>();
        propsMap.put("TMODE", "ANSI");
        propsMap.put("CHARSET", "UTF8");
        return propsMap;
    }

    private static AuthenticationConfig createAuthConfig(String username, String password)
    {
        return new AuthenticationConfig(username, password);
    }
}
