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

import io.trino.plugin.teradata.util.TeradataTestConstants;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class DatabaseConfigFactory
{
    private static final String DEFAULT_LOG_MECH = "TD2";

    private DatabaseConfigFactory() {}

    public static DatabaseConfig create(String envName)
    {
        String userName = getEnvVar("username", null);
        String password = getEnvVar("password", null);
        String hostName = "";
        if (hasEnvVar("CLEARSCAPE_TOKEN")) {
            userName = TeradataTestConstants.ENV_CLEARSCAPE_USERNAME;
            password = requireEnvVar("CLEARSCAPE_PASSWORD", "ClearScape password is required");
        }
        else {
            hostName = requireEnvVar("hostname", "Hostname required for standard connection");
        }
        AuthenticationConfig authConfig = createAuthConfig(userName, password);
        LogonMechanism logMech = LogonMechanism.fromString(getEnvVar("logMech", DEFAULT_LOG_MECH));
        String databaseName = StringUtils.replace(envName, "-", "_");
        return DatabaseConfig.builder()
                .hostName(hostName)
                .databaseName(databaseName)
                .useClearScape(hasEnvVar("CLEARSCAPE_TOKEN"))
                .logMech(logMech)
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
        return new AuthenticationConfig(username, password,
                getEnvVar("jwt_token", null),
                getEnvVar("jws_private_key", null),
                getEnvVar("jws_cert", null),
                getEnvVar("oidc_clientid", null),
                getEnvVar("client_secret", null));
    }

    private static String getEnvVar(String name, String defaultValue)
    {
        String value = System.getenv(name);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }

    private static String requireEnvVar(String name, String errorMessage)
    {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException(errorMessage + ". Environment variable: " + name);
        }
        return value;
    }

    private static boolean hasEnvVar(String name)
    {
        String value = System.getenv(name);
        return value != null && !value.isEmpty();
    }
}
