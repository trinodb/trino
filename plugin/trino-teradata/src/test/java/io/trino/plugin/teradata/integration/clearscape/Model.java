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
package io.trino.plugin.teradata.integration.clearscape;

import static java.util.Objects.requireNonNull;

public class Model
{
    final String envName;
    final String userName;
    final String password;
    final String databaseName;
    final String token;
    final String region;
    String hostName;

    public Model(
            String envName,
            String hostName,
            String userName,
            String password,
            String databaseName,
            String token,
            String region)
    {
        this.envName = requireNonNull(envName, "envName is null");
        this.userName = requireNonNull(userName, "userName is null");
        this.password = requireNonNull(password, "password is null");
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.token = requireNonNull(token, "token is null");
        this.region = requireNonNull(region, "region is null");
        this.hostName = hostName;
    }

    public String getEnvName()
    {
        return envName;
    }

    public String getHostName()
    {
        return hostName;
    }

    public void setHostName(String hostName)
    {
        this.hostName = hostName;
    }

    public String getPassword()
    {
        return password;
    }

    public String getToken()
    {
        return token;
    }

    public String getRegion()
    {
        return region;
    }
}
