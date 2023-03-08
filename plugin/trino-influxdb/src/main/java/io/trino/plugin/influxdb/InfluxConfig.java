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

package io.trino.plugin.influxdb;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.AssertFalse;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import java.util.Optional;

import static java.util.concurrent.TimeUnit.SECONDS;

public class InfluxConfig
{
    private String endpoint;
    private String username;
    private String password;
    private Duration connectTimeOut = new Duration(10, SECONDS);
    private Duration readTimeOut = new Duration(60, SECONDS);

    @NotEmpty
    @Pattern(message = "Invalid endpoint. Expected http:// ", regexp = "^http://.*")
    public String getEndpoint()
    {
        return endpoint;
    }

    @Config("influx.endpoint")
    public InfluxConfig setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    @NotNull
    public Optional<String> getUsername()
    {
        return Optional.ofNullable(username);
    }

    @Config("influx.username")
    public InfluxConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @NotNull
    public Optional<String> getPassword()
    {
        return Optional.ofNullable(password);
    }

    @ConfigSecuritySensitive
    @Config("influx.password")
    public InfluxConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @MinDuration("0s")
    public Duration getConnectTimeOut()
    {
        return connectTimeOut;
    }

    @Config("influx.connect-timeout")
    public InfluxConfig setConnectTimeOut(Duration connectTimeOut)
    {
        this.connectTimeOut = connectTimeOut;
        return this;
    }

    @MinDuration("0s")
    public Duration getReadTimeOut()
    {
        return readTimeOut;
    }

    @Config("influx.read-timeout")
    public InfluxConfig setReadTimeOut(Duration readTimeOut)
    {
        this.readTimeOut = readTimeOut;
        return this;
    }

    @AssertFalse(message = "'influx.username' and 'influx.password' must be both empty or both non-empty.")
    public boolean isValidPasswordConfig()
    {
        return (username == null) ^ (password == null);
    }
}
