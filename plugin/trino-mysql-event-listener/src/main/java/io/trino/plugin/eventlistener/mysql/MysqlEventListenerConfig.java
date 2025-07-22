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
package io.trino.plugin.eventlistener.mysql;

import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.jdbc.Driver;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.sql.SQLException;
import java.util.Optional;

public class MysqlEventListenerConfig
{
    private String url;
    private Optional<String> user = Optional.empty();
    private Optional<String> password = Optional.empty();

    @NotNull
    public String getUrl()
    {
        return url;
    }

    @ConfigSecuritySensitive
    @Config("mysql-event-listener.db.url")
    public MysqlEventListenerConfig setUrl(String url)
    {
        this.url = url;
        return this;
    }

    public Optional<String> getUser()
    {
        return user;
    }

    @ConfigDescription("MySQL connection user")
    @Config("mysql-event-listener.db.user")
    public MysqlEventListenerConfig setUser(String user)
    {
        this.user = Optional.ofNullable(user);
        return this;
    }

    public Optional<String> getPassword()
    {
        return password;
    }

    @ConfigSecuritySensitive
    @ConfigDescription("MySQL connection password")
    @Config("mysql-event-listener.db.password")
    public MysqlEventListenerConfig setPassword(String password)
    {
        this.password = Optional.ofNullable(password);
        return this;
    }

    @AssertTrue(message = "'user' property is set, but JDBC URL also contains 'user'. Please specify the user only once.")
    public boolean isUserNotRedundant()
    {
        return !getUser().isPresent() || !getConnectionUrl().getProperties().containsKey("user");
    }

    @AssertTrue(message = "'password' property is set, but JDBC URL also contains 'password'. Please specify the password only once.")
    public boolean isPasswordNotRedundant()
    {
        return !getPassword().isPresent() || !getConnectionUrl().getProperties().containsKey("password");
    }

    @AssertTrue(message = "'user' property must be set when 'password' property is set")
    public boolean isUserPresentWithPassword()
    {
        return !getPassword().isPresent() || getUser().isPresent();
    }

    @AssertTrue(message = "Invalid JDBC URL for MySQL event listener")
    public boolean isValidUrl()
    {
        try {
            return new Driver().acceptsURL(getUrl());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private ConnectionUrlParser getConnectionUrl()
    {
        return ConnectionUrlParser.parseConnectionString(url);
    }
}
