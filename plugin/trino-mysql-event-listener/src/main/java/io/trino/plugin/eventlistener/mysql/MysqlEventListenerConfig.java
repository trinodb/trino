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

    @AssertTrue(message = "Invalid JDBC URL for MySQL event listener")
    public boolean isValidUrl()
    {
        try {
            String url = getUrl();
            // Check for user property
            if (getUser().isPresent() && url != null && url.matches(".*[?&]user=.+")) {
                throw new RuntimeException("'user' property is set, but JDBC URL also contains 'user' in the query string. Please specify the user only once.");
            }
            // Check for password property
            if (getPassword().isPresent() && url != null && url.matches(".*[?&]password=.+")) {
                throw new RuntimeException("'password' property is set, but JDBC URL also contains 'password' in the query string. Please specify the password only once.");
            }
            return new Driver().acceptsURL(url);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
