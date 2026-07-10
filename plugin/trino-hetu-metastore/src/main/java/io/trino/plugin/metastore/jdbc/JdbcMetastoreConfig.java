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
package io.trino.plugin.metastore.jdbc;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;

import jakarta.validation.constraints.NotNull;

/**
 * jdbc metastore config
 *
 * @since 2020-02-27
 */
public class JdbcMetastoreConfig
{
    private String dbUrl;
    private String dbUser;
    private String dbPassword;

    @NotNull
    public String getDbUrl()
    {
        return dbUrl;
    }

    /**
     * db url
     *
     * @param dbUrl db url
     * @return JdbcMetastoreConfig
     */
    @Config("trino.metastore.db.url")
    public JdbcMetastoreConfig setDbUrl(String dbUrl)
    {
        this.dbUrl = dbUrl;
        return this;
    }

    @NotNull
    public String getDbUser()
    {
        return dbUser;
    }

    /**
     * connection user
     *
     * @param dbUser db user
     * @return JdbcMetastoreConfig
     */
    @Config("trino.metastore.db.user")
    public JdbcMetastoreConfig setDbUser(String dbUser)
    {
        this.dbUser = dbUser;
        return this;
    }

    @NotNull
    public String getDbPassword()
    {
        return dbPassword;
    }

    /**
     * db password
     *
     * @param dbPassword db password
     * @return JdbcMetastoreConfig
     */
    @Config("trino.metastore.db.password")
    @ConfigSecuritySensitive
    public JdbcMetastoreConfig setDbPassword(String dbPassword)
    {
        this.dbPassword = dbPassword;
        return this;
    }
}
