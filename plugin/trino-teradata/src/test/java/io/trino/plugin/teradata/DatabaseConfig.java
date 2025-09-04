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

/**
 * Holds Teradata database connection configuration,
 * typically loaded from environment variables.
 */
public class DatabaseConfig
{
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private String databaseName;

    public DatabaseConfig(String jdbcUrl, String username, String password, String databaseName)
    {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.databaseName = databaseName;
    }

    /**
     * Loads config from environment variables:
     * hostname, user, password
     *
     * @throws IllegalStateException if required env vars are missing.
     */
    public static DatabaseConfig fromEnv()
    {
        String host = System.getenv("hostname");
        String user = System.getenv("user");
        String pass = System.getenv("password");
        String database = System.getenv("database");
        if (database == null || database.isEmpty()) {
            database = "trino";
        }

        if (host == null || user == null || pass == null) {
            throw new IllegalStateException("Environment variables [hostname, user, password] must be set.");
        }

        String jdbcUrl = String.format("jdbc:teradata://%s/TMODE=ANSI,CHARSET=UTF8", host);
        return new DatabaseConfig(jdbcUrl, user, pass, database);
    }

    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public void setDatabaseName(String databaseName)
    {
        this.databaseName = databaseName;
    }
}
