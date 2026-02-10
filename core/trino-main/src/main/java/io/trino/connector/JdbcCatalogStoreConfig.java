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
package io.trino.connector;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotNull;

public class JdbcCatalogStoreConfig {
    private String url;
    private String user;
    private String password;
    private boolean readOnly;

    @NotNull
    public String getUrl() {
        return url;
    }

    @Config("catalog.jdbc.url")
    @ConfigDescription("JDBC connection URL for the catalog store database")
    public JdbcCatalogStoreConfig setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getUser() {
        return user;
    }

    @Config("catalog.jdbc.user")
    @ConfigDescription("Database username")
    public JdbcCatalogStoreConfig setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    @ConfigSecuritySensitive
    @Config("catalog.jdbc.password")
    @ConfigDescription("Database password")
    public JdbcCatalogStoreConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    @Config("catalog.jdbc.read-only")
    @ConfigDescription("Whether the catalog store is read-only")
    public JdbcCatalogStoreConfig setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
        return this;
    }
}
