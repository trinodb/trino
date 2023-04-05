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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.LegacyConfig;

import javax.validation.constraints.NotNull;

import java.util.List;

public class JdbcCatalogStoreConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private String configUrl;
    private String configUser;
    private String configPassword;
    private List<String> disabledCatalogs;
    private boolean readOnly;

    @NotNull
    public String getCatalogConfigDbUrl()
    {
        return configUrl;
    }

    public String getCatalogConfigDbUser()
    {
        return configUser;
    }

    public String getCatalogConfigDbPassword()
    {
        return configPassword;
    }

    @LegacyConfig("plugin.config-dir")
    @Config("catalog.config-db-url")
    public JdbcCatalogStoreConfig setCatalogConfigDbUrl(String configUrl)
    {
        this.configUrl = configUrl;
        return this;
    }

    @Config("catalog.config-db-user")
    public JdbcCatalogStoreConfig setCatalogConfigDbUser(String configUser)
    {
        this.configUser = configUser;
        return this;
    }

    @Config("catalog.config-db-password")
    public JdbcCatalogStoreConfig setCatalogConfigDbPassword(String configPassword)
    {
        this.configPassword = configPassword;
        return this;
    }

    public List<String> getDisabledCatalogs()
    {
        return disabledCatalogs;
    }

    @Config("catalog.disabled-catalogs")
    public JdbcCatalogStoreConfig setDisabledCatalogs(String catalogs)
    {
        this.disabledCatalogs = (catalogs == null) ? null : SPLITTER.splitToList(catalogs);
        return this;
    }

    public JdbcCatalogStoreConfig setDisabledCatalogs(List<String> catalogs)
    {
        this.disabledCatalogs = (catalogs == null) ? null : ImmutableList.copyOf(catalogs);
        return this;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    @Config("catalog.read-only")
    public JdbcCatalogStoreConfig setReadOnly(boolean readOnly)
    {
        this.readOnly = readOnly;
        return this;
    }
}
