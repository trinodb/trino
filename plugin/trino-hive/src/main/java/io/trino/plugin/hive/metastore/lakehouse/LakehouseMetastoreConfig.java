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
package io.trino.plugin.hive.metastore.lakehouse;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class LakehouseMetastoreConfig
{
    private String projectId;
    private String catalogId;
    private String jsonKey;
    private String jsonKeyFilePath;
    private Duration requestTimeout = new Duration(1, TimeUnit.MINUTES);

    @NotNull
    public String getProjectId()
    {
        return projectId;
    }

    @Config("hive.metastore.lakehouse.project-id")
    @ConfigDescription("Google Cloud project that owns the Lakehouse catalog")
    public LakehouseMetastoreConfig setProjectId(String projectId)
    {
        this.projectId = projectId;
        return this;
    }

    @NotNull
    public String getCatalogId()
    {
        return catalogId;
    }

    @Config("hive.metastore.lakehouse.catalog-id")
    @ConfigDescription("Lakehouse Hive catalog to expose as this Hive metastore")
    public LakehouseMetastoreConfig setCatalogId(String catalogId)
    {
        this.catalogId = catalogId;
        return this;
    }

    @NotNull
    public Optional<String> getJsonKey()
    {
        return Optional.ofNullable(jsonKey);
    }

    @Config("hive.metastore.lakehouse.google-json-key")
    @ConfigSecuritySensitive
    @ConfigDescription("Google Cloud service account key in JSON format, used to authenticate with the Lakehouse metastore")
    public LakehouseMetastoreConfig setJsonKey(String jsonKey)
    {
        this.jsonKey = jsonKey;
        return this;
    }

    public Optional<@FileExists String> getJsonKeyFilePath()
    {
        return Optional.ofNullable(jsonKeyFilePath);
    }

    @Config("hive.metastore.lakehouse.google-json-key-file-path")
    @ConfigDescription("Path to a Google Cloud service account key file in JSON format, used to authenticate with the Lakehouse metastore")
    public LakehouseMetastoreConfig setJsonKeyFilePath(String jsonKeyFilePath)
    {
        this.jsonKeyFilePath = jsonKeyFilePath;
        return this;
    }

    // When neither is set, Application Default Credentials (ADC) are used
    @AssertTrue(message = "hive.metastore.lakehouse.google-json-key and hive.metastore.lakehouse.google-json-key-file-path cannot be set at the same time")
    public boolean isAuthMethodValid()
    {
        return !(jsonKey != null && jsonKeyFilePath != null);
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("hive.metastore.lakehouse.request-timeout")
    @ConfigDescription("Timeout of a single Lakehouse request")
    public LakehouseMetastoreConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }
}
