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
package io.trino.metastore.lakehouse;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class LakehouseMetastoreConfig
{
    private String projectId;
    private String catalogId;
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
