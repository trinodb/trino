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
package io.trino.plugin.databricks;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;

import java.util.Optional;

public class DatabricksConfig
{
    private String httpPath;
    private String catalog;
    private String schema;

    public Optional<String> getHttpPath()
    {
        return Optional.ofNullable(httpPath);
    }

    @Config("databricks.http-path")
    public DatabricksConfig setHttpPath(String httpPath)
    {
        this.httpPath = httpPath;
        return this;
    }

    public Optional<String> getCatalog()
    {
        return Optional.ofNullable(catalog);
    }

    @Config("databricks.catalog")
    public DatabricksConfig setCatalog(String catalog)
    {
        this.catalog = catalog;
        return this;
    }

    public Optional<String> getSchema()
    {
        return Optional.ofNullable(schema);
    }

    @Config("databricks.schema")
    @ConfigSecuritySensitive
    public DatabricksConfig setSchema(String schema)
    {
        this.schema = schema;
        return this;
    }
}
