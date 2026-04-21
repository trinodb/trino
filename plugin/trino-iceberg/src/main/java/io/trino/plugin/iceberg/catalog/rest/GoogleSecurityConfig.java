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
package io.trino.plugin.iceberg.catalog.rest;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.NotNull;

public class GoogleSecurityConfig
{
    private String projectId;
    private String jsonKeyFilePath;

    @NotNull
    public String getProjectId()
    {
        return projectId;
    }

    @Config("iceberg.rest-catalog.google-project-id")
    @ConfigDescription("Google project ID")
    public GoogleSecurityConfig setProjectId(String projectId)
    {
        this.projectId = projectId;
        return this;
    }

    @NotNull
    @FileExists
    public String getJsonKeyFilePath()
    {
        return jsonKeyFilePath;
    }

    // Duplicated from GcsFileSystemConfig to enforce the property in BigLake metastore
    @Config("gcs.json-key-file-path")
    @ConfigDescription("JSON key file used to access Google Cloud Storage")
    public GoogleSecurityConfig setJsonKeyFilePath(String jsonKeyFilePath)
    {
        this.jsonKeyFilePath = jsonKeyFilePath;
        return this;
    }
}
