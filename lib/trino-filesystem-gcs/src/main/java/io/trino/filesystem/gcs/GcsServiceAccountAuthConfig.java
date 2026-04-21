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
package io.trino.filesystem.gcs;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.AssertTrue;

public class GcsServiceAccountAuthConfig
{
    private String jsonKey;
    private String jsonKeyFilePath;

    @Nullable
    public String getJsonKey()
    {
        return jsonKey;
    }

    @Config("gcs.json-key")
    @ConfigSecuritySensitive
    public GcsServiceAccountAuthConfig setJsonKey(String jsonKey)
    {
        this.jsonKey = jsonKey;
        return this;
    }

    @Nullable
    @FileExists
    public String getJsonKeyFilePath()
    {
        return jsonKeyFilePath;
    }

    @Config("gcs.json-key-file-path")
    @ConfigDescription("JSON key file used to access Google Cloud Storage")
    public GcsServiceAccountAuthConfig setJsonKeyFilePath(String jsonKeyFilePath)
    {
        this.jsonKeyFilePath = jsonKeyFilePath;
        return this;
    }

    @AssertTrue(message = "Either gcs.json-key or gcs.json-key-file-path must be set")
    public boolean isAuthMethodValid()
    {
        return (jsonKey != null) ^ (jsonKeyFilePath != null);
    }
}
