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
package io.trino.hdfs.gcs;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import jakarta.annotation.Nullable;

import static com.google.common.base.Preconditions.checkState;

public class HiveGcsConfig
{
    private boolean useGcsAccessToken;
    private String jsonKey;
    private String jsonKeyFilePath;

    public boolean isUseGcsAccessToken()
    {
        return useGcsAccessToken;
    }

    @Config("hive.gcs.use-access-token")
    @ConfigDescription("Use client-provided OAuth token to access Google Cloud Storage")
    public HiveGcsConfig setUseGcsAccessToken(boolean useGcsAccessToken)
    {
        this.useGcsAccessToken = useGcsAccessToken;
        return this;
    }

    @Nullable
    public String getJsonKey()
    {
        return jsonKey;
    }

    @Config("hive.gcs.json-key")
    @ConfigSecuritySensitive
    public HiveGcsConfig setJsonKey(String jsonKey)
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

    @Config("hive.gcs.json-key-file-path")
    @ConfigDescription("JSON key file used to access Google Cloud Storage")
    public HiveGcsConfig setJsonKeyFilePath(String jsonKeyFilePath)
    {
        this.jsonKeyFilePath = jsonKeyFilePath;
        return this;
    }

    public void validate()
    {
        // This cannot be normal validation, as it would make it impossible to write TestHiveGcsConfig.testExplicitPropertyMappings

        if (useGcsAccessToken) {
            checkState(jsonKey == null, "Cannot specify 'hive.gcs.json-key' when 'hive.gcs.use-access-token' is set");
            checkState(jsonKeyFilePath == null, "Cannot specify 'hive.gcs.json-key-file-path' when 'hive.gcs.use-access-token' is set");
        }
        checkState(jsonKey == null || jsonKeyFilePath == null, "'hive.gcs.json-key' and 'hive.gcs.json-key-file-path' cannot be both set");
    }
}
