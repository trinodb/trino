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
package io.trino.plugin.db2;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import javax.validation.constraints.Min;

public class DB2Config
{
    // this value comes from the official document
    private int varcharMaxLength = 32672;
    // this value is for IAM authentication
    private String apiKey;

    @Min(1)
    public int getVarcharMaxLength()
    {
        return varcharMaxLength;
    }

    @Config("db2.varchar-max-length")
    @ConfigDescription("Max length of varchar type in CREATE TABLE statement")
    public DB2Config setVarcharMaxLength(int varcharMaxLength)
    {
        this.varcharMaxLength = varcharMaxLength;
        return this;
    }

    public String getApiKey()
    {
        return apiKey;
    }

    @Config("db2.iam-api-key")
    @ConfigSecuritySensitive
    @ConfigDescription("API key for IAM authentication")
    public DB2Config setApiKey(String apiKey)
    {
        this.apiKey = apiKey;
        return this;
    }
}
