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
import jakarta.validation.constraints.NotNull;
import org.apache.iceberg.aws.AwsProperties;

public class IcebergRestCatalogSigV4Config
{
    private String signingName = AwsProperties.REST_SIGNING_NAME_DEFAULT;

    @NotNull
    public String getSigningName()
    {
        return signingName;
    }

    @Config("iceberg.rest-catalog.signing-name")
    @ConfigDescription("AWS SigV4 signing service name")
    public IcebergRestCatalogSigV4Config setSigningName(String signingName)
    {
        this.signingName = signingName;
        return this;
    }
}
