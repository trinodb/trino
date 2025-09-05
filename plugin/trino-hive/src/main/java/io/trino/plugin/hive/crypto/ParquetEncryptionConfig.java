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
package io.trino.plugin.hive.crypto;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.util.Optional;

public class ParquetEncryptionConfig
{
    private boolean environmentKeyRetrieverEnabled;
    private Optional<String> aadPrefix = Optional.empty();
    private boolean checkFooterIntegrity = true;

    @Config("pme.environment-key-retriever.enabled")
    @ConfigDescription("Enable the key retriever that retrieves keys from the environment variable")
    public ParquetEncryptionConfig setEnvironmentKeyRetrieverEnabled(boolean enabled)
    {
        this.environmentKeyRetrieverEnabled = enabled;
        return this;
    }

    public boolean isEnvironmentKeyRetrieverEnabled()
    {
        return environmentKeyRetrieverEnabled;
    }

    @Config("pme.aad-prefix")
    @ConfigDescription("AAD prefix used to decode Parquet files")
    public ParquetEncryptionConfig setAadPrefix(String prefix)
    {
        this.aadPrefix = Optional.ofNullable(prefix);
        return this;
    }

    public Optional<String> getAadPrefix()
    {
        return aadPrefix;
    }

    @Config("pme.check-footer-integrity")
    @ConfigDescription("Validate signature for plaintext footer files")
    public ParquetEncryptionConfig setCheckFooterIntegrity(boolean checkFooterIntegrity)
    {
        this.checkFooterIntegrity = checkFooterIntegrity;
        return this;
    }

    public boolean isCheckFooterIntegrity()
    {
        return checkFooterIntegrity;
    }
}
