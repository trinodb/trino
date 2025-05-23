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
package io.trino.parquet.crypto;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FileDecryptionProperties
{
    private final DecryptionKeyRetriever keyRetriever;
    private final Optional<byte[]> aadPrefix;
    private final boolean checkFooterIntegrity;

    private FileDecryptionProperties(DecryptionKeyRetriever keyRetriever, Optional<byte[]> aadPrefix, boolean checkFooterIntegrity)
    {
        this.keyRetriever = requireNonNull(keyRetriever, "keyRetriever is null");
        this.aadPrefix = requireNonNull(aadPrefix, "aadPrefix is null");
        this.checkFooterIntegrity = checkFooterIntegrity;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public DecryptionKeyRetriever getKeyRetriever()
    {
        return keyRetriever;
    }

    public Optional<byte[]> getAadPrefix()
    {
        return aadPrefix;
    }

    public boolean isCheckFooterIntegrity()
    {
        return checkFooterIntegrity;
    }

    public static class Builder
    {
        private DecryptionKeyRetriever keyRetriever;
        private Optional<byte[]> aadPrefix = Optional.empty();
        private boolean checkFooterIntegrity = true;

        public Builder withKeyRetriever(DecryptionKeyRetriever keyRetriever)
        {
            this.keyRetriever = requireNonNull(keyRetriever, "keyRetriever is null");
            return this;
        }

        public Builder withAadPrefix(byte[] aadPrefix)
        {
            this.aadPrefix = Optional.of(requireNonNull(aadPrefix, "aadPrefix is null"));
            return this;
        }

        public Builder withCheckFooterIntegrity(boolean checkFooterIntegrity)
        {
            this.checkFooterIntegrity = checkFooterIntegrity;
            return this;
        }

        public FileDecryptionProperties build()
        {
            return new FileDecryptionProperties(keyRetriever, aadPrefix, checkFooterIntegrity);
        }
    }
}
