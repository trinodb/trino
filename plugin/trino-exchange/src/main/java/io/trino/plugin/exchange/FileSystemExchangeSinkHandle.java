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
package io.trino.plugin.exchange;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.trino.spi.exchange.ExchangeSinkHandle;

import javax.crypto.SecretKey;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSinkHandle
        implements ExchangeSinkHandle
{
    private final int partitionId;
    private final Optional<SecretKey> secretKey;

    @JsonCreator
    public FileSystemExchangeSinkHandle(
            @JsonProperty("partitionId") int partitionId,
            @JsonProperty("secretKey") Optional<SecretKey> secretKey)
    {
        this.partitionId = partitionId;
        this.secretKey = requireNonNull(secretKey, "secretKey is null");
    }

    @JsonProperty
    public int getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    @JsonSerialize(contentUsing = SecretKeySerializer.class)
    @JsonDeserialize(contentUsing = SecretKeyDeserializer.class)
    public Optional<SecretKey> getSecretKey()
    {
        return secretKey;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitionId", partitionId)
                .add("secretKey", secretKey.map(value -> "[REDACTED]"))
                .toString();
    }
}
