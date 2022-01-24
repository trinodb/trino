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
import io.trino.spi.exchange.ExchangeSinkHandle;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSinkHandle
        implements ExchangeSinkHandle
{
    private final int partitionId;
    private final Optional<byte[]> secretKey;

    @JsonCreator
    public FileSystemExchangeSinkHandle(
            @JsonProperty("partitionId") int partitionId,
            @JsonProperty("secretKey") Optional<byte[]> secretKey)
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
    public Optional<byte[]> getSecretKey()
    {
        return secretKey;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileSystemExchangeSinkHandle that = (FileSystemExchangeSinkHandle) o;
        if (secretKey.isPresent() && that.secretKey.isPresent()) {
            return partitionId == that.getPartitionId() && Arrays.equals(secretKey.get(), that.secretKey.get());
        }
        else {
            return partitionId == that.getPartitionId() && secretKey.isEmpty() && that.secretKey.isEmpty();
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionId, secretKey);
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
