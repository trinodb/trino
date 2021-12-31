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
import com.google.common.collect.ImmutableList;
import io.trino.spi.exchange.ExchangeSourceHandle;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSourceHandle
        implements ExchangeSourceHandle
{
    private final int partitionId;
    private final List<URI> files;
    private final Optional<byte[]> secretKey;

    @JsonCreator
    public FileSystemExchangeSourceHandle(
            @JsonProperty("partitionId") int partitionId,
            @JsonProperty("files") List<URI> files,
            @JsonProperty("secretKey") Optional<byte[]> secretKey)
    {
        this.partitionId = partitionId;
        this.files = ImmutableList.copyOf(requireNonNull(files, "files is null"));
        this.secretKey = requireNonNull(secretKey, "secretKey is null");
    }

    @Override
    @JsonProperty
    public int getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    public List<URI> getFiles()
    {
        return files;
    }

    @JsonProperty
    public Optional<byte[]> getSecretKey()
    {
        return secretKey;
    }
}
