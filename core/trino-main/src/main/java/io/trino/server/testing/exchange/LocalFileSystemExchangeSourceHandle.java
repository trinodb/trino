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
package io.trino.server.testing.exchange;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.exchange.ExchangeSourceHandle;

import java.nio.file.Path;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class LocalFileSystemExchangeSourceHandle
        implements ExchangeSourceHandle
{
    private final int partitionId;
    private final List<Path> files;

    @JsonCreator
    public LocalFileSystemExchangeSourceHandle(@JsonProperty("partitionId") int partitionId, @JsonProperty("files") List<Path> files)
    {
        this.partitionId = partitionId;
        this.files = ImmutableList.copyOf(requireNonNull(files, "files is null"));
    }

    @Override
    @JsonProperty
    public int getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    public List<Path> getFiles()
    {
        return files;
    }
}
