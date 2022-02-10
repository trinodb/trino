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
import io.airlift.slice.SizeOf;
import io.trino.spi.exchange.ExchangeSourceHandle;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.util.Objects.requireNonNull;

public class LocalFileSystemExchangeSourceHandle
        implements ExchangeSourceHandle
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LocalFileSystemExchangeSourceHandle.class).instanceSize();

    private final int partitionId;
    private final List<String> files;

    @JsonCreator
    public LocalFileSystemExchangeSourceHandle(@JsonProperty("partitionId") int partitionId, @JsonProperty("files") List<String> files)
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
    public List<String> getFiles()
    {
        return files;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(files, SizeOf::estimatedSizeOf);
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
        LocalFileSystemExchangeSourceHandle that = (LocalFileSystemExchangeSourceHandle) o;
        return partitionId == that.partitionId && Objects.equals(files, that.files);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionId, files);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitionId", partitionId)
                .add("files", files)
                .toString();
    }
}
