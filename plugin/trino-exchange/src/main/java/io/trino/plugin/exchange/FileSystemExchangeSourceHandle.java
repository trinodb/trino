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
import io.airlift.slice.SizeOf;
import io.trino.spi.exchange.ExchangeSourceHandle;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSourceHandle
        implements ExchangeSourceHandle
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FileSystemExchangeSourceHandle.class).instanceSize();

    private final int partitionId;
    private final List<FileStatus> files;
    private final Optional<byte[]> secretKey;

    @JsonCreator
    public FileSystemExchangeSourceHandle(
            @JsonProperty("partitionId") int partitionId,
            @JsonProperty("files") List<FileStatus> files,
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

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(files, FileStatus::getRetainedSizeInBytes)
                + sizeOf(secretKey, SizeOf::sizeOf);
    }

    @JsonProperty
    public List<FileStatus> getFiles()
    {
        return files;
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
        FileSystemExchangeSourceHandle that = (FileSystemExchangeSourceHandle) o;
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
        return Objects.hash(partitionId, files, secretKey);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitionId", partitionId)
                .add("files", files)
                .add("secretKey", secretKey.map(value -> "[REDACTED]"))
                .toString();
    }
}
