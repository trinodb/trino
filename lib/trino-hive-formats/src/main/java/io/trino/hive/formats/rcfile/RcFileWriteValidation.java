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
package io.trino.hive.formats.rcfile;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RcFileWriteValidation
{
    private final byte version;
    private final Map<String, String> metadata;
    private final Optional<String> codecClassName;
    private final long syncFirst;
    private final long syncSecond;
    private final WriteChecksum checksum;

    private RcFileWriteValidation(byte version, Map<String, String> metadata, Optional<String> codecClassName, long syncFirst, long syncSecond, WriteChecksum checksum)
    {
        this.version = version;
        this.metadata = ImmutableMap.copyOf(requireNonNull(metadata, "metadata is null"));
        this.codecClassName = requireNonNull(codecClassName, "codecClassName is null");
        this.syncFirst = syncFirst;
        this.syncSecond = syncSecond;
        this.checksum = requireNonNull(checksum, "checksum is null");
    }

    public byte getVersion()
    {
        return version;
    }

    public Map<String, String> getMetadata()
    {
        return metadata;
    }

    public Optional<String> getCodecClassName()
    {
        return codecClassName;
    }

    public long getSyncFirst()
    {
        return syncFirst;
    }

    public long getSyncSecond()
    {
        return syncSecond;
    }

    public WriteChecksum getChecksum()
    {
        return checksum;
    }

    public static class WriteChecksum
    {
        private final long totalRowCount;
        private final long rowGroupHash;
        private final List<Long> columnHashes;

        public WriteChecksum(long totalRowCount, long rowGroupHash, List<Long> columnHashes)
        {
            this.totalRowCount = totalRowCount;
            this.rowGroupHash = rowGroupHash;
            this.columnHashes = columnHashes;
        }

        public long getTotalRowCount()
        {
            return totalRowCount;
        }

        public long getRowGroupHash()
        {
            return rowGroupHash;
        }

        public List<Long> getColumnHashes()
        {
            return columnHashes;
        }
    }

    public static class WriteChecksumBuilder
    {
        private final List<ValidationHash> validationHashes;
        private long totalRowCount;
        private final List<XxHash64> columnHashes;
        private final XxHash64 rowGroupHash = new XxHash64();

        private final byte[] longBuffer = new byte[Long.BYTES];
        private final Slice longSlice = Slices.wrappedBuffer(longBuffer);

        private WriteChecksumBuilder(List<Type> types)
        {
            this.validationHashes = types.stream()
                    .map(ValidationHash::createValidationHash)
                    .collect(toImmutableList());

            ImmutableList.Builder<XxHash64> columnHashes = ImmutableList.builder();
            for (Type ignored : types) {
                columnHashes.add(new XxHash64());
            }
            this.columnHashes = columnHashes.build();
        }

        public static WriteChecksumBuilder createWriteChecksumBuilder(Map<Integer, Type> readColumns)
        {
            requireNonNull(readColumns, "readColumns is null");
            checkArgument(!readColumns.isEmpty(), "readColumns is empty");
            int columnCount = readColumns.keySet().stream()
                    .mapToInt(Integer::intValue)
                    .max().getAsInt() + 1;
            checkArgument(readColumns.size() == columnCount, "checksum requires all columns to be read");

            ImmutableList.Builder<Type> types = ImmutableList.builder();
            for (int column = 0; column < columnCount; column++) {
                Type type = readColumns.get(column);
                checkArgument(type != null, "checksum requires all columns to be read");
                types.add(type);
            }
            return new WriteChecksumBuilder(types.build());
        }

        public void addRowGroup(int rowCount)
        {
            longSlice.setInt(0, rowCount);
            rowGroupHash.update(longBuffer, 0, Integer.BYTES);
        }

        public void addPage(Page page)
        {
            requireNonNull(page, "page is null");
            checkArgument(page.getChannelCount() == columnHashes.size(), "invalid page");

            totalRowCount += page.getPositionCount();
            for (int channel = 0; channel < columnHashes.size(); channel++) {
                ValidationHash validationHash = validationHashes.get(channel);
                Block block = page.getBlock(channel);
                XxHash64 xxHash64 = columnHashes.get(channel);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    long hash = validationHash.hash(block, position);
                    longSlice.setLong(0, hash);
                    xxHash64.update(longBuffer);
                }
            }
        }

        public WriteChecksum build()
        {
            return new WriteChecksum(
                    totalRowCount,
                    rowGroupHash.hash(),
                    columnHashes.stream()
                            .map(XxHash64::hash)
                            .collect(toList()));
        }
    }

    public static class RcFileWriteValidationBuilder
    {
        private byte version;
        private final Map<String, String> metadata = new HashMap<>();
        private Optional<String> codecClassName;
        private long syncFirst;
        private long syncSecond;
        private final WriteChecksumBuilder checksum;

        public RcFileWriteValidationBuilder(List<Type> types)
        {
            this.checksum = new WriteChecksumBuilder(types);
        }

        public void setVersion(byte version)
        {
            this.version = version;
        }

        public void addMetadataProperty(String key, String value)
        {
            metadata.put(key, value);
        }

        public void setCodecClassName(Optional<String> codecClassName)
        {
            this.codecClassName = codecClassName;
        }

        public void setSyncFirst(long syncFirst)
        {
            this.syncFirst = syncFirst;
        }

        public void setSyncSecond(long syncSecond)
        {
            this.syncSecond = syncSecond;
        }

        public void addRowGroup(int rowCount)
        {
            checksum.addRowGroup(rowCount);
        }

        public void addPage(Page page)
        {
            checksum.addPage(page);
        }

        public RcFileWriteValidation build()
        {
            return new RcFileWriteValidation(version, metadata, codecClassName, syncFirst, syncSecond, checksum.build());
        }
    }
}
