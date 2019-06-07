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
package io.prestosql.orc.reader;

import com.google.common.io.Closer;
import io.airlift.slice.Slice;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.StreamDescriptor;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.ZoneId;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static io.prestosql.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static io.prestosql.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static io.prestosql.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static io.prestosql.orc.reader.ReaderUtils.verifyStreamType;
import static io.prestosql.spi.type.Chars.byteCountWithoutTrailingSpace;
import static io.prestosql.spi.type.Chars.isCharType;
import static io.prestosql.spi.type.VarbinaryType.isVarbinaryType;
import static io.prestosql.spi.type.Varchars.byteCount;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.util.Objects.requireNonNull;

public class SliceStreamReader
        implements StreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;
    private final SliceDirectStreamReader directReader;
    private final SliceDictionaryStreamReader dictionaryReader;
    private StreamReader currentReader;

    public SliceStreamReader(Type type, StreamDescriptor streamDescriptor, AggregatedMemoryContext systemMemoryContext)
            throws OrcCorruptionException
    {
        requireNonNull(type, "type is null");
        verifyStreamType(streamDescriptor, type, t -> t instanceof VarcharType || t instanceof CharType || t instanceof VarbinaryType);

        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");

        int maxCodePointCount = getMaxCodePointCount(type);
        boolean charType = isCharType(type);
        directReader = new SliceDirectStreamReader(streamDescriptor, maxCodePointCount, charType);
        dictionaryReader = new SliceDictionaryStreamReader(streamDescriptor, systemMemoryContext.newLocalMemoryContext(SliceStreamReader.class.getSimpleName()), maxCodePointCount, charType);
    }

    @Override
    public Block readBlock()
            throws IOException
    {
        return currentReader.readBlock();
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        currentReader.prepareNextRead(batchSize);
    }

    @Override
    public void startStripe(ZoneId timeZone, InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        ColumnEncodingKind columnEncodingKind = encoding.get(streamDescriptor.getStreamId()).getColumnEncodingKind();
        if (columnEncodingKind == DIRECT || columnEncodingKind == DIRECT_V2) {
            currentReader = directReader;
        }
        else if (columnEncodingKind == DICTIONARY || columnEncodingKind == DICTIONARY_V2) {
            currentReader = dictionaryReader;
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding " + columnEncodingKind);
        }

        currentReader.startStripe(timeZone, dictionaryStreamSources, encoding);
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        currentReader.startRowGroup(dataStreamSources);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    private static int getMaxCodePointCount(Type type)
    {
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            return varcharType.isUnbounded() ? -1 : varcharType.getBoundedLength();
        }
        if (isCharType(type)) {
            return ((CharType) type).getLength();
        }
        if (isVarbinaryType(type)) {
            return -1;
        }
        throw new IllegalArgumentException("Unsupported encoding " + type.getDisplayName());
    }

    public static int computeTruncatedLength(Slice slice, int offset, int length, int maxCodePointCount, boolean isCharType)
    {
        if (isCharType) {
            // truncate the characters and then remove the trailing white spaces
            return byteCountWithoutTrailingSpace(slice, offset, length, maxCodePointCount);
        }
        if (maxCodePointCount >= 0 && length > maxCodePointCount) {
            return byteCount(slice, offset, length, maxCodePointCount);
        }
        return length;
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            closer.register(directReader::close);
            closer.register(dictionaryReader::close);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + directReader.getRetainedSizeInBytes() + dictionaryReader.getRetainedSizeInBytes();
    }
}
