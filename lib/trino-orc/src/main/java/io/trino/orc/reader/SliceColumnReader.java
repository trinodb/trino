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
package io.trino.orc.reader;

import com.google.common.io.Closer;
import io.airlift.slice.Slice;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.metadata.ColumnEncoding;
import io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.stream.InputStreamSources;
import io.trino.spi.block.Block;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.ZoneId;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static io.trino.orc.reader.ReaderUtils.verifyStreamType;
import static io.trino.spi.type.Chars.byteCountWithoutTrailingSpace;
import static io.trino.spi.type.Varchars.byteCount;
import static java.util.Objects.requireNonNull;

public class SliceColumnReader
        implements ColumnReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceColumnReader.class).instanceSize();

    private final OrcColumn column;
    private final SliceDirectColumnReader directReader;
    private final SliceDictionaryColumnReader dictionaryReader;
    private ColumnReader currentReader;

    public SliceColumnReader(Type type, OrcColumn column, AggregatedMemoryContext memoryContext)
            throws OrcCorruptionException
    {
        requireNonNull(type, "type is null");
        verifyStreamType(column, type, t -> t instanceof VarcharType || t instanceof CharType || t instanceof VarbinaryType);

        this.column = requireNonNull(column, "column is null");

        int maxCodePointCount = getMaxCodePointCount(type);
        boolean charType = type instanceof CharType;
        directReader = new SliceDirectColumnReader(column, maxCodePointCount, charType);
        dictionaryReader = new SliceDictionaryColumnReader(column, memoryContext.newLocalMemoryContext(SliceColumnReader.class.getSimpleName()), maxCodePointCount, charType);
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
    public void startStripe(ZoneId fileTimeZone, InputStreamSources dictionaryStreamSources, ColumnMetadata<ColumnEncoding> encoding)
            throws IOException
    {
        ColumnEncodingKind columnEncodingKind = encoding.get(column.getColumnId()).getColumnEncodingKind();
        if (columnEncodingKind == DIRECT || columnEncodingKind == DIRECT_V2) {
            currentReader = directReader;
        }
        else if (columnEncodingKind == DICTIONARY || columnEncodingKind == DICTIONARY_V2) {
            currentReader = dictionaryReader;
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding " + columnEncodingKind);
        }

        currentReader.startStripe(fileTimeZone, dictionaryStreamSources, encoding);
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
                .addValue(column)
                .toString();
    }

    private static int getMaxCodePointCount(Type type)
    {
        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            return varcharType.isUnbounded() ? -1 : varcharType.getBoundedLength();
        }
        if (type instanceof CharType) {
            return ((CharType) type).getLength();
        }
        if (type instanceof VarbinaryType) {
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
