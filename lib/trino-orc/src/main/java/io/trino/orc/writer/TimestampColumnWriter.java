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
package io.trino.orc.writer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.orc.checkpoint.BooleanStreamCheckpoint;
import io.trino.orc.checkpoint.LongStreamCheckpoint;
import io.trino.orc.metadata.ColumnEncoding;
import io.trino.orc.metadata.CompressedMetadataWriter;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.RowGroupIndex;
import io.trino.orc.metadata.Stream;
import io.trino.orc.metadata.Stream.StreamKind;
import io.trino.orc.metadata.statistics.ColumnStatistics;
import io.trino.orc.metadata.statistics.LongValueStatisticsBuilder;
import io.trino.orc.metadata.statistics.TimestampStatisticsBuilder;
import io.trino.orc.stream.LongOutputStream;
import io.trino.orc.stream.LongOutputStreamV2;
import io.trino.orc.stream.PresentOutputStream;
import io.trino.orc.stream.StreamDataOutput;
import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.orc.metadata.Stream.StreamKind.DATA;
import static io.trino.orc.metadata.Stream.StreamKind.SECONDARY;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

// The ORC encoding erroneously uses normal integer division to compute seconds,
// rather than floor modulus, which produces the wrong result for negative values
// (those that are before the epoch). Readers must correct for this. It also makes
// it impossible to represent values less than one second before the epoch, which
// must also be handled in OrcWriteValidation.
//
// The sub-second value (nanoseconds) typically has a large number of trailing zeroes,
// as many systems only record millisecond or microsecond precision. To optimize storage,
// if the value has at least two trailing zeros, the trailing decimal zero digits are
// removed, and the last three bits record how many zeros were removed, minus one:
//
//   # Trailing 0s   Last 3 Bits   Example nanos       Example encoding
//         0            0b000        123456789     (123456789 << 3) | 0b000
//         1            0b000        123456780     (123456780 << 3) | 0b000
//         2            0b001        123456700       (1234567 << 3) | 0b001
//         3            0b010        123456000        (123456 << 3) | 0b010
//         4            0b011        123450000         (12345 << 3) | 0b011
//         5            0b100        123400000          (1234 << 3) | 0b100
//         6            0b101        123000000           (123 << 3) | 0b101
//         7            0b110        120000000            (12 << 3) | 0b110
//         8            0b111        100000000             (1 << 3) | 0b111

public class TimestampColumnWriter
        implements ColumnWriter
{
    private enum TimestampKind
    {
        TIMESTAMP_MILLIS,
        TIMESTAMP_MICROS,
        TIMESTAMP_NANOS,
        INSTANT_MILLIS,
        INSTANT_MICROS,
        INSTANT_NANOS,
    }

    private static final int INSTANCE_SIZE = instanceSize(TimestampColumnWriter.class);

    private static final long ORC_EPOCH_IN_SECONDS = OffsetDateTime.of(2015, 1, 1, 0, 0, 0, 0, UTC).toEpochSecond();

    private final OrcColumnId columnId;
    private final Type type;
    private final TimestampKind timestampKind;
    private final boolean compressed;
    private final ColumnEncoding columnEncoding;
    private final LongOutputStream secondsStream;
    private final LongOutputStream nanosStream;
    private final PresentOutputStream presentStream;

    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();

    private final Supplier<TimestampStatisticsBuilder> statisticsBuilderSupplier;
    private LongValueStatisticsBuilder statisticsBuilder;

    private boolean closed;

    public TimestampColumnWriter(OrcColumnId columnId, Type type, CompressionKind compression, int bufferSize, Supplier<TimestampStatisticsBuilder> statisticsBuilderSupplier)
    {
        this.columnId = requireNonNull(columnId, "columnId is null");
        this.type = requireNonNull(type, "type is null");
        this.timestampKind = timestampKindForType(type);
        this.compressed = requireNonNull(compression, "compression is null") != NONE;
        this.columnEncoding = new ColumnEncoding(DIRECT_V2, 0);
        this.secondsStream = new LongOutputStreamV2(compression, bufferSize, true, DATA);
        this.nanosStream = new LongOutputStreamV2(compression, bufferSize, false, SECONDARY);
        this.presentStream = new PresentOutputStream(compression, bufferSize);
        this.statisticsBuilderSupplier = requireNonNull(statisticsBuilderSupplier, "statisticsBuilderSupplier is null");
        this.statisticsBuilder = statisticsBuilderSupplier.get();
    }

    private static TimestampKind timestampKindForType(Type type)
    {
        if (type.equals(TIMESTAMP_MILLIS)) {
            return TimestampKind.TIMESTAMP_MILLIS;
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            return TimestampKind.TIMESTAMP_MICROS;
        }
        if (type.equals(TIMESTAMP_NANOS)) {
            return TimestampKind.TIMESTAMP_NANOS;
        }
        if (type.equals(TIMESTAMP_TZ_MILLIS)) {
            return TimestampKind.INSTANT_MILLIS;
        }
        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            return TimestampKind.INSTANT_MICROS;
        }
        if (type.equals(TIMESTAMP_TZ_NANOS)) {
            return TimestampKind.INSTANT_NANOS;
        }
        throw new IllegalArgumentException("Unsupported type for ORC timestamp writer: " + type);
    }

    @Override
    public Map<OrcColumnId, ColumnEncoding> getColumnEncodings()
    {
        return ImmutableMap.of(columnId, columnEncoding);
    }

    @Override
    public void beginRowGroup()
    {
        presentStream.recordCheckpoint();
        secondsStream.recordCheckpoint();
        nanosStream.recordCheckpoint();
    }

    @Override
    public void writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        // record nulls
        for (int position = 0; position < block.getPositionCount(); position++) {
            presentStream.writeBoolean(!block.isNull(position));
        }

        // record values
        switch (timestampKind) {
            case TIMESTAMP_MILLIS:
            case TIMESTAMP_MICROS:
                writeTimestampMicros(block);
                break;
            case TIMESTAMP_NANOS:
                writeTimestampNanos(block);
                break;
            case INSTANT_MILLIS:
                writeInstantShort(block);
                break;
            case INSTANT_MICROS:
            case INSTANT_NANOS:
                writeInstantLong(block);
                break;
        }
    }

    @Override
    public Map<OrcColumnId, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);
        ColumnStatistics statistics = statisticsBuilder.buildColumnStatistics();
        rowGroupColumnStatistics.add(statistics);
        statisticsBuilder = statisticsBuilderSupplier.get();
        return ImmutableMap.of(columnId, statistics);
    }

    @Override
    public void close()
    {
        closed = true;
        secondsStream.close();
        nanosStream.close();
        presentStream.close();
    }

    @Override
    public Map<OrcColumnId, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        return ImmutableMap.of(columnId, ColumnStatistics.mergeColumnStatistics(rowGroupColumnStatistics));
    }

    @Override
    public List<StreamDataOutput> getIndexStreams(CompressedMetadataWriter metadataWriter)
            throws IOException
    {
        checkState(closed);

        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();

        List<LongStreamCheckpoint> secondsCheckpoints = secondsStream.getCheckpoints();
        List<LongStreamCheckpoint> nanosCheckpoints = nanosStream.getCheckpoints();
        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroupColumnStatistics.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroupColumnStatistics.get(groupId);
            LongStreamCheckpoint secondsCheckpoint = secondsCheckpoints.get(groupId);
            LongStreamCheckpoint nanosCheckpoint = nanosCheckpoints.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createTimestampColumnPositionList(compressed, secondsCheckpoint, nanosCheckpoint, presentCheckpoint);
            rowGroupIndexes.add(new RowGroupIndex(positions, columnStatistics));
        }

        Slice slice = metadataWriter.writeRowIndexes(rowGroupIndexes.build());
        Stream stream = new Stream(columnId, StreamKind.ROW_INDEX, slice.length(), false);
        return ImmutableList.of(new StreamDataOutput(slice, stream));
    }

    private static List<Integer> createTimestampColumnPositionList(
            boolean compressed,
            LongStreamCheckpoint secondsCheckpoint,
            LongStreamCheckpoint nanosCheckpoint,
            Optional<BooleanStreamCheckpoint> presentCheckpoint)
    {
        ImmutableList.Builder<Integer> positionList = ImmutableList.builder();
        presentCheckpoint.ifPresent(booleanStreamCheckpoint -> positionList.addAll(booleanStreamCheckpoint.toPositionList(compressed)));
        positionList.addAll(secondsCheckpoint.toPositionList(compressed));
        positionList.addAll(nanosCheckpoint.toPositionList(compressed));
        return positionList.build();
    }

    @Override
    public List<StreamDataOutput> getBloomFilters(CompressedMetadataWriter metadataWriter)
    {
        return ImmutableList.of();
    }

    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);

        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        presentStream.getStreamDataOutput(columnId).ifPresent(outputDataStreams::add);
        outputDataStreams.add(secondsStream.getStreamDataOutput(columnId));
        outputDataStreams.add(nanosStream.getStreamDataOutput(columnId));
        return outputDataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        return secondsStream.getBufferedBytes() + nanosStream.getBufferedBytes() + presentStream.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        long retainedBytes = INSTANCE_SIZE + secondsStream.getRetainedBytes() + nanosStream.getRetainedBytes() + presentStream.getRetainedBytes();
        for (ColumnStatistics statistics : rowGroupColumnStatistics) {
            retainedBytes += statistics.getRetainedSizeInBytes();
        }
        return retainedBytes;
    }

    @Override
    public void reset()
    {
        closed = false;
        secondsStream.reset();
        nanosStream.reset();
        presentStream.reset();
        rowGroupColumnStatistics.clear();
        statisticsBuilder = statisticsBuilderSupplier.get();
    }

    private void writeTimestampMicros(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!block.isNull(i)) {
                long micros = type.getLong(block, i);

                // ORC erroneously uses normal division for seconds
                long seconds = micros / MICROSECONDS_PER_SECOND;
                long microsFraction = floorMod(micros, MICROSECONDS_PER_SECOND);
                long nanosFraction = microsFraction * NANOSECONDS_PER_MICROSECOND;

                long millis = floorDiv(micros, MICROSECONDS_PER_MILLISECOND);

                writeValues(seconds, nanosFraction);
                statisticsBuilder.addValue(millis);
            }
        }
    }

    private void writeTimestampNanos(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!block.isNull(i)) {
                LongTimestamp timestamp = (LongTimestamp) type.getObject(block, i);

                // ORC erroneously uses normal division for seconds
                long seconds = timestamp.getEpochMicros() / MICROSECONDS_PER_SECOND;
                long microsFraction = floorMod(timestamp.getEpochMicros(), MICROSECONDS_PER_SECOND);
                long nanosFraction = (microsFraction * NANOSECONDS_PER_MICROSECOND) +
                        // no rounding since the data has nanosecond precision, at most
                        (timestamp.getPicosOfMicro() / PICOSECONDS_PER_NANOSECOND);

                long millis = floorDiv(timestamp.getEpochMicros(), MICROSECONDS_PER_MILLISECOND);

                writeValues(seconds, nanosFraction);
                statisticsBuilder.addValue(millis);
            }
        }
    }

    private void writeInstantShort(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!block.isNull(i)) {
                writeMillis(unpackMillisUtc(type.getLong(block, i)));
            }
        }
    }

    private void writeInstantLong(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!block.isNull(i)) {
                LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) type.getObject(block, i);
                long millis = timestamp.getEpochMillis();

                // ORC erroneously uses normal division for seconds
                long seconds = millis / MILLISECONDS_PER_SECOND;
                long millisFraction = floorMod(millis, MILLISECONDS_PER_SECOND);
                long nanosFraction = (millisFraction * NANOSECONDS_PER_MILLISECOND) +
                        (timestamp.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND);

                writeValues(seconds, nanosFraction);
                statisticsBuilder.addValue(millis);
            }
        }
    }

    private void writeMillis(long millis)
    {
        // ORC erroneously uses normal division for seconds
        long seconds = millis / MILLISECONDS_PER_SECOND;
        long millisFraction = floorMod(millis, MILLISECONDS_PER_SECOND);
        long nanosFraction = millisFraction * NANOSECONDS_PER_MILLISECOND;

        writeValues(seconds, nanosFraction);
        statisticsBuilder.addValue(millis);
    }

    private void writeValues(long seconds, long nanosFraction)
    {
        secondsStream.writeLong(seconds - ORC_EPOCH_IN_SECONDS);
        nanosStream.writeLong(encodeNanos(nanosFraction));
    }

    // this comes from the Apache ORC code
    private static long encodeNanos(long nanos)
    {
        if (nanos == 0) {
            return 0;
        }
        if ((nanos % 100) != 0) {
            return nanos << 3;
        }
        nanos /= 100;
        int trailingZeros = 1;
        while (((nanos % 10) == 0) && (trailingZeros < 7)) {
            nanos /= 10;
            trailingZeros++;
        }
        return (nanos << 3) | trailingZeros;
    }
}
