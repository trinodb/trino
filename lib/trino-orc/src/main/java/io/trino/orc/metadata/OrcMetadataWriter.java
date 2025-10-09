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
package io.trino.orc.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.io.CountingOutputStream;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.orc.OrcWriterOptions.WriterIdentification;
import io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import io.trino.orc.metadata.OrcType.OrcTypeKind;
import io.trino.orc.metadata.Stream.StreamKind;
import io.trino.orc.metadata.statistics.BloomFilter;
import io.trino.orc.metadata.statistics.ColumnStatistics;
import io.trino.orc.metadata.statistics.StripeStatistics;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcProto.RowIndexEntry;
import org.apache.orc.OrcProto.Type;
import org.apache.orc.OrcProto.Type.Builder;
import org.apache.orc.OrcProto.UserMetadataItem;
import org.apache.orc.protobuf.ByteString;
import org.apache.orc.protobuf.MessageLite;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.orc.metadata.PostScript.MAGIC;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class OrcMetadataWriter
        implements MetadataWriter
{
    // see https://github.com/trinodb/orc-protobuf/blob/master/src/main/protobuf/orc_proto.proto
    public static final int TRINO_WRITER_ID = 4;
    // in order to change this value, the master Apache ORC proto file must be updated
    private static final int TRINO_WRITER_VERSION = 6;

    // see https://github.com/trinodb/orc-protobuf/blob/master/src/main/protobuf/orc_proto.proto
    public static final int PRESTO_WRITER_ID = 2;

    // maximum version readable by Hive 2.x before the ORC-125 fix
    private static final int HIVE_LEGACY_WRITER_VERSION = 4;

    private static final List<Integer> ORC_METADATA_VERSION = ImmutableList.of(0, 12);

    private final WriterIdentification writerIdentification;

    public OrcMetadataWriter(WriterIdentification writerIdentification)
    {
        this.writerIdentification = requireNonNull(writerIdentification, "writerIdentification is null");
    }

    @Override
    public List<Integer> getOrcMetadataVersion()
    {
        return ORC_METADATA_VERSION;
    }

    @Override
    public int writePostscript(SliceOutput output, int footerLength, int metadataLength, CompressionKind compression, int compressionBlockSize)
            throws IOException
    {
        OrcProto.PostScript postScriptProtobuf = OrcProto.PostScript.newBuilder()
                .addAllVersion(ORC_METADATA_VERSION)
                .setFooterLength(footerLength)
                .setMetadataLength(metadataLength)
                .setCompression(toCompression(compression))
                .setCompressionBlockSize(compressionBlockSize)
                .setWriterVersion(getOrcWriterVersion())
                .setMagic(MAGIC.toStringUtf8())
                .build();

        return writeProtobufObject(output, postScriptProtobuf);
    }

    private int getOrcWriterVersion()
    {
        return switch (writerIdentification) {
            case LEGACY_HIVE_COMPATIBLE -> HIVE_LEGACY_WRITER_VERSION;
            case TRINO -> TRINO_WRITER_VERSION;
        };
    }

    @Override
    public int writeMetadata(SliceOutput output, Metadata metadata)
            throws IOException
    {
        OrcProto.Metadata metadataProtobuf = OrcProto.Metadata.newBuilder()
                .addAllStripeStats(metadata.getStripeStatsList().stream()
                        .map(Optional::get)
                        .map(OrcMetadataWriter::toStripeStatistics)
                        .collect(toList()))
                .build();

        return writeProtobufObject(output, metadataProtobuf);
    }

    private static OrcProto.StripeStatistics toStripeStatistics(StripeStatistics stripeStatistics)
    {
        return OrcProto.StripeStatistics.newBuilder()
                .addAllColStats(stripeStatistics.getColumnStatistics().stream()
                        .map(OrcMetadataWriter::toColumnStatistics)
                        .collect(toList()))
                .build();
    }

    @Override
    public int writeFooter(SliceOutput output, Footer footer)
            throws IOException
    {
        OrcProto.Footer.Builder builder = OrcProto.Footer.newBuilder()
                .setNumberOfRows(footer.getNumberOfRows())
                .setRowIndexStride(footer.getRowsInRowGroup().orElse(0))
                .addAllStripes(footer.getStripes().stream()
                        .map(OrcMetadataWriter::toStripeInformation)
                        .collect(toList()))
                .addAllTypes(footer.getTypes().stream()
                        .map(OrcMetadataWriter::toType)
                        .collect(toList()))
                .addAllStatistics(footer.getFileStats().map(ColumnMetadata::stream).orElseGet(java.util.stream.Stream::empty)
                        .map(OrcMetadataWriter::toColumnStatistics)
                        .collect(toList()))
                .addAllMetadata(footer.getUserMetadata().entrySet().stream()
                        .map(OrcMetadataWriter::toUserMetadata)
                        .collect(toList()))
                .setCalendar(toOrcCalendarKind(footer.getCalendar()));

        setWriter(builder);

        return writeProtobufObject(output, builder.build());
    }

    private void setWriter(OrcProto.Footer.Builder builder)
    {
        switch (writerIdentification) {
            case LEGACY_HIVE_COMPATIBLE:
                return;
            case TRINO:
                builder.setWriter(TRINO_WRITER_ID);
                return;
        }
        throw new IllegalStateException("Unexpected value: " + writerIdentification);
    }

    private static OrcProto.StripeInformation toStripeInformation(StripeInformation stripe)
    {
        return OrcProto.StripeInformation.newBuilder()
                .setNumberOfRows(stripe.getNumberOfRows())
                .setOffset(stripe.getOffset())
                .setIndexLength(stripe.getIndexLength())
                .setDataLength(stripe.getDataLength())
                .setFooterLength(stripe.getFooterLength())
                .build();
    }

    private static Type toType(OrcType type)
    {
        Builder builder = Type.newBuilder()
                .setKind(toTypeKind(type.getOrcTypeKind()))
                .addAllSubtypes(type.getFieldTypeIndexes().stream()
                        .map(OrcColumnId::getId)
                        .collect(toList()))
                .addAllFieldNames(type.getFieldNames())
                .addAllAttributes(toStringPairList(type.getAttributes()));

        if (type.getLength().isPresent()) {
            builder.setMaximumLength(type.getLength().get());
        }
        if (type.getPrecision().isPresent()) {
            builder.setPrecision(type.getPrecision().get());
        }
        if (type.getScale().isPresent()) {
            builder.setScale(type.getScale().get());
        }
        return builder.build();
    }

    private static OrcProto.Type.Kind toTypeKind(OrcTypeKind orcTypeKind)
    {
        return switch (orcTypeKind) {
            case BOOLEAN -> Type.Kind.BOOLEAN;
            case BYTE -> Type.Kind.BYTE;
            case SHORT -> Type.Kind.SHORT;
            case INT -> Type.Kind.INT;
            case LONG -> Type.Kind.LONG;
            case DECIMAL -> Type.Kind.DECIMAL;
            case FLOAT -> Type.Kind.FLOAT;
            case DOUBLE -> Type.Kind.DOUBLE;
            case STRING -> Type.Kind.STRING;
            case VARCHAR -> Type.Kind.VARCHAR;
            case CHAR -> Type.Kind.CHAR;
            case BINARY -> Type.Kind.BINARY;
            case DATE -> Type.Kind.DATE;
            case TIMESTAMP -> Type.Kind.TIMESTAMP;
            case TIMESTAMP_INSTANT -> Type.Kind.TIMESTAMP_INSTANT;
            case LIST -> Type.Kind.LIST;
            case MAP -> Type.Kind.MAP;
            case STRUCT -> Type.Kind.STRUCT;
            case UNION -> Type.Kind.UNION;
        };
    }

    private static List<OrcProto.StringPair> toStringPairList(Map<String, String> attributes)
    {
        return attributes.entrySet().stream()
                .map(entry -> OrcProto.StringPair.newBuilder()
                        .setKey(entry.getKey())
                        .setValue(entry.getValue())
                        .build())
                .collect(toImmutableList());
    }

    private static OrcProto.ColumnStatistics toColumnStatistics(ColumnStatistics columnStatistics)
    {
        OrcProto.ColumnStatistics.Builder builder = OrcProto.ColumnStatistics.newBuilder();

        if (columnStatistics.hasNumberOfValues()) {
            builder.setNumberOfValues(columnStatistics.getNumberOfValues());
        }

        if (columnStatistics.getBooleanStatistics() != null) {
            builder.setBucketStatistics(OrcProto.BucketStatistics.newBuilder()
                    .addCount(columnStatistics.getBooleanStatistics().getTrueValueCount())
                    .build());
        }

        if (columnStatistics.getIntegerStatistics() != null) {
            OrcProto.IntegerStatistics.Builder integerStatistics = OrcProto.IntegerStatistics.newBuilder()
                    .setMinimum(columnStatistics.getIntegerStatistics().getMin())
                    .setMaximum(columnStatistics.getIntegerStatistics().getMax());
            if (columnStatistics.getIntegerStatistics().getSum() != null) {
                integerStatistics.setSum(columnStatistics.getIntegerStatistics().getSum());
            }
            builder.setIntStatistics(integerStatistics.build());
        }

        if (columnStatistics.getDoubleStatistics() != null) {
            builder.setDoubleStatistics(OrcProto.DoubleStatistics.newBuilder()
                    .setMinimum(columnStatistics.getDoubleStatistics().getMin())
                    .setMaximum(columnStatistics.getDoubleStatistics().getMax())
                    .build());
        }

        if (columnStatistics.getStringStatistics() != null) {
            OrcProto.StringStatistics.Builder statisticsBuilder = OrcProto.StringStatistics.newBuilder();
            if (columnStatistics.getStringStatistics().getMin() != null) {
                statisticsBuilder.setMinimumBytes(ByteString.copyFrom(columnStatistics.getStringStatistics().getMin().getBytes()));
            }
            if (columnStatistics.getStringStatistics().getMax() != null) {
                statisticsBuilder.setMaximumBytes(ByteString.copyFrom(columnStatistics.getStringStatistics().getMax().getBytes()));
            }
            statisticsBuilder.setSum(columnStatistics.getStringStatistics().getSum());
            builder.setStringStatistics(statisticsBuilder.build());
        }

        if (columnStatistics.getDateStatistics() != null) {
            builder.setDateStatistics(OrcProto.DateStatistics.newBuilder()
                    .setMinimum(columnStatistics.getDateStatistics().getMin())
                    .setMaximum(columnStatistics.getDateStatistics().getMax())
                    .build());
        }

        if (columnStatistics.getTimestampStatistics() != null) {
            builder.setTimestampStatistics(OrcProto.TimestampStatistics.newBuilder()
                    .setMinimumUtc(columnStatistics.getTimestampStatistics().getMin())
                    .setMaximumUtc(columnStatistics.getTimestampStatistics().getMax())
                    .build());
        }

        if (columnStatistics.getDecimalStatistics() != null) {
            builder.setDecimalStatistics(OrcProto.DecimalStatistics.newBuilder()
                    .setMinimum(columnStatistics.getDecimalStatistics().getMin().toString())
                    .setMaximum(columnStatistics.getDecimalStatistics().getMax().toString())
                    .build());
        }

        if (columnStatistics.getBinaryStatistics() != null) {
            builder.setBinaryStatistics(OrcProto.BinaryStatistics.newBuilder()
                    .setSum(columnStatistics.getBinaryStatistics().getSum())
                    .build());
        }

        return builder.build();
    }

    private static UserMetadataItem toUserMetadata(Entry<String, Slice> entry)
    {
        return OrcProto.UserMetadataItem.newBuilder()
                .setName(entry.getKey())
                .setValue(ByteString.copyFrom(entry.getValue().getBytes()))
                .build();
    }

    @Override
    public int writeStripeFooter(SliceOutput output, StripeFooter footer)
            throws IOException
    {
        OrcProto.StripeFooter footerProtobuf = OrcProto.StripeFooter.newBuilder()
                .addAllStreams(footer.getStreams().stream()
                        .map(OrcMetadataWriter::toStream)
                        .collect(toList()))
                .addAllColumns(footer.getColumnEncodings().stream()
                        .map(OrcMetadataWriter::toColumnEncoding)
                        .collect(toList()))
                .setWriterTimezone(footer.getTimeZone().getId())
                .build();

        return writeProtobufObject(output, footerProtobuf);
    }

    private static OrcProto.Stream toStream(Stream stream)
    {
        return OrcProto.Stream.newBuilder()
                .setColumn(stream.getColumnId().getId())
                .setKind(toStreamKind(stream.getStreamKind()))
                .setLength(stream.getLength())
                .build();
    }

    private static OrcProto.Stream.Kind toStreamKind(StreamKind streamKind)
    {
        switch (streamKind) {
            case PRESENT:
                return OrcProto.Stream.Kind.PRESENT;
            case DATA:
                return OrcProto.Stream.Kind.DATA;
            case LENGTH:
                return OrcProto.Stream.Kind.LENGTH;
            case DICTIONARY_DATA:
                return OrcProto.Stream.Kind.DICTIONARY_DATA;
            case DICTIONARY_COUNT:
                return OrcProto.Stream.Kind.DICTIONARY_COUNT;
            case SECONDARY:
                return OrcProto.Stream.Kind.SECONDARY;
            case ROW_INDEX:
                return OrcProto.Stream.Kind.ROW_INDEX;
            case BLOOM_FILTER:
                // unsupported
                break;
            case BLOOM_FILTER_UTF8:
                return OrcProto.Stream.Kind.BLOOM_FILTER_UTF8;
        }
        throw new IllegalArgumentException("Unsupported stream kind: " + streamKind);
    }

    private static OrcProto.CalendarKind toOrcCalendarKind(CalendarKind calendarKind)
    {
        return switch (calendarKind) {
            case UNKNOWN_CALENDAR -> OrcProto.CalendarKind.UNKNOWN_CALENDAR;
            case JULIAN_GREGORIAN -> OrcProto.CalendarKind.JULIAN_GREGORIAN;
            case PROLEPTIC_GREGORIAN -> OrcProto.CalendarKind.PROLEPTIC_GREGORIAN;
        };
    }

    private static OrcProto.ColumnEncoding toColumnEncoding(ColumnEncoding columnEncodings)
    {
        return OrcProto.ColumnEncoding.newBuilder()
                .setKind(toColumnEncoding(columnEncodings.getColumnEncodingKind()))
                .setDictionarySize(columnEncodings.getDictionarySize())
                .build();
    }

    private static OrcProto.ColumnEncoding.Kind toColumnEncoding(ColumnEncodingKind columnEncodingKind)
    {
        return switch (columnEncodingKind) {
            case DIRECT -> OrcProto.ColumnEncoding.Kind.DIRECT;
            case DICTIONARY -> OrcProto.ColumnEncoding.Kind.DICTIONARY;
            case DIRECT_V2 -> OrcProto.ColumnEncoding.Kind.DIRECT_V2;
            case DICTIONARY_V2 -> OrcProto.ColumnEncoding.Kind.DICTIONARY_V2;
        };
    }

    @Override
    public int writeRowIndexes(SliceOutput output, List<RowGroupIndex> rowGroupIndexes)
            throws IOException
    {
        OrcProto.RowIndex rowIndexProtobuf = OrcProto.RowIndex.newBuilder()
                .addAllEntry(rowGroupIndexes.stream()
                        .map(OrcMetadataWriter::toRowGroupIndex)
                        .collect(toList()))
                .build();
        return writeProtobufObject(output, rowIndexProtobuf);
    }

    private static RowIndexEntry toRowGroupIndex(RowGroupIndex rowGroupIndex)
    {
        return OrcProto.RowIndexEntry.newBuilder()
                .addAllPositions(rowGroupIndex.getPositions().stream()
                        .map(Integer::longValue)
                        .collect(toList()))
                .setStatistics(toColumnStatistics(rowGroupIndex.getColumnStatistics()))
                .build();
    }

    @Override
    public int writeBloomFilters(SliceOutput output, List<BloomFilter> bloomFilters)
            throws IOException
    {
        OrcProto.BloomFilterIndex bloomFilterIndex = OrcProto.BloomFilterIndex.newBuilder()
                .addAllBloomFilter(bloomFilters.stream()
                        .map(OrcMetadataWriter::toBloomFilter)
                        .collect(toList()))
                .build();
        return writeProtobufObject(output, bloomFilterIndex);
    }

    private static OrcProto.BloomFilter toBloomFilter(BloomFilter bloomFilter)
    {
        return OrcProto.BloomFilter.newBuilder()
                .addAllBitset(Longs.asList(bloomFilter.getBitSet()))
                .setNumHashFunctions(bloomFilter.getNumHashFunctions())
                .build();
    }

    private static OrcProto.CompressionKind toCompression(CompressionKind compressionKind)
    {
        return switch (compressionKind) {
            case NONE -> OrcProto.CompressionKind.NONE;
            case ZLIB -> OrcProto.CompressionKind.ZLIB;
            case SNAPPY -> OrcProto.CompressionKind.SNAPPY;
            case LZ4 -> OrcProto.CompressionKind.LZ4;
            case ZSTD -> OrcProto.CompressionKind.ZSTD;
        };
    }

    private static int writeProtobufObject(OutputStream output, MessageLite object)
            throws IOException
    {
        CountingOutputStream countingOutput = new CountingOutputStream(output);
        object.writeTo(countingOutput);
        return toIntExact(countingOutput.getCount());
    }
}
