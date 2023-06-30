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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import io.trino.orc.metadata.OrcType.OrcTypeKind;
import io.trino.orc.metadata.PostScript.HiveWriterVersion;
import io.trino.orc.metadata.Stream.StreamKind;
import io.trino.orc.metadata.statistics.BinaryStatistics;
import io.trino.orc.metadata.statistics.BloomFilter;
import io.trino.orc.metadata.statistics.BooleanStatistics;
import io.trino.orc.metadata.statistics.ColumnStatistics;
import io.trino.orc.metadata.statistics.DateStatistics;
import io.trino.orc.metadata.statistics.DecimalStatistics;
import io.trino.orc.metadata.statistics.DoubleStatistics;
import io.trino.orc.metadata.statistics.IntegerStatistics;
import io.trino.orc.metadata.statistics.StringStatistics;
import io.trino.orc.metadata.statistics.StripeStatistics;
import io.trino.orc.metadata.statistics.TimestampStatistics;
import io.trino.orc.proto.OrcProto;
import io.trino.orc.proto.OrcProto.RowIndexEntry;
import io.trino.orc.protobuf.ByteString;
import io.trino.orc.protobuf.CodedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteOrder;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.SliceUtf8.tryGetCodePointAt;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.orc.metadata.CompressionKind.LZ4;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.orc.metadata.CompressionKind.SNAPPY;
import static io.trino.orc.metadata.CompressionKind.ZLIB;
import static io.trino.orc.metadata.CompressionKind.ZSTD;
import static io.trino.orc.metadata.PostScript.HiveWriterVersion.ORC_HIVE_8732;
import static io.trino.orc.metadata.PostScript.HiveWriterVersion.ORIGINAL;
import static io.trino.orc.metadata.statistics.BinaryStatistics.BINARY_VALUE_BYTES_OVERHEAD;
import static io.trino.orc.metadata.statistics.BooleanStatistics.BOOLEAN_VALUE_BYTES;
import static io.trino.orc.metadata.statistics.DateStatistics.DATE_VALUE_BYTES;
import static io.trino.orc.metadata.statistics.DecimalStatistics.DECIMAL_VALUE_BYTES_OVERHEAD;
import static io.trino.orc.metadata.statistics.DoubleStatistics.DOUBLE_VALUE_BYTES;
import static io.trino.orc.metadata.statistics.IntegerStatistics.INTEGER_VALUE_BYTES;
import static io.trino.orc.metadata.statistics.ShortDecimalStatisticsBuilder.SHORT_DECIMAL_VALUE_BYTES;
import static io.trino.orc.metadata.statistics.StringStatistics.STRING_VALUE_BYTES_OVERHEAD;
import static io.trino.orc.metadata.statistics.TimestampStatistics.TIMESTAMP_VALUE_BYTES;
import static java.lang.Character.MIN_SUPPLEMENTARY_CODE_POINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class OrcMetadataReader
        implements MetadataReader
{
    private static final int REPLACEMENT_CHARACTER_CODE_POINT = 0xFFFD;
    private static final int PROTOBUF_MESSAGE_MAX_LIMIT = toIntExact(DataSize.of(1, GIGABYTE).toBytes());

    private final OrcReaderOptions orcReaderOptions;

    public OrcMetadataReader(OrcReaderOptions orcReaderOptions)
    {
        this.orcReaderOptions = requireNonNull(orcReaderOptions, "orcReaderOptions is null");
    }

    @Override
    public PostScript readPostScript(InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.PostScript postScript = OrcProto.PostScript.parseFrom(input);

        return new PostScript(
                postScript.getVersionList(),
                postScript.getFooterLength(),
                postScript.getMetadataLength(),
                toCompression(postScript.getCompression()),
                postScript.getCompressionBlockSize(),
                toHiveWriterVersion(postScript.getWriterVersion()));
    }

    private static HiveWriterVersion toHiveWriterVersion(int writerVersion)
    {
        if (writerVersion >= 1) {
            return ORC_HIVE_8732;
        }
        return ORIGINAL;
    }

    @Override
    public Metadata readMetadata(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        input.setSizeLimit(PROTOBUF_MESSAGE_MAX_LIMIT);
        OrcProto.Metadata metadata = OrcProto.Metadata.parseFrom(input);
        return new Metadata(toStripeStatistics(hiveWriterVersion, metadata.getStripeStatsList()));
    }

    private static List<Optional<StripeStatistics>> toStripeStatistics(HiveWriterVersion hiveWriterVersion, List<OrcProto.StripeStatistics> types)
    {
        return types.stream()
                .map(stripeStatistics -> toStripeStatistics(hiveWriterVersion, stripeStatistics))
                .collect(toImmutableList());
    }

    private static Optional<StripeStatistics> toStripeStatistics(HiveWriterVersion hiveWriterVersion, OrcProto.StripeStatistics stripeStatistics)
    {
        return toColumnStatistics(hiveWriterVersion, stripeStatistics.getColStatsList(), false)
                .map(StripeStatistics::new);
    }

    @Override
    public Footer readFooter(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        input.setSizeLimit(PROTOBUF_MESSAGE_MAX_LIMIT);
        OrcProto.Footer footer = OrcProto.Footer.parseFrom(input);
        return new Footer(
                footer.getNumberOfRows(),
                footer.getRowIndexStride() == 0 ? OptionalInt.empty() : OptionalInt.of(footer.getRowIndexStride()),
                toStripeInformation(footer.getStripesList()),
                toType(footer.getTypesList()),
                toColumnStatistics(hiveWriterVersion, footer.getStatisticsList(), false),
                toUserMetadata(footer.getMetadataList()),
                Optional.of(footer.getWriter()));
    }

    private static List<StripeInformation> toStripeInformation(List<OrcProto.StripeInformation> types)
    {
        return types.stream()
                .map(OrcMetadataReader::toStripeInformation)
                .collect(toImmutableList());
    }

    private static StripeInformation toStripeInformation(OrcProto.StripeInformation stripeInformation)
    {
        return new StripeInformation(
                toIntExact(stripeInformation.getNumberOfRows()),
                stripeInformation.getOffset(),
                stripeInformation.getIndexLength(),
                stripeInformation.getDataLength(),
                stripeInformation.getFooterLength());
    }

    @Override
    public StripeFooter readStripeFooter(ColumnMetadata<OrcType> types, InputStream inputStream, ZoneId legacyFileTimeZone)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.StripeFooter stripeFooter = OrcProto.StripeFooter.parseFrom(input);
        return new StripeFooter(
                toStream(stripeFooter.getStreamsList()),
                toColumnEncoding(stripeFooter.getColumnsList()),
                Optional.ofNullable(emptyToNull(stripeFooter.getWriterTimezone()))
                        .map(zoneId ->
                                orcReaderOptions.isReadLegacyShortZoneId()
                                        ? ZoneId.of(zoneId, ZoneId.SHORT_IDS)
                                        : ZoneId.of(zoneId))
                        .orElse(legacyFileTimeZone));
    }

    private static Stream toStream(OrcProto.Stream stream)
    {
        return new Stream(new OrcColumnId(stream.getColumn()), toStreamKind(stream.getKind()), toIntExact(stream.getLength()), true);
    }

    private static List<Stream> toStream(List<OrcProto.Stream> streams)
    {
        return streams.stream()
                .map(OrcMetadataReader::toStream)
                .collect(toImmutableList());
    }

    private static ColumnEncoding toColumnEncoding(OrcProto.ColumnEncoding columnEncoding)
    {
        return new ColumnEncoding(toColumnEncodingKind(columnEncoding.getKind()), columnEncoding.getDictionarySize());
    }

    private static ColumnMetadata<ColumnEncoding> toColumnEncoding(List<OrcProto.ColumnEncoding> columnEncodings)
    {
        return new ColumnMetadata<>(columnEncodings.stream()
                .map(OrcMetadataReader::toColumnEncoding)
                .collect(toImmutableList()));
    }

    @Override
    public List<RowGroupIndex> readRowIndexes(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.RowIndex rowIndex = OrcProto.RowIndex.parseFrom(input);
        return rowIndex.getEntryList().stream()
                .map(rowIndexEntry -> toRowGroupIndex(hiveWriterVersion, rowIndexEntry))
                .collect(toImmutableList());
    }

    @Override
    public List<BloomFilter> readBloomFilterIndexes(InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.BloomFilterIndex bloomFilter = OrcProto.BloomFilterIndex.parseFrom(input);
        List<OrcProto.BloomFilter> bloomFilterList = bloomFilter.getBloomFilterList();
        ImmutableList.Builder<BloomFilter> builder = ImmutableList.builder();
        for (OrcProto.BloomFilter orcBloomFilter : bloomFilterList) {
            if (orcBloomFilter.hasUtf8Bitset()) {
                ByteString utf8Bitset = orcBloomFilter.getUtf8Bitset();
                long[] bits = new long[utf8Bitset.size() / 8];
                utf8Bitset.asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get(bits);
                builder.add(new BloomFilter(bits, orcBloomFilter.getNumHashFunctions()));
            }
            else {
                int length = orcBloomFilter.getBitsetCount();
                long[] bits = new long[length];
                for (int i = 0; i < length; i++) {
                    bits[i] = orcBloomFilter.getBitset(i);
                }
                builder.add(new BloomFilter(bits, orcBloomFilter.getNumHashFunctions()));
            }
        }
        return builder.build();
    }

    private static RowGroupIndex toRowGroupIndex(HiveWriterVersion hiveWriterVersion, RowIndexEntry rowIndexEntry)
    {
        List<Long> positionsList = rowIndexEntry.getPositionsList();
        ImmutableList.Builder<Integer> positions = ImmutableList.builder();
        for (int index = 0; index < positionsList.size(); index++) {
            long longPosition = positionsList.get(index);

            @SuppressWarnings("NumericCastThatLosesPrecision")
            int intPosition = (int) longPosition;
            checkArgument(intPosition == longPosition, "Expected checkpoint position [%s] value [%s] to be an integer", index, longPosition);

            positions.add(intPosition);
        }
        return new RowGroupIndex(positions.build(), toColumnStatistics(hiveWriterVersion, rowIndexEntry.getStatistics(), true));
    }

    private static ColumnStatistics toColumnStatistics(HiveWriterVersion hiveWriterVersion, OrcProto.ColumnStatistics statistics, boolean isRowGroup)
    {
        long minAverageValueBytes;

        if (statistics.hasBucketStatistics()) {
            minAverageValueBytes = BOOLEAN_VALUE_BYTES;
        }
        else if (statistics.hasIntStatistics()) {
            minAverageValueBytes = INTEGER_VALUE_BYTES;
        }
        else if (statistics.hasDoubleStatistics()) {
            minAverageValueBytes = DOUBLE_VALUE_BYTES;
        }
        else if (statistics.hasStringStatistics()) {
            minAverageValueBytes = STRING_VALUE_BYTES_OVERHEAD;
            if (statistics.hasNumberOfValues() && statistics.getNumberOfValues() > 0) {
                minAverageValueBytes += statistics.getStringStatistics().getSum() / statistics.getNumberOfValues();
            }
        }
        else if (statistics.hasDateStatistics()) {
            minAverageValueBytes = DATE_VALUE_BYTES;
        }
        else if (statistics.hasTimestampStatistics()) {
            minAverageValueBytes = TIMESTAMP_VALUE_BYTES;
        }
        else if (statistics.hasDecimalStatistics()) {
            // could be 8 or 16; return the smaller one given it is a min average
            minAverageValueBytes = DECIMAL_VALUE_BYTES_OVERHEAD + SHORT_DECIMAL_VALUE_BYTES;
        }
        else if (statistics.hasBinaryStatistics()) {
            // offset and value length
            minAverageValueBytes = BINARY_VALUE_BYTES_OVERHEAD;
            if (statistics.hasNumberOfValues() && statistics.getNumberOfValues() > 0) {
                minAverageValueBytes += statistics.getBinaryStatistics().getSum() / statistics.getNumberOfValues();
            }
        }
        else {
            minAverageValueBytes = 0;
        }

        // To handle an existing issue of Hive writer during minor compaction (HIVE-20604):
        // After minor compaction, stripe stats don't have column statistics and bit field of numberOfValues
        // is set to 1, but the value is wrongly set to default 0 which implies there is something wrong with
        // the stats. Drop the column statistics altogether.
        if (statistics.hasHasNull() && statistics.getNumberOfValues() == 0 && !statistics.getHasNull()) {
            return new ColumnStatistics(null, 0, null, null, null, null, null, null, null, null, null, null);
        }

        return new ColumnStatistics(
                statistics.getNumberOfValues(),
                minAverageValueBytes,
                statistics.hasBucketStatistics() ? toBooleanStatistics(statistics.getBucketStatistics()) : null,
                statistics.hasIntStatistics() ? toIntegerStatistics(statistics.getIntStatistics()) : null,
                statistics.hasDoubleStatistics() ? toDoubleStatistics(statistics.getDoubleStatistics()) : null,
                null,
                statistics.hasStringStatistics() ? toStringStatistics(hiveWriterVersion, statistics.getStringStatistics(), isRowGroup) : null,
                statistics.hasDateStatistics() ? toDateStatistics(hiveWriterVersion, statistics.getDateStatistics(), isRowGroup) : null,
                statistics.hasTimestampStatistics() ? toTimestampStatistics(hiveWriterVersion, statistics.getTimestampStatistics(), isRowGroup) : null,
                statistics.hasDecimalStatistics() ? toDecimalStatistics(statistics.getDecimalStatistics()) : null,
                statistics.hasBinaryStatistics() ? toBinaryStatistics(statistics.getBinaryStatistics()) : null,
                null);
    }

    private static Optional<ColumnMetadata<ColumnStatistics>> toColumnStatistics(HiveWriterVersion hiveWriterVersion, List<OrcProto.ColumnStatistics> columnStatistics, boolean isRowGroup)
    {
        if (columnStatistics == null || columnStatistics.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new ColumnMetadata<>(columnStatistics.stream()
                .map(statistics -> toColumnStatistics(hiveWriterVersion, statistics, isRowGroup))
                .collect(toImmutableList())));
    }

    private static Map<String, Slice> toUserMetadata(List<OrcProto.UserMetadataItem> metadataList)
    {
        ImmutableMap.Builder<String, Slice> mapBuilder = ImmutableMap.builder();
        for (OrcProto.UserMetadataItem item : metadataList) {
            mapBuilder.put(item.getName(), byteStringToSlice(item.getValue()));
        }
        return mapBuilder.buildOrThrow();
    }

    private static BooleanStatistics toBooleanStatistics(OrcProto.BucketStatistics bucketStatistics)
    {
        if (bucketStatistics.getCountCount() == 0) {
            return null;
        }

        return new BooleanStatistics(bucketStatistics.getCount(0));
    }

    private static IntegerStatistics toIntegerStatistics(OrcProto.IntegerStatistics integerStatistics)
    {
        return new IntegerStatistics(
                integerStatistics.hasMinimum() ? integerStatistics.getMinimum() : null,
                integerStatistics.hasMaximum() ? integerStatistics.getMaximum() : null,
                integerStatistics.hasSum() ? integerStatistics.getSum() : null);
    }

    private static DoubleStatistics toDoubleStatistics(OrcProto.DoubleStatistics doubleStatistics)
    {
        // TODO remove this when double statistics are changed to correctly deal with NaNs
        // if either min, max, or sum is NaN, ignore the stat
        if ((doubleStatistics.hasMinimum() && Double.isNaN(doubleStatistics.getMinimum())) ||
                (doubleStatistics.hasMaximum() && Double.isNaN(doubleStatistics.getMaximum())) ||
                (doubleStatistics.hasSum() && Double.isNaN(doubleStatistics.getSum()))) {
            return null;
        }

        return new DoubleStatistics(
                doubleStatistics.hasMinimum() ? doubleStatistics.getMinimum() : null,
                doubleStatistics.hasMaximum() ? doubleStatistics.getMaximum() : null);
    }

    static StringStatistics toStringStatistics(HiveWriterVersion hiveWriterVersion, OrcProto.StringStatistics stringStatistics, boolean isRowGroup)
    {
        if (hiveWriterVersion == ORIGINAL && !isRowGroup) {
            return null;
        }

        Slice maximum = stringStatistics.hasMaximum() ? maxStringTruncateToValidRange(byteStringToSlice(stringStatistics.getMaximumBytes()), hiveWriterVersion) : null;
        Slice minimum = stringStatistics.hasMinimum() ? minStringTruncateToValidRange(byteStringToSlice(stringStatistics.getMinimumBytes()), hiveWriterVersion) : null;
        long sum = stringStatistics.hasSum() ? stringStatistics.getSum() : 0;
        return new StringStatistics(minimum, maximum, sum);
    }

    private static DecimalStatistics toDecimalStatistics(OrcProto.DecimalStatistics decimalStatistics)
    {
        BigDecimal minimum = decimalStatistics.hasMinimum() ? new BigDecimal(decimalStatistics.getMinimum()) : null;
        BigDecimal maximum = decimalStatistics.hasMaximum() ? new BigDecimal(decimalStatistics.getMaximum()) : null;

        // could be long (16 bytes) or short (8 bytes); use short for estimation
        return new DecimalStatistics(minimum, maximum, SHORT_DECIMAL_VALUE_BYTES);
    }

    private static BinaryStatistics toBinaryStatistics(OrcProto.BinaryStatistics binaryStatistics)
    {
        if (!binaryStatistics.hasSum()) {
            return null;
        }

        return new BinaryStatistics(binaryStatistics.getSum());
    }

    private static Slice byteStringToSlice(ByteString value)
    {
        return Slices.wrappedBuffer(value.toByteArray());
    }

    @VisibleForTesting
    public static Slice maxStringTruncateToValidRange(Slice value, HiveWriterVersion version)
    {
        if (value == null) {
            return null;
        }

        if (version != ORIGINAL) {
            return value;
        }

        int index = findStringStatisticTruncationPositionForOriginalOrcWriter(value);
        if (index == value.length()) {
            return value;
        }
        // Append 0xFF so that it is larger than value
        Slice newValue = Slices.copyOf(value, 0, index + 1);
        newValue.setByte(index, 0xFF);
        return newValue;
    }

    @VisibleForTesting
    public static Slice minStringTruncateToValidRange(Slice value, HiveWriterVersion version)
    {
        if (value == null) {
            return null;
        }

        if (version != ORIGINAL) {
            return value;
        }

        int index = findStringStatisticTruncationPositionForOriginalOrcWriter(value);
        if (index == value.length()) {
            return value;
        }
        return Slices.copyOf(value, 0, index);
    }

    @VisibleForTesting
    static int findStringStatisticTruncationPositionForOriginalOrcWriter(Slice utf8)
    {
        int length = utf8.length();

        int position = 0;
        while (position < length) {
            int codePoint = tryGetCodePointAt(utf8, position);

            // stop at invalid sequences
            if (codePoint < 0) {
                break;
            }

            // The original ORC writers round trip string min and max values through java.lang.String which
            // replaces invalid UTF-8 sequences with the unicode replacement character.  This can cause the min value to be
            // greater than expected which can result in data sections being skipped instead of being processed. As a work around,
            // the string stats are truncated at the first replacement character.
            if (codePoint == REPLACEMENT_CHARACTER_CODE_POINT) {
                break;
            }

            // The original writer performs comparisons using java Strings to determine the minimum and maximum
            // values. This results in weird behaviors in the presence of surrogate pairs and special characters.
            //
            // For example, unicode code point 0x1D403 has the following representations:
            // UTF-16: [0xD835, 0xDC03]
            // UTF-8: [0xF0, 0x9D, 0x90, 0x83]
            //
            // while code point 0xFFFD (the replacement character) has the following representations:
            // UTF-16: [0xFFFD]
            // UTF-8: [0xEF, 0xBF, 0xBD]
            //
            // when comparisons between strings containing these characters are done with Java Strings (UTF-16),
            // 0x1D403 < 0xFFFD, but when comparisons are done using raw codepoints or UTF-8, 0x1D403 > 0xFFFD
            //
            // We use the following logic to ensure that we have a wider range of min-max
            // * if a min string has a surrogate character, the min string is truncated
            //   at the first occurrence of the surrogate character (to exclude the surrogate character)
            // * if a max string has a surrogate character, the max string is truncated
            //   at the first occurrence the surrogate character and 0xFF byte is appended to it.
            if (codePoint >= MIN_SUPPLEMENTARY_CODE_POINT) {
                break;
            }

            position += lengthOfCodePoint(codePoint);
        }
        return position;
    }

    private static DateStatistics toDateStatistics(HiveWriterVersion hiveWriterVersion, OrcProto.DateStatistics dateStatistics, boolean isRowGroup)
    {
        if (hiveWriterVersion == ORIGINAL && !isRowGroup) {
            return null;
        }

        return new DateStatistics(
                dateStatistics.hasMinimum() ? dateStatistics.getMinimum() : null,
                dateStatistics.hasMaximum() ? dateStatistics.getMaximum() : null);
    }

    private static TimestampStatistics toTimestampStatistics(HiveWriterVersion hiveWriterVersion, OrcProto.TimestampStatistics timestampStatistics, boolean isRowGroup)
    {
        if (hiveWriterVersion == ORIGINAL && !isRowGroup) {
            return null;
        }

        return new TimestampStatistics(
                timestampStatistics.hasMinimumUtc() ? timestampStatistics.getMinimumUtc() : null,
                timestampStatistics.hasMaximumUtc() ? timestampStatistics.getMaximumUtc() : null);
    }

    private static OrcType toType(OrcProto.Type type)
    {
        Optional<Integer> length = Optional.empty();
        if (type.getKind() == OrcProto.Type.Kind.VARCHAR || type.getKind() == OrcProto.Type.Kind.CHAR) {
            length = Optional.of(type.getMaximumLength());
        }
        Optional<Integer> precision = Optional.empty();
        Optional<Integer> scale = Optional.empty();
        if (type.getKind() == OrcProto.Type.Kind.DECIMAL) {
            precision = Optional.of(type.getPrecision());
            scale = Optional.of(type.getScale());
        }
        return new OrcType(toTypeKind(type.getKind()), toOrcColumnId(type.getSubtypesList()), type.getFieldNamesList(), length, precision, scale, toMap(type.getAttributesList()));
    }

    private static List<OrcColumnId> toOrcColumnId(List<Integer> columnIds)
    {
        return columnIds.stream()
                .map(OrcColumnId::new)
                .collect(toImmutableList());
    }

    private static ColumnMetadata<OrcType> toType(List<OrcProto.Type> types)
    {
        return new ColumnMetadata<>(types.stream()
                .map(OrcMetadataReader::toType)
                .collect(toImmutableList()));
    }

    private static OrcTypeKind toTypeKind(OrcProto.Type.Kind typeKind)
    {
        switch (typeKind) {
            case BOOLEAN:
                return OrcTypeKind.BOOLEAN;
            case BYTE:
                return OrcTypeKind.BYTE;
            case SHORT:
                return OrcTypeKind.SHORT;
            case INT:
                return OrcTypeKind.INT;
            case LONG:
                return OrcTypeKind.LONG;
            case FLOAT:
                return OrcTypeKind.FLOAT;
            case DOUBLE:
                return OrcTypeKind.DOUBLE;
            case STRING:
                return OrcTypeKind.STRING;
            case BINARY:
                return OrcTypeKind.BINARY;
            case TIMESTAMP:
                return OrcTypeKind.TIMESTAMP;
            case TIMESTAMP_INSTANT:
                return OrcTypeKind.TIMESTAMP_INSTANT;
            case LIST:
                return OrcTypeKind.LIST;
            case MAP:
                return OrcTypeKind.MAP;
            case STRUCT:
                return OrcTypeKind.STRUCT;
            case UNION:
                return OrcTypeKind.UNION;
            case DECIMAL:
                return OrcTypeKind.DECIMAL;
            case DATE:
                return OrcTypeKind.DATE;
            case VARCHAR:
                return OrcTypeKind.VARCHAR;
            case CHAR:
                return OrcTypeKind.CHAR;
        }
        throw new IllegalStateException(typeKind + " stream type not implemented yet");
    }

    // This method assumes type attributes have no duplicate key
    private static Map<String, String> toMap(List<OrcProto.StringPair> attributes)
    {
        ImmutableMap.Builder<String, String> results = ImmutableMap.builder();
        if (attributes != null) {
            for (OrcProto.StringPair attribute : attributes) {
                if (attribute.hasKey() && attribute.hasValue()) {
                    results.put(attribute.getKey(), attribute.getValue());
                }
            }
        }
        return results.buildOrThrow();
    }

    private static StreamKind toStreamKind(OrcProto.Stream.Kind streamKind)
    {
        switch (streamKind) {
            case PRESENT:
                return StreamKind.PRESENT;
            case DATA:
                return StreamKind.DATA;
            case LENGTH:
                return StreamKind.LENGTH;
            case DICTIONARY_DATA:
                return StreamKind.DICTIONARY_DATA;
            case DICTIONARY_COUNT:
                return StreamKind.DICTIONARY_COUNT;
            case SECONDARY:
                return StreamKind.SECONDARY;
            case ROW_INDEX:
                return StreamKind.ROW_INDEX;
            case BLOOM_FILTER:
                return StreamKind.BLOOM_FILTER;
            case BLOOM_FILTER_UTF8:
                return StreamKind.BLOOM_FILTER_UTF8;
            case ENCRYPTED_INDEX:
            case ENCRYPTED_DATA:
            case STRIPE_STATISTICS:
            case FILE_STATISTICS:
                // unsupported
                break;
        }
        throw new IllegalStateException(streamKind + " stream type not implemented yet");
    }

    private static ColumnEncodingKind toColumnEncodingKind(OrcProto.ColumnEncoding.Kind columnEncodingKind)
    {
        switch (columnEncodingKind) {
            case DIRECT:
                return ColumnEncodingKind.DIRECT;
            case DIRECT_V2:
                return ColumnEncodingKind.DIRECT_V2;
            case DICTIONARY:
                return ColumnEncodingKind.DICTIONARY;
            case DICTIONARY_V2:
                return ColumnEncodingKind.DICTIONARY_V2;
        }
        throw new IllegalStateException(columnEncodingKind + " stream encoding not implemented yet");
    }

    private static CompressionKind toCompression(OrcProto.CompressionKind compression)
    {
        switch (compression) {
            case NONE:
                return NONE;
            case ZLIB:
                return ZLIB;
            case SNAPPY:
                return SNAPPY;
            case LZO:
                // TODO unsupported
                break;
            case LZ4:
                return LZ4;
            case ZSTD:
                return ZSTD;
        }
        throw new IllegalStateException(compression + " compression not implemented yet");
    }
}
