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
package io.trino.hive.formats.encodings.text;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.hive.formats.HiveFormatUtils.TIMESTAMP_FORMATS_KEY;
import static io.trino.hive.formats.HiveFormatUtils.getTimestampFormatsSchemaProperty;
import static io.trino.hive.formats.encodings.text.TextEncodingOptions.NestingLevels.EXTENDED;
import static io.trino.hive.formats.encodings.text.TextEncodingOptions.NestingLevels.EXTENDED_ADDITIONAL;

public class TextEncodingOptions
{
    private static final String FORMAT_KEY = "serialization.format";
    private static final String NULL_FORMAT_KEY = "serialization.null.format";
    private static final String LAST_COLUMN_TAKES_REST_KEY = "serialization.last.column.takes.rest";

    private static final String FIELD_DELIMITER_KEY = "field.delim";
    private static final String COLLECTION_DELIMITER_KEY = "collection.delim";
    private static final String MAP_KEY_DELIMITER_KEY = "mapkey.delim";
    private static final String ESCAPE_CHAR_KEY = "escape.delim";

    private static final Slice DEFAULT_NULL_SEQUENCE = Slices.utf8Slice("\\N");

    private static final byte[] DEFAULT_SEPARATORS = new byte[] {
            1,  // Start of Heading
            2,  // Start of text
            3,  // End of Text
            4,  // End of Transmission
            5,  // Enquiry
            6,  // Acknowledge
            7,  // Bell
            8,  // Backspace
            // RESERVED 9,  // Horizontal Tab
            // RESERVED 10, // Line Feed
            11, // Vertical Tab
            // RESERVED 12, // Form Feed
            // RESERVED 13, // Carriage Return
            14, // Shift Out
            15, // Shift In
            16, // Data Link Escape
            17, // Device Control One
            18, // Device Control Two
            19, // Device Control Three
            20, // Device Control Four
            21, // Negative Acknowledge
            22, // Synchronous Idle
            23, // End of Transmission Block
            24, // Cancel
            25, // End of medium
            26, // Substitute
            // RESERVED 27, // Escape
            28, // File Separator
            29, // Group separator
            30, // Record Separator
            31, // Unit separator
            // All negative values
            -128, -127, -126, -125, -124, -123, -122, -121, -120, -119, -118, -117, -116, -115, -114, -113,
            -112, -111, -110, -109, -108, -107, -106, -105, -104, -103, -102, -101, -100, -99, -98, -97,
            -96, -95, -94, -93, -92, -91, -90, -89, -88, -87, -86, -85, -84, -83, -82, -81,
            -80, -79, -78, -77, -76, -75, -74, -73, -72, -71, -70, -69, -68, -67, -66, -65,
            -64, -63, -62, -61, -60, -59, -58, -57, -56, -55, -54, -53, -52, -51, -50, -49,
            -48, -47, -46, -45, -44, -43, -42, -41, -40, -39, -38, -37, -36, -35, -34, -33,
            -32, -31, -30, -29, -28, -27, -26, -25, -24, -23, -22, -21, -20, -19, -18, -17,
            -16, -15, -14, -13, -12, -11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1};

    public enum NestingLevels
    {
        LEGACY(8, null),
        EXTENDED(24, "hive.serialization.extend.nesting.levels"),
        EXTENDED_ADDITIONAL(DEFAULT_SEPARATORS.length, "hive.serialization.extend.additional.nesting.levels");

        private final int levels;
        private final String tableProperty;

        NestingLevels(int levels, String tableProperty)
        {
            this.levels = levels;
            this.tableProperty = tableProperty;
        }

        public int getLevels()
        {
            return levels;
        }

        public String getTableProperty()
        {
            return tableProperty;
        }

        public Slice getSeparators(byte fieldDelimiter, byte collectionDelimiter, byte mapKeyDelimiter)
        {
            byte[] separators = Arrays.copyOf(DEFAULT_SEPARATORS, levels);
            separators[0] = fieldDelimiter;
            separators[1] = collectionDelimiter;
            separators[2] = mapKeyDelimiter;
            return Slices.wrappedBuffer(separators);
        }
    }

    public static final TextEncodingOptions DEFAULT_SIMPLE_OPTIONS = builder().build();

    private final Slice nullSequence;
    private final NestingLevels nestingLevels;
    private final Slice separators;
    private final Byte escapeByte;
    private final boolean lastColumnTakesRest;
    private final List<String> timestampFormats;

    private TextEncodingOptions(
            Slice nullSequence,
            NestingLevels nestingLevels,
            Slice separators,
            Byte escapeByte,
            boolean lastColumnTakesRest,
            List<String> timestampFormats)
    {
        this.nullSequence = nullSequence;
        this.nestingLevels = nestingLevels;
        this.separators = separators;
        this.escapeByte = escapeByte;
        this.lastColumnTakesRest = lastColumnTakesRest;
        this.timestampFormats = timestampFormats;
    }

    public Slice getNullSequence()
    {
        return nullSequence;
    }

    public NestingLevels getNestingLevels()
    {
        return nestingLevels;
    }

    public Slice getSeparators()
    {
        return separators;
    }

    public Byte getEscapeByte()
    {
        return escapeByte;
    }

    public boolean isLastColumnTakesRest()
    {
        return lastColumnTakesRest;
    }

    public List<String> getTimestampFormats()
    {
        return timestampFormats;
    }

    public Map<String, String> toSchema()
    {
        ImmutableMap.Builder<String, String> schema = ImmutableMap.builder();

        // nesting levels
        if (nestingLevels.getTableProperty() != null) {
            schema.put(nestingLevels.getTableProperty(), "true");
        }

        addSeparatorProperty(schema, 0, FIELD_DELIMITER_KEY);
        addSeparatorProperty(schema, 1, COLLECTION_DELIMITER_KEY);
        addSeparatorProperty(schema, 2, MAP_KEY_DELIMITER_KEY);

        if (!DEFAULT_NULL_SEQUENCE.equals(nullSequence)) {
            schema.put(NULL_FORMAT_KEY, nullSequence.toStringUtf8());
        }
        if (lastColumnTakesRest) {
            schema.put(LAST_COLUMN_TAKES_REST_KEY, "true");
        }
        if (escapeByte != null) {
            schema.put(ESCAPE_CHAR_KEY, String.valueOf(escapeByte));
        }
        if (!timestampFormats.isEmpty()) {
            schema.put(TIMESTAMP_FORMATS_KEY, timestampFormats.stream()
                    .map(format -> format.replace("\\", "\\\\"))
                    .map(format -> format.replace(",", "\\,"))
                    .collect(Collectors.joining(",")));
        }
        return schema.buildOrThrow();
    }

    private void addSeparatorProperty(ImmutableMap.Builder<String, String> schema, int index, String propertyName)
    {
        if (separators.getByte(index) != DEFAULT_SEPARATORS[index]) {
            schema.put(propertyName, String.valueOf(separators.getByte(index)));
        }
    }

    public static TextEncodingOptions fromSchema(Map<String, String> serdeProperties)
    {
        Builder builder = builder();

        if ("true".equalsIgnoreCase(serdeProperties.get(EXTENDED_ADDITIONAL.getTableProperty()))) {
            builder.extendedAdditionalNestingLevels();
        }
        else if ("true".equalsIgnoreCase(serdeProperties.get(EXTENDED.getTableProperty()))) {
            builder.extendedNestingLevels();
        }

        // the first three separators are set by old-old properties
        builder.fieldDelimiter(getByte(serdeProperties.getOrDefault(FIELD_DELIMITER_KEY, serdeProperties.get(FORMAT_KEY)), DEFAULT_SEPARATORS[0]));
        // for map field collection delimiter, Hive 1.x uses "colelction.delim" but Hive 3.x uses "collection.delim"
        // https://issues.apache.org/jira/browse/HIVE-16922
        builder.collectionDelimiter(getByte(serdeProperties.getOrDefault(COLLECTION_DELIMITER_KEY, serdeProperties.get("colelction.delim")), DEFAULT_SEPARATORS[1]));
        builder.mapKeyDelimiter(getByte(serdeProperties.get(MAP_KEY_DELIMITER_KEY), DEFAULT_SEPARATORS[2]));

        // null sequence
        String nullSequenceString = serdeProperties.get(NULL_FORMAT_KEY);
        if (nullSequenceString != null) {
            builder.nullSequence(Slices.utf8Slice(nullSequenceString));
        }

        // last column takes rest
        String lastColumnTakesRestString = serdeProperties.get(LAST_COLUMN_TAKES_REST_KEY);
        if ("true".equalsIgnoreCase(lastColumnTakesRestString)) {
            builder.lastColumnTakesRest();
        }

        // escaped
        String escapeProperty = serdeProperties.get(ESCAPE_CHAR_KEY);
        if (escapeProperty != null) {
            builder.escapeByte(getByte(escapeProperty, (byte) '\\'));
        }

        // timestamp formats
        builder.timestampFormats(getTimestampFormatsSchemaProperty(serdeProperties));

        return builder.build();
    }

    private static byte getByte(String value, byte defaultValue)
    {
        if (Strings.isNullOrEmpty(value)) {
            return defaultValue;
        }

        try {
            return Byte.parseByte(value);
        }
        catch (NumberFormatException e) {
            return (byte) value.charAt(0);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(TextEncodingOptions textEncodingOptions)
    {
        return new Builder(textEncodingOptions);
    }

    public static class Builder
    {
        private Slice nullSequence = DEFAULT_NULL_SEQUENCE;
        private NestingLevels nestingLevels = NestingLevels.LEGACY;
        private byte fieldDelimiter = DEFAULT_SEPARATORS[0];
        private byte collectionDelimiter = DEFAULT_SEPARATORS[1];
        private byte mapKeyDelimiter = DEFAULT_SEPARATORS[2];
        private Byte escapeByte;
        private boolean lastColumnTakesRest;
        private List<String> timestampFormats = ImmutableList.of();

        public Builder() {}

        public Builder(TextEncodingOptions textEncodingOptions)
        {
            nullSequence = textEncodingOptions.getNullSequence();
            nestingLevels = textEncodingOptions.getNestingLevels();
            fieldDelimiter = textEncodingOptions.getSeparators().getByte(0);
            collectionDelimiter = textEncodingOptions.getSeparators().getByte(1);
            mapKeyDelimiter = textEncodingOptions.getSeparators().getByte(2);
            escapeByte = textEncodingOptions.getEscapeByte();
            lastColumnTakesRest = textEncodingOptions.isLastColumnTakesRest();
            timestampFormats = textEncodingOptions.getTimestampFormats();
        }

        public Builder nullSequence(Slice nullSequence)
        {
            this.nullSequence = nullSequence;
            return this;
        }

        public Builder extendedNestingLevels()
        {
            return nestingLevels(EXTENDED);
        }

        public Builder extendedAdditionalNestingLevels()
        {
            return nestingLevels(EXTENDED_ADDITIONAL);
        }

        public Builder nestingLevels(NestingLevels nestingLevels)
        {
            this.nestingLevels = nestingLevels;
            return this;
        }

        public Builder fieldDelimiter(byte fieldDelimiter)
        {
            this.fieldDelimiter = fieldDelimiter;
            return this;
        }

        public Builder collectionDelimiter(byte collectionDelimiter)
        {
            this.collectionDelimiter = collectionDelimiter;
            return this;
        }

        public Builder mapKeyDelimiter(byte mapKeyDelimiter)
        {
            this.mapKeyDelimiter = mapKeyDelimiter;
            return this;
        }

        public Builder escapeByte(byte escapeByte)
        {
            this.escapeByte = escapeByte;
            return this;
        }

        public Builder lastColumnTakesRest()
        {
            this.lastColumnTakesRest = true;
            return this;
        }

        public Builder timestampFormats(String... timestampFormats)
        {
            return timestampFormats(ImmutableList.copyOf(timestampFormats));
        }

        public Builder timestampFormats(List<String> timestampFormats)
        {
            this.timestampFormats = timestampFormats;
            return this;
        }

        public TextEncodingOptions build()
        {
            Slice separators = nestingLevels.getSeparators(fieldDelimiter, collectionDelimiter, mapKeyDelimiter);
            return new TextEncodingOptions(nullSequence, nestingLevels, separators, escapeByte, lastColumnTakesRest, timestampFormats);
        }
    }
}
