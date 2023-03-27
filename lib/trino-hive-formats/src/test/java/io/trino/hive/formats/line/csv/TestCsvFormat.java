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
package io.trino.hive.formats.line.csv;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.hive.formats.line.LineSerializer;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.RowType;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.OpenCSVSerde;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.hive.formats.FormatTestUtils.createLineBuffer;
import static io.trino.hive.formats.FormatTestUtils.getJavaObjectInspector;
import static io.trino.hive.formats.FormatTestUtils.toSingleRowPage;
import static io.trino.hive.formats.line.csv.CsvConstants.ESCAPE_KEY;
import static io.trino.hive.formats.line.csv.CsvConstants.QUOTE_KEY;
import static io.trino.hive.formats.line.csv.CsvConstants.SEPARATOR_KEY;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCsvFormat
{
    @Test
    public void testCsv()
            throws Exception
    {
        assertLine(true, "", Arrays.asList(null, null, null));
        assertLine(true, " ", Arrays.asList(" ", null, null));

        assertLine(true, "a,b,c", Arrays.asList("a", "b", "c"));
        assertLine(true, "a,b", Arrays.asList("a", "b", null));
        assertLine(true, "a,b,", Arrays.asList("a", "b", ""));
        assertLine(true, "a", Arrays.asList("a", null, null));
        assertLine(true, "a,", Arrays.asList("a", "", null));
        assertLine(true, "a,,", Arrays.asList("a", "", ""));

        // Leading and trailing whitespace is preserved
        assertLine(true, "  a  ,  b  ,  c  ", Arrays.asList("  a  ", "  b  ", "  c  "));

        // Quoting
        assertLine(true, "\"a\",\"b\",\"c\"", Arrays.asList("a", "b", "c"));
        // Quoting with escaped data
        assertLine(false, "\"a\\\"a\",\"b\\,b\",\"c\\\\c\"", Arrays.asList("a\"a", "b,b", "c\\c"));
        // Non-special characters can also be escaped
        assertLine(true, "\"a\\xa\",\"b\\yb\",\"c\\zc\"", Arrays.asList("axa", "byb", "czc"));
        // Quote character can also be escaped with a quote in a quoted field
        assertLine(true, "\"a\"\"a\",b,c", Arrays.asList("a\"a", "b", "c"));
        // Separator character does not have to be escaped in a quoted value
        assertLine(true, "\"a,a\",b,c", Arrays.asList("a,a", "b", "c"));
        // Unterminated quoted value is just ignored
        assertLine(true, "\"a,b,c", Arrays.asList(null, null, null));
        assertLine(true, "a,b,\"c", Arrays.asList("a", "b", null));

        // extra data past end of line is ignored
        assertLine(true, "a,b,c,anything can go here , \" \\", Arrays.asList("a", "b", "c"));

        // Escape is allowed for unquoted data
        assertLine(false, "a\\\"a,b\\\\b,c", Arrays.asList("a\"a", "b\\b", "c"));
        // but separator character can not be escaped without of a quote
        // NOTE escape character is dropped here
        assertLine(true, "a\\,a,b,c", Arrays.asList("a", "a", "b"));

        // Quote is allowed in unquoted field (after first two characters)
        assertLine(true, "12x\"a\"x,x\"b\"x,x\"c\"x", Arrays.asList("12x\"a\"x", "x\"b\"x", "x\"c\"x"));
        // but close quote is ignored if it appears immediately before a separator
        assertLine(true, "12x\"a\",x\"b\",x\"c\"", Arrays.asList("12x\"a", "x\"b", "x\"c"));
        // but quoting rules are applied, so strings must match
        // NOTE: last field is ignored ue to unterminated quoted string
        assertLine(true, "12x\"ax,x\"bx,x\"cx", Arrays.asList("12x\"ax,x\"bx", null, null));

        // whitespace before a quoted it normally ignored, except for the first two characters at the beginning of the line
        assertLine(true, " \"a\",  \"b\",  \"c\"", Arrays.asList(" a", "b", "c"));
        assertLine(true, "  \"a\",  \"b\",  \"c\"", Arrays.asList("  a", "b", "c"));
        assertLine(true, "   \"a\",  \"b\",  \"c\"", Arrays.asList("a", "b", "c"));

        // trailing white space is not ignored, and closing quote is considered part of the value
        // Note: the quote is ignored in the first value due to strange special handling at beginning of line
        assertLine(true, "\"a\"  ,  \"b\"  ,  \"c\"  ", Arrays.asList("a  ", "b\"  ", "c\"  "));

        // If quote character is `\0` then quoting is simply disabled, even if this would cause output that does not round trip
        assertTrinoHiveByteForByte(true, Arrays.asList("foo", "bar", "baz"), Optional.of('\t'), Optional.of('\0'), Optional.of('\\'));
        assertTrinoHiveByteForByte(false, Arrays.asList("f\0\0", "\0bar\0", "baz"), Optional.of('\t'), Optional.of('\0'), Optional.of('\\'));

        // If escape character is `\0` then escaping is simply disabled, even if this would cause output that does not round trip
        assertTrinoHiveByteForByte(true, Arrays.asList("f**", "b*r", "b*z"), Optional.of('\t'), Optional.of('*'), Optional.of('#'));
        assertTrinoHiveByteForByte(false, Arrays.asList("f**", "b*r", "b*z"), Optional.of('\t'), Optional.of('*'), Optional.of('\0'));

        // These cases don't round trip, because Hive uses different default escape characters for serialization and deserialization.
        // For serialization the pipe character is escaped with a quote char, but for deserialization escape character is the backslash character
        assertTrinoHiveByteForByte(false, Arrays.asList("|", "a", "b"), Optional.empty(), Optional.of('|'), Optional.empty());
        assertTrinoHiveByteForByte(false, Arrays.asList("|", "a", "|"), Optional.empty(), Optional.of('|'), Optional.empty());
    }

    private static void assertLine(boolean shouldRoundTrip, String csvLine, List<String> expectedValues)
            throws Exception
    {
        assertHiveLine(csvLine, expectedValues, Optional.empty(), Optional.empty(), Optional.empty());
        assertTrinoLine(csvLine, expectedValues, Optional.empty(), Optional.empty(), Optional.empty());
        assertTrinoHiveByteForByte(shouldRoundTrip, expectedValues, Optional.empty(), Optional.empty(), Optional.empty());

        csvLine = rewriteSpecialChars(csvLine, '_', '|', '~');
        expectedValues = expectedValues.stream()
                .map(value -> value == null ? null : rewriteSpecialChars(value, '_', '|', '~'))
                .collect(Collectors.toList());

        assertHiveLine(csvLine, expectedValues, Optional.of('_'), Optional.of('|'), Optional.of('~'));
        assertTrinoLine(csvLine, expectedValues, Optional.of('_'), Optional.of('|'), Optional.of('~'));
        // after switching the special characters the values will round trip
        assertTrinoHiveByteForByte(true, expectedValues, Optional.of('_'), Optional.of('|'), Optional.of('~'));
    }

    private static String rewriteSpecialChars(String csvLine, char newSeparator, char newQuote, char newEscape)
    {
        return csvLine.replace(',', newSeparator)
                .replace('\"', newQuote)
                .replace('\\', newEscape);
    }

    private static void assertTrinoHiveByteForByte(
            boolean shouldRoundTrip,
            List<String> expectedValues,
            Optional<Character> separatorChar,
            Optional<Character> quoteChar,
            Optional<Character> escapeChar)
            throws SerDeException, IOException
    {
        String trinoLine = writeTrinoLine(createReadColumns(expectedValues.size()), expectedValues, separatorChar, quoteChar, escapeChar);
        String hiveLine = writeHiveLine(expectedValues, separatorChar, quoteChar, escapeChar);
        assertThat(trinoLine).isEqualTo(hiveLine);

        // verify csv can be decoded by both engines using this configuration
        // NOTE: nulls become empty because a null can not be written to CSV
        expectedValues = expectedValues.stream().map(Strings::nullToEmpty).toList();
        List<String> trinoActualValues = readTrinoLine(expectedValues.size(), trinoLine, separatorChar, quoteChar, escapeChar);

        Object hiveActualValues = readHiveLine(expectedValues.size(), trinoLine, separatorChar, quoteChar, escapeChar);
        assertThat(trinoActualValues).isEqualTo(hiveActualValues);
        if (shouldRoundTrip) {
            assertThat(trinoActualValues).isEqualTo(expectedValues);
        }
        else {
            assertThat(trinoActualValues).isNotEqualTo(expectedValues);
        }
    }

    private static void assertTrinoLine(String csvLine, List<String> expectedValues, Optional<Character> separatorChar, Optional<Character> quoteChar, Optional<Character> escapeChar)
            throws IOException
    {
        List<String> actualValues = readTrinoLine(expectedValues.size(), csvLine, separatorChar, quoteChar, escapeChar);
        assertThat(actualValues).isEqualTo(expectedValues);
    }

    private static List<String> readTrinoLine(int columnCount, String csvLine, Optional<Character> separatorChar, Optional<Character> quoteChar, Optional<Character> escapeChar)
            throws IOException
    {
        LineDeserializer deserializer = new CsvDeserializerFactory().create(createReadColumns(columnCount), createCsvProperties(separatorChar, quoteChar, escapeChar));

        PageBuilder pageBuilder = new PageBuilder(1, deserializer.getTypes());
        deserializer.deserialize(createLineBuffer(csvLine), pageBuilder);
        Page page = pageBuilder.build();
        return IntStream.range(0, deserializer.getTypes().size())
                .mapToObj(page::getBlock)
                .map(block -> block.isNull(0) ? null : VARCHAR.getSlice(block, 0).toStringUtf8())
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private static String writeTrinoLine(List<Column> readColumns, List<String> expectedValues, Optional<Character> separatorChar, Optional<Character> quoteChar, Optional<Character> escapeChar)
            throws IOException
    {
        Page page = toSingleRowPage(readColumns, expectedValues);

        LineSerializer serializer = new CsvSerializerFactory().create(readColumns, createCsvProperties(separatorChar, quoteChar, escapeChar));
        SliceOutput sliceOutput = new DynamicSliceOutput(1024);
        serializer.write(page, 0, sliceOutput);
        return sliceOutput.slice().toStringUtf8();
    }

    private static List<Column> createReadColumns(int columnCount)
    {
        return IntStream.range(0, columnCount)
                .mapToObj(ordinal -> new Column("field_" + ordinal, VARCHAR, ordinal))
                .toList();
    }

    private static void assertHiveLine(String csvLine, List<String> expectedValues, Optional<Character> separatorChar, Optional<Character> quoteChar, Optional<Character> escapeChar)
            throws SerDeException
    {
        Object actualValues = readHiveLine(expectedValues.size(), csvLine, separatorChar, quoteChar, escapeChar);
        assertThat(actualValues).isEqualTo(expectedValues);
    }

    private static Object readHiveLine(int columnCount, String csvLine, Optional<Character> separatorChar, Optional<Character> quoteChar, Optional<Character> escapeChar)
            throws SerDeException
    {
        Deserializer deserializer = createHiveSerDe(columnCount, separatorChar, quoteChar, escapeChar);
        return deserializer.deserialize(new Text(csvLine));
    }

    private static String writeHiveLine(List<String> expectedValues, Optional<Character> separatorChar, Optional<Character> quoteChar, Optional<Character> escapeChar)
            throws SerDeException
    {
        Serializer serializer = createHiveSerDe(expectedValues.size(), separatorChar, quoteChar, escapeChar);
        ObjectInspector javaObjectInspector = getJavaObjectInspector(RowType.from(IntStream.range(0, expectedValues.size())
                .mapToObj(i -> field("value_" + i, VARCHAR))
                .toList()));
        return serializer.serialize(expectedValues, javaObjectInspector).toString();
    }

    private static OpenCSVSerde createHiveSerDe(int columnCount, Optional<Character> separatorChar, Optional<Character> quoteChar, Optional<Character> escapeChar)
            throws SerDeException
    {
        JobConf configuration = new JobConf(newEmptyConfiguration());

        Properties schema = new Properties();
        schema.setProperty(META_TABLE_COLUMNS, IntStream.range(0, columnCount).mapToObj(i -> "value_" + i).collect(joining(",")));
        schema.setProperty(META_TABLE_COLUMN_TYPES, String.join(",", nCopies(columnCount, "STRING")));
        schema.putAll(createCsvProperties(separatorChar, quoteChar, escapeChar));

        OpenCSVSerde deserializer = new OpenCSVSerde();
        deserializer.initialize(configuration, schema);
        configuration.set(SERIALIZATION_LIB, deserializer.getClass().getName());
        return deserializer;
    }

    private static Map<String, String> createCsvProperties(Optional<Character> separatorChar, Optional<Character> quoteChar, Optional<Character> escapeChar)
    {
        ImmutableMap.Builder<String, String> schema = ImmutableMap.builder();
        separatorChar.ifPresent(character -> schema.put(SEPARATOR_KEY, String.valueOf(character)));
        quoteChar.ifPresent(character -> schema.put(QUOTE_KEY, String.valueOf(character)));
        escapeChar.ifPresent(character -> schema.put(ESCAPE_KEY, String.valueOf(character)));
        return schema.buildOrThrow();
    }
}
