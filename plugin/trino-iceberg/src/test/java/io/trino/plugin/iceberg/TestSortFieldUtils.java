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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.iceberg.SortFieldUtils.parseSortFields;
import static io.trino.plugin.iceberg.SortFieldUtils.toSortFields;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSortFieldUtils
{
    @Test
    public void testParse()
    {
        assertParse("order_key", sortOrder(builder -> builder.asc("order_key")), "order_key ASC NULLS FIRST");
        assertParse("order_key ASC", sortOrder(builder -> builder.asc("order_key")), "order_key ASC NULLS FIRST");
        assertParse("order_key ASC NULLS FIRST", sortOrder(builder -> builder.asc("order_key")), "order_key ASC NULLS FIRST");
        assertParse("order_key ASC NULLS FIRST", sortOrder(builder -> builder.asc("order_key", NullOrder.NULLS_FIRST)), "order_key ASC NULLS FIRST");
        assertParse("order_key ASC NULLS LAST", sortOrder(builder -> builder.asc("order_key", NullOrder.NULLS_LAST)), "order_key ASC NULLS LAST");
        assertParse("order_key DESC", sortOrder(builder -> builder.desc("order_key")), "order_key DESC NULLS LAST");
        assertParse("order_key DESC NULLS FIRST", sortOrder(builder -> builder.desc("order_key", NullOrder.NULLS_FIRST)), "order_key DESC NULLS FIRST");
        assertParse("order_key DESC NULLS LAST", sortOrder(builder -> builder.desc("order_key", NullOrder.NULLS_LAST)), "order_key DESC NULLS LAST");
        assertParse("order_key DESC NULLS LAST", sortOrder(builder -> builder.desc("order_key")), "order_key DESC NULLS LAST");

        // lowercase
        assertParse("order_key asc nulls last", sortOrder(builder -> builder.asc("order_key", NullOrder.NULLS_LAST)), "order_key ASC NULLS LAST");
        assertParse("order_key desc nulls first", sortOrder(builder -> builder.desc("order_key", NullOrder.NULLS_FIRST)), "order_key DESC NULLS FIRST");
        assertParse("\"order_key\" asc nulls last", sortOrder(builder -> builder.asc("order_key", NullOrder.NULLS_LAST)), "order_key ASC NULLS LAST");
        assertParse("\"order_key\" desc nulls first", sortOrder(builder -> builder.desc("order_key", NullOrder.NULLS_FIRST)), "order_key DESC NULLS FIRST");

        // uppercase
        assertParse("ORDER_KEY ASC NULLS LAST", sortOrder(builder -> builder.asc("order_key", NullOrder.NULLS_LAST)), "order_key ASC NULLS LAST");
        assertParse("ORDER_KEY DESC NULLS FIRST", sortOrder(builder -> builder.desc("order_key", NullOrder.NULLS_FIRST)), "order_key DESC NULLS FIRST");
        assertDoesNotParse("\"ORDER_KEY\" ASC NULLS LAST", "Cannot find field 'ORDER_KEY' .*");
        assertDoesNotParse("\"ORDER_KEY\" DESC NULLS FIRST", "Cannot find field 'ORDER_KEY' .*");

        // mixed case
        assertParse("\"MixedCase\" ASC NULLS LAST", sortOrder(builder -> builder.asc("MixedCase", NullOrder.NULLS_LAST)), "\"MixedCase\" ASC NULLS LAST");
        assertParse("\"MixedCase\" DESC NULLS FIRST", sortOrder(builder -> builder.desc("MixedCase", NullOrder.NULLS_FIRST)), "\"MixedCase\" DESC NULLS FIRST");
        assertParse("OrDER_keY Asc NullS LAst", sortOrder(builder -> builder.asc("order_key", NullOrder.NULLS_LAST)), "order_key ASC NULLS LAST");
        assertParse("OrDER_keY Desc NullS FIrsT", sortOrder(builder -> builder.desc("order_key", NullOrder.NULLS_FIRST)), "order_key DESC NULLS FIRST");
        assertDoesNotParse("\"OrDER_keY\" Asc NullS LAst", "Cannot find field 'OrDER_keY' .*");
        assertDoesNotParse("\"OrDER_keY\" Desc NullS FIrsT", "Cannot find field 'OrDER_keY' .*");

        assertParse("comment", sortOrder(builder -> builder.asc("comment")), "comment ASC NULLS FIRST");
        assertParse("\"comment\"", sortOrder(builder -> builder.asc("comment")), "comment ASC NULLS FIRST");
        assertParse("\"quoted field\"", sortOrder(builder -> builder.asc("quoted field")), "\"quoted field\" ASC NULLS FIRST");
        assertParse("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\"", sortOrder(builder -> builder.asc("\"another\" \"quoted\" \"field\"")), "\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\" ASC NULLS FIRST");
        assertParse("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\" ASC    NULLS   FIRST  ", sortOrder(builder -> builder.asc("\"another\" \"quoted\" \"field\"")), "\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\" ASC NULLS FIRST");
        assertParse("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\" ASC    NULLS   LAST    ", sortOrder(builder -> builder.asc("\"another\" \"quoted\" \"field\"", NullOrder.NULLS_LAST)), "\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\" ASC NULLS LAST");
        assertParse("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\" DESC NULLS FIRST", sortOrder(builder -> builder.desc("\"another\" \"quoted\" \"field\"", NullOrder.NULLS_FIRST)), "\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\" DESC NULLS FIRST");
        assertParse(" comment   ", sortOrder(builder -> builder.asc("comment")), "comment ASC NULLS FIRST");
        assertParse("comment ASC", sortOrder(builder -> builder.asc("comment")), "comment ASC NULLS FIRST");
        assertParse("  comment    ASC  ", sortOrder(builder -> builder.asc("comment")), "comment ASC NULLS FIRST");
        assertParse("comment ASC NULLS FIRST", sortOrder(builder -> builder.asc("comment")), "comment ASC NULLS FIRST");
        assertParse("  comment    ASC     NULLS     FIRST    ", sortOrder(builder -> builder.asc("comment")), "comment ASC NULLS FIRST");
        assertParse("comment ASC NULLS FIRST", sortOrder(builder -> builder.asc("comment", NullOrder.NULLS_FIRST)), "comment ASC NULLS FIRST");
        assertParse("     comment   ASC       NULLS       FIRST    ", sortOrder(builder -> builder.asc("comment", NullOrder.NULLS_FIRST)), "comment ASC NULLS FIRST");
        assertParse("comment ASC NULLS FIRST", sortOrder(builder -> builder.asc("comment", NullOrder.NULLS_FIRST)), "comment ASC NULLS FIRST");
        assertParse("    comment     ASC    NULLS   FIRST      ", sortOrder(builder -> builder.asc("comment", NullOrder.NULLS_FIRST)), "comment ASC NULLS FIRST");
        assertParse("comment ASC NULLS LAST", sortOrder(builder -> builder.asc("comment", NullOrder.NULLS_LAST)), "comment ASC NULLS LAST");
        assertParse("  comment   ASC    NULLS     LAST    ", sortOrder(builder -> builder.asc("comment", NullOrder.NULLS_LAST)), "comment ASC NULLS LAST");
        assertParse("comment DESC", sortOrder(builder -> builder.desc("comment")), "comment DESC NULLS LAST");
        assertParse("  comment   DESC  ", sortOrder(builder -> builder.desc("comment")), "comment DESC NULLS LAST");
        assertParse("comment DESC NULLS FIRST", sortOrder(builder -> builder.desc("comment", NullOrder.NULLS_FIRST)), "comment DESC NULLS FIRST");
        assertParse("  comment     DESC  NULLS   FIRST ", sortOrder(builder -> builder.desc("comment", NullOrder.NULLS_FIRST)), "comment DESC NULLS FIRST");
        assertParse("comment DESC NULLS LAST", sortOrder(builder -> builder.desc("comment", NullOrder.NULLS_LAST)), "comment DESC NULLS LAST");
        assertParse("  comment   DESC    NULLS   LAST   ", sortOrder(builder -> builder.desc("comment", NullOrder.NULLS_LAST)), "comment DESC NULLS LAST");
        assertParse("comment DESC NULLS LAST", sortOrder(builder -> builder.desc("comment")), "comment DESC NULLS LAST");
        assertParse("    comment     DESC   NULLS    LAST   ", sortOrder(builder -> builder.desc("comment")), "comment DESC NULLS LAST");

        // transforms
        assertParse("bucket(comment, 3)", sortOrder(builder -> builder.asc(Expressions.bucket("comment", 3))), "bucket(comment, 3) ASC NULLS FIRST");
        assertParse("truncate(comment, 3)", sortOrder(builder -> builder.asc(Expressions.truncate("comment", 3))), "truncate(comment, 3) ASC NULLS FIRST");
        assertParse("year(ts)", sortOrder(builder -> builder.asc(Expressions.year("ts"))), "year(ts) ASC NULLS FIRST");
        assertParse("month(ts)", sortOrder(builder -> builder.asc(Expressions.month("ts"))), "month(ts) ASC NULLS FIRST");
        assertParse("day(ts)", sortOrder(builder -> builder.asc(Expressions.day("ts"))), "day(ts) ASC NULLS FIRST");
        assertParse("hour(ts)", sortOrder(builder -> builder.asc(Expressions.hour("ts"))), "hour(ts) ASC NULLS FIRST");
        assertParse("void(comment)", sortOrder(builder -> builder.asc(Expressions.transform("comment", Transforms.alwaysNull()))), "void(comment) ASC NULLS FIRST");

        assertParse("bucket(comment, 3) ASC", sortOrder(builder -> builder.asc(Expressions.bucket("comment", 3))), "bucket(comment, 3) ASC NULLS FIRST");
        assertParse("bucket(comment, 3) ASC NULLS LAST", sortOrder(builder -> builder.asc(Expressions.bucket("comment", 3), NullOrder.NULLS_LAST)), "bucket(comment, 3) ASC NULLS LAST");
        assertParse("bucket(comment, 3) DESC", sortOrder(builder -> builder.desc(Expressions.bucket("comment", 3))), "bucket(comment, 3) DESC NULLS LAST");
        assertParse("bucket(comment, 3) DESC NULLS FIRST", sortOrder(builder -> builder.desc(Expressions.bucket("comment", 3), NullOrder.NULLS_FIRST)), "bucket(comment, 3) DESC NULLS FIRST");
        assertParse("truncate(\"quoted field\", 5) DESC NULLS FIRST", sortOrder(builder -> builder.desc(Expressions.truncate("quoted field", 5), NullOrder.NULLS_FIRST)), "truncate(\"quoted field\", 5) DESC NULLS FIRST");
        assertParse("year(\"quoted ts\") ASC NULLS LAST", sortOrder(builder -> builder.asc(Expressions.year("quoted ts"), NullOrder.NULLS_LAST)), "year(\"quoted ts\") ASC NULLS LAST");
        assertParse("YEAR(TS) desc nulls first", sortOrder(builder -> builder.desc(Expressions.year("ts"), NullOrder.NULLS_FIRST)), "year(ts) DESC NULLS FIRST");
        assertParse("TRUNCATE(COMMENT, 3) asc nulls last", sortOrder(builder -> builder.asc(Expressions.truncate("comment", 3), NullOrder.NULLS_LAST)), "truncate(comment, 3) ASC NULLS LAST");

        assertDoesNotParse("bucket()");
        assertDoesNotParse("date(ts)");
        assertDoesNotParse("year(comment)", "Cannot bind: year cannot transform string values from 'comment'");
        assertDoesNotParse("month(comment)", "Cannot bind: month cannot transform string values from 'comment'");
        assertDoesNotParse("day(comment)", "Cannot bind: day cannot transform string values from 'comment'");
        assertDoesNotParse("hour(comment)", "Cannot bind: hour cannot transform string values from 'comment'");
        assertDoesNotParse("truncate(ts, 3)", "Cannot bind: truncate\\[3] cannot transform timestamp values from 'ts'");
        assertDoesNotParse("bucket(price, 3)", "Cannot bind: bucket\\[3] cannot transform double values from 'price'");
    }

    private static void assertParse(@Language("SQL") String value, SortOrder expected, String canonicalRepresentation)
    {
        assertThat(expected.fields()).hasSize(1);
        assertThat(parseField(value)).isEqualTo(expected);
        assertThat(getOnlyElement(toSortFields(expected))).isEqualTo(canonicalRepresentation);
    }

    private static void assertDoesNotParse(@Language("SQL") String value)
    {
        assertThatThrownBy(() -> parseField(value))
                .hasMessage("Unable to parse sort field: [%s]".formatted(value));
    }

    private static void assertDoesNotParse(@Language("SQL") String value, @Language("RegExp") String expectedMessage)
    {
        assertThatThrownBy(() -> parseField(value))
                .hasMessageStartingWith("Invalid sorted_by definition: ")
                .cause()
                .hasMessageMatching(expectedMessage);
    }

    private static SortOrder parseField(String value)
    {
        return sortOrder(builder -> parseSortFields(builder, ImmutableList.of(value)));
    }

    private static SortOrder sortOrder(Consumer<SortOrder.Builder> consumer)
    {
        Schema schema = new Schema(
                Types.NestedField.required(1, "order_key", Types.LongType.get()),
                Types.NestedField.required(2, "ts", Types.TimestampType.withoutZone()),
                Types.NestedField.required(3, "price", Types.DoubleType.get()),
                Types.NestedField.optional(4, "comment", Types.StringType.get()),
                Types.NestedField.optional(5, "notes", Types.ListType.ofRequired(6, Types.StringType.get())),
                Types.NestedField.optional(7, "quoted field", Types.StringType.get()),
                Types.NestedField.optional(8, "quoted ts", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(9, "\"another\" \"quoted\" \"field\"", Types.StringType.get()),
                Types.NestedField.optional(10, "MixedCase", Types.StringType.get()));

        SortOrder.Builder builder = SortOrder.builderFor(schema);
        consumer.accept(builder);
        return builder.build();
    }
}
