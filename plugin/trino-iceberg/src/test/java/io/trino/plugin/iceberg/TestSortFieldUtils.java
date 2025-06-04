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
import org.apache.iceberg.types.Types;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static io.trino.plugin.iceberg.SortFieldUtils.parseSortFields;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSortFieldUtils
{
    @Test
    public void testParse()
    {
        assertParse("order_key", sortOrder(builder -> builder.asc("order_key")));
        assertParse("order_key ASC", sortOrder(builder -> builder.asc("order_key")));
        assertParse("order_key ASC NULLS FIRST", sortOrder(builder -> builder.asc("order_key")));
        assertParse("order_key ASC NULLS FIRST", sortOrder(builder -> builder.asc("order_key", NullOrder.NULLS_FIRST)));
        assertParse("order_key ASC NULLS LAST", sortOrder(builder -> builder.asc("order_key", NullOrder.NULLS_LAST)));
        assertParse("order_key DESC", sortOrder(builder -> builder.desc("order_key")));
        assertParse("order_key DESC NULLS FIRST", sortOrder(builder -> builder.desc("order_key", NullOrder.NULLS_FIRST)));
        assertParse("order_key DESC NULLS LAST", sortOrder(builder -> builder.desc("order_key", NullOrder.NULLS_LAST)));
        assertParse("order_key DESC NULLS LAST", sortOrder(builder -> builder.desc("order_key")));

        // lowercase
        assertParse("order_key asc nulls last", sortOrder(builder -> builder.asc("order_key", NullOrder.NULLS_LAST)));
        assertParse("order_key desc nulls first", sortOrder(builder -> builder.desc("order_key", NullOrder.NULLS_FIRST)));
        assertParse("\"order_key\" asc nulls last", sortOrder(builder -> builder.asc("order_key", NullOrder.NULLS_LAST)));
        assertParse("\"order_key\" desc nulls first", sortOrder(builder -> builder.desc("order_key", NullOrder.NULLS_FIRST)));

        // uppercase
        assertParse("ORDER_KEY ASC NULLS LAST", sortOrder(builder -> builder.asc("order_key", NullOrder.NULLS_LAST)));
        assertParse("ORDER_KEY DESC NULLS FIRST", sortOrder(builder -> builder.desc("order_key", NullOrder.NULLS_FIRST)));
        assertDoesNotParse("\"ORDER_KEY\" ASC NULLS LAST", "Cannot find field 'ORDER_KEY' .*");
        assertDoesNotParse("\"ORDER_KEY\" DESC NULLS FIRST", "Cannot find field 'ORDER_KEY' .*");

        // mixed case
        assertParse("\"MixedCase\" ASC NULLS LAST", sortOrder(builder -> builder.asc("MixedCase", NullOrder.NULLS_LAST)));
        assertParse("\"MixedCase\" DESC NULLS FIRST", sortOrder(builder -> builder.desc("MixedCase", NullOrder.NULLS_FIRST)));
        assertParse("OrDER_keY Asc NullS LAst", sortOrder(builder -> builder.asc("order_key", NullOrder.NULLS_LAST)));
        assertParse("OrDER_keY Desc NullS FIrsT", sortOrder(builder -> builder.desc("order_key", NullOrder.NULLS_FIRST)));
        assertDoesNotParse("\"OrDER_keY\" Asc NullS LAst", "Cannot find field 'OrDER_keY' .*");
        assertDoesNotParse("\"OrDER_keY\" Desc NullS FIrsT", "Cannot find field 'OrDER_keY' .*");

        assertParse("comment", sortOrder(builder -> builder.asc("comment")));
        assertParse("\"comment\"", sortOrder(builder -> builder.asc("comment")));
        assertParse("\"quoted field\"", sortOrder(builder -> builder.asc("quoted field")));
        assertParse("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\"", sortOrder(builder -> builder.asc("\"another\" \"quoted\" \"field\"")));
        assertParse("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\" ASC    NULLS   FIRST  ", sortOrder(builder -> builder.asc("\"another\" \"quoted\" \"field\"")));
        assertParse("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\" ASC    NULLS   LAST    ", sortOrder(builder -> builder.asc("\"another\" \"quoted\" \"field\"", NullOrder.NULLS_LAST)));
        assertParse("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\" DESC NULLS FIRST", sortOrder(builder -> builder.desc("\"another\" \"quoted\" \"field\"", NullOrder.NULLS_FIRST)));
        assertParse(" comment   ", sortOrder(builder -> builder.asc("comment")));
        assertParse("comment ASC", sortOrder(builder -> builder.asc("comment")));
        assertParse("  comment    ASC  ", sortOrder(builder -> builder.asc("comment")));
        assertParse("comment ASC NULLS FIRST", sortOrder(builder -> builder.asc("comment")));
        assertParse("  comment    ASC     NULLS     FIRST    ", sortOrder(builder -> builder.asc("comment")));
        assertParse("comment ASC NULLS FIRST", sortOrder(builder -> builder.asc("comment", NullOrder.NULLS_FIRST)));
        assertParse("     comment   ASC       NULLS       FIRST    ", sortOrder(builder -> builder.asc("comment", NullOrder.NULLS_FIRST)));
        assertParse("comment ASC NULLS FIRST", sortOrder(builder -> builder.asc("comment", NullOrder.NULLS_FIRST)));
        assertParse("    comment     ASC    NULLS   FIRST      ", sortOrder(builder -> builder.asc("comment", NullOrder.NULLS_FIRST)));
        assertParse("comment ASC NULLS LAST", sortOrder(builder -> builder.asc("comment", NullOrder.NULLS_LAST)));
        assertParse("  comment   ASC    NULLS     LAST    ", sortOrder(builder -> builder.asc("comment", NullOrder.NULLS_LAST)));
        assertParse("comment DESC", sortOrder(builder -> builder.desc("comment")));
        assertParse("  comment   DESC  ", sortOrder(builder -> builder.desc("comment")));
        assertParse("comment DESC NULLS FIRST", sortOrder(builder -> builder.desc("comment", NullOrder.NULLS_FIRST)));
        assertParse("  comment     DESC  NULLS   FIRST ", sortOrder(builder -> builder.desc("comment", NullOrder.NULLS_FIRST)));
        assertParse("comment DESC NULLS LAST", sortOrder(builder -> builder.desc("comment", NullOrder.NULLS_LAST)));
        assertParse("  comment   DESC    NULLS   LAST   ", sortOrder(builder -> builder.desc("comment", NullOrder.NULLS_LAST)));
        assertParse("comment DESC NULLS LAST", sortOrder(builder -> builder.desc("comment")));
        assertParse("    comment     DESC   NULLS    LAST   ", sortOrder(builder -> builder.desc("comment")));

        assertDoesNotParse("bucket(comment, 3)");
        assertDoesNotParse("truncate(comment, 3)");
        assertDoesNotParse("year(comment)");
        assertDoesNotParse("month(comment)");
        assertDoesNotParse("day(comment)");
        assertDoesNotParse("hour(comment)");

        assertDoesNotParse("bucket(comment, 3) ASC");
        assertDoesNotParse("bucket(comment, 3) ASC NULLS LAST");
    }

    private static void assertParse(@Language("SQL") String value, SortOrder expected)
    {
        assertThat(expected.fields()).hasSize(1);
        assertThat(parseField(value)).isEqualTo(expected);
    }

    private static void assertDoesNotParse(@Language("SQL") String value)
    {
        assertDoesNotParse(value, "\\QUnable to parse sort field: [%s]".formatted(value));
    }

    private static void assertDoesNotParse(@Language("SQL") String value, @Language("RegExp") String expectedMessage)
    {
        assertThatThrownBy(() -> parseField(value))
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
