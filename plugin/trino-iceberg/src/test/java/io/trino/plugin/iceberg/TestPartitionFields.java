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
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.TimestampType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.iceberg.PartitionFields.parsePartitionField;
import static io.trino.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.trino.plugin.iceberg.PartitionFields.toPartitionFields;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPartitionFields
{
    @Test
    public void testParse()
    {
        assertParse("order_key", partitionSpec(builder -> builder.identity("order_key")));
        assertParse("comment", partitionSpec(builder -> builder.identity("comment")));
        assertParse("COMMENT", partitionSpec(builder -> builder.identity("comment")), "comment");
        assertParse("year(ts)", partitionSpec(builder -> builder.year("ts")));
        assertParse("month(ts)", partitionSpec(builder -> builder.month("ts")));
        assertParse("day(ts)", partitionSpec(builder -> builder.day("ts")));
        assertParse("hour(ts)", partitionSpec(builder -> builder.hour("ts")));
        assertParse("bucket(order_key, 42)", partitionSpec(builder -> builder.bucket("order_key", 42)));
        assertParse("truncate(comment, 13)", partitionSpec(builder -> builder.truncate("comment", 13)));
        assertParse("truncate(order_key, 88)", partitionSpec(builder -> builder.truncate("order_key", 88)));
        assertParse("void(order_key)", partitionSpec(builder -> builder.alwaysNull("order_key")));
        assertParse("YEAR(ts)", partitionSpec(builder -> builder.year("ts")), "year(ts)");
        assertParse("MONtH(ts)", partitionSpec(builder -> builder.month("ts")), "month(ts)");
        assertParse("DaY(ts)", partitionSpec(builder -> builder.day("ts")), "day(ts)");
        assertParse("HoUR(ts)", partitionSpec(builder -> builder.hour("ts")), "hour(ts)");
        assertParse("BuCKET(order_key, 42)", partitionSpec(builder -> builder.bucket("order_key", 42)), "bucket(order_key, 42)");
        assertParse("TRuncate(comment, 13)", partitionSpec(builder -> builder.truncate("comment", 13)), "truncate(comment, 13)");
        assertParse("TRUNCATE(order_key, 88)", partitionSpec(builder -> builder.truncate("order_key", 88)), "truncate(order_key, 88)");
        assertParse("VOId(order_key)", partitionSpec(builder -> builder.alwaysNull("order_key")), "void(order_key)");
        assertParse("\"quoted field\"", partitionSpec(builder -> builder.identity("quoted field")));
        assertParse("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\"", partitionSpec(builder -> builder.identity("\"another\" \"quoted\" \"field\"")));
        assertParse("year(\"quoted ts\")", partitionSpec(builder -> builder.year("quoted ts")));
        assertParse("month(\"quoted ts\")", partitionSpec(builder -> builder.month("quoted ts")));
        assertParse("day(\"quoted ts\")", partitionSpec(builder -> builder.day("quoted ts")));
        assertParse("hour(\"quoted ts\")", partitionSpec(builder -> builder.hour("quoted ts")));
        assertParse("bucket(\"quoted field\", 42)", partitionSpec(builder -> builder.bucket("quoted field", 42)));
        assertParse("truncate(\"quoted field\", 13)", partitionSpec(builder -> builder.truncate("quoted field", 13)));
        assertParse("void(\"quoted field\")", partitionSpec(builder -> builder.alwaysNull("quoted field")));
        assertParse("truncate(\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\", 13)", partitionSpec(builder -> builder.truncate("\"another\" \"quoted\" \"field\"", 13)));
        assertParse("void(\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\")", partitionSpec(builder -> builder.alwaysNull("\"another\" \"quoted\" \"field\"")));
        assertParse("\"nested.value\"", partitionSpec(builder -> builder.identity("nested.value")));
        assertParse("year(\"nested.ts\")", partitionSpec(builder -> builder.year("nested.ts")));
        assertParse("month(\"nested.ts\")", partitionSpec(builder -> builder.month("nested.ts")));
        assertParse("day(\"nested.ts\")", partitionSpec(builder -> builder.day("nested.ts")));
        assertParse("hour(\"nested.nested.ts\")", partitionSpec(builder -> builder.hour("nested.nested.ts")));
        assertParse("truncate(\"nested.nested.value\", 13)", partitionSpec(builder -> builder.truncate("nested.nested.value", 13)));
        assertParse("bucket(\"nested.nested.value\", 42)", partitionSpec(builder -> builder.bucket("nested.nested.value", 42)));
        assertParse("void(\"nested.nested.value\")", partitionSpec(builder -> builder.alwaysNull("nested.nested.value")));
        assertParse("\"MixedTs\"", partitionSpec(builder -> builder.identity("MixedTs")));
        assertParse("\"MixedNested.MixedValue\"", partitionSpec(builder -> builder.identity("MixedNested.MixedValue")));
        assertParse("year(\"MixedTs\")", partitionSpec(builder -> builder.year("MixedTs")));
        assertParse("month(\"MixedTs\")", partitionSpec(builder -> builder.month("MixedTs")));
        assertParse("day(\"MixedTs\")", partitionSpec(builder -> builder.day("MixedTs")));
        assertParse("hour(\"MixedTs\")", partitionSpec(builder -> builder.hour("MixedTs")));
        assertParse("bucket(\"MixedTs\", 42)", partitionSpec(builder -> builder.bucket("MixedTs", 42)));
        assertParse("truncate(\"MixedString\", 13)", partitionSpec(builder -> builder.truncate("MixedString", 13)));
        assertParse("void(\"MixedString\")", partitionSpec(builder -> builder.alwaysNull("MixedString")));

        assertInvalid("bucket()", "Invalid partition field declaration: bucket()");
        assertInvalid(".nested", "Invalid partition field declaration: .nested");
        assertInvalid("abc", "Cannot find source column: abc");
        assertInvalid("notes", "Cannot partition by non-primitive source field: list<string>");
        assertInvalid("bucket(price, 42)", "Invalid source type double for transform: bucket[42]");
        assertInvalid("bucket(notes, 88)", "Cannot partition by non-primitive source field: list<string>");
        assertInvalid("truncate(ts, 13)", "Invalid source type timestamp for transform: truncate[13]");
        assertInvalid("year(order_key)", "Invalid source type long for transform: year");
        assertInvalid("\"test\"", "Cannot find source column: test");
        assertInvalid("\"test with space\"", "Cannot find source column: test with space");
        assertInvalid("\"test \"with space\"", "Invalid partition field declaration: \"test \"with space\"");
        assertInvalid("\"test \"\"\"with space\"", "Invalid partition field declaration: \"test \"\"\"with space\"");
        assertInvalid("ABC", "Cannot find source column: abc");
        assertInvalid("\"ABC\"", "Cannot find source column: ABC");
        assertInvalid("year(ABC)", "Cannot find source column: abc");
        assertInvalid("bucket(\"ABC\", 12)", "Cannot find source column: ABC");
        assertInvalid("\"nested.list\"", "Cannot partition by non-primitive source field: list<string>");
    }

    @Test
    public void testConflicts()
    {
        assertParseName(List.of("col", "col_year"), TimestampType.withZone(), List.of("year(col)"), List.of("col_year_2"));
        assertParseName(List.of("col", "col_month"), TimestampType.withZone(), List.of("month(col)"), List.of("col_month_2"));
        assertParseName(List.of("col", "col_day"), TimestampType.withZone(), List.of("day(col)"), List.of("col_day_2"));
        assertParseName(List.of("col", "col_hour"), TimestampType.withZone(), List.of("hour(col)"), List.of("col_hour_2"));
        assertParseName(List.of("col", "col_bucket"), TimestampType.withZone(), List.of("bucket(col,10)"), List.of("col_bucket_2"));
        assertParseName(List.of("col", "col_trunc"), StringType.get(), List.of("truncate(col,10)"), List.of("col_trunc_2"));
        assertParseName(List.of("col", "col_null"), TimestampType.withZone(), List.of("void(col)"), List.of("col_null_2"));

        assertParseName(List.of("col", "col_year", "col_year_2"), TimestampType.withZone(), List.of("year(col)"), List.of("col_year_3"));
        assertParseName(List.of("col", "col_year", "col_year_3"), TimestampType.withZone(), List.of("year(col)"), List.of("col_year_2"));

        assertParseName(List.of("col", "col_year", "col_year_2"), TimestampType.withZone(), List.of("year(col)", "col_year_2"), List.of("col_year_3", "col_year_2"));
    }

    private static void assertParseName(List<String> columnNames, Type type, List<String> partitions, List<String> expected)
    {
        ImmutableList.Builder<NestedField> columns = ImmutableList.builderWithExpectedSize(columnNames.size());
        int i = 1;
        for (String name : columnNames) {
            columns.add(NestedField.required(i++, name, type));
        }
        PartitionSpec spec = parsePartitionFields(new Schema(columns.build()), partitions);
        assertThat(spec.fields()).extracting(PartitionField::name)
                .containsExactlyElementsOf(expected);
    }

    private static void assertParse(String value, PartitionSpec expected, String canonicalRepresentation)
    {
        assertThat(expected.fields()).hasSize(1);
        assertThat(parseField(value)).isEqualTo(expected);
        assertThat(getOnlyElement(toPartitionFields(expected))).isEqualTo(canonicalRepresentation);
    }

    private static void assertParse(String value, PartitionSpec expected)
    {
        assertParse(value, expected, value);
    }

    private static void assertInvalid(String value, String message)
    {
        assertThatThrownBy(() -> parseField(value))
                .isInstanceOfAny(
                        IllegalArgumentException.class,
                        UnsupportedOperationException.class,
                        ValidationException.class)
                .hasMessage(message);
    }

    private static PartitionSpec parseField(String value)
    {
        return partitionSpec(builder -> parsePartitionField(builder, value, ""));
    }

    private static PartitionSpec partitionSpec(Consumer<PartitionSpec.Builder> consumer)
    {
        Schema schema = new Schema(
                NestedField.required(1, "order_key", LongType.get()),
                NestedField.required(2, "ts", TimestampType.withoutZone()),
                NestedField.required(3, "price", DoubleType.get()),
                NestedField.optional(4, "comment", StringType.get()),
                NestedField.optional(5, "notes", ListType.ofRequired(6, StringType.get())),
                NestedField.optional(7, "quoted field", StringType.get()),
                NestedField.optional(8, "quoted ts", TimestampType.withoutZone()),
                NestedField.optional(9, "\"another\" \"quoted\" \"field\"", StringType.get()),
                NestedField.required(10, "nested", Types.StructType.of(
                        NestedField.required(12, "value", StringType.get()),
                        NestedField.required(13, "ts", TimestampType.withZone()),
                        NestedField.required(14, "list", ListType.ofRequired(15, StringType.get())),
                        NestedField.required(16, "nested", Types.StructType.of(
                                NestedField.required(17, "value", StringType.get()),
                                NestedField.required(18, "ts", TimestampType.withZone()))))),
                NestedField.required(19, "MixedTs", TimestampType.withoutZone()),
                NestedField.optional(20, "MixedString", StringType.get()),
                NestedField.required(21, "MixedNested", Types.StructType.of(
                        NestedField.required(22, "MixedValue", StringType.get()))));

        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        consumer.accept(builder);
        return builder.build();
    }
}
