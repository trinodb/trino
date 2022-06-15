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

import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.TimestampType;
import org.assertj.core.api.AbstractThrowableAssert;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.function.Consumer;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.iceberg.IcebergUtil.fromIdentifier;
import static io.trino.plugin.iceberg.IcebergUtil.toIdentifier;
import static io.trino.plugin.iceberg.SortFields.parseSortField;
import static io.trino.plugin.iceberg.SortFields.toSortFields;
import static io.trino.testing.assertions.Assert.assertEquals;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestSortFields
{
    @Test
    public void testParse()
    {
        assertParse("order_key", sortedOrder(builder -> builder.asc("order_key")));
        assertParse("order_key ASC", sortedOrder(builder -> builder.asc("order_key")));
        assertParse("order_key ASC NULLS FIRST", sortedOrder(builder -> builder.asc("order_key")));
        assertParse("order_key ASC NULLS FIRST", sortedOrder(builder -> builder.asc("order_key", NullOrder.NULLS_FIRST)));
        assertParse("order_key ASC NULLS LAST", sortedOrder(builder -> builder.asc("order_key", NullOrder.NULLS_LAST)));
        assertParse("order_key DESC", sortedOrder(builder -> builder.desc("order_key")));
        assertParse("order_key DESC NULLS FIRST", sortedOrder(builder -> builder.desc("order_key", NullOrder.NULLS_FIRST)));
        assertParse("order_key DESC NULLS LAST", sortedOrder(builder -> builder.desc("order_key", NullOrder.NULLS_LAST)));
        assertParse("order_key DESC NULLS LAST", sortedOrder(builder -> builder.desc("order_key")));

        assertParse("comment", sortedOrder(builder -> builder.asc("comment")));
        assertParse("\"comment\"", sortedOrder(builder -> builder.asc("comment")));
        assertParse("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\"", sortedOrder(builder -> builder.asc("\"another\" \"quoted\" \"field\"")));
        assertParse("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\" ASC    NULLS   FIRST  ", sortedOrder(builder -> builder.asc("\"another\" \"quoted\" \"field\"")));
        assertParse("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\" ASC    NULLS   LAST    ", sortedOrder(builder -> builder.asc("\"another\" \"quoted\" \"field\"", NullOrder.NULLS_LAST)));
        assertParse("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\" DESC NULLS FIRST", sortedOrder(builder -> builder.desc("\"another\" \"quoted\" \"field\"", NullOrder.NULLS_FIRST)));
        assertParse(" comment   ", sortedOrder(builder -> builder.asc("comment")));
        assertParse("comment ASC", sortedOrder(builder -> builder.asc("comment")));
        assertParse("  comment    ASC  ", sortedOrder(builder -> builder.asc("comment")));
        assertParse("comment ASC NULLS FIRST", sortedOrder(builder -> builder.asc("comment")));
        assertParse("  comment    ASC     NULLS     FIRST    ", sortedOrder(builder -> builder.asc("comment")));
        assertParse("comment ASC NULLS FIRST", sortedOrder(builder -> builder.asc("comment", NullOrder.NULLS_FIRST)));
        assertParse("     comment   ASC       NULLS       FIRST    ", sortedOrder(builder -> builder.asc("comment", NullOrder.NULLS_FIRST)));
        assertParse("comment ASC NULLS FIRST", sortedOrder(builder -> builder.asc("comment", NullOrder.NULLS_FIRST)));
        assertParse("    comment     ASC    NULLS   FIRST      ", sortedOrder(builder -> builder.asc("comment", NullOrder.NULLS_FIRST)));
        assertParse("comment ASC NULLS LAST", sortedOrder(builder -> builder.asc("comment", NullOrder.NULLS_LAST)));
        assertParse("  comment   ASC    NULLS     LAST    ", sortedOrder(builder -> builder.asc("comment", NullOrder.NULLS_LAST)));
        assertParse("comment DESC", sortedOrder(builder -> builder.desc("comment")));
        assertParse("  comment   DESC  ", sortedOrder(builder -> builder.desc("comment")));
        assertParse("comment DESC NULLS FIRST", sortedOrder(builder -> builder.desc("comment", NullOrder.NULLS_FIRST)));
        assertParse("  comment     DESC  NULLS   FIRST ", sortedOrder(builder -> builder.desc("comment", NullOrder.NULLS_FIRST)));
        assertParse("comment DESC NULLS LAST", sortedOrder(builder -> builder.desc("comment", NullOrder.NULLS_LAST)));
        assertParse("  comment   DESC    NULLS   LAST   ", sortedOrder(builder -> builder.desc("comment", NullOrder.NULLS_LAST)));
        assertParse("comment DESC NULLS LAST", sortedOrder(builder -> builder.desc("comment")));
        assertParse("    comment     DESC   NULLS    LAST   ", sortedOrder(builder -> builder.desc("comment")));

        assertParse("year(ts)", sortedOrder(builder -> builder.asc(Expressions.year("ts"))));
        assertParse("YEAR(ts)", sortedOrder(builder -> builder.asc(Expressions.year("ts"))), "year(ts) ASC NULLS FIRST");
        assertParse("YeaR(TS)", sortedOrder(builder -> builder.asc(Expressions.year("ts"))));
        assertParse("yEAR(TS)", sortedOrder(builder -> builder.asc(Expressions.year("ts"))));
        assertParse("  year( ts   )", sortedOrder(builder -> builder.asc(Expressions.year("ts"))));
        assertParse("year(\"quoted ts\")", sortedOrder(builder -> builder.asc(Expressions.year("quoted ts"))));
        assertParse("year(ts) ASC", sortedOrder(builder -> builder.asc(Expressions.year("ts"))));
        assertParse("  year(  ts )   ASC   ", sortedOrder(builder -> builder.asc(Expressions.year("ts"))));
        assertParse("year(\"quoted ts\")    ASC", sortedOrder(builder -> builder.asc(Expressions.year("quoted ts"))));
        assertParse("year(ts) ASC NULLS FIRST", sortedOrder(builder -> builder.asc(Expressions.year("ts"))));
        assertParse("  year(  ts )   ASC    NULLS      FIRST   ", sortedOrder(builder -> builder.asc(Expressions.year("ts"))));
        assertParse("year(ts) ASC NULLS LAST", sortedOrder(builder -> builder.asc(Expressions.year("ts"), NullOrder.NULLS_LAST)));
        assertParse("year(\"quoted ts\") ASC NULLS LAST", sortedOrder(builder -> builder.asc(Expressions.year("quoted ts"), NullOrder.NULLS_LAST)));
        assertParse("  year( ts   )       ASC      NULLS      LAST   ", sortedOrder(builder -> builder.asc(Expressions.year("ts"), NullOrder.NULLS_LAST)));
        assertParse("    year(  ts  )    DESC   ", sortedOrder(builder -> builder.desc(Expressions.year("ts"))));
        assertParse("year(ts) DESC", sortedOrder(builder -> builder.desc(Expressions.year("ts"))));
        assertParse("  year(ts)    DESC    ", sortedOrder(builder -> builder.desc(Expressions.year("ts"))));
        assertParse("year(ts) DESC NULLS FIRST", sortedOrder(builder -> builder.desc(Expressions.year("ts"), NullOrder.NULLS_FIRST)));
        assertParse("   year(ts)   DESC   NULLS     FIRST   ", sortedOrder(builder -> builder.desc(Expressions.year("ts"), NullOrder.NULLS_FIRST)));
        assertParse("year(ts) DESC NULLS LAST", sortedOrder(builder -> builder.desc(Expressions.year("ts"), NullOrder.NULLS_LAST)));
        assertParse("    year(   ts   )    DESC    NULLS   LAST    ", sortedOrder(builder -> builder.desc(Expressions.year("ts"), NullOrder.NULLS_LAST)));

        assertParse("month(ts)", sortedOrder(builder -> builder.asc(Expressions.month("ts"))));
        assertParse("MONTH(  ts  )", sortedOrder(builder -> builder.asc(Expressions.month("ts"))));
        assertParse("MonTH(  ts  )", sortedOrder(builder -> builder.asc(Expressions.month("ts"))));
        assertParse("monTH(  ts  )", sortedOrder(builder -> builder.asc(Expressions.month("ts"))));
        assertParse(" month(  ts  )", sortedOrder(builder -> builder.asc(Expressions.month("ts"))));
        assertParse("month(ts) ASC", sortedOrder(builder -> builder.asc(Expressions.month("ts"))));
        assertParse("   month(   ts   )    ASC    ", sortedOrder(builder -> builder.asc(Expressions.month("ts"))));
        assertParse("month(ts) ASC NULLS FIRST", sortedOrder(builder -> builder.asc(Expressions.month("ts"))));
        assertParse("  month(  ts  )    ASC    NULLS    FIRST", sortedOrder(builder -> builder.asc(Expressions.month("ts"))));
        assertParse("month(ts) ASC NULLS LAST", sortedOrder(builder -> builder.asc(Expressions.month("ts"), NullOrder.NULLS_LAST)));
        assertParse("   month(  ts   )    ASC    NULLS    LAST   ", sortedOrder(builder -> builder.asc(Expressions.month("ts"), NullOrder.NULLS_LAST)));
        assertParse("month(ts) DESC", sortedOrder(builder -> builder.desc(Expressions.month("ts"))));
        assertParse("   month( ts   )    DESC    ", sortedOrder(builder -> builder.desc(Expressions.month("ts"))));
        assertParse("month(ts) DESC NULLS FIRST", sortedOrder(builder -> builder.desc(Expressions.month("ts"), NullOrder.NULLS_FIRST)));
        assertParse("   month(  ts )       DESC    NULLS   FIRST      ", sortedOrder(builder -> builder.desc(Expressions.month("ts"), NullOrder.NULLS_FIRST)));
        assertParse("month(ts) DESC NULLS LAST", sortedOrder(builder -> builder.desc(Expressions.month("ts"), NullOrder.NULLS_LAST)));
        assertParse("   month(  ts   )     DESC   NULLS  LAST     ", sortedOrder(builder -> builder.desc(Expressions.month("ts"), NullOrder.NULLS_LAST)));

        assertParse("day(ts)", sortedOrder(builder -> builder.asc(Expressions.day("ts"))));
        assertParse("DAY(ts)", sortedOrder(builder -> builder.asc(Expressions.day("ts"))));
        assertParse("DaY(ts)", sortedOrder(builder -> builder.asc(Expressions.day("ts"))));
        assertParse("daY(ts)", sortedOrder(builder -> builder.asc(Expressions.day("ts"))));
        assertParse("  day(  ts   )", sortedOrder(builder -> builder.asc(Expressions.day("ts"))));
        assertParse("day(ts) ASC", sortedOrder(builder -> builder.asc(Expressions.day("ts"))));
        assertParse("   day(  ts  )       ASC      ", sortedOrder(builder -> builder.asc(Expressions.day("ts"))));
        assertParse("day(ts) ASC NULLS FIRST", sortedOrder(builder -> builder.asc(Expressions.day("ts"))));
        assertParse(" day(  ts  )    ASC     NULLS   FIRST    ", sortedOrder(builder -> builder.asc(Expressions.day("ts"))));
        assertParse("day(ts) ASC NULLS LAST", sortedOrder(builder -> builder.asc(Expressions.day("ts"), NullOrder.NULLS_LAST)));
        assertParse("   day(  ts )   ASC    NULLS    LAST   ", sortedOrder(builder -> builder.asc(Expressions.day("ts"), NullOrder.NULLS_LAST)));
        assertParse("day(ts) DESC", sortedOrder(builder -> builder.desc(Expressions.day("ts"))));
        assertParse("   day(   ts   )    DESC   ", sortedOrder(builder -> builder.desc(Expressions.day("ts"))));
        assertParse("day(ts) DESC NULLS FIRST", sortedOrder(builder -> builder.desc(Expressions.day("ts"), NullOrder.NULLS_FIRST)));
        assertParse("   day(   ts   )    DESC    NULLS     FIRST   ", sortedOrder(builder -> builder.desc(Expressions.day("ts"), NullOrder.NULLS_FIRST)));
        assertParse("day(ts) DESC NULLS LAST", sortedOrder(builder -> builder.desc(Expressions.day("ts"), NullOrder.NULLS_LAST)));
        assertParse("   day(   ts   )    DESC    NULLS   LAST  ", sortedOrder(builder -> builder.desc(Expressions.day("ts"), NullOrder.NULLS_LAST)));

        assertParse("hour(ts)", sortedOrder(builder -> builder.asc(Expressions.hour("ts"))));
        assertParse("HOUR(ts)", sortedOrder(builder -> builder.asc(Expressions.hour("ts"))));
        assertParse("HouR(ts)", sortedOrder(builder -> builder.asc(Expressions.hour("ts"))));
        assertParse("houR(ts)", sortedOrder(builder -> builder.asc(Expressions.hour("ts"))));
        assertParse("  hour( ts  )", sortedOrder(builder -> builder.asc(Expressions.hour("ts"))));
        assertParse("hour(ts) ASC", sortedOrder(builder -> builder.asc(Expressions.hour("ts"))));
        assertParse("  hour(  ts  )   ASC  ", sortedOrder(builder -> builder.asc(Expressions.hour("ts"))));
        assertParse("hour(ts) ASC NULLS FIRST", sortedOrder(builder -> builder.asc(Expressions.hour("ts"))));
        assertParse(" hour(  ts )    ASC   NULLS   FIRST   ", sortedOrder(builder -> builder.asc(Expressions.hour("ts"))));
        assertParse("hour(ts) ASC NULLS LAST", sortedOrder(builder -> builder.asc(Expressions.hour("ts"), NullOrder.NULLS_LAST)));
        assertParse(" hour( ts  )   ASC   NULLS   LAST ", sortedOrder(builder -> builder.asc(Expressions.hour("ts"), NullOrder.NULLS_LAST)));
        assertParse("hour(ts) DESC", sortedOrder(builder -> builder.desc(Expressions.hour("ts"))));
        assertParse("    hour( ts    )    DESC             ", sortedOrder(builder -> builder.desc(Expressions.hour("ts"))));
        assertParse("hour(ts) DESC NULLS FIRST", sortedOrder(builder -> builder.desc(Expressions.hour("ts"), NullOrder.NULLS_FIRST)));
        assertParse("   hour(   ts          )          DESC   NULLS   FIRST   ", sortedOrder(builder -> builder.desc(Expressions.hour("ts"), NullOrder.NULLS_FIRST)));
        assertParse("hour(ts) DESC NULLS LAST", sortedOrder(builder -> builder.desc(Expressions.hour("ts"), NullOrder.NULLS_LAST)));
        assertParse(" hour(    ts    )        DESC       NULLS   LAST      ", sortedOrder(builder -> builder.desc(Expressions.hour("ts"), NullOrder.NULLS_LAST)));

        assertParse("bucket(order_key,42)", sortedOrder(builder -> builder.asc(Expressions.bucket("order_key", 42))));
        assertParse("BUCKET(order_key, 42)", sortedOrder(builder -> builder.asc(Expressions.bucket("order_key", 42))));
        assertParse("BUckeT(order_key, 42)", sortedOrder(builder -> builder.asc(Expressions.bucket("order_key", 42))));
        assertParse("buckET(order_key,    42  )", sortedOrder(builder -> builder.asc(Expressions.bucket("order_key", 42))));
        assertParse("   bucket(  order_key  , 42  )", sortedOrder(builder -> builder.asc(Expressions.bucket("order_key", 42))));
        assertParse("bucket(order_key, 42) ASC", sortedOrder(builder -> builder.asc(Expressions.bucket("order_key", 42))));
        assertParse("  bucket(  order_key  , 42)   ASC  ", sortedOrder(builder -> builder.asc(Expressions.bucket("order_key", 42))));
        assertParse("bucket(order_key, 42) ASC NULLS FIRST", sortedOrder(builder -> builder.asc(Expressions.bucket("order_key", 42), NullOrder.NULLS_FIRST)));
        assertParse(" bucket(  order_key , 42)    ASC    NULLS  FIRST  ", sortedOrder(builder -> builder.asc(Expressions.bucket("order_key", 42), NullOrder.NULLS_FIRST)));
        assertParse("bucket(order_key, 42) ASC NULLS LAST", sortedOrder(builder -> builder.asc(Expressions.bucket("order_key", 42), NullOrder.NULLS_LAST)));
        assertParse("   bucket( order_key , 42)   ASC   NULLS   LAST", sortedOrder(builder -> builder.asc(Expressions.bucket("order_key", 42), NullOrder.NULLS_LAST)));
        assertParse("bucket(order_key, 42) DESC", sortedOrder(builder -> builder.desc(Expressions.bucket("order_key", 42), NullOrder.NULLS_LAST)));
        assertParse("  bucket(  order_key , 42)   DESC  ", sortedOrder(builder -> builder.desc(Expressions.bucket("order_key", 42), NullOrder.NULLS_LAST)));
        assertParse("bucket(order_key, 42) DESC NULLS FIRST", sortedOrder(builder -> builder.desc(Expressions.bucket("order_key", 42), NullOrder.NULLS_FIRST)));
        assertParse("   bucket(  order_key  , 42)   DESC   NULLS   FIRST  ", sortedOrder(builder -> builder.desc(Expressions.bucket("order_key", 42), NullOrder.NULLS_FIRST)));
        assertParse("bucket(order_key, 42) DESC NULLS LAST", sortedOrder(builder -> builder.desc(Expressions.bucket("order_key", 42), NullOrder.NULLS_LAST)));
        assertParse("  bucket(   order_key , 42)   DESC    NULLS  LAST  ", sortedOrder(builder -> builder.desc(Expressions.bucket("order_key", 42), NullOrder.NULLS_LAST)));

        assertParse("truncate(comment, 10)", sortedOrder(builder -> builder.asc(Expressions.truncate("comment", 10))));
        assertParse("TRUNCATE(comment, 10)", sortedOrder(builder -> builder.asc(Expressions.truncate("comment", 10))));
        assertParse("TRuncaTE(comment, 10)", sortedOrder(builder -> builder.asc(Expressions.truncate("comment", 10))));
        assertParse("truncaTE(comment, 10)", sortedOrder(builder -> builder.asc(Expressions.truncate("comment", 10))));
        assertParse("   truncate(   comment , 10)", sortedOrder(builder -> builder.asc(Expressions.truncate("comment", 10))));
        assertParse("truncate(comment, 10) ASC", sortedOrder(builder -> builder.asc(Expressions.truncate("comment", 10))));
        assertParse(" truncate( comment , 10)    ASC   ", sortedOrder(builder -> builder.asc(Expressions.truncate("comment", 10))));
        assertParse("truncate(comment, 10) ASC NULLS FIRST", sortedOrder(builder -> builder.asc(Expressions.truncate("comment", 10), NullOrder.NULLS_FIRST)));
        assertParse(" truncate( comment , 10)   ASC   NULLS   FIRST   ", sortedOrder(builder -> builder.asc(Expressions.truncate("comment", 10), NullOrder.NULLS_FIRST)));
        assertParse("truncate(comment, 10) ASC NULLS LAST", sortedOrder(builder -> builder.asc(Expressions.truncate("comment", 10), NullOrder.NULLS_LAST)));
        assertParse("  truncate(  comment , 10)    ASC    NULLS   LAST ", sortedOrder(builder -> builder.asc(Expressions.truncate("comment", 10), NullOrder.NULLS_LAST)));
        assertParse("truncate(comment, 10) DESC", sortedOrder(builder -> builder.desc(Expressions.truncate("comment", 10), NullOrder.NULLS_LAST)));
        assertParse(" truncate(  comment , 10)   DESC ", sortedOrder(builder -> builder.desc(Expressions.truncate("comment", 10), NullOrder.NULLS_LAST)));
        assertParse("truncate(comment, 10) DESC NULLS FIRST", sortedOrder(builder -> builder.desc(Expressions.truncate("comment", 10), NullOrder.NULLS_FIRST)));
        assertParse("  truncate(  comment  , 10)   DESC   NULLS  FIRST  ", sortedOrder(builder -> builder.desc(Expressions.truncate("comment", 10), NullOrder.NULLS_FIRST)));
        assertParse("truncate(comment, 10) DESC NULLS LAST", sortedOrder(builder -> builder.desc(Expressions.truncate("comment", 10), NullOrder.NULLS_LAST)));
        assertParse("   truncate( comment  , 10)   DESC   NULLS LAST  ", sortedOrder(builder -> builder.desc(Expressions.truncate("comment", 10), NullOrder.NULLS_LAST)));
        assertParse("truncate(\"quoted field\", 5) DESC NULLS FIRST", sortedOrder(builder -> builder.desc(Expressions.truncate("quoted field", 5), NullOrder.NULLS_FIRST)));
        assertParse("truncate(\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\", 5) DESC NULLS FIRST",
                sortedOrder(builder -> builder.desc(Expressions.truncate("\"another\" \"quoted\" \"field\"", 5), NullOrder.NULLS_FIRST)),
                "truncate(\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\", 5) DESC NULLS FIRST");
        assertInvalid("bucket()", "Invalid sort field declaration: bucket()");
        assertInvalid("abc", "Cannot find field 'abc' in struct: struct<1: order_key: required long, 2: ts: required timestamp, 3: price: required double, 4: comment: optional string, 5: notes: optional list<string>, 7: quoted field: optional string, 8: quoted ts: optional timestamp, 9: \"another\" \"quoted\" \"field\": optional string>");
        assertInvalid("notes", "Cannot sort by non-primitive source field: list<string>");
        assertInvalid("bucket(price, 42)", "Cannot bind: bucket[42] cannot transform double values from 'price'");
        assertInvalid("bucket(notes, 88)", "Cannot bind: bucket[88] cannot transform list<string> values from 'notes'");
        assertInvalid("truncate(ts, 13)", "Cannot truncate type: timestamp");
    }

    private static void assertParse(String value, SortOrder expected, String canonicalRepresentation)
    {
        assertParse(value, expected);
        assertEquals(getOnlyElement(toSortFields(expected)), canonicalRepresentation);
    }

    private static void assertParse(String value, SortOrder expected)
    {
        assertEquals(expected.fields().size(), 1);
        assertEquals(parseField(value), expected);
    }

    private static void assertInvalid(String value, String message)
    {
        AbstractThrowableAssert throwableAssert = assertThatThrownBy(() -> parseField(value))
                .isInstanceOfAny(
                        IllegalArgumentException.class,
                        UnsupportedOperationException.class,
                        ValidationException.class);

        throwableAssert.hasMessage(message);
    }

    private static SortOrder parseField(String value)
    {
        return sortedOrder(builder -> parseSortField(builder, value));
    }

    private static SortOrder sortedOrder(Consumer<SortOrder.Builder> consumer)
    {
        Schema schema = new Schema(
                NestedField.required(1, "order_key", LongType.get()),
                NestedField.required(2, "ts", TimestampType.withoutZone()),
                NestedField.required(3, "price", DoubleType.get()),
                NestedField.optional(4, "comment", StringType.get()),
                NestedField.optional(5, "notes", ListType.ofRequired(6, StringType.get())),
                NestedField.optional(7, "quoted field", StringType.get()),
                NestedField.optional(8, "quoted ts", TimestampType.withoutZone()),
                NestedField.optional(9, "\"another\" \"quoted\" \"field\"", StringType.get()));

        SortOrder.Builder builder = SortOrder.builderFor(schema);
        consumer.accept(builder);
        return builder.build();
    }

    @Test
    public void testFromIdentifier()
    {
        assertEquals(fromIdentifier("test"), "test");
        assertEquals(fromIdentifier("TEST"), "test");
        assertEquals(fromIdentifier("TEST"), "TEST".toLowerCase(Locale.ROOT));
        assertEquals(fromIdentifier("\" test\""), " test");
        assertEquals(fromIdentifier("\"20days\""), "20days");
        assertEquals(fromIdentifier("\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\""), "\"another\" \"quoted\" \"field\"");
    }

    @Test
    public void testToIdentifier()
    {
        assertEquals(toIdentifier("test"), "test");
        assertEquals(toIdentifier("TEST"), "test".toUpperCase(Locale.ROOT));
        assertEquals(toIdentifier(" test"), " test");
        assertEquals(toIdentifier("20days"), "\"20days\"");
        assertEquals(toIdentifier("\"another\" \"quoted\" \"field\""), "\"\"\"another\"\" \"\"quoted\"\" \"\"field\"\"\"");
    }
}
