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
package io.trino.spi.predicate;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.block.Block;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.block.TestingBlockJsonSerde;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.type.TestingTypeDeserializer;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.predicate.TupleDomain.all;
import static io.trino.spi.predicate.TupleDomain.columnWiseUnion;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTupleDomain
{
    private static final ColumnHandle A = new TestingColumnHandle("a");
    private static final ColumnHandle B = new TestingColumnHandle("b");
    private static final ColumnHandle C = new TestingColumnHandle("c");
    private static final ColumnHandle D = new TestingColumnHandle("d");
    private static final ColumnHandle E = new TestingColumnHandle("e");

    @Test
    public void testNone()
    {
        assertTrue(TupleDomain.none().isNone());
        assertEquals(TupleDomain.<ColumnHandle>none(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        A, Domain.none(BIGINT))));
        assertEquals(TupleDomain.<ColumnHandle>none(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        A, Domain.all(BIGINT),
                        B, Domain.none(VARCHAR))));
    }

    @Test
    public void testAll()
    {
        assertTrue(TupleDomain.all().isAll());
        assertEquals(TupleDomain.<ColumnHandle>all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        A, Domain.all(BIGINT))));
        assertEquals(TupleDomain.<ColumnHandle>all(),
                TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of()));
    }

    @Test
    public void testIntersection()
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.all(VARCHAR))
                        .put(B, Domain.notNull(DOUBLE))
                        .put(C, Domain.singleValue(BIGINT, 1L))
                        .put(D, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DOUBLE, 0.0)), true))
                        .buildOrThrow());

        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.singleValue(VARCHAR, utf8Slice("value")))
                        .put(B, Domain.singleValue(DOUBLE, 0.0))
                        .put(C, Domain.singleValue(BIGINT, 1L))
                        .put(D, Domain.create(ValueSet.ofRanges(Range.lessThan(DOUBLE, 10.0)), false))
                        .buildOrThrow());

        TupleDomain<ColumnHandle> expectedTupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.singleValue(VARCHAR, utf8Slice("value")))
                        .put(B, Domain.singleValue(DOUBLE, 0.0))
                        .put(C, Domain.singleValue(BIGINT, 1L))
                        .put(D, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 0.0, true, 10.0, false)), false))
                        .buildOrThrow());

        assertEquals(tupleDomain1.intersect(tupleDomain2), expectedTupleDomain);
        assertEquals(tupleDomain2.intersect(tupleDomain1), expectedTupleDomain);

        assertEquals(TupleDomain.intersect(ImmutableList.of()), all());
        assertEquals(TupleDomain.intersect(ImmutableList.of(tupleDomain1)), tupleDomain1);
        assertEquals(TupleDomain.intersect(ImmutableList.of(tupleDomain1, tupleDomain2)), expectedTupleDomain);

        TupleDomain<ColumnHandle> tupleDomain3 = TupleDomain.withColumnDomains(ImmutableMap.of(
                C, Domain.singleValue(BIGINT, 1L),
                D, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 5.0, true, 100.0, true)), true)));
        expectedTupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, Domain.singleValue(VARCHAR, utf8Slice("value")),
                        B, Domain.singleValue(DOUBLE, 0.0),
                        C, Domain.singleValue(BIGINT, 1L),
                        D, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 5.0, true, 10.0, false)), false)));
        assertEquals(TupleDomain.intersect(ImmutableList.of(tupleDomain1, tupleDomain2, tupleDomain3)), expectedTupleDomain);
    }

    @Test
    public void testNoneIntersection()
    {
        assertEquals(TupleDomain.none().intersect(TupleDomain.all()), TupleDomain.none());
        assertEquals(TupleDomain.all().intersect(TupleDomain.none()), TupleDomain.none());
        assertEquals(TupleDomain.none().intersect(TupleDomain.none()), TupleDomain.none());
        assertEquals(
                TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.onlyNull(BIGINT)))
                        .intersect(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.notNull(BIGINT)))),
                TupleDomain.<ColumnHandle>none());
    }

    @Test
    public void testMismatchedColumnIntersection()
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, Domain.all(DOUBLE),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))));

        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DOUBLE, 0.0)), true),
                        C, Domain.singleValue(BIGINT, 1L)));

        TupleDomain<ColumnHandle> expectedTupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                A, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DOUBLE, 0.0)), true),
                B, Domain.singleValue(VARCHAR, utf8Slice("value")),
                C, Domain.singleValue(BIGINT, 1L)));

        assertEquals(tupleDomain1.intersect(tupleDomain2), expectedTupleDomain);
    }

    @Test
    public void testColumnWiseUnion()
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.all(VARCHAR))
                        .put(B, Domain.notNull(DOUBLE))
                        .put(C, Domain.onlyNull(BIGINT))
                        .put(D, Domain.singleValue(BIGINT, 1L))
                        .put(E, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DOUBLE, 0.0)), true))
                        .buildOrThrow());

        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.singleValue(VARCHAR, utf8Slice("value")))
                        .put(B, Domain.singleValue(DOUBLE, 0.0))
                        .put(C, Domain.notNull(BIGINT))
                        .put(D, Domain.singleValue(BIGINT, 1L))
                        .put(E, Domain.create(ValueSet.ofRanges(Range.lessThan(DOUBLE, 10.0)), false))
                        .buildOrThrow());

        TupleDomain<ColumnHandle> expectedTupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.all(VARCHAR))
                        .put(B, Domain.notNull(DOUBLE))
                        .put(C, Domain.all(BIGINT))
                        .put(D, Domain.singleValue(BIGINT, 1L))
                        .put(E, Domain.all(DOUBLE))
                        .buildOrThrow());

        assertEquals(columnWiseUnion(tupleDomain1, tupleDomain2), expectedTupleDomain);
    }

    @Test
    public void testNoneColumnWiseUnion()
    {
        assertEquals(columnWiseUnion(TupleDomain.none(), TupleDomain.all()), TupleDomain.all());
        assertEquals(columnWiseUnion(TupleDomain.all(), TupleDomain.none()), TupleDomain.all());
        assertEquals(columnWiseUnion(TupleDomain.none(), TupleDomain.none()), TupleDomain.none());
        assertEquals(
                columnWiseUnion(
                        TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.onlyNull(BIGINT))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.notNull(BIGINT)))),
                TupleDomain.<ColumnHandle>all());
    }

    @Test
    public void testMismatchedColumnWiseUnion()
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, Domain.all(DOUBLE),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))));

        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DOUBLE, 0.0)), true),
                        C, Domain.singleValue(BIGINT, 1L)));

        TupleDomain<ColumnHandle> expectedTupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.all(DOUBLE)));

        assertEquals(columnWiseUnion(tupleDomain1, tupleDomain2), expectedTupleDomain);
    }

    @Test
    public void testOverlaps()
    {
        TupleDomain<ColumnHandle> emptyTupleDomain = TupleDomain.withColumnDomains(
                Map.of(A, Domain.create(ValueSet.copyOf(BIGINT, List.of()), false)));
        assertThat(emptyTupleDomain.overlaps(emptyTupleDomain)).isEqualTo(false);

        verifyOverlaps(ImmutableMap.of(), ImmutableMap.of(), true);

        verifyOverlaps(
                ImmutableMap.of(),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L)),
                true);

        verifyOverlaps(
                ImmutableMap.of(),
                ImmutableMap.of(A, Domain.none(BIGINT)),
                false);

        verifyOverlaps(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of(A, Domain.none(BIGINT)),
                false);

        verifyOverlaps(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of(A, Domain.all(BIGINT)),
                true);

        verifyOverlaps(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 1L)),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 1L)),
                true);

        verifyOverlaps(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 1L)),
                ImmutableMap.of(B, Domain.singleValue(VARCHAR, utf8Slice("value"))),
                true);

        verifyOverlaps(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 1L)),
                ImmutableMap.of(A, Domain.all(BIGINT)),
                true);

        verifyOverlaps(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 1L)),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 2L)),
                false);

        verifyOverlaps(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L),
                        B, Domain.singleValue(BIGINT, 1L)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L),
                        B, Domain.singleValue(BIGINT, 2L)),
                false);

        verifyOverlaps(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L),
                        B, Domain.singleValue(BIGINT, 1L)),
                ImmutableMap.of(
                        B, Domain.singleValue(BIGINT, 1L)),
                true);

        verifyOverlaps(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L),
                        B, Domain.singleValue(BIGINT, 1L)),
                ImmutableMap.of(
                        B, Domain.singleValue(BIGINT, 2L)),
                false);

        verifyOverlaps(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L)),
                ImmutableMap.of(
                        B, Domain.singleValue(BIGINT, 2L)),
                true);

        verifyOverlaps(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L),
                        B, Domain.all(BIGINT)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L),
                        B, Domain.singleValue(BIGINT, 2L)),
                true);
    }

    @Test
    public void testContains()
    {
        assertTrue(contains(
                ImmutableMap.of(),
                ImmutableMap.of()));

        assertTrue(contains(
                ImmutableMap.of(),
                ImmutableMap.of(A, Domain.none(BIGINT))));

        assertTrue(contains(
                ImmutableMap.of(),
                ImmutableMap.of(A, Domain.all(BIGINT))));

        assertTrue(contains(
                ImmutableMap.of(),
                ImmutableMap.of(A, Domain.singleValue(DOUBLE, 0.0))));

        assertFalse(contains(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of()));

        assertTrue(contains(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of(A, Domain.none(BIGINT))));

        assertFalse(contains(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of(A, Domain.all(BIGINT))));

        assertFalse(contains(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))));

        assertTrue(contains(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of()));

        assertTrue(contains(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of(A, Domain.none(BIGINT))));

        assertTrue(contains(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of(A, Domain.all(BIGINT))));

        assertTrue(contains(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))));

        assertFalse(contains(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L)),
                ImmutableMap.of()));

        assertTrue(contains(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L)),
                ImmutableMap.of(A, Domain.none(BIGINT))));

        assertFalse(contains(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L)),
                ImmutableMap.of(A, Domain.all(BIGINT))));

        assertTrue(contains(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L)),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))));

        assertFalse(contains(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L)),
                ImmutableMap.of(B, Domain.singleValue(VARCHAR, utf8Slice("value")))));

        assertFalse(contains(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))),
                ImmutableMap.of(B, Domain.singleValue(VARCHAR, utf8Slice("value")))));

        assertTrue(contains(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))),
                ImmutableMap.of(B, Domain.none(VARCHAR))));

        assertTrue(contains(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L),
                        B, Domain.none(VARCHAR))));

        assertTrue(contains(
                ImmutableMap.of(
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value")))));

        assertTrue(contains(
                ImmutableMap.of(
                        A, Domain.all(BIGINT),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value")))));

        assertFalse(contains(
                ImmutableMap.of(
                        A, Domain.all(BIGINT),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value2")))));

        assertTrue(contains(
                ImmutableMap.of(
                        A, Domain.all(BIGINT),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value"))),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value2")),
                        C, Domain.none(VARCHAR))));

        assertFalse(contains(
                ImmutableMap.of(
                        A, Domain.all(BIGINT),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value")),
                        C, Domain.none(VARCHAR)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value2")))));

        assertTrue(contains(
                ImmutableMap.of(
                        A, Domain.all(BIGINT),
                        B, Domain.singleValue(VARCHAR, utf8Slice("value")),
                        C, Domain.none(VARCHAR)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.none(VARCHAR))));
    }

    @Test
    public void testEquals()
    {
        assertTrue(equals(
                ImmutableMap.of(),
                ImmutableMap.of()));

        assertTrue(equals(
                ImmutableMap.of(),
                ImmutableMap.of(A, Domain.all(BIGINT))));

        assertFalse(equals(
                ImmutableMap.of(),
                ImmutableMap.of(A, Domain.none(BIGINT))));

        assertFalse(equals(
                ImmutableMap.of(),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))));

        assertTrue(equals(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of(A, Domain.all(BIGINT))));

        assertFalse(equals(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of(A, Domain.none(BIGINT))));

        assertFalse(equals(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))));

        assertTrue(equals(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of(A, Domain.none(BIGINT))));

        assertFalse(equals(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))));

        assertTrue(equals(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L)),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))));

        assertFalse(equals(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L)),
                ImmutableMap.of(B, Domain.singleValue(BIGINT, 0L))));

        assertFalse(equals(
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L)),
                ImmutableMap.of(A, Domain.singleValue(BIGINT, 1L))));

        assertTrue(equals(
                ImmutableMap.of(A, Domain.all(BIGINT)),
                ImmutableMap.of(B, Domain.all(VARCHAR))));

        assertTrue(equals(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of(B, Domain.none(VARCHAR))));

        assertTrue(equals(
                ImmutableMap.of(A, Domain.none(BIGINT)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.none(VARCHAR))));

        assertFalse(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.none(VARCHAR))));

        assertTrue(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L),
                        C, Domain.none(DOUBLE)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.none(VARCHAR))));

        assertTrue(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.all(DOUBLE)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.all(DOUBLE))));

        assertTrue(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.all(VARCHAR)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        C, Domain.all(DOUBLE))));

        assertFalse(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.all(VARCHAR)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 1L),
                        C, Domain.all(DOUBLE))));

        assertFalse(equals(
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        B, Domain.all(VARCHAR)),
                ImmutableMap.of(
                        A, Domain.singleValue(BIGINT, 0L),
                        C, Domain.singleValue(DOUBLE, 0.0))));
    }

    @Test
    public void testIsNone()
    {
        assertFalse(TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of()).isNone());
        assertFalse(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))).isNone());
        assertTrue(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.none(BIGINT))).isNone());
        assertFalse(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.all(BIGINT))).isNone());
        assertTrue(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.all(BIGINT), B, Domain.none(BIGINT))).isNone());
    }

    @Test
    public void testIsAll()
    {
        assertTrue(TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of()).isAll());
        assertFalse(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L))).isAll());
        assertTrue(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.all(BIGINT))).isAll());
        assertFalse(TupleDomain.withColumnDomains(ImmutableMap.of(A, Domain.singleValue(BIGINT, 0L), B, Domain.all(BIGINT))).isAll());
    }

    @Test
    public void testExtractFixedValues()
    {
        assertEquals(
                TupleDomain.extractFixedValues(TupleDomain.withColumnDomains(
                        ImmutableMap.<ColumnHandle, Domain>builder()
                                .put(A, Domain.all(DOUBLE))
                                .put(B, Domain.singleValue(VARCHAR, utf8Slice("value")))
                                .put(C, Domain.onlyNull(BIGINT))
                                .put(D, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true))
                                .buildOrThrow())).get(),
                ImmutableMap.of(
                        B, NullableValue.of(VARCHAR, utf8Slice("value")),
                        C, NullableValue.asNull(BIGINT)));
    }

    @Test
    public void testExtractFixedValuesFromNone()
    {
        assertFalse(TupleDomain.extractFixedValues(TupleDomain.none()).isPresent());
    }

    @Test
    public void testExtractFixedValuesFromAll()
    {
        assertEquals(TupleDomain.extractFixedValues(TupleDomain.all()).get(), ImmutableMap.of());
    }

    @Test
    public void testSingleValuesMapToDomain()
    {
        assertEquals(
                TupleDomain.fromFixedValues(
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(A, NullableValue.of(BIGINT, 1L))
                                .put(B, NullableValue.of(VARCHAR, utf8Slice("value")))
                                .put(C, NullableValue.of(DOUBLE, 0.01))
                                .put(D, NullableValue.asNull(BOOLEAN))
                                .buildOrThrow()),
                TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>builder()
                        .put(A, Domain.singleValue(BIGINT, 1L))
                        .put(B, Domain.singleValue(VARCHAR, utf8Slice("value")))
                        .put(C, Domain.singleValue(DOUBLE, 0.01))
                        .put(D, Domain.onlyNull(BOOLEAN))
                        .buildOrThrow()));
    }

    @Test
    public void testEmptySingleValuesMapToDomain()
    {
        assertEquals(TupleDomain.fromFixedValues(ImmutableMap.of()), TupleDomain.all());
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

        ObjectMapper mapper = new ObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(ColumnHandle.class, new JsonDeserializer<>()
                        {
                            @Override
                            public ColumnHandle deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                                    throws IOException
                            {
                                return new ObjectMapperProvider().get().readValue(jsonParser, TestingColumnHandle.class);
                            }
                        })
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)));

        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.all();
        assertEquals(tupleDomain, mapper.readValue(mapper.writeValueAsString(tupleDomain), new TypeReference<TupleDomain<ColumnHandle>>() {}));

        tupleDomain = TupleDomain.none();
        assertEquals(tupleDomain, mapper.readValue(mapper.writeValueAsString(tupleDomain), new TypeReference<TupleDomain<ColumnHandle>>() {}));

        tupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(A, NullableValue.of(BIGINT, 1L), B, NullableValue.asNull(VARCHAR)));
        assertEquals(tupleDomain, mapper.readValue(mapper.writeValueAsString(tupleDomain), new TypeReference<TupleDomain<ColumnHandle>>() {}));
    }

    @Test
    public void testTransformKeys()
    {
        Map<Integer, Domain> domains = ImmutableMap.<Integer, Domain>builder()
                .put(1, Domain.singleValue(BIGINT, 1L))
                .put(2, Domain.singleValue(BIGINT, 2L))
                .put(3, Domain.singleValue(BIGINT, 3L))
                .buildOrThrow();

        TupleDomain<Integer> domain = TupleDomain.withColumnDomains(domains);
        TupleDomain<String> transformed = domain.transformKeys(Object::toString);

        Map<String, Domain> expected = ImmutableMap.<String, Domain>builder()
                .put("1", Domain.singleValue(BIGINT, 1L))
                .put("2", Domain.singleValue(BIGINT, 2L))
                .put("3", Domain.singleValue(BIGINT, 3L))
                .buildOrThrow();

        assertEquals(transformed.getDomains().get(), expected);
    }

    @Test
    public void testTransformKeysFailsWithNonUniqueMapping()
    {
        Map<Integer, Domain> domains = ImmutableMap.<Integer, Domain>builder()
                .put(1, Domain.singleValue(BIGINT, 1L))
                .put(2, Domain.singleValue(BIGINT, 2L))
                .put(3, Domain.singleValue(BIGINT, 3L))
                .buildOrThrow();

        TupleDomain<Integer> domain = TupleDomain.withColumnDomains(domains);

        assertThatThrownBy(() -> domain.transformKeys(input -> "x"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Every argument must have a unique mapping. 2 maps to [ SortedRangeSet[type=bigint, ranges=1, {[2]}] ] and [ SortedRangeSet[type=bigint, ranges=1, {[1]}] ]");
    }

    /**
     * Ensure {@link TupleDomain#transformKeys(Function)} fails when the function returns {@code null}.
     * {@code null}-friendliness could be a source of potential bugs. For example code like
     * <pre>{@code
     * TupleDomain<Symbol> tupleDomain = ...;
     * TupleDomain<ColumnHandle> converted = tupleDomain.transform(tableScan.getAssignments()::get);
     * }</pre>
     * would silently drops information about correlated symbols.
     */
    @Test
    public void testTransformKeysRejectsNull()
    {
        Map<Integer, Domain> domains = ImmutableMap.<Integer, Domain>builder()
                .put(1, Domain.singleValue(BIGINT, 1L))
                .put(2, Domain.singleValue(BIGINT, 2L))
                .put(3, Domain.singleValue(BIGINT, 3L))
                .buildOrThrow();

        TupleDomain<Integer> domain = TupleDomain.withColumnDomains(domains);

        assertThatThrownBy(() -> domain.transformKeys(input -> input == 2 ? null : input))
                .isInstanceOf(NullPointerException.class)
                .hasMessageMatching("mapping function \\S+ returned null for 2");
    }

    @Test
    public void testAsPredicate()
    {
        NullableValue doubleNull = NullableValue.asNull(DOUBLE);
        NullableValue doubleZero = NullableValue.of(DOUBLE, 0.0);
        NullableValue doubleOne = NullableValue.of(DOUBLE, 1.0);

        ValueSet doublePositiveValues = ValueSet.ofRanges(Range.greaterThan(DOUBLE, 0.0));

        TupleDomain<ColumnHandle> aJustZero = TupleDomain.withColumnDomains(Map.of(A, Domain.singleValue(DOUBLE, 0.0)));
        TupleDomain<ColumnHandle> aJustNull = TupleDomain.withColumnDomains(Map.of(A, Domain.onlyNull(DOUBLE)));
        TupleDomain<ColumnHandle> aZeroAndNull = TupleDomain.withColumnDomains(Map.of(A, Domain.create(ValueSet.of(DOUBLE, 0.0), true)));
        TupleDomain<ColumnHandle> aPositive = TupleDomain.withColumnDomains(Map.of(A, Domain.create(doublePositiveValues, false)));
        TupleDomain<ColumnHandle> bPositive = TupleDomain.withColumnDomains(Map.of(B, Domain.create(doublePositiveValues, false)));
        TupleDomain<ColumnHandle> abPositive = TupleDomain.withColumnDomains(Map.of(
                A, Domain.create(doublePositiveValues, false),
                B, Domain.create(doublePositiveValues, false)));

        // all
        testAsPredicate(TupleDomain.all(), Map.of(), true);
        testAsPredicate(TupleDomain.all(), Map.of(A, doubleZero), true);
        testAsPredicate(TupleDomain.all(), Map.of(A, doubleNull), true);

        // none
        testAsPredicate(TupleDomain.none(), Map.of(), false);
        testAsPredicate(TupleDomain.none(), Map.of(A, doubleZero), false);
        testAsPredicate(TupleDomain.none(), Map.of(A, doubleNull), false);

        // empty bindings
        testAsPredicate(aJustZero, Map.of(), true);
        testAsPredicate(aJustNull, Map.of(), true);
        testAsPredicate(aPositive, Map.of(), true);
        testAsPredicate(bPositive, Map.of(), true);
        testAsPredicate(abPositive, Map.of(), true);

        // constraint on same column
        testAsPredicate(aJustZero, Map.of(A, doubleZero), true);
        testAsPredicate(aJustZero, Map.of(A, doubleNull), false);

        testAsPredicate(aJustNull, Map.of(A, doubleZero), false);
        testAsPredicate(aJustNull, Map.of(A, doubleNull), true);

        testAsPredicate(aZeroAndNull, Map.of(A, doubleZero), true);
        testAsPredicate(aZeroAndNull, Map.of(A, doubleNull), true);

        testAsPredicate(aPositive, Map.of(A, doubleZero), false);
        testAsPredicate(aPositive, Map.of(A, doubleNull), false);

        // constraint on different column
        testAsPredicate(bPositive, Map.of(A, doubleZero), true);
        testAsPredicate(bPositive, Map.of(A, doubleNull), true);

        // constraint and binding keys intersecting
        testAsPredicate(abPositive,
                Map.of(
                        B, doubleZero,
                        C, doubleOne),
                false);
        testAsPredicate(abPositive,
                Map.of(
                        B, doubleOne,
                        C, doubleOne),
                true);
        testAsPredicate(abPositive,
                Map.of(
                        B, doubleOne,
                        C, doubleZero),
                true);
    }

    private void testAsPredicate(TupleDomain<ColumnHandle> tupleDomain, Map<ColumnHandle, NullableValue> bindings, boolean expected)
    {
        Predicate<Map<ColumnHandle, NullableValue>> predicate = tupleDomain.asPredicate();
        boolean result = predicate.test(bindings);
        if (result != expected) {
            fail(format("asPredicate(%s).test(%s) returned %s instead of %s", tupleDomain, bindings, result, expected));
        }
    }

    private void verifyOverlaps(Map<ColumnHandle, Domain> domains1, Map<ColumnHandle, Domain> domains2, boolean expected)
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(domains1);
        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(domains2);
        assertThat(tupleDomain1.overlaps(tupleDomain2)).isEqualTo(expected);
        assertThat(tupleDomain2.overlaps(tupleDomain1)).isEqualTo(expected);
        assertThat(tupleDomain1.intersect(tupleDomain2).isNone()).isEqualTo(!expected);
    }

    private boolean contains(Map<ColumnHandle, Domain> superSet, Map<ColumnHandle, Domain> subSet)
    {
        TupleDomain<ColumnHandle> superSetTupleDomain = TupleDomain.withColumnDomains(superSet);
        TupleDomain<ColumnHandle> subSetTupleDomain = TupleDomain.withColumnDomains(subSet);
        boolean contains = superSetTupleDomain.contains(subSetTupleDomain);
        // Results from computing contains using union and using the method directly should match
        assertThat(contains).isEqualTo(containsFromUnion(superSetTupleDomain, subSetTupleDomain));
        return contains;
    }

    private static boolean containsFromUnion(TupleDomain<ColumnHandle> superSetTupleDomain, TupleDomain<ColumnHandle> subSetTupleDomain)
    {
        return subSetTupleDomain.isNone()
                || columnWiseUnion(superSetTupleDomain, subSetTupleDomain).equals(superSetTupleDomain);
    }

    private boolean equals(Map<ColumnHandle, Domain> domains1, Map<ColumnHandle, Domain> domains2)
    {
        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.withColumnDomains(domains1);
        TupleDomain<ColumnHandle> tupleDOmain2 = TupleDomain.withColumnDomains(domains2);
        return tupleDomain1.equals(tupleDOmain2);
    }
}
