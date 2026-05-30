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
package io.trino.spi.type;

import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlockBuilder;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestMultisetType
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final MultisetType MULTISET_BIGINT = new MultisetType(BIGINT, TYPE_OPERATORS);
    private static final RowType ROW_BIGINT_BIGINT = RowType.anonymousRow(BIGINT, BIGINT);
    private static final MultisetType MULTISET_ROW = new MultisetType(ROW_BIGINT_BIGINT, TYPE_OPERATORS);
    private static final MultisetType MULTISET_DOUBLE = new MultisetType(DOUBLE, TYPE_OPERATORS);

    @Test
    void testDisplayNameAndDescriptor()
    {
        assertThat(MULTISET_BIGINT.getDisplayName()).isEqualTo("multiset(bigint)");
        assertThat(MULTISET_BIGINT.getTypeDescriptor().toString()).isEqualTo("multiset(bigint)");
        assertThat(MULTISET_BIGINT.getElementType()).isEqualTo(BIGINT);
        // equality-comparable only: SQL:2023 forbids multisets in ordering operations
        assertThat(MULTISET_BIGINT.isComparable()).isTrue();
        assertThat(MULTISET_BIGINT.isOrderable()).isFalse();
    }

    @Test
    void testNotOrderable()
    {
        // no comparison operator is registered, so ORDER BY / < / MAX over a multiset cannot resolve
        assertThatThrownBy(() -> TYPE_OPERATORS.getComparisonUnorderedLastOperator(MULTISET_BIGINT, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testEqualityIsOrderIndependent()
            throws Throwable
    {
        // same elements, different order and different storage cardinality of duplicates
        assertThat(equal(multiset(1L, 2L, 2L), multiset(2L, 1L, 2L))).isTrue();
        assertThat(equal(multiset(1L, 2L), multiset(2L, 1L))).isTrue();
    }

    @Test
    void testEqualityRespectsMultiplicity()
            throws Throwable
    {
        assertThat(equal(multiset(1L, 2L, 2L), multiset(1L, 2L))).isFalse();
        assertThat(equal(multiset(1L, 1L, 2L), multiset(1L, 2L, 2L))).isFalse();
        assertThat(equal(multiset(1L), multiset(2L))).isFalse();
    }

    @Test
    void testEqualityWithNullElementsIsUnknown()
            throws Throwable
    {
        // a null element makes the pairing undecidable, mirroring SQL three-valued logic
        assertThat(equal(multiset(1L, null), multiset(1L, 2L))).isNull();
        assertThat(equal(multiset(1L, null), multiset(1L, null))).isNull();
        assertThat(equal(multiset(null, null), multiset(1L, 2L))).isNull();
        // cardinality mismatch is decided before inspecting elements
        assertThat(equal(multiset(1L, null), multiset(1L))).isFalse();
    }

    @Test
    void testEqualityIsUnknownOnlyWhenACompletePairingExists()
            throws Throwable
    {
        // ISO/IEC 9075-2:2023 section 8.2 GR 2)b)iii: unknown requires a complete pairing in which
        // every pair is true or unknown; otherwise the result is definite false. Both 1 and 2 can
        // individually pair with the null, but only one null exists — the other element must pair
        // with 3, which is definite false.
        assertThat(equal(multiset(1L, 2L), multiset(3L, null))).isFalse();
        // the same pigeonhole with the null on the left: 2 and 3 cannot both pair with it
        assertThat(equal(multiset(null, 1L), multiset(2L, 3L))).isFalse();
        // with a true pair for 1, the null is free to absorb the 2, so a complete pairing exists
        assertThat(equal(multiset(1L, 2L), multiset(1L, null))).isNull();
        assertThat(equal(multiset(1L, 1L), multiset(1L, null))).isNull();
    }

    @Test
    void testEqualityWithIndeterminateElements()
            throws Throwable
    {
        // an indeterminate element (a nested null) pairs as unknown with some values and as definite
        // false with others, so deciding whether a complete true-or-unknown pairing exists needs the
        // bipartite matching path
        assertThat(rowEqual(
                rowMultiset(row(1L, null), row(2L, null)),
                rowMultiset(row(1L, 1L), row(2L, 2L)))).isNull();
        // pigeonhole: both left rows are pairable only with (1, 1)
        assertThat(rowEqual(
                rowMultiset(row(1L, null), row(1L, null)),
                rowMultiset(row(1L, 1L), row(2L, 2L)))).isFalse();
        // the matching must reroute: (1, 1) pairs only with the right (1, 1), so (1, null) — which
        // is pairable with both — has to take (1, 2)
        assertThat(rowEqual(
                rowMultiset(row(1L, 1L), row(1L, null)),
                rowMultiset(row(1L, 1L), row(1L, 2L)))).isNull();
        // an element pairable with nothing decides false outright
        assertThat(rowEqual(
                rowMultiset(row(1L, null)),
                rowMultiset(row(2L, 2L)))).isFalse();
    }

    @Test
    void testEqualityUsesValueEqualityNotIdentity()
            throws Throwable
    {
        // NaN is the one determinate value where = and IDENTICAL diverge: NaN = NaN is false while
        // NaN IS NOT DISTINCT FROM NaN is true, so the bags compare unequal but identical
        assertThat(doubleEqual(doubleMultiset(Double.NaN), doubleMultiset(Double.NaN))).isFalse();
        assertThat(doubleIdentical(doubleMultiset(Double.NaN), doubleMultiset(Double.NaN))).isTrue();
    }

    @Test
    void testNonComparableElementTypeRejected()
    {
        // a multiset is a bag keyed by element equality, so a non-comparable element type has no
        // usable value (mirrors the map key requirement)
        assertThatThrownBy(() -> new MultisetType(HYPER_LOG_LOG, TYPE_OPERATORS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("element type must be comparable, got HyperLogLog");
    }

    @Test
    void testIdenticalIsOrderIndependentAndMultiplicitySensitive()
            throws Throwable
    {
        assertThat(identical(multiset(1L, 2L, 2L), multiset(2L, 1L, 2L))).isTrue();
        assertThat(identical(multiset(1L, 2L, 2L), multiset(1L, 2L))).isFalse();
        assertThat(identical(multiset(1L, 1L, 2L), multiset(1L, 2L, 2L))).isFalse();
    }

    @Test
    void testIdenticalTreatsNullAsNotDistinct()
            throws Throwable
    {
        // unlike value equality, IDENTICAL is total: null is not distinct from null, giving definite results
        assertThat(identical(multiset(1L, null), multiset(null, 1L))).isTrue();
        assertThat(identical(multiset(null, null), multiset(null, null))).isTrue();
        assertThat(identical(multiset(null, null), multiset((Long) null))).isFalse();
        assertThat(identical(multiset(1L, null), multiset(1L, 2L))).isFalse();
    }

    @Test
    void testHashIsOrderIndependentAndMultiplicitySensitive()
            throws Throwable
    {
        assertThat(hash(multiset(1L, 2L, 2L))).isEqualTo(hash(multiset(2L, 2L, 1L)));
        // duplicate elements must not cancel out (as XOR would)
        assertThat(hash(multiset(1L, 1L))).isNotEqualTo(hash(multiset(1L)));
    }

    private static Boolean equal(Block left, Block right)
            throws Throwable
    {
        MethodHandle handle = TYPE_OPERATORS.getEqualOperator(MULTISET_BIGINT, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION));
        return (Boolean) handle.invokeExact(left, 0, right, 0);
    }

    private static boolean identical(Block left, Block right)
            throws Throwable
    {
        MethodHandle handle = TYPE_OPERATORS.getIdenticalOperator(MULTISET_BIGINT, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));
        return (boolean) handle.invokeExact(left, 0, right, 0);
    }

    private static long hash(Block block)
            throws Throwable
    {
        MethodHandle handle = TYPE_OPERATORS.getHashCodeOperator(MULTISET_BIGINT, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION));
        return (long) handle.invokeExact(block, 0);
    }

    private static Boolean rowEqual(Block left, Block right)
            throws Throwable
    {
        MethodHandle handle = TYPE_OPERATORS.getEqualOperator(MULTISET_ROW, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION));
        return (Boolean) handle.invokeExact(left, 0, right, 0);
    }

    private static Boolean doubleEqual(Block left, Block right)
            throws Throwable
    {
        MethodHandle handle = TYPE_OPERATORS.getEqualOperator(MULTISET_DOUBLE, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION));
        return (Boolean) handle.invokeExact(left, 0, right, 0);
    }

    private static boolean doubleIdentical(Block left, Block right)
            throws Throwable
    {
        MethodHandle handle = TYPE_OPERATORS.getIdenticalOperator(MULTISET_DOUBLE, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));
        return (boolean) handle.invokeExact(left, 0, right, 0);
    }

    private static Block multiset(Long... elements)
    {
        ArrayBlockBuilder builder = MULTISET_BIGINT.createBlockBuilder(null, 1);
        builder.buildEntry(elementBuilder -> {
            for (Long element : Arrays.asList(elements)) {
                if (element == null) {
                    elementBuilder.appendNull();
                }
                else {
                    BIGINT.writeLong(elementBuilder, element);
                }
            }
        });
        return builder.build();
    }

    private static Long[] row(Long first, Long second)
    {
        return new Long[] {first, second};
    }

    private static Block rowMultiset(Long[]... rows)
    {
        ArrayBlockBuilder builder = MULTISET_ROW.createBlockBuilder(null, 1);
        builder.buildEntry(elementBuilder -> {
            RowBlockBuilder rowBuilder = (RowBlockBuilder) elementBuilder;
            for (Long[] row : Arrays.asList(rows)) {
                rowBuilder.buildEntry(fieldBuilders -> {
                    for (int field = 0; field < row.length; field++) {
                        if (row[field] == null) {
                            fieldBuilders.get(field).appendNull();
                        }
                        else {
                            BIGINT.writeLong(fieldBuilders.get(field), row[field]);
                        }
                    }
                });
            }
        });
        return builder.build();
    }

    private static Block doubleMultiset(Double... elements)
    {
        ArrayBlockBuilder builder = MULTISET_DOUBLE.createBlockBuilder(null, 1);
        builder.buildEntry(elementBuilder -> {
            for (Double element : Arrays.asList(elements)) {
                if (element == null) {
                    elementBuilder.appendNull();
                }
                else {
                    DOUBLE.writeDouble(elementBuilder, element);
                }
            }
        });
        return builder.build();
    }
}
