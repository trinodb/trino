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
package io.trino.type;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.UnknownType.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestMultiset
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testConstructor()
    {
        assertThat(assertions.expression("MULTISET[]"))
                .hasType(new MultisetType(UNKNOWN, TYPE_OPERATORS))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.expression("MULTISET[a]")
                .binding("a", "7"))
                .hasType(new MultisetType(INTEGER, TYPE_OPERATORS))
                .isEqualTo(ImmutableList.of(7));

        // duplicates are retained
        assertThat(assertions.expression("MULTISET[1, 2, 2]"))
                .hasType(new MultisetType(INTEGER, TYPE_OPERATORS))
                .isEqualTo(ImmutableList.of(1, 2, 2));

        // the element type is the common supertype of the elements
        assertThat(assertions.expression("MULTISET[CAST(1 AS bigint), 2]"))
                .hasType(new MultisetType(BIGINT, TYPE_OPERATORS))
                .isEqualTo(ImmutableList.of(1L, 2L));

        // a multiset is a bag keyed by element equality, so the element type must be comparable
        assertThat(assertions.query("SELECT MULTISET[approx_set('x')]"))
                .failure()
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessageContaining("Multiset element type must be comparable");
    }

    @Test
    public void testTypeSyntax()
    {
        // the <type> MULTISET form names a multiset type
        assertThat(assertions.expression("CAST(MULTISET[1, 2, 2] AS integer MULTISET)"))
                .hasType(new MultisetType(INTEGER, TYPE_OPERATORS))
                .isEqualTo(ImmutableList.of(1, 2, 2));
    }

    @Test
    public void testCardinality()
    {
        assertThat(assertions.expression("CARDINALITY(MULTISET[1, 2, 2])"))
                .isEqualTo(3L);
        assertThat(assertions.expression("CARDINALITY(MULTISET[])"))
                .isEqualTo(0L);
    }

    @Test
    public void testUnnest()
    {
        // UNNEST expands a multiset into one row per element, retaining duplicates
        assertThat(assertions.query("SELECT x FROM UNNEST(MULTISET[1, 2, 2]) t(x)"))
                .matches("VALUES 1, 2, 2");

        // a multiset of rows expands each field into a column
        assertThat(assertions.query("SELECT a, b FROM UNNEST(MULTISET[ROW(1, 'a'), ROW(2, 'b')]) t(a, b)"))
                .matches("VALUES (1, 'a'), (2, 'b')");

        // a multiset has no ordinal positions, so WITH ORDINALITY is rejected
        assertThat(assertions.query("SELECT x, n FROM UNNEST(MULTISET[10, 20]) WITH ORDINALITY t(x, n)"))
                .failure()
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("UNNEST of a multiset does not support WITH ORDINALITY");

        // a multiset must be the only UNNEST operand
        assertThat(assertions.query("SELECT x, y FROM UNNEST(MULTISET[1, 2], ARRAY[3, 4]) t(x, y)"))
                .failure()
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("UNNEST of a multiset cannot be combined with other expressions");
    }

    @Test
    public void testCastToAndFromArray()
    {
        // array -> multiset discards order; the result compares as a bag
        assertThat(assertions.expression("CAST(ARRAY[1, 2, 2] AS multiset(integer))"))
                .hasType(new MultisetType(INTEGER, TYPE_OPERATORS))
                .isEqualTo(ImmutableList.of(1, 2, 2));
        assertThat(assertions.expression("CAST(ARRAY[2, 1, 2] AS multiset(integer)) = MULTISET[1, 2, 2]"))
                .isEqualTo(true);

        // multiset -> array materializes the elements as an array
        assertThat(assertions.expression("CAST(MULTISET[1, 2, 2] AS array(integer))"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2, 2));

        // the element type is coerced
        assertThat(assertions.expression("CAST(ARRAY[1, 2] AS multiset(bigint))"))
                .hasType(new MultisetType(BIGINT, TYPE_OPERATORS))
                .isEqualTo(ImmutableList.of(1L, 2L));

        // round trip
        assertThat(assertions.expression("CAST(CAST(MULTISET[1, 2, 2] AS array(integer)) AS multiset(integer)) = MULTISET[1, 2, 2]"))
                .isEqualTo(true);
    }

    @Test
    public void testCastBetweenMultisets()
    {
        // multiset -> multiset coerces the element type
        assertThat(assertions.expression("CAST(MULTISET[1, 2, 2] AS multiset(bigint))"))
                .hasType(new MultisetType(BIGINT, TYPE_OPERATORS))
                .isEqualTo(ImmutableList.of(1L, 2L, 2L));

        // an empty multiset(unknown) is usable where a typed multiset is expected
        assertThat(assertions.expression("CARDINALITY(CAST(MULTISET[] AS multiset(integer)))"))
                .isEqualTo(0L);
    }

    @Test
    public void testElement()
    {
        assertThat(assertions.expression("ELEMENT(MULTISET[42])"))
                .hasType(INTEGER)
                .isEqualTo(42);
        // the sole element may itself be the null value
        assertThat(assertions.expression("ELEMENT(MULTISET[NULL]) IS NULL"))
                .isEqualTo(true);
        // empty multiset yields the null value
        assertThat(assertions.expression("ELEMENT(MULTISET[]) IS NULL"))
                .isEqualTo(true);
        // more than one element is a cardinality violation
        assertTrinoExceptionThrownBy(() -> assertions.expression("ELEMENT(MULTISET[1, 2])").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("Cardinality violation: multiset passed to ELEMENT has more than one element");
        // the violation counts elements, not distinct values: two copies of one value still violate
        assertTrinoExceptionThrownBy(() -> assertions.expression("ELEMENT(MULTISET[1, 1])").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("Cardinality violation: multiset passed to ELEMENT has more than one element");
    }

    @Test
    public void testSet()
    {
        assertThat(assertions.expression("SET(MULTISET[1, 1, 2, 2, 3]) = MULTISET[1, 2, 3]"))
                .isEqualTo(true);
        assertThat(assertions.expression("CARDINALITY(SET(MULTISET[1, 1, 2]))"))
                .isEqualTo(2L);
        // already a set: unchanged
        assertThat(assertions.expression("SET(MULTISET[1, 2, 3]) = MULTISET[3, 2, 1]"))
                .isEqualTo(true);
        // null is not distinct from null: duplicate nulls collapse to a single element
        assertThat(assertions.expression("CARDINALITY(SET(MULTISET[NULL, NULL]))"))
                .isEqualTo(1L);
        assertThat(assertions.expression("CARDINALITY(SET(MULTISET[1, NULL, 1, NULL]))"))
                .isEqualTo(2L);
    }

    @Test
    public void testNoImplicitCoercionWithArray()
    {
        // ARRAY and MULTISET are distinct kinds: comparing them requires an explicit cast
        assertTrinoExceptionThrownBy(() -> assertions.expression("MULTISET[1] = ARRAY[1]").evaluate())
                .hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testUnion()
    {
        // ALL adds multiplicities (and is the default when neither ALL nor DISTINCT is given)
        assertThat(assertions.expression("MULTISET[1, 2] MULTISET UNION ALL MULTISET[2, 3] = MULTISET[1, 2, 2, 3]"))
                .isEqualTo(true);
        assertThat(assertions.expression("CARDINALITY(MULTISET[1, 2] MULTISET UNION MULTISET[2, 3])"))
                .isEqualTo(4L);
        // DISTINCT yields the set union
        assertThat(assertions.expression("MULTISET[1, 2, 2] MULTISET UNION DISTINCT MULTISET[2, 3] = MULTISET[1, 2, 3]"))
                .isEqualTo(true);
        // operands with different element types: the element coerces to the common supertype during
        // binding, because multiset is covariant in its element type (like array)
        assertThat(assertions.expression("CARDINALITY(MULTISET[] MULTISET UNION ALL MULTISET[1])"))
                .isEqualTo(1L);
        assertThat(assertions.expression("MULTISET[CAST(1 AS bigint)] MULTISET UNION ALL MULTISET[1]"))
                .hasType(new MultisetType(BIGINT, TYPE_OPERATORS))
                .isEqualTo(ImmutableList.of(1L, 1L));
    }

    @Test
    public void testIntersect()
    {
        // ALL takes the minimum multiplicity per value
        assertThat(assertions.expression("MULTISET[1, 1, 2, 3] MULTISET INTERSECT ALL MULTISET[1, 2, 2] = MULTISET[1, 2]"))
                .isEqualTo(true);
        // DISTINCT keeps each shared value once
        assertThat(assertions.expression("CARDINALITY(MULTISET[1, 1, 2] MULTISET INTERSECT DISTINCT MULTISET[2, 2, 3])"))
                .isEqualTo(1L);
        // null is not distinct from null: a null element matches and is retained (here MULTISET[NULL])
        assertThat(assertions.expression("CARDINALITY(MULTISET[1, NULL] MULTISET INTERSECT ALL MULTISET[NULL])"))
                .isEqualTo(1L);
        assertThat(assertions.expression("ELEMENT(MULTISET[1, NULL] MULTISET INTERSECT ALL MULTISET[NULL]) IS NULL"))
                .isEqualTo(true);
    }

    @Test
    public void testExcept()
    {
        // ALL subtracts multiplicities
        assertThat(assertions.expression("MULTISET[1, 1, 2, 3] MULTISET EXCEPT ALL MULTISET[1, 3] = MULTISET[1, 2]"))
                .isEqualTo(true);
        // DISTINCT keeps each left value not present on the right, once
        assertThat(assertions.expression("CARDINALITY(MULTISET[1, 1, 2, 3] MULTISET EXCEPT DISTINCT MULTISET[1, 3])"))
                .isEqualTo(1L);
        // null is not distinct from null: one null is cancelled and the other remains (MULTISET[NULL])
        assertThat(assertions.expression("CARDINALITY(MULTISET[NULL, NULL] MULTISET EXCEPT ALL MULTISET[NULL])"))
                .isEqualTo(1L);
        assertThat(assertions.expression("ELEMENT(MULTISET[NULL, NULL] MULTISET EXCEPT ALL MULTISET[NULL]) IS NULL"))
                .isEqualTo(true);
    }

    @Test
    public void testSetOperationPrecedenceAndNull()
    {
        // INTERSECT binds tighter than UNION: 1 UNION (1 INTERSECT 1) = {1, 1}, cardinality 2
        assertThat(assertions.expression("CARDINALITY(MULTISET[1] MULTISET UNION MULTISET[1] MULTISET INTERSECT MULTISET[1])"))
                .isEqualTo(2L);
        // a null operand yields a null result
        assertThat(assertions.expression("(CAST(NULL AS multiset(integer)) MULTISET UNION ALL MULTISET[1]) IS NULL"))
                .isEqualTo(true);
    }

    @Test
    public void testSubmultiset()
    {
        // every element present with at least the same multiplicity
        assertThat(assertions.expression("MULTISET[1, 1, 2] SUBMULTISET OF MULTISET[1, 1, 1, 2]"))
                .isEqualTo(true);
        // too many copies of 1
        assertThat(assertions.expression("MULTISET[1, 1, 2] SUBMULTISET OF MULTISET[1, 2]"))
                .isEqualTo(false);
        // the empty multiset is a submultiset of anything
        assertThat(assertions.expression("MULTISET[] SUBMULTISET OF MULTISET[1, 2]"))
                .isEqualTo(true);
        // OF is optional, and NOT negates
        assertThat(assertions.expression("MULTISET[3] NOT SUBMULTISET MULTISET[1, 2]"))
                .isEqualTo(true);
        // a null operand yields null
        assertThat(assertions.expression("(MULTISET[1] SUBMULTISET OF CAST(NULL AS multiset(integer))) IS NULL"))
                .isEqualTo(true);
        // null elements match by IDENTICAL (not distinct from null), so the multiplicity of null is
        // what counts -- this is a definite true/false, never unknown
        assertThat(assertions.expression("MULTISET[NULL] SUBMULTISET OF MULTISET[NULL]"))
                .isEqualTo(true);
        assertThat(assertions.expression("MULTISET[NULL, NULL] SUBMULTISET OF MULTISET[NULL]"))
                .isEqualTo(false);
    }

    @Test
    public void testIsASet()
    {
        assertThat(assertions.expression("MULTISET[1, 2, 3] IS A SET"))
                .isEqualTo(true);
        assertThat(assertions.expression("MULTISET[1, 2, 2] IS A SET"))
                .isEqualTo(false);
        assertThat(assertions.expression("MULTISET[1, 2, 2] IS NOT A SET"))
                .isEqualTo(true);
        // the empty multiset is a set
        assertThat(assertions.expression("MULTISET[] IS A SET"))
                .isEqualTo(true);
        // null is not distinct from null, so two nulls are duplicates: a definite false, not unknown
        assertThat(assertions.expression("MULTISET[NULL, NULL] IS A SET"))
                .isEqualTo(false);
        assertThat(assertions.expression("MULTISET[1, NULL] IS A SET"))
                .isEqualTo(true);
    }

    @Test
    public void testMember()
    {
        assertThat(assertions.expression("1 MEMBER OF MULTISET[1, 2]"))
                .isEqualTo(true);
        assertThat(assertions.expression("3 MEMBER OF MULTISET[1, 2]"))
                .isEqualTo(false);
        // OF is optional, and NOT negates
        assertThat(assertions.expression("3 NOT MEMBER MULTISET[1, 2]"))
                .isEqualTo(true);
        // the empty multiset has no members
        assertThat(assertions.expression("1 MEMBER OF MULTISET[]"))
                .isEqualTo(false);
        // the element type is coerced to a common supertype with the value
        assertThat(assertions.expression("CAST(1 AS bigint) MEMBER OF MULTISET[1, 2]"))
                .isEqualTo(true);
        // three-valued like IN: a null value, or no match in the presence of a null element, is unknown
        assertThat(assertions.expression("(CAST(NULL AS integer) MEMBER OF MULTISET[1]) IS NULL"))
                .isEqualTo(true);
        assertThat(assertions.expression("(1 MEMBER OF MULTISET[NULL, 2]) IS NULL"))
                .isEqualTo(true);
        // a definite match is true even when a null element is present
        assertThat(assertions.expression("1 MEMBER OF MULTISET[NULL, 1]"))
                .isEqualTo(true);
        // the section 8.16 rules are ordered: the empty multiset decides false before the null
        // value rule
        assertThat(assertions.expression("CAST(NULL AS integer) MEMBER OF MULTISET[]"))
                .isEqualTo(false);
        assertThat(assertions.expression("CAST(NULL AS integer) NOT MEMBER OF MULTISET[]"))
                .isEqualTo(true);
        // membership follows the element = semantics, not IDENTICAL: nan() = nan() is false, so
        // nan() is not a member even of a multiset containing it (matching nan() IN (nan()))
        assertThat(assertions.expression("nan() MEMBER OF MULTISET[nan()]"))
                .isEqualTo(false);
        // the same answer must come from both paths: the index-backed fast path on a fully
        // determinate multiset, and the = scan once an indeterminate element forces the fallback
        assertThat(assertions.expression("ROW(nan(), 1) MEMBER OF MULTISET[ROW(nan(), 1)]"))
                .isEqualTo(false);
        assertThat(assertions.expression("ROW(nan(), 1) MEMBER OF MULTISET[ROW(nan(), 1), CAST(ROW(2e0, NULL) AS ROW(double, integer))]"))
                .isEqualTo(false);
        // an element whose equality with the value is unknown (a nested null next to matching
        // fields) makes an unmatched probe unknown
        assertThat(assertions.expression("(ROW(2e0, 1) MEMBER OF MULTISET[CAST(ROW(2e0, NULL) AS ROW(double, integer))]) IS NULL"))
                .isEqualTo(true);
    }

    @Test
    public void testCollect()
    {
        // collects values into a multiset, retaining duplicates
        assertThat(assertions.query("SELECT CARDINALITY(COLLECT(x)) FROM (VALUES 1, 1, 2) t(x)"))
                .matches("VALUES BIGINT '3'");
        assertThat(assertions.query("SELECT COLLECT(x) = MULTISET[1, 1, 2] FROM (VALUES 1, 1, 2) t(x)"))
                .matches("VALUES true");
        // grouped
        assertThat(assertions.query("SELECT k, CARDINALITY(COLLECT(v)) FROM (VALUES (1, 10), (1, 20), (2, 30)) t(k, v) GROUP BY k"))
                .matches("VALUES (1, BIGINT '2'), (2, BIGINT '1')");
        // null inputs are retained as null elements (SQL:2023), not skipped
        assertThat(assertions.query("SELECT CARDINALITY(COLLECT(x)) FROM (VALUES 1, NULL, 2) t(x)"))
                .matches("VALUES BIGINT '3'");
        assertThat(assertions.query("SELECT CARDINALITY(COLLECT(x)) FROM (VALUES CAST(NULL AS integer), NULL) t(x)"))
                .matches("VALUES BIGINT '2'");
    }

    @Test
    public void testFusion()
    {
        // multiplicities add across the input multisets
        assertThat(assertions.query("SELECT FUSION(m) = MULTISET[1, 1, 1, 2] FROM (VALUES MULTISET[1, 1], MULTISET[1, 2]) t(m)"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT CARDINALITY(FUSION(m)) FROM (VALUES MULTISET[1, 1], MULTISET[1, 2], MULTISET[3]) t(m)"))
                .matches("VALUES BIGINT '5'");
        // a NULL multiset operand is ignored
        assertThat(assertions.query("SELECT FUSION(m) = MULTISET[1, 1, 1, 2] FROM (VALUES MULTISET[1, 1], CAST(NULL AS multiset(integer)), MULTISET[1, 2]) t(m)"))
                .matches("VALUES true");
        // null elements inside the inputs are retained
        assertThat(assertions.query("SELECT CARDINALITY(FUSION(m)) FROM (VALUES MULTISET[1, NULL], MULTISET[NULL]) t(m)"))
                .matches("VALUES BIGINT '3'");
        // grouped, exercising the partial-merge path
        assertThat(assertions.query("SELECT k, CARDINALITY(FUSION(m)) FROM (VALUES (1, MULTISET[1, 1]), (1, MULTISET[2]), (2, MULTISET[3, 3, 3])) t(k, m) GROUP BY k"))
                .matches("VALUES (1, BIGINT '3'), (2, BIGINT '3')");
    }

    @Test
    public void testIntersection()
    {
        // minimum multiplicity per value across the input multisets
        assertThat(assertions.query("SELECT INTERSECTION(m) = MULTISET[1] FROM (VALUES MULTISET[1, 1, 2], MULTISET[1, 3]) t(m)"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT CARDINALITY(INTERSECTION(m)) FROM (VALUES MULTISET[1, 1, 2], MULTISET[1, 1, 2, 2]) t(m)"))
                .matches("VALUES BIGINT '3'");
        // disjoint inputs intersect to the empty multiset
        assertThat(assertions.query("SELECT CARDINALITY(INTERSECTION(m)) FROM (VALUES MULTISET[1], MULTISET[2]) t(m)"))
                .matches("VALUES BIGINT '0'");
        // a single input is returned unchanged
        assertThat(assertions.query("SELECT INTERSECTION(m) = MULTISET[1, 1, 2] FROM (VALUES MULTISET[1, 1, 2]) t(m)"))
                .matches("VALUES true");
        // a NULL multiset operand is ignored, not allowed to collapse the result
        assertThat(assertions.query("SELECT CARDINALITY(INTERSECTION(m)) FROM (VALUES MULTISET[1, 1, 2], CAST(NULL AS multiset(integer)), MULTISET[1, 3]) t(m)"))
                .matches("VALUES BIGINT '1'");
        // null elements are matched by IDENTICAL (null not distinct from null)
        assertThat(assertions.query("SELECT CARDINALITY(INTERSECTION(m)) FROM (VALUES MULTISET[1, NULL], MULTISET[NULL]) t(m)"))
                .matches("VALUES BIGINT '1'");
        assertThat(assertions.query("SELECT ELEMENT(INTERSECTION(m)) IS NULL FROM (VALUES MULTISET[1, NULL], MULTISET[NULL]) t(m)"))
                .matches("VALUES true");
        // a single empty multiset seeds a real empty state (non-null), distinct from the zero-row NULL case
        assertThat(assertions.query("SELECT INTERSECTION(m) IS NOT NULL FROM (VALUES CAST(MULTISET[] AS multiset(integer))) t(m)"))
                .matches("VALUES true");
        // grouped, exercising the partial-merge path
        assertThat(assertions.query("SELECT k, CARDINALITY(INTERSECTION(m)) FROM (VALUES (1, MULTISET[1, 1, 2]), (1, MULTISET[1, 2, 2]), (2, MULTISET[3, 3])) t(k, m) GROUP BY k"))
                .matches("VALUES (1, BIGINT '2'), (2, BIGINT '2')");
    }

    @Test
    public void testEqualityIsOrderIndependent()
    {
        assertThat(assertions.expression("MULTISET[1, 2, 2] = MULTISET[2, 1, 2]"))
                .isEqualTo(true);
        assertThat(assertions.expression("MULTISET[1, 2] = MULTISET[2, 1]"))
                .isEqualTo(true);
        // multiplicity matters
        assertThat(assertions.expression("MULTISET[1, 2, 2] = MULTISET[1, 2]"))
                .isEqualTo(false);
        assertThat(assertions.expression("MULTISET[1, 2, 2] <> MULTISET[1, 1, 2]"))
                .isEqualTo(true);
        // value equality is three-valued: a null element makes the comparison unknown, mirroring
        // ARRAY[null] = ARRAY[null]. This is the deliberate counterpoint to the bag operators (IS A
        // SET, SUBMULTISET, ...), which treat null as not distinct from null and so stay definite.
        assertThat(assertions.expression("(MULTISET[NULL] = MULTISET[NULL]) IS NULL"))
                .isEqualTo(true);
        assertThat(assertions.expression("(MULTISET[1, NULL] = MULTISET[1, NULL]) IS NULL"))
                .isEqualTo(true);
    }

    @Test
    public void testEqualityUnknownRequiresCompletePairing()
    {
        // ISO/IEC 9075-2:2023 section 8.2: the comparison is unknown only when some complete
        // pairing of the elements is all true-or-unknown; otherwise it is definite false. Both 1
        // and 2 could individually pair with the null, but only one null exists — the other
        // element must pair with 3, which is definite false.
        assertThat(assertions.expression("MULTISET[1, 2] = MULTISET[3, NULL]"))
                .isEqualTo(false);
        assertThat(assertions.expression("MULTISET[NULL, 1] = MULTISET[2, 3]"))
                .isEqualTo(false);
        // with a true pair for the 1, the null is free to absorb the 2
        assertThat(assertions.expression("(MULTISET[1, 2] = MULTISET[1, NULL]) IS NULL"))
                .isEqualTo(true);
        // an indeterminate element (a nested null) is unknown against some values and definite
        // false against others, so deciding whether a complete pairing exists is a genuine
        // matching problem
        assertThat(assertions.expression("(MULTISET[ROW(1, CAST(NULL AS integer)), ROW(2, CAST(NULL AS integer))] = MULTISET[ROW(1, 1), ROW(2, 2)]) IS NULL"))
                .isEqualTo(true);
        assertThat(assertions.expression("MULTISET[ROW(1, CAST(NULL AS integer)), ROW(1, CAST(NULL AS integer))] = MULTISET[ROW(1, 1), ROW(2, 2)]"))
                .isEqualTo(false);
        // nan() = nan() is false, so the bags compare unequal even though the elements are
        // identical (IS NOT DISTINCT FROM); equality follows the element = semantics
        assertThat(assertions.expression("MULTISET[nan()] = MULTISET[nan()]"))
                .isEqualTo(false);
    }

    @Test
    public void testNotOrderable()
    {
        // SQL:2023 makes multisets equality-comparable only; ordering operations are rejected
        assertThat(assertions.query("SELECT m FROM (VALUES MULTISET[1], MULTISET[2]) t(m) ORDER BY m"))
                .failure()
                .hasErrorCode(TYPE_MISMATCH);
        assertThat(assertions.query("SELECT MULTISET[1] < MULTISET[2]"))
                .failure()
                .hasErrorCode(TYPE_MISMATCH);
    }
}
