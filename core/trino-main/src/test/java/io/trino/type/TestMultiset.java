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
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
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
