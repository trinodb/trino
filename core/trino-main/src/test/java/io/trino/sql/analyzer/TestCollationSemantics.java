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
package io.trino.sql.analyzer;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.Collation;
import io.trino.sql.analyzer.CollationSemantics.Collated;
import io.trino.sql.analyzer.CollationSemantics.CollationConflictException;
import io.trino.sql.analyzer.CollationSemantics.CollationShape;
import io.trino.sql.analyzer.CollationSemantics.CollationShape.ArrayShape;
import io.trino.sql.analyzer.CollationSemantics.CollationShape.Leaf;
import io.trino.sql.analyzer.CollationSemantics.CollationShape.MapShape;
import io.trino.sql.analyzer.CollationSemantics.CollationShape.RowShape;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.Collation.UCS_BASIC;
import static io.trino.sql.analyzer.CollationSemantics.Derivation.EXPLICIT;
import static io.trino.sql.analyzer.CollationSemantics.Derivation.IMPLICIT;
import static io.trino.sql.analyzer.CollationSemantics.Derivation.NONE;
import static io.trino.sql.analyzer.CollationSemantics.combine;
import static io.trino.sql.analyzer.CollationSemantics.combineShapes;
import static io.trino.sql.analyzer.CollationSemantics.determine;
import static io.trino.sql.analyzer.CollationSemantics.determineShapes;
import static io.trino.type.collation.CollationRegistry.collationRegistry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCollationSemantics
{
    private static final Collation UNICODE = collationRegistry().getCollation("unicode");
    private static final Collation UNICODE_CI = collationRegistry().getCollation("unicode_ci");

    @Test
    public void testCoercibleYields()
    {
        // a literal takes the collation of the other operand
        assertThat(determine(List.of(Collated.implicit(UNICODE), Collated.coercible()))).isEqualTo(UNICODE);
        assertThat(determine(List.of(Collated.coercible(), Collated.explicit(UNICODE_CI)))).isEqualTo(UNICODE_CI);
        assertThat(combine(List.of(Collated.coercible(), Collated.coercible()))).isEqualTo(Collated.coercible());
        assertThat(determine(List.of(Collated.coercible(), Collated.coercible()))).isEqualTo(UCS_BASIC);
    }

    @Test
    public void testImplicit()
    {
        assertThat(combine(List.of(Collated.implicit(UNICODE), Collated.implicit(UNICODE)))).isEqualTo(Collated.implicit(UNICODE));
        // differing implicit collations combine to none, which is not an error until compared
        Collated mixed = combine(List.of(Collated.implicit(UNICODE), Collated.implicit(UNICODE_CI)));
        assertThat(mixed.derivation()).isEqualTo(NONE);
        assertThat(mixed.conflicting()).containsExactly("unicode", "unicode_ci");
        assertThatThrownBy(() -> determine(List.of(Collated.implicit(UNICODE), Collated.implicit(UNICODE_CI))))
                .isInstanceOfSatisfying(CollationConflictException.class, e -> {
                    assertThat(e.derivation()).isEqualTo(IMPLICIT);
                    assertThat(e.collations()).containsExactly("unicode", "unicode_ci");
                });
        // the default collation is an implicit collation like any other
        assertThat(combine(List.of(Collated.implicit(UNICODE), Collated.implicit(UCS_BASIC))).derivation()).isEqualTo(NONE);
        // none stays none when combined further, unless an explicit collation is present
        assertThat(combine(List.of(mixed, Collated.implicit(UNICODE))).derivation()).isEqualTo(NONE);
        assertThat(combine(List.of(mixed, Collated.explicit(UNICODE_CI)))).isEqualTo(Collated.explicit(UNICODE_CI));
    }

    @Test
    public void testExplicit()
    {
        assertThat(determine(List.of(Collated.implicit(UNICODE), Collated.explicit(UNICODE_CI)))).isEqualTo(UNICODE_CI);
        assertThat(determine(List.of(Collated.implicit(UNICODE), Collated.implicit(UCS_BASIC), Collated.explicit(UNICODE_CI)))).isEqualTo(UNICODE_CI);
        assertThatThrownBy(() -> combine(List.of(Collated.explicit(UNICODE), Collated.explicit(UNICODE_CI))))
                .isInstanceOfSatisfying(CollationConflictException.class, e -> {
                    assertThat(e.derivation()).isEqualTo(EXPLICIT);
                    assertThat(e.collations()).containsExactly("unicode", "unicode_ci");
                });
    }

    @Test
    public void testOrderIndependence()
    {
        List<Collated> values = ImmutableList.of(
                Collated.coercible(),
                Collated.implicit(UNICODE),
                Collated.implicit(UNICODE_CI),
                Collated.explicit(UNICODE));
        // every non-empty subset, in every order and grouping, has one result
        for (int mask = 1; mask < 1 << values.size(); mask++) {
            ImmutableList.Builder<Collated> subset = ImmutableList.builder();
            for (int i = 0; i < values.size(); i++) {
                if ((mask & (1 << i)) != 0) {
                    subset.add(values.get(i));
                }
            }
            Collated expected = combine(subset.build());
            for (List<Collated> permutation : Collections2.permutations(subset.build())) {
                assertThat(combine(permutation)).isEqualTo(expected);
                Collated folded = permutation.getFirst();
                for (Collated operand : permutation.subList(1, permutation.size())) {
                    folded = combine(List.of(folded, operand));
                }
                assertThat(folded).isEqualTo(expected);
            }
        }
    }

    @Test
    public void testShapes()
    {
        Leaf german = new Leaf(Collated.implicit(UNICODE));
        Leaf english = new Leaf(Collated.implicit(UNICODE_CI));
        Leaf literal = new Leaf(Collated.coercible());

        // positions combine independently, and opaque operands such as nulls take no part
        assertThat(combineShapes(ImmutableList.of(new RowShape(ImmutableList.of(german, literal)), new RowShape(ImmutableList.of(literal, english)), CollationShape.Opaque.OPAQUE)))
                .isEqualTo(new RowShape(ImmutableList.of(german, english)));
        assertThat(combineShapes(ImmutableList.of(new ArrayShape(german), new ArrayShape(english))).firstNone())
                .hasValueSatisfying(none -> assertThat(none.conflicting()).containsExactly("unicode", "unicode_ci"));
        // operands of different structure leave the mismatch to type checking
        assertThat(combineShapes(ImmutableList.of(new RowShape(ImmutableList.of(german)), new RowShape(ImmutableList.of(german, german)))))
                .isEqualTo(CollationShape.Opaque.OPAQUE);

        // determination applies at each position: a literal yields, and none fails
        assertThat(determineShapes(ImmutableList.of(new MapShape(german, literal), new MapShape(literal, english))))
                .isEqualTo(new MapShape(german, english));
        assertThatThrownBy(() -> determineShapes(ImmutableList.of(new ArrayShape(new Leaf(combine(ImmutableList.of(Collated.implicit(UNICODE), Collated.implicit(UNICODE_CI))))), new ArrayShape(literal))))
                .isInstanceOf(CollationConflictException.class);

        // as a relation field, an explicit collation anywhere in a value becomes implicit
        assertThat(new RowShape(ImmutableList.of(new Leaf(Collated.explicit(UNICODE)), literal)).asField())
                .isEqualTo(new RowShape(ImmutableList.of(german, literal)));
    }
}
