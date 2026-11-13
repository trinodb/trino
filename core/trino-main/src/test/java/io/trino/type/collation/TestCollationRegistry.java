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
package io.trino.type.collation;

import io.trino.spi.TrinoException;
import io.trino.spi.type.Collation;
import io.trino.spi.type.CollationComparison;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.COLLATION_NOT_FOUND;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.Collation.UCS_BASIC;
import static io.trino.spi.type.TypeParameter.numericParameter;
import static io.trino.spi.type.TypeParameter.textParameter;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static io.trino.type.collation.CollationRegistry.collationRegistry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCollationRegistry
{
    private final CollationRegistry registry = collationRegistry();

    @Test
    public void testNames()
    {
        // the catalog is a fixed set of well-known collations
        assertThat(registry.collationNames()).containsExactly("ucs_basic", "unicode", "unicode_ci");
        assertThat(registry.getCollation("ucs_basic")).isSameAs(UCS_BASIC);
        // the registry resolves canonical names; SQL canonicalizes the identifier first
        assertThat(registry.findCollation("UCS_BASIC")).isEmpty();
        assertThat(registry.findCollation("Unicode_CI")).isEmpty();
    }

    @Test
    public void testCollationNames()
    {
        // every listed name resolves, to a collation of that name
        for (String name : registry.collationNames()) {
            assertThat(registry.findCollation(name)).as(name).map(Collation::name).contains(name);
        }
    }

    @Test
    public void testSpacesAndPunctuationAreSignificant()
    {
        // no collation ignores spaces or punctuation, so trailing spaces are significant
        for (String name : registry.collationNames()) {
            if (name.equals("ucs_basic")) {
                continue;
            }
            CollationComparison comparison = comparison(name);
            assertThat(comparison.equal(utf8Slice("a"), utf8Slice("a "))).as("%s: trailing space", name).isFalse();
            assertThat(comparison.equal(utf8Slice("ab"), utf8Slice("a b"))).as("%s: interior space", name).isFalse();
            assertThat(comparison.equal(utf8Slice("ab"), utf8Slice("a-b"))).as("%s: punctuation", name).isFalse();
            assertThat(comparison.compare(utf8Slice("a"), utf8Slice("a "))).as("%s: order", name).isNegative();
        }
    }

    @Test
    public void testUnknownNames()
    {
        // a name outside the catalog is an error, including locale names that ICU knows
        for (String name : new String[] {"de", "en_ci", "unicode_ai", "xx", "_ci", "ci", "unicode-ci", "", "1en", "unicode ci"}) {
            assertThat(registry.findCollation(name)).as(name).isEmpty();
            assertThatThrownBy(() -> registry.getCollation(name))
                    .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(COLLATION_NOT_FOUND.toErrorCode()));
        }
    }

    @Test
    public void testStrength()
    {
        // unicode: case and accents distinguish values
        assertEqual("unicode", "resume", "Resume", false);
        assertEqual("unicode", "resume", "résumé", false);
        // unicode_ci: accents distinguish values, case does not
        assertEqual("unicode_ci", "resume", "RESUME", true);
        assertEqual("unicode_ci", "resume", "résumé", false);
        assertEqual("unicode_ci", "Ærøskøbing", "ærøskøbing", true);
        // the root ordering places accented letters with their base letters, unlike code point order
        assertThat(comparison("unicode").compare(utf8Slice("ä"), utf8Slice("z"))).isNegative();
        assertThat(UCS_BASIC.equals(registry.getCollation("ucs_basic"))).isTrue();
        // the key preserves the collation's order
        CollationComparison unicode = comparison("unicode");
        assertThat(unicode.collationKey(utf8Slice("ä")).compareTo(unicode.collationKey(utf8Slice("z")))).isNegative();
    }

    @Test
    public void testTypeResolution()
    {
        Type type = TESTING_TYPE_MANAGER.getType(new TypeDescriptor("varchar", numericParameter(10), textParameter("unicode_ci")));
        assertThat(type).isEqualTo(createVarcharType(10, registry.getCollation("unicode_ci")));
        assertThat(TESTING_TYPE_MANAGER.getType(TypeDescriptor.fromString("varchar(10,'unicode_ci')"))).isEqualTo(type);

        // an explicit default collation is the existing type
        assertThat(TESTING_TYPE_MANAGER.getType(new TypeDescriptor("varchar", numericParameter(10), textParameter("ucs_basic")))).isSameAs(createVarcharType(10));
        assertThat(TESTING_TYPE_MANAGER.getType(new TypeDescriptor("varchar", numericParameter(VarcharType.UNBOUNDED_LENGTH), textParameter("unicode"))))
                .isEqualTo(VarcharType.VARCHAR.withCollation(registry.getCollation("unicode")));

        assertThat(TESTING_TYPE_MANAGER.getType(new TypeDescriptor("varchar", numericParameter(10), textParameter("unicode_ci pad space"))))
                .isEqualTo(createVarcharType(10, registry.getCollation("unicode_ci").withPadSpace()));

        assertThatThrownBy(() -> TESTING_TYPE_MANAGER.getType(new TypeDescriptor("varchar", numericParameter(10), textParameter("xx"))))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(COLLATION_NOT_FOUND.toErrorCode()));
    }

    @Test
    public void testTypeIdRoundTrip()
    {
        for (Type type : new Type[] {
                createVarcharType(10, registry.getCollation("unicode_ci")),
                VarcharType.VARCHAR.withCollation(registry.getCollation("unicode")),
                createVarcharType(0, registry.getCollation("unicode")),
                createVarcharType(10, registry.getCollation("unicode_ci").withPadSpace()),
                VarcharType.VARCHAR.withCollation(UCS_BASIC.withPadSpace()),
        }) {
            TypeId typeId = type.getTypeId();
            assertThat(TESTING_TYPE_MANAGER.getType(typeId)).as(typeId.getId()).isEqualTo(type);
            assertThat(TESTING_TYPE_MANAGER.getType(TypeDescriptor.fromString(type.getTypeDescriptor().toString()))).isEqualTo(type);
        }
    }

    @Test
    public void testOperatorsUseEngineComparisons()
            throws Throwable
    {
        // a collated type's operators compare with the comparison the engine's type operators resolve
        VarcharType type = VarcharType.VARCHAR.withCollation(registry.getCollation("unicode_ci"));
        MethodHandle equal = new TypeOperators(collationRegistry()).getEqualOperator(type, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));
        assertThat((Boolean) equal.invokeExact(utf8Slice("Alice"), utf8Slice("ALICE"))).isTrue();
        // a collation is a value: any instance with the name selects the engine's comparison
        assertThat(VarcharType.VARCHAR.withCollation(Collation.of("unicode_ci"))).isEqualTo(type);
        assertThatThrownBy(() -> Collation.of("Not Valid"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testEveryCollationRoundTrips()
    {
        // every catalog name, with each pad attribute, survives the type's SQL spelling
        for (String name : registry.collationNames()) {
            Collation collation = registry.getCollation(name);
            for (Type type : new Type[] {createVarcharType(10, collation), createVarcharType(10, collation.withPadSpace())}) {
                assertThat(TESTING_TYPE_MANAGER.getType(type.getTypeId())).as(type.getTypeId().getId()).isEqualTo(type);
                assertThat(TESTING_TYPE_MANAGER.fromSqlType(type.getDisplayName())).as(type.getDisplayName()).isEqualTo(type);
            }
        }
        // the name is quoted, as row field names are, so the spelling parses back for any name;
        // the unquoted name parses to the same type
        Type type = createVarcharType(10, registry.getCollation("unicode_ci"));
        assertThat(type.getDisplayName()).isEqualTo("varchar(10) COLLATE \"unicode_ci\"");
        assertThat(TESTING_TYPE_MANAGER.fromSqlType("varchar(10) COLLATE unicode_ci")).isEqualTo(type);
        assertThat(TESTING_TYPE_MANAGER.fromSqlType("varchar(10) COLLATE UNICODE_CI")).isEqualTo(type);
        assertThat(createVarcharType(10, registry.getCollation("unicode_ci").withPadSpace()).getDisplayName())
                .isEqualTo("varchar(10) COLLATE \"unicode_ci\" PAD SPACE");
    }

    private CollationComparison comparison(String name)
    {
        return registry.comparison(registry.getCollation(name));
    }

    private void assertEqual(String collation, String left, String right, boolean equal)
    {
        CollationComparison value = comparison(collation);
        assertThat(value.equal(utf8Slice(left), utf8Slice(right))).as("%s: %s = %s", collation, left, right).isEqualTo(equal);
        assertThat(value.compare(utf8Slice(left), utf8Slice(right)) == 0).isEqualTo(equal);
        assertThat(value.collationKey(utf8Slice(left)).equals(value.collationKey(utf8Slice(right)))).isEqualTo(equal);
    }
}
