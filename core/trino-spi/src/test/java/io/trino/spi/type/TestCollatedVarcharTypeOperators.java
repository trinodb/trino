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

import io.airlift.slice.Slice;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestCollatedVarcharTypeOperators
{
    // compares ASCII strings ignoring case
    private static final CollationComparison CASE_INSENSITIVE = new CollationComparison()
    {
        @Override
        public int compare(Slice left, Slice right)
        {
            return collationKey(left).compareTo(collationKey(right));
        }

        @Override
        public boolean equal(Slice left, Slice right)
        {
            return collationKey(left).equals(collationKey(right));
        }

        @Override
        public Slice collationKey(Slice value)
        {
            return utf8Slice(value.toStringUtf8().toLowerCase(ENGLISH));
        }
    };

    @Test
    void testOperatorsResolveTheCollation()
            throws Throwable
    {
        // a collated type names its collation; its operators compare as the type operators
        // resolve the collation
        VarcharType type = createVarcharType(10, Collation.of("ascii_ci"));
        TypeOperators typeOperators = new TypeOperators(_ -> CASE_INSENSITIVE);
        MethodHandle equal = typeOperators.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));
        MethodHandle hash = typeOperators.getXxHash64Operator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL));
        MethodHandle comparison = typeOperators.getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
        assertThat((Boolean) equal.invokeExact(utf8Slice("Alice"), utf8Slice("ALICE"))).isTrue();
        assertThat((long) hash.invokeExact(utf8Slice("Alice"))).isEqualTo((long) hash.invokeExact(utf8Slice("ALICE")));
        assertThat((long) comparison.invokeExact(utf8Slice("alice"), utf8Slice("BOB"))).isNegative();

        assertThat(type.getCollation()).isEqualTo(Collation.of("ascii_ci"));
        assertThat(type.getDisplayName()).isEqualTo("varchar(10) COLLATE \"ascii_ci\"");
    }

    @Test
    void testWithoutCollationComparisons()
            throws Throwable
    {
        // type operators created without collation comparisons can read collated values, but
        // comparing them fails rather than comparing bytes
        VarcharType type = createVarcharType(10, Collation.of("ascii_ci"));
        TypeOperators typeOperators = new TypeOperators();
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        type.writeSlice(blockBuilder, utf8Slice("Alice"));
        MethodHandle read = typeOperators.getReadValueOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL));
        assertThat((Slice) read.invokeExact(blockBuilder.build(), 0)).isEqualTo(utf8Slice("Alice"));

        MethodHandle equal = typeOperators.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));
        assertThatThrownBy(() -> {
            Boolean _ = (Boolean) equal.invokeExact(utf8Slice("Alice"), utf8Slice("ALICE"));
        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("created without collation comparisons");
    }

    @Test
    void testValueSetsRejectCollatedStrings()
    {
        // value sets compare with type operators that have no collation comparisons, so a value set
        // of collated strings fails rather than ordering the strings by their bytes
        VarcharType type = createVarcharType(10, Collation.of("ascii_ci"));
        assertThatThrownBy(() -> ValueSet.of(type, utf8Slice("b"), utf8Slice("a")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("created without collation comparisons");
    }

    @Test
    void testDefaultCollation()
    {
        // the default collation is the existing type
        assertThat(createVarcharType(10, Collation.UCS_BASIC)).isEqualTo(createVarcharType(10));
        assertThat(createVarcharType(VarcharType.UNBOUNDED_LENGTH, Collation.UCS_BASIC)).isSameAs(VarcharType.VARCHAR);
        assertThat(createVarcharType(10, Collation.of("ascii_ci")).withCollation(Collation.UCS_BASIC)).isEqualTo(createVarcharType(10));
    }
}
