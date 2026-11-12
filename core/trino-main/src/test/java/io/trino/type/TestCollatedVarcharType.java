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

import io.airlift.slice.Slice;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.collation.CollationRegistry.collationRegistry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCollatedVarcharType
        extends AbstractTestType
{
    private static final VarcharType TYPE = createVarcharType(10, collationRegistry().getCollation("unicode"));

    public TestCollatedVarcharType()
    {
        super(TYPE, String.class, createTestBlock());
    }

    // ordered under the unicode collation, which differs from code point order: "Äpfel" sorts
    // between "apfel" and "Birne", while its first byte is greater than every ASCII letter
    private static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = TYPE.createBlockBuilder(null, 6);
        TYPE.writeString(blockBuilder, "apfel");
        TYPE.writeString(blockBuilder, "apfel");
        TYPE.writeString(blockBuilder, "Äpfel");
        TYPE.writeString(blockBuilder, "Birne");
        TYPE.writeString(blockBuilder, "Birne");
        TYPE.writeString(blockBuilder, "zitrone");
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return utf8Slice(((Slice) value).toStringUtf8() + "z");
    }

    @Test
    public void testNoDiscreteValues()
    {
        // a collation is not code point order, so there are no extremes or adjacent values
        assertThat(type.getRange()).isEmpty();
        assertThat(type.getPreviousValue(utf8Slice("b"))).isEmpty();
        assertThat(type.getNextValue(utf8Slice("b"))).isEmpty();
    }

    @Test
    public void testValueSetsRejectCollatedStrings()
    {
        // a value set compares with type operators that have no collation comparisons, so a set of
        // collated strings fails rather than ordering them by their bytes; the planner builds none
        assertThatThrownBy(() -> ValueSet.copyOf(TYPE, List.of(utf8Slice("zitrone"), utf8Slice("Äpfel"), utf8Slice("Birne"))))
                .hasStackTraceContaining("created without collation comparisons");
    }

    @Test
    public void testCollationSemantics()
            throws Throwable
    {
        TypeOperators typeOperators = new TypeOperators(collationRegistry());
        VarcharType caseInsensitive = createVarcharType(20, collationRegistry().getCollation("unicode_ci"));
        MethodHandle equal = typeOperators.getEqualOperator(caseInsensitive, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));
        MethodHandle hash = typeOperators.getHashCodeOperator(caseInsensitive, simpleConvention(FAIL_ON_NULL, NEVER_NULL));
        MethodHandle xxHash64 = typeOperators.getXxHash64Operator(caseInsensitive, simpleConvention(FAIL_ON_NULL, NEVER_NULL));
        MethodHandle identical = typeOperators.getIdenticalOperator(caseInsensitive, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
        MethodHandle comparison = typeOperators.getComparisonUnorderedLastOperator(caseInsensitive, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));

        // distinct strings that the collation considers equal are equal, identical, and hash equally
        Slice upper = utf8Slice("MÜLLER");
        Slice lower = utf8Slice("müller");
        assertThat((Boolean) equal.invokeExact(upper, lower)).isTrue();
        assertThat((boolean) identical.invokeExact(upper, lower)).isTrue();
        assertThat((long) hash.invokeExact(upper)).isEqualTo((long) hash.invokeExact(lower));
        assertThat((long) xxHash64.invokeExact(upper)).isEqualTo((long) xxHash64.invokeExact(lower));
        assertThat((long) comparison.invokeExact(upper, lower)).isZero();

        // base letters and accents still distinguish values
        assertThat((Boolean) equal.invokeExact(utf8Slice("muller"), utf8Slice("mullet"))).isFalse();
        assertThat((long) comparison.invokeExact(utf8Slice("muller"), utf8Slice("mullet"))).isNegative();
        assertThat((Boolean) equal.invokeExact(utf8Slice("muller"), utf8Slice("müller"))).isFalse();

        // the default collation compares bytes
        VarcharType binary = createVarcharType(20);
        MethodHandle binaryEqual = typeOperators.getEqualOperator(binary, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));
        assertThat((Boolean) binaryEqual.invokeExact(upper, lower)).isFalse();
    }

    @Test
    public void testTypeIdentity()
    {
        // the collation is part of the type: equal only with the same length and collation
        assertThat(TYPE).isEqualTo(createVarcharType(10, collationRegistry().getCollation("unicode")));
        assertThat(TYPE).isNotEqualTo(createVarcharType(10));
        assertThat(TYPE).isNotEqualTo(createVarcharType(10, collationRegistry().getCollation("unicode_ci")));
        assertThat(TYPE).isNotEqualTo(createVarcharType(11, collationRegistry().getCollation("unicode")));
        assertThat(TYPE.withCollation(collationRegistry().getCollation("ucs_basic"))).isSameAs(createVarcharType(10));

        assertThat(TYPE.getDisplayName()).isEqualTo("varchar(10) COLLATE \"unicode\"");
        assertThat(TYPE.getTypeId().getId()).isEqualTo("varchar(10) COLLATE \"unicode\"");
        assertThat(TYPE.getTypeDescriptor().toString()).isEqualTo("varchar(10,'unicode')");
        VarcharType unbounded = VarcharType.VARCHAR.withCollation(collationRegistry().getCollation("unicode_ci"));
        assertThat(unbounded.isUnbounded()).isTrue();
        assertThat(unbounded.getDisplayName()).isEqualTo("varchar COLLATE \"unicode_ci\"");
        assertThat(unbounded.getTypeDescriptor().toString()).isEqualTo("varchar(2147483647,'unicode_ci')");
    }
}
