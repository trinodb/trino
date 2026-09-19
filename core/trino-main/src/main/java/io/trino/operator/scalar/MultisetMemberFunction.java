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
package io.trino.operator.scalar;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMultiset;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.Convention;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.util.Failures.internalError;
import static java.lang.Boolean.TRUE;

/// Implements `value MEMBER OF multiset` per ISO/IEC 9075-2:2023 section 8.16, whose rules apply
/// in order: an empty multiset decides false before nulls are considered, then a null value (or
/// multiset) is unknown, then membership is three-valued like `IN` — true on an `=` match,
/// unknown when the value matches no element but some element is null (or its equality with the
/// value is unknown), and false otherwise.
///
/// The multiset carries a hash index keyed by `IDENTICAL`. When neither the value nor any element
/// is indeterminate, every element `=` comparison is definite, so the index decides membership
/// with one equality check against the representative of the value's `IDENTICAL` class — O(1) per
/// probe once the index is built, which pays off when one multiset value is probed repeatedly (a
/// constant-folded multiset literal; the index lives on the value, see [SqlMultiset]). Otherwise
/// it falls back to a three-valued `=` scan, where the index cannot localize the unknown cases.
@ScalarFunction(value = "$member", hidden = true, neverFails = true)
public final class MultisetMemberFunction
{
    private MultisetMemberFunction() {}

    @TypeParameter("E")
    @SqlNullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean member(
            @OperatorDependency(
                    operator = EQUAL,
                    argumentTypes = {"E", "E"},
                    convention = @Convention(arguments = {VALUE_BLOCK_POSITION_NOT_NULL, VALUE_BLOCK_POSITION_NOT_NULL}, result = NULLABLE_RETURN))
            MethodHandle elementEqual,
            @OperatorDependency(
                    operator = INDETERMINATE,
                    argumentTypes = "E",
                    convention = @Convention(arguments = VALUE_BLOCK_POSITION_NOT_NULL, result = FAIL_ON_NULL))
            MethodHandle elementIndeterminate,
            @TypeParameter("E") Type elementType,
            @SqlType("multiset(E)") SqlMultiset multiset,
            @SqlNullable @SqlType("E") Object value)
    {
        // the section 8.16 rules are ordered: the empty multiset decides false before the null
        // value rule, so NULL MEMBER OF MULTISET[] is false, not unknown (a null multiset operand
        // is unknown through the engine's null convention, since its cardinality is not 0)
        if (multiset.getSize() == 0) {
            return false;
        }
        if (value == null) {
            return null;
        }

        // materialize the probe value into a single-position block so it can be matched against
        // the elements with the block-position element operators
        BlockBuilder valueBuilder = elementType.createBlockBuilder(null, 1);
        writeNativeValue(elementType, valueBuilder, value);
        ValueBlock valueBlock = valueBuilder.buildValueBlock();

        try {
            boolean valueIndeterminate = (boolean) elementIndeterminate.invokeExact(valueBlock, 0);
            if (!valueIndeterminate && !multiset.hasIndeterminateElement()) {
                // Every element = comparison against the probe is definite, so the IDENTICAL index
                // decides membership with one equality check: an = match implies an IDENTICAL match
                // (no determinate value is = some value without being identical to it), so testing
                // the representative of the probe's IDENTICAL class suffices. The check is not
                // redundant: = and IDENTICAL diverge on NaN, which is identical to itself but not
                // equal to itself, and the elements identical to the probe all compare alike.
                int representative = multiset.representativePosition(valueBlock, 0);
                if (representative >= 0) {
                    Boolean equal = (Boolean) elementEqual.invokeExact(multiset.getUnderlyingElementBlock(), multiset.getUnderlyingElementPosition(representative), valueBlock, 0);
                    if (TRUE.equals(equal)) {
                        return true;
                    }
                }
                // the value matches no element, so the result is unknown only against a null element
                return multiset.hasNullElement() ? null : false;
            }

            // indeterminacy present: an element comparison can be unknown, which the IDENTICAL index
            // cannot localize, so fall back to a three-valued = scan (the IN semantics)
            ValueBlock elements = multiset.getUnderlyingElementBlock();
            boolean unknown = false;
            for (int i = 0; i < multiset.getSize(); i++) {
                int position = multiset.getUnderlyingElementPosition(i);
                if (elements.isNull(position)) {
                    unknown = true;
                    continue;
                }
                Boolean equal = (Boolean) elementEqual.invokeExact(elements, position, valueBlock, 0);
                if (equal == null) {
                    unknown = true;
                }
                else if (equal) {
                    return true;
                }
            }
            return unknown ? null : false;
        }
        catch (Throwable throwable) {
            throw internalError(throwable);
        }
    }
}
