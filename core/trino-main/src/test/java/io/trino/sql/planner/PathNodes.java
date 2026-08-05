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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import io.trino.jsonpath.XQueryRegex;
import io.trino.jsonpath.ir.IrAbsMethod;
import io.trino.jsonpath.ir.IrArithmeticBinary;
import io.trino.jsonpath.ir.IrArithmeticUnary;
import io.trino.jsonpath.ir.IrArrayAccessor;
import io.trino.jsonpath.ir.IrArrayAccessor.Subscript;
import io.trino.jsonpath.ir.IrCeilingMethod;
import io.trino.jsonpath.ir.IrComparisonPredicate;
import io.trino.jsonpath.ir.IrConjunctionPredicate;
import io.trino.jsonpath.ir.IrContextVariable;
import io.trino.jsonpath.ir.IrDescendantMemberAccessor;
import io.trino.jsonpath.ir.IrDisjunctionPredicate;
import io.trino.jsonpath.ir.IrDoubleMethod;
import io.trino.jsonpath.ir.IrExistsPredicate;
import io.trino.jsonpath.ir.IrFilter;
import io.trino.jsonpath.ir.IrFloorMethod;
import io.trino.jsonpath.ir.IrIsUnknownPredicate;
import io.trino.jsonpath.ir.IrJsonPath;
import io.trino.jsonpath.ir.IrKeyValueMethod;
import io.trino.jsonpath.ir.IrLastIndexVariable;
import io.trino.jsonpath.ir.IrLikeRegexPredicate;
import io.trino.jsonpath.ir.IrLiteral;
import io.trino.jsonpath.ir.IrMemberAccessor;
import io.trino.jsonpath.ir.IrNamedJsonVariable;
import io.trino.jsonpath.ir.IrNamedValueVariable;
import io.trino.jsonpath.ir.IrNegationPredicate;
import io.trino.jsonpath.ir.IrPathNode;
import io.trino.jsonpath.ir.IrPredicate;
import io.trino.jsonpath.ir.IrPredicateCurrentItemVariable;
import io.trino.jsonpath.ir.IrSizeMethod;
import io.trino.jsonpath.ir.IrStartsWithPredicate;
import io.trino.jsonpath.ir.IrTypeMethod;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static io.trino.jsonpath.ir.IrArithmeticBinary.Operator.ADD;
import static io.trino.jsonpath.ir.IrArithmeticBinary.Operator.DIVIDE;
import static io.trino.jsonpath.ir.IrArithmeticBinary.Operator.MODULO;
import static io.trino.jsonpath.ir.IrArithmeticBinary.Operator.MULTIPLY;
import static io.trino.jsonpath.ir.IrArithmeticBinary.Operator.SUBTRACT;
import static io.trino.jsonpath.ir.IrArithmeticUnary.Sign.MINUS;
import static io.trino.jsonpath.ir.IrArithmeticUnary.Sign.PLUS;
import static io.trino.jsonpath.ir.IrComparisonPredicate.Operator.EQUAL;
import static io.trino.jsonpath.ir.IrComparisonPredicate.Operator.GREATER_THAN;
import static io.trino.jsonpath.ir.IrComparisonPredicate.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.jsonpath.ir.IrComparisonPredicate.Operator.LESS_THAN;
import static io.trino.jsonpath.ir.IrComparisonPredicate.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.jsonpath.ir.IrComparisonPredicate.Operator.NOT_EQUAL;
import static io.trino.jsonpath.ir.IrJsonNull.JSON_NULL;
import static io.trino.spi.type.VarcharType.createVarcharType;

public class PathNodes
{
    private PathNodes() {}

    public static IrJsonPath path(boolean lax, IrPathNode root)
    {
        return new IrJsonPath(lax, root);
    }

    // PATH NODE
    public static IrPathNode abs(IrPathNode base)
    {
        return new IrAbsMethod(base, Optional.empty());
    }

    public static IrPathNode add(IrPathNode left, IrPathNode right)
    {
        return new IrArithmeticBinary(ADD, left, right, Optional.empty());
    }

    public static IrPathNode subtract(IrPathNode left, IrPathNode right)
    {
        return new IrArithmeticBinary(SUBTRACT, left, right, Optional.empty());
    }

    public static IrPathNode multiply(IrPathNode left, IrPathNode right)
    {
        return new IrArithmeticBinary(MULTIPLY, left, right, Optional.empty());
    }

    public static IrPathNode divide(IrPathNode left, IrPathNode right)
    {
        return new IrArithmeticBinary(DIVIDE, left, right, Optional.empty());
    }

    public static IrPathNode modulo(IrPathNode left, IrPathNode right)
    {
        return new IrArithmeticBinary(MODULO, left, right, Optional.empty());
    }

    public static IrPathNode plus(IrPathNode base)
    {
        return new IrArithmeticUnary(PLUS, base, Optional.empty());
    }

    public static IrPathNode minus(IrPathNode base)
    {
        return new IrArithmeticUnary(MINUS, base, Optional.empty());
    }

    public static IrPathNode wildcardArrayAccessor(IrPathNode base)
    {
        return new IrArrayAccessor(base, ImmutableList.of(), Optional.empty());
    }

    public static IrPathNode arrayAccessor(IrPathNode base, Subscript... subscripts)
    {
        return new IrArrayAccessor(base, ImmutableList.copyOf(subscripts), Optional.empty());
    }

    public static Subscript at(IrPathNode path)
    {
        return new Subscript(path, Optional.empty());
    }

    public static Subscript range(IrPathNode fromInclusive, IrPathNode toInclusive)
    {
        return new Subscript(fromInclusive, Optional.of(toInclusive));
    }

    public static IrPathNode ceiling(IrPathNode base)
    {
        return new IrCeilingMethod(base, Optional.empty());
    }

    public static IrPathNode contextVariable()
    {
        return new IrContextVariable(Optional.empty());
    }

    public static IrPathNode toDouble(IrPathNode base)
    {
        return new IrDoubleMethod(base, Optional.empty());
    }

    public static IrPathNode filter(IrPathNode base, IrPredicate predicate)
    {
        return new IrFilter(base, predicate, Optional.empty());
    }

    public static IrPathNode floor(IrPathNode base)
    {
        return new IrFloorMethod(base, Optional.empty());
    }

    public static IrPathNode jsonNull()
    {
        return JSON_NULL;
    }

    public static IrPathNode keyValue(IrPathNode base)
    {
        return new IrKeyValueMethod(base);
    }

    public static IrPathNode last()
    {
        return new IrLastIndexVariable(Optional.empty());
    }

    public static IrPathNode literal(Type type, Object value)
    {
        return new IrLiteral(Optional.of(type), value);
    }

    public static IrPathNode wildcardMemberAccessor(IrPathNode base)
    {
        return new IrMemberAccessor(base, Optional.empty(), Optional.empty());
    }

    public static IrPathNode memberAccessor(IrPathNode base, String key)
    {
        return new IrMemberAccessor(base, Optional.of(key), Optional.empty());
    }

    public static IrPathNode descendantMemberAccessor(IrPathNode base, String key)
    {
        return new IrDescendantMemberAccessor(base, key, Optional.empty());
    }

    public static IrPathNode jsonVariable(int index)
    {
        return new IrNamedJsonVariable(index, Optional.empty());
    }

    public static IrPathNode variable(int index)
    {
        return new IrNamedValueVariable(index, Optional.empty());
    }

    public static IrPathNode currentItem()
    {
        return new IrPredicateCurrentItemVariable(Optional.empty());
    }

    public static IrPathNode size(IrPathNode base)
    {
        return new IrSizeMethod(base, Optional.empty());
    }

    public static IrPathNode type(IrPathNode base)
    {
        return new IrTypeMethod(base, Optional.of(createVarcharType(27)));
    }

    // PATH PREDICATE
    public static IrPredicate equal(IrPathNode left, IrPathNode right)
    {
        return new IrComparisonPredicate(EQUAL, left, right);
    }

    public static IrPredicate notEqual(IrPathNode left, IrPathNode right)
    {
        return new IrComparisonPredicate(NOT_EQUAL, left, right);
    }

    public static IrPredicate lessThan(IrPathNode left, IrPathNode right)
    {
        return new IrComparisonPredicate(LESS_THAN, left, right);
    }

    public static IrPredicate greaterThan(IrPathNode left, IrPathNode right)
    {
        return new IrComparisonPredicate(GREATER_THAN, left, right);
    }

    public static IrPredicate lessThanOrEqual(IrPathNode left, IrPathNode right)
    {
        return new IrComparisonPredicate(LESS_THAN_OR_EQUAL, left, right);
    }

    public static IrPredicate greaterThanOrEqual(IrPathNode left, IrPathNode right)
    {
        return new IrComparisonPredicate(GREATER_THAN_OR_EQUAL, left, right);
    }

    public static IrPredicate conjunction(IrPredicate left, IrPredicate right)
    {
        return new IrConjunctionPredicate(left, right);
    }

    public static IrPredicate disjunction(IrPredicate left, IrPredicate right)
    {
        return new IrDisjunctionPredicate(left, right);
    }

    public static IrPredicate exists(IrPathNode path)
    {
        return new IrExistsPredicate(path);
    }

    public static IrPredicate isUnknown(IrPredicate predicate)
    {
        return new IrIsUnknownPredicate(predicate);
    }

    public static IrPredicate likeRegex(IrPathNode path, String pattern)
    {
        return new IrLikeRegexPredicate(path, XQueryRegex.patternWithFlags(pattern, XQueryRegex.parseFlags("")));
    }

    public static IrPredicate likeRegex(IrPathNode path, String pattern, String flag)
    {
        return new IrLikeRegexPredicate(path, XQueryRegex.patternWithFlags(pattern, XQueryRegex.parseFlags(flag)));
    }

    public static IrPredicate negation(IrPredicate predicate)
    {
        return new IrNegationPredicate(predicate);
    }

    public static IrPredicate startsWith(IrPathNode whole, IrPathNode initial)
    {
        return new IrStartsWithPredicate(whole, initial);
    }

    // SQL/JSON ITEM SEQUENCE
    public static List<Object> sequence(Object... items)
    {
        return ImmutableList.copyOf(items);
    }

    public static List<Object> singletonSequence(Object item)
    {
        return ImmutableList.of(item);
    }

    public static List<Object> emptySequence()
    {
        return ImmutableList.of();
    }
}
