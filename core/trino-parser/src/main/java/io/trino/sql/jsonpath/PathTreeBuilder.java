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
package io.trino.sql.jsonpath;

import com.google.common.collect.ImmutableList;
import io.trino.jsonpath.JsonPathBaseVisitor;
import io.trino.jsonpath.JsonPathParser;
import io.trino.sql.jsonpath.tree.AbsMethod;
import io.trino.sql.jsonpath.tree.ArithmeticBinary;
import io.trino.sql.jsonpath.tree.ArithmeticBinary.Operator;
import io.trino.sql.jsonpath.tree.ArithmeticUnary;
import io.trino.sql.jsonpath.tree.ArithmeticUnary.Sign;
import io.trino.sql.jsonpath.tree.ArrayAccessor;
import io.trino.sql.jsonpath.tree.CeilingMethod;
import io.trino.sql.jsonpath.tree.ComparisonPredicate;
import io.trino.sql.jsonpath.tree.ConjunctionPredicate;
import io.trino.sql.jsonpath.tree.ContextVariable;
import io.trino.sql.jsonpath.tree.DatetimeMethod;
import io.trino.sql.jsonpath.tree.DescendantMemberAccessor;
import io.trino.sql.jsonpath.tree.DisjunctionPredicate;
import io.trino.sql.jsonpath.tree.DoubleMethod;
import io.trino.sql.jsonpath.tree.ExistsPredicate;
import io.trino.sql.jsonpath.tree.Filter;
import io.trino.sql.jsonpath.tree.FloorMethod;
import io.trino.sql.jsonpath.tree.IsUnknownPredicate;
import io.trino.sql.jsonpath.tree.JsonPath;
import io.trino.sql.jsonpath.tree.KeyValueMethod;
import io.trino.sql.jsonpath.tree.LastIndexVariable;
import io.trino.sql.jsonpath.tree.LikeRegexPredicate;
import io.trino.sql.jsonpath.tree.MemberAccessor;
import io.trino.sql.jsonpath.tree.NamedVariable;
import io.trino.sql.jsonpath.tree.NegationPredicate;
import io.trino.sql.jsonpath.tree.PathNode;
import io.trino.sql.jsonpath.tree.Predicate;
import io.trino.sql.jsonpath.tree.PredicateCurrentItemVariable;
import io.trino.sql.jsonpath.tree.SizeMethod;
import io.trino.sql.jsonpath.tree.SqlValueLiteral;
import io.trino.sql.jsonpath.tree.StartsWithPredicate;
import io.trino.sql.jsonpath.tree.TypeMethod;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.StringLiteral;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.Optional;

import static io.trino.sql.jsonpath.tree.JsonNullLiteral.JSON_NULL;

public class PathTreeBuilder
        extends JsonPathBaseVisitor<PathNode>
{
    @Override
    public PathNode visitPath(JsonPathParser.PathContext context)
    {
        boolean lax = context.pathMode().LAX() != null;
        PathNode path = visit(context.pathExpression());
        return new JsonPath(lax, path);
    }

    @Override
    public PathNode visitDecimalLiteral(JsonPathParser.DecimalLiteralContext context)
    {
        return new SqlValueLiteral(new DecimalLiteral(context.getText()));
    }

    @Override
    public PathNode visitDoubleLiteral(JsonPathParser.DoubleLiteralContext context)
    {
        return new SqlValueLiteral(new DoubleLiteral(context.getText()));
    }

    @Override
    public PathNode visitIntegerLiteral(JsonPathParser.IntegerLiteralContext context)
    {
        return new SqlValueLiteral(new LongLiteral(context.getText()));
    }

    @Override
    public PathNode visitStringLiteral(JsonPathParser.StringLiteralContext context)
    {
        return new SqlValueLiteral(new StringLiteral(unquote(context.STRING().getText())));
    }

    private static String unquote(String quoted)
    {
        return quoted.substring(1, quoted.length() - 1)
                .replace("\"\"", "\"");
    }

    @Override
    public PathNode visitNullLiteral(JsonPathParser.NullLiteralContext context)
    {
        return JSON_NULL;
    }

    @Override
    public PathNode visitBooleanLiteral(JsonPathParser.BooleanLiteralContext context)
    {
        return new SqlValueLiteral(new BooleanLiteral(context.getText()));
    }

    @Override
    public PathNode visitContextVariable(JsonPathParser.ContextVariableContext context)
    {
        return new ContextVariable();
    }

    @Override
    public PathNode visitNamedVariable(JsonPathParser.NamedVariableContext context)
    {
        return namedVariable(context.NAMED_VARIABLE());
    }

    private static NamedVariable namedVariable(TerminalNode namedVariable)
    {
        // drop leading `$`
        return new NamedVariable(namedVariable.getText().substring(1));
    }

    @Override
    public PathNode visitLastIndexVariable(JsonPathParser.LastIndexVariableContext context)
    {
        return new LastIndexVariable();
    }

    @Override
    public PathNode visitPredicateCurrentItemVariable(JsonPathParser.PredicateCurrentItemVariableContext context)
    {
        return new PredicateCurrentItemVariable();
    }

    @Override
    public PathNode visitParenthesizedPath(JsonPathParser.ParenthesizedPathContext context)
    {
        return visit(context.pathExpression());
    }

    @Override
    public PathNode visitMemberAccessor(JsonPathParser.MemberAccessorContext context)
    {
        PathNode base = visit(context.accessorExpression());
        Optional<String> key = Optional.empty();
        if (context.stringLiteral() != null) {
            key = Optional.of(unquote(context.stringLiteral().getText()));
        }
        else if (context.identifier() != null) {
            key = Optional.of(context.identifier().getText());
        }
        return new MemberAccessor(base, key);
    }

    @Override
    public PathNode visitWildcardMemberAccessor(JsonPathParser.WildcardMemberAccessorContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new MemberAccessor(base, Optional.empty());
    }

    @Override
    public PathNode visitDescendantMemberAccessor(JsonPathParser.DescendantMemberAccessorContext context)
    {
        PathNode base = visit(context.accessorExpression());
        String key;
        if (context.stringLiteral() != null) {
            key = unquote(context.stringLiteral().getText());
        }
        else {
            key = context.identifier().getText();
        }
        return new DescendantMemberAccessor(base, key);
    }

    @Override
    public PathNode visitArrayAccessor(JsonPathParser.ArrayAccessorContext context)
    {
        PathNode base = visit(context.accessorExpression());
        ImmutableList.Builder<ArrayAccessor.Subscript> subscripts = ImmutableList.builder();
        for (JsonPathParser.SubscriptContext subscript : context.subscript()) {
            if (subscript.singleton != null) {
                subscripts.add(new ArrayAccessor.Subscript(visit(subscript.singleton)));
            }
            else {
                subscripts.add(new ArrayAccessor.Subscript(visit(subscript.from), visit(subscript.to)));
            }
        }
        return new ArrayAccessor(base, subscripts.build());
    }

    @Override
    public PathNode visitWildcardArrayAccessor(JsonPathParser.WildcardArrayAccessorContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new ArrayAccessor(base, ImmutableList.of());
    }

    @Override
    public PathNode visitFilter(JsonPathParser.FilterContext context)
    {
        PathNode base = visit(context.accessorExpression());
        Predicate predicate = (Predicate) visit(context.predicate());
        return new Filter(base, predicate);
    }

    @Override
    public PathNode visitTypeMethod(JsonPathParser.TypeMethodContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new TypeMethod(base);
    }

    @Override
    public PathNode visitSizeMethod(JsonPathParser.SizeMethodContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new SizeMethod(base);
    }

    @Override
    public PathNode visitDoubleMethod(JsonPathParser.DoubleMethodContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new DoubleMethod(base);
    }

    @Override
    public PathNode visitCeilingMethod(JsonPathParser.CeilingMethodContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new CeilingMethod(base);
    }

    @Override
    public PathNode visitFloorMethod(JsonPathParser.FloorMethodContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new FloorMethod(base);
    }

    @Override
    public PathNode visitAbsMethod(JsonPathParser.AbsMethodContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new AbsMethod(base);
    }

    @Override
    public PathNode visitDatetimeMethod(JsonPathParser.DatetimeMethodContext context)
    {
        PathNode base = visit(context.accessorExpression());
        Optional<String> format = Optional.empty();
        if (context.stringLiteral() != null) {
            format = Optional.of(unquote(context.stringLiteral().getText()));
        }
        return new DatetimeMethod(base, format);
    }

    @Override
    public PathNode visitKeyValueMethod(JsonPathParser.KeyValueMethodContext context)
    {
        PathNode base = visit(context.accessorExpression());
        return new KeyValueMethod(base);
    }

    @Override
    public PathNode visitSignedUnary(JsonPathParser.SignedUnaryContext context)
    {
        PathNode base = visit(context.pathExpression());
        return new ArithmeticUnary(getSign(context.sign.getText()), base);
    }

    private static Sign getSign(String operator)
    {
        switch (operator) {
            case "+":
                return Sign.PLUS;
            case "-":
                return Sign.MINUS;
            default:
                throw new UnsupportedOperationException("unexpected unary operator: " + operator);
        }
    }

    @Override
    public PathNode visitBinary(JsonPathParser.BinaryContext context)
    {
        PathNode left = visit(context.left);
        PathNode right = visit(context.right);
        return new ArithmeticBinary(getOperator(context.operator.getText()), left, right);
    }

    private static Operator getOperator(String operator)
    {
        switch (operator) {
            case "+":
                return Operator.ADD;
            case "-":
                return Operator.SUBTRACT;
            case "*":
                return Operator.MULTIPLY;
            case "/":
                return Operator.DIVIDE;
            case "%":
                return Operator.MODULUS;
            default:
                throw new UnsupportedOperationException("unexpected binary operator: " + operator);
        }
    }

    // predicate

    @Override
    public PathNode visitComparisonPredicate(JsonPathParser.ComparisonPredicateContext context)
    {
        PathNode left = visit(context.left);
        PathNode right = visit(context.right);
        return new ComparisonPredicate(getComparisonOperator(context.comparisonOperator().getText()), left, right);
    }

    private static ComparisonPredicate.Operator getComparisonOperator(String operator)
    {
        switch (operator) {
            case "==":
                return ComparisonPredicate.Operator.EQUAL;
            case "<>":
            case "!=":
                return ComparisonPredicate.Operator.NOT_EQUAL;
            case "<":
                return ComparisonPredicate.Operator.LESS_THAN;
            case ">":
                return ComparisonPredicate.Operator.GREATER_THAN;
            case "<=":
                return ComparisonPredicate.Operator.LESS_THAN_OR_EQUAL;
            case ">=":
                return ComparisonPredicate.Operator.GREATER_THAN_OR_EQUAL;
            default:
                throw new UnsupportedOperationException("unexpected comparison operator: " + operator);
        }
    }

    @Override
    public PathNode visitConjunctionPredicate(JsonPathParser.ConjunctionPredicateContext context)
    {
        Predicate left = (Predicate) visit(context.left);
        Predicate right = (Predicate) visit(context.right);
        return new ConjunctionPredicate(left, right);
    }

    @Override
    public PathNode visitDisjunctionPredicate(JsonPathParser.DisjunctionPredicateContext context)
    {
        Predicate left = (Predicate) visit(context.left);
        Predicate right = (Predicate) visit(context.right);
        return new DisjunctionPredicate(left, right);
    }

    @Override
    public PathNode visitExistsPredicate(JsonPathParser.ExistsPredicateContext context)
    {
        PathNode path = visit(context.pathExpression());
        return new ExistsPredicate(path);
    }

    @Override
    public PathNode visitIsUnknownPredicate(JsonPathParser.IsUnknownPredicateContext context)
    {
        Predicate predicate = (Predicate) visit(context.predicate());
        return new IsUnknownPredicate(predicate);
    }

    @Override
    public PathNode visitLikeRegexPredicate(JsonPathParser.LikeRegexPredicateContext context)
    {
        PathNode path = visit(context.base);
        String pattern = unquote(context.pattern.getText());
        Optional<String> flag = Optional.empty();
        if (context.flag != null) {
            flag = Optional.of(unquote(context.flag.getText()));
        }
        return new LikeRegexPredicate(path, pattern, flag);
    }

    @Override
    public PathNode visitNegationPredicate(JsonPathParser.NegationPredicateContext context)
    {
        Predicate predicate = (Predicate) visit(context.delimitedPredicate());
        return new NegationPredicate(predicate);
    }

    @Override
    public PathNode visitParenthesizedPredicate(JsonPathParser.ParenthesizedPredicateContext context)
    {
        return visit(context.predicate());
    }

    @Override
    public PathNode visitStartsWithPredicate(JsonPathParser.StartsWithPredicateContext context)
    {
        PathNode whole = visit(context.whole);
        PathNode initial;
        if (context.string != null) {
            initial = visit(context.string);
        }
        else {
            initial = namedVariable(context.NAMED_VARIABLE());
        }
        return new StartsWithPredicate(whole, initial);
    }

    @Override
    protected PathNode aggregateResult(PathNode aggregate, PathNode nextResult)
    {
        // do not skip over unrecognized nodes
        if (nextResult == null) {
            throw new UnsupportedOperationException("not yet implemented");
        }

        if (aggregate == null) {
            return nextResult;
        }

        throw new UnsupportedOperationException("not yet implemented");
    }
}
