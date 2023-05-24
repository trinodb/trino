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

import io.trino.Session;
import io.trino.json.ir.IrAbsMethod;
import io.trino.json.ir.IrArithmeticBinary;
import io.trino.json.ir.IrArithmeticBinary.Operator;
import io.trino.json.ir.IrArithmeticUnary;
import io.trino.json.ir.IrArithmeticUnary.Sign;
import io.trino.json.ir.IrArrayAccessor;
import io.trino.json.ir.IrArrayAccessor.Subscript;
import io.trino.json.ir.IrCeilingMethod;
import io.trino.json.ir.IrComparisonPredicate;
import io.trino.json.ir.IrConjunctionPredicate;
import io.trino.json.ir.IrContextVariable;
import io.trino.json.ir.IrDescendantMemberAccessor;
import io.trino.json.ir.IrDisjunctionPredicate;
import io.trino.json.ir.IrDoubleMethod;
import io.trino.json.ir.IrExistsPredicate;
import io.trino.json.ir.IrFilter;
import io.trino.json.ir.IrFloorMethod;
import io.trino.json.ir.IrIsUnknownPredicate;
import io.trino.json.ir.IrJsonPath;
import io.trino.json.ir.IrKeyValueMethod;
import io.trino.json.ir.IrLastIndexVariable;
import io.trino.json.ir.IrLiteral;
import io.trino.json.ir.IrMemberAccessor;
import io.trino.json.ir.IrNamedJsonVariable;
import io.trino.json.ir.IrNamedValueVariable;
import io.trino.json.ir.IrNegationPredicate;
import io.trino.json.ir.IrPathNode;
import io.trino.json.ir.IrPredicate;
import io.trino.json.ir.IrPredicateCurrentItemVariable;
import io.trino.json.ir.IrSizeMethod;
import io.trino.json.ir.IrStartsWithPredicate;
import io.trino.json.ir.IrTypeMethod;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.JsonPathAnalyzer.JsonPathAnalysis;
import io.trino.sql.jsonpath.PathNodeRef;
import io.trino.sql.jsonpath.tree.AbsMethod;
import io.trino.sql.jsonpath.tree.ArithmeticBinary;
import io.trino.sql.jsonpath.tree.ArithmeticUnary;
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
import io.trino.sql.jsonpath.tree.JsonNullLiteral;
import io.trino.sql.jsonpath.tree.JsonPathTreeVisitor;
import io.trino.sql.jsonpath.tree.KeyValueMethod;
import io.trino.sql.jsonpath.tree.LastIndexVariable;
import io.trino.sql.jsonpath.tree.LikeRegexPredicate;
import io.trino.sql.jsonpath.tree.MemberAccessor;
import io.trino.sql.jsonpath.tree.NamedVariable;
import io.trino.sql.jsonpath.tree.NegationPredicate;
import io.trino.sql.jsonpath.tree.PathNode;
import io.trino.sql.jsonpath.tree.PredicateCurrentItemVariable;
import io.trino.sql.jsonpath.tree.SizeMethod;
import io.trino.sql.jsonpath.tree.SqlValueLiteral;
import io.trino.sql.jsonpath.tree.StartsWithPredicate;
import io.trino.sql.jsonpath.tree.TypeMethod;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.json.ir.IrArithmeticBinary.Operator.ADD;
import static io.trino.json.ir.IrArithmeticBinary.Operator.DIVIDE;
import static io.trino.json.ir.IrArithmeticBinary.Operator.MODULUS;
import static io.trino.json.ir.IrArithmeticBinary.Operator.MULTIPLY;
import static io.trino.json.ir.IrArithmeticBinary.Operator.SUBTRACT;
import static io.trino.json.ir.IrArithmeticUnary.Sign.MINUS;
import static io.trino.json.ir.IrArithmeticUnary.Sign.PLUS;
import static io.trino.json.ir.IrComparisonPredicate.Operator.EQUAL;
import static io.trino.json.ir.IrComparisonPredicate.Operator.GREATER_THAN;
import static io.trino.json.ir.IrComparisonPredicate.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.json.ir.IrComparisonPredicate.Operator.LESS_THAN;
import static io.trino.json.ir.IrComparisonPredicate.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.json.ir.IrComparisonPredicate.Operator.NOT_EQUAL;
import static io.trino.json.ir.IrJsonNull.JSON_NULL;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

class JsonPathTranslator
{
    private final Session session;
    private final PlannerContext plannerContext;

    public JsonPathTranslator(Session session, PlannerContext plannerContext)
    {
        this.session = requireNonNull(session, "session is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    public IrJsonPath rewriteToIr(JsonPathAnalysis pathAnalysis, List<String> parametersOrder)
    {
        PathNode root = pathAnalysis.getPath().getRoot();
        IrPathNode rewritten = new Rewriter(session, plannerContext, pathAnalysis.getTypes(), pathAnalysis.getJsonParameters(), parametersOrder).process(root);

        return new IrJsonPath(pathAnalysis.getPath().isLax(), rewritten);
    }

    private static class Rewriter
            extends JsonPathTreeVisitor<IrPathNode, Void>
    {
        private final LiteralInterpreter literalInterpreter;
        private final Map<PathNodeRef<PathNode>, Type> types;
        private final Set<PathNodeRef<PathNode>> jsonParameters;
        private final List<String> parametersOrder;

        public Rewriter(Session session, PlannerContext plannerContext, Map<PathNodeRef<PathNode>, Type> types, Set<PathNodeRef<PathNode>> jsonParameters, List<String> parametersOrder)
        {
            requireNonNull(session, "session is null");
            requireNonNull(plannerContext, "plannerContext is null");
            requireNonNull(types, "types is null");
            requireNonNull(jsonParameters, "jsonParameters is null");
            requireNonNull(jsonParameters, "jsonParameters is null");
            requireNonNull(parametersOrder, "parametersOrder is null");

            this.literalInterpreter = new LiteralInterpreter(plannerContext, session);
            this.types = types;
            this.jsonParameters = jsonParameters;
            this.parametersOrder = parametersOrder;
        }

        @Override
        protected IrPathNode visitPathNode(PathNode node, Void context)
        {
            throw new UnsupportedOperationException("rewrite not implemented for " + node.getClass().getSimpleName());
        }

        @Override
        protected IrPathNode visitAbsMethod(AbsMethod node, Void context)
        {
            IrPathNode base = process(node.getBase());
            return new IrAbsMethod(base, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitArithmeticBinary(ArithmeticBinary node, Void context)
        {
            IrPathNode left = process(node.getLeft());
            IrPathNode right = process(node.getRight());
            return new IrArithmeticBinary(binaryOperator(node.getOperator()), left, right, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        private Operator binaryOperator(ArithmeticBinary.Operator operator)
        {
            return switch (operator) {
                case ADD -> ADD;
                case SUBTRACT -> SUBTRACT;
                case MULTIPLY -> MULTIPLY;
                case DIVIDE -> DIVIDE;
                case MODULUS -> MODULUS;
            };
        }

        @Override
        protected IrPathNode visitArithmeticUnary(ArithmeticUnary node, Void context)
        {
            IrPathNode base = process(node.getBase());
            Sign sign = switch (node.getSign()) {
                case PLUS -> PLUS;
                case MINUS -> MINUS;
            };
            return new IrArithmeticUnary(sign, base, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitArrayAccessor(ArrayAccessor node, Void context)
        {
            IrPathNode base = process(node.getBase());
            List<Subscript> subscripts = node.getSubscripts().stream()
                    .map(subscript -> {
                        IrPathNode from = process(subscript.getFrom());
                        Optional<IrPathNode> to = subscript.getTo().map(this::process);
                        return new Subscript(from, to);
                    })
                    .collect(toImmutableList());
            return new IrArrayAccessor(base, subscripts, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitCeilingMethod(CeilingMethod node, Void context)
        {
            IrPathNode base = process(node.getBase());
            return new IrCeilingMethod(base, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitContextVariable(ContextVariable node, Void context)
        {
            return new IrContextVariable(Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitDatetimeMethod(DatetimeMethod node, Void context)
        {
            // TODO
            throw new IllegalStateException("datetime method is not yet supported. The query should have failed in JsonPathAnalyzer.");

//            IrPathNode base = process(node.getBase());
//            return new IrDatetimeMethod(base, /*parsed format*/, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitDescendantMemberAccessor(DescendantMemberAccessor node, Void context)
        {
            IrPathNode base = process(node.getBase());
            return new IrDescendantMemberAccessor(base, node.getKey(), Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitDoubleMethod(DoubleMethod node, Void context)
        {
            IrPathNode base = process(node.getBase());
            return new IrDoubleMethod(base, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitFilter(Filter node, Void context)
        {
            IrPathNode base = process(node.getBase());
            IrPredicate predicate = (IrPredicate) process(node.getPredicate());
            return new IrFilter(base, predicate, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitFloorMethod(FloorMethod node, Void context)
        {
            IrPathNode base = process(node.getBase());
            return new IrFloorMethod(base, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitJsonNullLiteral(JsonNullLiteral node, Void context)
        {
            return JSON_NULL;
        }

        @Override
        protected IrPathNode visitKeyValueMethod(KeyValueMethod node, Void context)
        {
            IrPathNode base = process(node.getBase());
            return new IrKeyValueMethod(base);
        }

        @Override
        protected IrPathNode visitLastIndexVariable(LastIndexVariable node, Void context)
        {
            return new IrLastIndexVariable(Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitMemberAccessor(MemberAccessor node, Void context)
        {
            IrPathNode base = process(node.getBase());
            return new IrMemberAccessor(base, node.getKey(), Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitNamedVariable(NamedVariable node, Void context)
        {
            if (jsonParameters.contains(PathNodeRef.of(node))) {
                return new IrNamedJsonVariable(parametersOrder.indexOf(node.getName()), Optional.ofNullable(types.get(PathNodeRef.of(node))));
            }
            return new IrNamedValueVariable(parametersOrder.indexOf(node.getName()), Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitPredicateCurrentItemVariable(PredicateCurrentItemVariable node, Void context)
        {
            return new IrPredicateCurrentItemVariable(Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitSizeMethod(SizeMethod node, Void context)
        {
            IrPathNode base = process(node.getBase());
            return new IrSizeMethod(base, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitSqlValueLiteral(SqlValueLiteral node, Void context)
        {
            Expression value = node.getValue();
            return new IrLiteral(Optional.of(types.get(PathNodeRef.of(node))), literalInterpreter.evaluate(value, types.get(PathNodeRef.of(node))));
        }

        @Override
        protected IrPathNode visitTypeMethod(TypeMethod node, Void context)
        {
            IrPathNode base = process(node.getBase());
            return new IrTypeMethod(base, Optional.ofNullable(types.get(PathNodeRef.of(node))));
        }

        // predicate

        @Override
        protected IrPathNode visitComparisonPredicate(ComparisonPredicate node, Void context)
        {
            checkArgument(BOOLEAN.equals(types.get(PathNodeRef.of(node))), "Wrong predicate type. Expected BOOLEAN");

            IrPathNode left = process(node.getLeft());
            IrPathNode right = process(node.getRight());
            IrComparisonPredicate.Operator operator = comparisonOperator(node.getOperator());
            return new IrComparisonPredicate(operator, left, right);
        }

        private IrComparisonPredicate.Operator comparisonOperator(ComparisonPredicate.Operator operator)
        {
            return switch (operator) {
                case EQUAL -> EQUAL;
                case NOT_EQUAL -> NOT_EQUAL;
                case LESS_THAN -> LESS_THAN;
                case GREATER_THAN -> GREATER_THAN;
                case LESS_THAN_OR_EQUAL -> LESS_THAN_OR_EQUAL;
                case GREATER_THAN_OR_EQUAL -> GREATER_THAN_OR_EQUAL;
            };
        }

        @Override
        protected IrPathNode visitConjunctionPredicate(ConjunctionPredicate node, Void context)
        {
            checkArgument(BOOLEAN.equals(types.get(PathNodeRef.of(node))), "Wrong predicate type. Expected BOOLEAN");

            IrPredicate left = (IrPredicate) process(node.getLeft());
            IrPredicate right = (IrPredicate) process(node.getRight());
            return new IrConjunctionPredicate(left, right);
        }

        @Override
        protected IrPathNode visitDisjunctionPredicate(DisjunctionPredicate node, Void context)
        {
            checkArgument(BOOLEAN.equals(types.get(PathNodeRef.of(node))), "Wrong predicate type. Expected BOOLEAN");

            IrPredicate left = (IrPredicate) process(node.getLeft());
            IrPredicate right = (IrPredicate) process(node.getRight());
            return new IrDisjunctionPredicate(left, right);
        }

        @Override
        protected IrPathNode visitExistsPredicate(ExistsPredicate node, Void context)
        {
            checkArgument(BOOLEAN.equals(types.get(PathNodeRef.of(node))), "Wrong predicate type. Expected BOOLEAN");

            IrPathNode path = process(node.getPath());
            return new IrExistsPredicate(path);
        }

        @Override
        protected IrPathNode visitIsUnknownPredicate(IsUnknownPredicate node, Void context)
        {
            checkArgument(BOOLEAN.equals(types.get(PathNodeRef.of(node))), "Wrong predicate type. Expected BOOLEAN");

            IrPredicate predicate = (IrPredicate) process(node.getPredicate());
            return new IrIsUnknownPredicate(predicate);
        }

        @Override
        protected IrPathNode visitLikeRegexPredicate(LikeRegexPredicate node, Void context)
        {
            checkArgument(BOOLEAN.equals(types.get(PathNodeRef.of(node))), "Wrong predicate type. Expected BOOLEAN");

            // TODO
            throw new IllegalStateException("like_regex predicate is not yet supported. The query should have failed in JsonPathAnalyzer.");
        }

        @Override
        protected IrPathNode visitNegationPredicate(NegationPredicate node, Void context)
        {
            checkArgument(BOOLEAN.equals(types.get(PathNodeRef.of(node))), "Wrong predicate type. Expected BOOLEAN");

            IrPredicate predicate = (IrPredicate) process(node.getPredicate());
            return new IrNegationPredicate(predicate);
        }

        @Override
        protected IrPathNode visitStartsWithPredicate(StartsWithPredicate node, Void context)
        {
            checkArgument(BOOLEAN.equals(types.get(PathNodeRef.of(node))), "Wrong predicate type. Expected BOOLEAN");

            IrPathNode whole = process(node.getWhole());
            IrPathNode initial = process(node.getInitial());
            return new IrStartsWithPredicate(whole, initial);
        }
    }
}
