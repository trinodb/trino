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
package io.trino.sql.relational;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.trino.Session;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.DecimalParseResult;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.sql.relational.SpecialForm.Form;
import io.trino.sql.relational.optimizer.ExpressionOptimizer;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BindExpression;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.ComparisonExpression.Operator;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.TimeLiteral;
import io.trino.sql.tree.TimestampLiteral;
import io.trino.sql.tree.WhenClause;
import io.trino.type.UnknownType;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.constantNull;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.sql.relational.SpecialForm.Form.AND;
import static io.trino.sql.relational.SpecialForm.Form.BETWEEN;
import static io.trino.sql.relational.SpecialForm.Form.BIND;
import static io.trino.sql.relational.SpecialForm.Form.COALESCE;
import static io.trino.sql.relational.SpecialForm.Form.DEREFERENCE;
import static io.trino.sql.relational.SpecialForm.Form.IF;
import static io.trino.sql.relational.SpecialForm.Form.IN;
import static io.trino.sql.relational.SpecialForm.Form.IS_NULL;
import static io.trino.sql.relational.SpecialForm.Form.NULL_IF;
import static io.trino.sql.relational.SpecialForm.Form.OR;
import static io.trino.sql.relational.SpecialForm.Form.ROW_CONSTRUCTOR;
import static io.trino.sql.relational.SpecialForm.Form.SWITCH;
import static io.trino.sql.relational.SpecialForm.Form.WHEN;
import static io.trino.type.DateTimes.parseTime;
import static io.trino.type.DateTimes.parseTimeWithTimeZone;
import static io.trino.type.DateTimes.parseTimestamp;
import static io.trino.type.DateTimes.parseTimestampWithTimeZone;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.DateTimeUtils.parseDayTimeInterval;
import static io.trino.util.DateTimeUtils.parseYearMonthInterval;
import static java.util.Objects.requireNonNull;

public final class SqlToRowExpressionTranslator
{
    private SqlToRowExpressionTranslator() {}

    public static RowExpression translate(
            Expression expression,
            Map<NodeRef<Expression>, Type> types,
            Map<Symbol, Integer> layout,
            Metadata metadata,
            FunctionManager functionManager,
            Session session,
            boolean optimize)
    {
        Visitor visitor = new Visitor(
                metadata,
                types,
                layout,
                session);
        RowExpression result = visitor.process(expression, null);

        requireNonNull(result, "result is null");

        if (optimize) {
            ExpressionOptimizer optimizer = new ExpressionOptimizer(metadata, functionManager, session);
            return optimizer.optimize(result);
        }

        return result;
    }

    private static class Visitor
            extends AstVisitor<RowExpression, Void>
    {
        private final Metadata metadata;
        private final Map<NodeRef<Expression>, Type> types;
        private final Map<Symbol, Integer> layout;
        private final Session session;
        private final StandardFunctionResolution standardFunctionResolution;

        private Visitor(
                Metadata metadata,
                Map<NodeRef<Expression>, Type> types,
                Map<Symbol, Integer> layout,
                Session session)
        {
            this.metadata = metadata;
            this.types = ImmutableMap.copyOf(requireNonNull(types, "types is null"));
            this.layout = layout;
            this.session = session;
            standardFunctionResolution = new StandardFunctionResolution(session, metadata);
        }

        private Type getType(Expression node)
        {
            return types.get(NodeRef.of(node));
        }

        @Override
        protected RowExpression visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: expression translator for " + node.getClass().getName());
        }

        @Override
        protected RowExpression visitFieldReference(FieldReference node, Void context)
        {
            return field(node.getFieldIndex(), getType(node));
        }

        @Override
        protected RowExpression visitNullLiteral(NullLiteral node, Void context)
        {
            return constantNull(UnknownType.UNKNOWN);
        }

        @Override
        protected RowExpression visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return constant(node.getValue(), BOOLEAN);
        }

        @Override
        protected RowExpression visitLongLiteral(LongLiteral node, Void context)
        {
            if (node.getValue() >= Integer.MIN_VALUE && node.getValue() <= Integer.MAX_VALUE) {
                return constant(node.getValue(), INTEGER);
            }
            return constant(node.getValue(), BIGINT);
        }

        @Override
        protected RowExpression visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return constant(node.getValue(), DOUBLE);
        }

        @Override
        protected RowExpression visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            DecimalParseResult parseResult = Decimals.parse(node.getValue());
            return constant(parseResult.getObject(), parseResult.getType());
        }

        @Override
        protected RowExpression visitStringLiteral(StringLiteral node, Void context)
        {
            return constant(utf8Slice(node.getValue()), createVarcharType(node.length()));
        }

        @Override
        protected RowExpression visitCharLiteral(CharLiteral node, Void context)
        {
            return constant(utf8Slice(node.getValue()), createCharType(node.length()));
        }

        @Override
        protected RowExpression visitBinaryLiteral(BinaryLiteral node, Void context)
        {
            return constant(wrappedBuffer(node.getValue()), VARBINARY);
        }

        @Override
        protected RowExpression visitGenericLiteral(GenericLiteral node, Void context)
        {
            Type type = getType(node);

            if (JSON.equals(type)) {
                return call(
                        metadata.resolveFunction(session, QualifiedName.of("json_parse"), fromTypes(VARCHAR)),
                        constant(utf8Slice(node.getValue()), VARCHAR));
            }

            return call(
                    metadata.getCoercion(session, VARCHAR, type),
                    constant(utf8Slice(node.getValue()), VARCHAR));
        }

        @Override
        protected RowExpression visitTimeLiteral(TimeLiteral node, Void context)
        {
            Type type = getType(node);
            Object value;
            if (type instanceof TimeWithTimeZoneType) {
                value = parseTimeWithTimeZone(((TimeWithTimeZoneType) type).getPrecision(), node.getValue());
            }
            else {
                value = parseTime(node.getValue());
            }
            return constant(value, type);
        }

        @Override
        protected RowExpression visitTimestampLiteral(TimestampLiteral node, Void context)
        {
            Type type = getType(node);

            Object value;
            if (type instanceof TimestampType) {
                int precision = ((TimestampType) type).getPrecision();
                value = parseTimestamp(precision, node.getValue());
            }
            else if (type instanceof TimestampWithTimeZoneType) {
                int precision = ((TimestampWithTimeZoneType) type).getPrecision();
                value = parseTimestampWithTimeZone(precision, node.getValue());
            }
            else {
                throw new IllegalStateException("Unexpected type: " + type);
            }

            return constant(value, type);
        }

        @Override
        protected RowExpression visitIntervalLiteral(IntervalLiteral node, Void context)
        {
            long value;
            if (node.isYearToMonth()) {
                value = node.getSign().multiplier() * parseYearMonthInterval(node.getValue(), node.getStartField(), node.getEndField());
            }
            else {
                value = node.getSign().multiplier() * parseDayTimeInterval(node.getValue(), node.getStartField(), node.getEndField());
            }
            return constant(value, getType(node));
        }

        @Override
        protected RowExpression visitComparisonExpression(ComparisonExpression node, Void context)
        {
            RowExpression left = process(node.getLeft(), context);
            RowExpression right = process(node.getRight(), context);
            Operator operator = node.getOperator();

            switch (node.getOperator()) {
                case NOT_EQUAL:
                    return new CallExpression(
                            metadata.resolveFunction(session, QualifiedName.of("not"), fromTypes(BOOLEAN)),
                            ImmutableList.of(visitComparisonExpression(Operator.EQUAL, left, right)));
                case GREATER_THAN:
                    return visitComparisonExpression(Operator.LESS_THAN, right, left);
                case GREATER_THAN_OR_EQUAL:
                    return visitComparisonExpression(Operator.LESS_THAN_OR_EQUAL, right, left);
                default:
                    return visitComparisonExpression(operator, left, right);
            }
        }

        private RowExpression visitComparisonExpression(Operator operator, RowExpression left, RowExpression right)
        {
            return call(
                    standardFunctionResolution.comparisonFunction(operator, left.getType(), right.getType()),
                    left,
                    right);
        }

        @Override
        protected RowExpression visitFunctionCall(FunctionCall node, Void context)
        {
            List<RowExpression> arguments = node.getArguments().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());

            return new CallExpression(metadata.decodeFunction(node.getName()), arguments);
        }

        @Override
        protected RowExpression visitSymbolReference(SymbolReference node, Void context)
        {
            Integer field = layout.get(Symbol.from(node));
            if (field != null) {
                return field(field, getType(node));
            }

            return new VariableReferenceExpression(node.getName(), getType(node));
        }

        @Override
        protected RowExpression visitLambdaExpression(LambdaExpression node, Void context)
        {
            RowExpression body = process(node.getBody(), context);

            Type type = getType(node);
            List<Type> typeParameters = type.getTypeParameters();
            List<Type> argumentTypes = typeParameters.subList(0, typeParameters.size() - 1);
            List<String> argumentNames = node.getArguments().stream()
                    .map(LambdaArgumentDeclaration::getName)
                    .map(Identifier::getValue)
                    .collect(toImmutableList());

            return new LambdaDefinitionExpression(argumentTypes, argumentNames, body);
        }

        @Override
        protected RowExpression visitBindExpression(BindExpression node, Void context)
        {
            ImmutableList.Builder<Type> valueTypesBuilder = ImmutableList.builder();
            ImmutableList.Builder<RowExpression> argumentsBuilder = ImmutableList.builder();
            for (Expression value : node.getValues()) {
                RowExpression valueRowExpression = process(value, context);
                valueTypesBuilder.add(valueRowExpression.getType());
                argumentsBuilder.add(valueRowExpression);
            }
            RowExpression function = process(node.getFunction(), context);
            argumentsBuilder.add(function);

            return new SpecialForm(BIND, getType(node), argumentsBuilder.build());
        }

        @Override
        protected RowExpression visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            RowExpression left = process(node.getLeft(), context);
            RowExpression right = process(node.getRight(), context);

            return call(
                    standardFunctionResolution.arithmeticFunction(node.getOperator(), left.getType(), right.getType()),
                    left,
                    right);
        }

        @Override
        protected RowExpression visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            RowExpression expression = process(node.getValue(), context);

            switch (node.getSign()) {
                case PLUS:
                    return expression;
                case MINUS:
                    return call(
                            metadata.resolveOperator(session, NEGATION, ImmutableList.of(expression.getType())),
                            expression);
            }

            throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
        }

        @Override
        protected RowExpression visitLogicalExpression(LogicalExpression node, Void context)
        {
            Form form;
            switch (node.getOperator()) {
                case AND:
                    form = AND;
                    break;
                case OR:
                    form = OR;
                    break;
                default:
                    throw new IllegalStateException("Unknown logical operator: " + node.getOperator());
            }
            return new SpecialForm(
                    form,
                    BOOLEAN,
                    node.getTerms().stream()
                            .map(term -> process(term, context))
                            .collect(toImmutableList()));
        }

        @Override
        protected RowExpression visitCast(Cast node, Void context)
        {
            RowExpression value = process(node.getExpression(), context);

            Type returnType = getType(node);
            if (node.isTypeOnly()) {
                return changeType(value, returnType);
            }

            if (node.isSafe()) {
                return call(
                        metadata.getCoercion(session, QualifiedName.of("TRY_CAST"), value.getType(), returnType),
                        value);
            }

            return call(
                    metadata.getCoercion(session, value.getType(), returnType),
                    value);
        }

        private static RowExpression changeType(RowExpression value, Type targetType)
        {
            ChangeTypeVisitor visitor = new ChangeTypeVisitor(targetType);
            return value.accept(visitor, null);
        }

        private static class ChangeTypeVisitor
                implements RowExpressionVisitor<RowExpression, Void>
        {
            private final Type targetType;

            private ChangeTypeVisitor(Type targetType)
            {
                this.targetType = targetType;
            }

            @Override
            public RowExpression visitCall(CallExpression call, Void context)
            {
                return new CallExpression(call.getResolvedFunction(), call.getArguments());
            }

            @Override
            public RowExpression visitSpecialForm(SpecialForm specialForm, Void context)
            {
                return new SpecialForm(specialForm.getForm(), targetType, specialForm.getArguments());
            }

            @Override
            public RowExpression visitInputReference(InputReferenceExpression reference, Void context)
            {
                return field(reference.getField(), targetType);
            }

            @Override
            public RowExpression visitConstant(ConstantExpression literal, Void context)
            {
                return constant(literal.getValue(), targetType);
            }

            @Override
            public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
            {
                return new VariableReferenceExpression(reference.getName(), targetType);
            }
        }

        @Override
        protected RowExpression visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            List<RowExpression> arguments = node.getOperands().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());

            return new SpecialForm(COALESCE, getType(node), arguments);
        }

        @Override
        protected RowExpression visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();

            RowExpression value = process(node.getOperand(), context);
            arguments.add(value);

            ImmutableList.Builder<ResolvedFunction> functionDependencies = ImmutableList.builder();
            for (WhenClause clause : node.getWhenClauses()) {
                RowExpression operand = process(clause.getOperand(), context);
                RowExpression result = process(clause.getResult(), context);

                functionDependencies.add(metadata.resolveOperator(session, EQUAL, ImmutableList.of(value.getType(), operand.getType())));

                arguments.add(new SpecialForm(
                        WHEN,
                        getType(clause),
                        operand,
                        result));
            }

            Type returnType = getType(node);

            arguments.add(node.getDefaultValue()
                    .map(defaultValue -> process(defaultValue, context))
                    .orElse(constantNull(returnType)));

            return new SpecialForm(SWITCH, returnType, arguments.build(), functionDependencies.build());
        }

        @Override
        protected RowExpression visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            /*
                Translates an expression like:

                    case when cond1 then value1
                         when cond2 then value2
                         when cond3 then value3
                         else value4
                    end

                To:

                    IF(cond1,
                        value1,
                        IF(cond2,
                            value2,
                                If(cond3,
                                    value3,
                                    value4)))

             */
            RowExpression expression = node.getDefaultValue()
                    .map(value -> process(value, context))
                    .orElse(constantNull(getType(node)));

            for (WhenClause clause : Lists.reverse(node.getWhenClauses())) {
                expression = new SpecialForm(
                        IF,
                        getType(node),
                        process(clause.getOperand(), context),
                        process(clause.getResult(), context),
                        expression);
            }

            return expression;
        }

        @Override
        protected RowExpression visitIfExpression(IfExpression node, Void context)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();

            arguments.add(process(node.getCondition(), context))
                    .add(process(node.getTrueValue(), context));

            if (node.getFalseValue().isPresent()) {
                arguments.add(process(node.getFalseValue().get(), context));
            }
            else {
                arguments.add(constantNull(getType(node)));
            }

            return new SpecialForm(IF, getType(node), arguments.build());
        }

        @Override
        protected RowExpression visitInPredicate(InPredicate node, Void context)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
            RowExpression value = process(node.getValue(), context);
            arguments.add(value);
            InListExpression values = (InListExpression) node.getValueList();
            for (Expression testValue : values.getValues()) {
                arguments.add(process(testValue, context));
            }

            List<ResolvedFunction> functionDependencies = ImmutableList.<ResolvedFunction>builder()
                    .add(metadata.resolveOperator(session, EQUAL, ImmutableList.of(value.getType(), value.getType())))
                    .add(metadata.resolveOperator(session, HASH_CODE, ImmutableList.of(value.getType())))
                    .add(metadata.resolveOperator(session, INDETERMINATE, ImmutableList.of(value.getType())))
                    .build();

            return new SpecialForm(IN, BOOLEAN, arguments.build(), functionDependencies);
        }

        @Override
        protected RowExpression visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            RowExpression expression = process(node.getValue(), context);

            return notExpression(new SpecialForm(IS_NULL, BOOLEAN, ImmutableList.of(expression)));
        }

        @Override
        protected RowExpression visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            RowExpression expression = process(node.getValue(), context);

            return new SpecialForm(IS_NULL, BOOLEAN, expression);
        }

        @Override
        protected RowExpression visitNotExpression(NotExpression node, Void context)
        {
            return notExpression(process(node.getValue(), context));
        }

        private RowExpression notExpression(RowExpression value)
        {
            return new CallExpression(
                    metadata.resolveFunction(session, QualifiedName.of("not"), fromTypes(BOOLEAN)),
                    ImmutableList.of(value));
        }

        @Override
        protected RowExpression visitNullIfExpression(NullIfExpression node, Void context)
        {
            RowExpression first = process(node.getFirst(), context);
            RowExpression second = process(node.getSecond(), context);

            ResolvedFunction resolvedFunction = metadata.resolveOperator(session, EQUAL, ImmutableList.of(first.getType(), second.getType()));
            List<ResolvedFunction> functionDependencies = ImmutableList.<ResolvedFunction>builder()
                    .add(resolvedFunction)
                    .add(metadata.getCoercion(session, first.getType(), resolvedFunction.getSignature().getArgumentTypes().get(0)))
                    .add(metadata.getCoercion(session, second.getType(), resolvedFunction.getSignature().getArgumentTypes().get(0)))
                    .build();

            return new SpecialForm(
                    NULL_IF,
                    getType(node),
                    ImmutableList.of(first, second),
                    functionDependencies);
        }

        @Override
        protected RowExpression visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            RowExpression value = process(node.getValue(), context);
            RowExpression min = process(node.getMin(), context);
            RowExpression max = process(node.getMax(), context);

            List<ResolvedFunction> functionDependencies = ImmutableList.<ResolvedFunction>builder()
                    .add(metadata.resolveOperator(session, LESS_THAN_OR_EQUAL, ImmutableList.of(value.getType(), max.getType())))
                    .build();

            return new SpecialForm(
                    BETWEEN,
                    BOOLEAN,
                    ImmutableList.of(value, min, max),
                    functionDependencies);
        }

        @Override
        protected RowExpression visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            RowExpression base = process(node.getBase(), context);
            RowExpression index = process(node.getIndex(), context);

            if (getType(node.getBase()) instanceof RowType) {
                long value = (Long) ((ConstantExpression) index).getValue();
                return new SpecialForm(DEREFERENCE, getType(node), base, constant((int) value - 1, INTEGER));
            }

            return call(
                    metadata.resolveOperator(session, SUBSCRIPT, ImmutableList.of(base.getType(), index.getType())),
                    base,
                    index);
        }

        @Override
        protected RowExpression visitRow(Row node, Void context)
        {
            List<RowExpression> arguments = node.getItems().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());
            Type returnType = getType(node);
            return new SpecialForm(ROW_CONSTRUCTOR, returnType, arguments);
        }
    }
}
