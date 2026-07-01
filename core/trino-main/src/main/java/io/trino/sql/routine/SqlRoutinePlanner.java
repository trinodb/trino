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
package io.trino.sql.routine;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Field;
import io.trino.sql.analyzer.RelationId;
import io.trino.sql.analyzer.RelationType;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.IrExpressionOptimizer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TranslationMap;
import io.trino.sql.planner.iterative.rule.LambdaCaptureDesugaringRewriter;
import io.trino.sql.routine.ir.IrBlock;
import io.trino.sql.routine.ir.IrBreak;
import io.trino.sql.routine.ir.IrContinue;
import io.trino.sql.routine.ir.IrIf;
import io.trino.sql.routine.ir.IrLabel;
import io.trino.sql.routine.ir.IrLoop;
import io.trino.sql.routine.ir.IrRepeat;
import io.trino.sql.routine.ir.IrReturn;
import io.trino.sql.routine.ir.IrRoutine;
import io.trino.sql.routine.ir.IrSet;
import io.trino.sql.routine.ir.IrStatement;
import io.trino.sql.routine.ir.IrVariable;
import io.trino.sql.routine.ir.IrWhile;
import io.trino.sql.tree.AssignmentStatement;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.CaseStatement;
import io.trino.sql.tree.CaseStatementWhenClause;
import io.trino.sql.tree.CompoundStatement;
import io.trino.sql.tree.ControlStatement;
import io.trino.sql.tree.ElseIfClause;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ForStatement;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfStatement;
import io.trino.sql.tree.IterateStatement;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LeaveStatement;
import io.trino.sql.tree.LoopStatement;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.RepeatStatement;
import io.trino.sql.tree.ReturnStatement;
import io.trino.sql.tree.VariableDeclaration;
import io.trino.sql.tree.WhileStatement;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.constantNull;
import static io.trino.sql.planner.LogicalPlanner.buildLambdaDeclarationToSymbolMap;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class SqlRoutinePlanner
{
    private final PlannerContext plannerContext;
    private final IrExpressionOptimizer optimizer;

    public SqlRoutinePlanner(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.optimizer = plannerContext.getExpressionOptimizer();
    }

    public IrRoutine planSqlFunction(Session session, SqlRoutineAnalysis analysis)
    {
        List<IrVariable> allVariables = new ArrayList<>();
        Map<String, IrVariable> scopeVariables = new LinkedHashMap<>();

        ImmutableList.Builder<IrVariable> parameters = ImmutableList.builder();
        analysis.arguments().forEach((name, type) -> {
            IrVariable variable = new IrVariable(allVariables.size(), type, constantNull(type));
            allVariables.add(variable);
            scopeVariables.put(name, variable);
            parameters.add(variable);
        });

        StatementVisitor visitor = new StatementVisitor(session, allVariables, analysis.analysis());
        IrStatement body = visitor.process(analysis.statement(), new Context(scopeVariables, Map.of(), Map.of()));

        return new IrRoutine(analysis.returnType(), parameters.build(), body);
    }

    private class StatementVisitor
            extends AstVisitor<IrStatement, Context>
    {
        private final Session session;
        private final List<IrVariable> allVariables;
        private final Analysis analysis;

        private final AtomicInteger labelCounter = new AtomicInteger();

        public StatementVisitor(
                Session session,
                List<IrVariable> allVariables,
                Analysis analysis)
        {
            this.session = requireNonNull(session, "session is null");
            this.allVariables = requireNonNull(allVariables, "allVariables is null");
            this.analysis = requireNonNull(analysis, "analysis is null");
        }

        @Override
        protected IrStatement visitNode(Node node, Context context)
        {
            throw new UnsupportedOperationException("Not implemented: " + node);
        }

        @Override
        protected IrStatement visitCompoundStatement(CompoundStatement node, Context context)
        {
            Context newContext = context.newScope();

            ImmutableList.Builder<IrVariable> blockVariables = ImmutableList.builder();
            for (VariableDeclaration declaration : node.getVariableDeclarations()) {
                Type type = analysis.getType(declaration.getType());
                io.trino.sql.ir.Expression defaultValue = declaration.getDefaultValue()
                        .map(expression -> toExpression(newContext, expression))
                        .orElse(constantNull(type));

                for (Identifier name : declaration.getNames()) {
                    IrVariable variable = new IrVariable(allVariables.size(), type, defaultValue);
                    allVariables.add(variable);
                    verify(newContext.variables().put(identifierValue(name), variable) == null, "Variable already declared in scope: %s", name);
                    blockVariables.add(variable);
                }
            }

            List<IrStatement> statements = node.getStatements().stream()
                    .map(statement -> process(statement, newContext))
                    .collect(toImmutableList());

            return new IrBlock(blockVariables.build(), statements);
        }

        @Override
        protected IrStatement visitIfStatement(IfStatement node, Context context)
        {
            Optional<IrStatement> ifFalse = node.getElseClause().map(clause -> block(statements(clause.getStatements(), context)));

            for (ElseIfClause elseIf : node.getElseIfClauses().reversed()) {
                ifFalse = Optional.of(new IrIf(
                        toExpression(context, elseIf.getExpression()),
                        block(statements(elseIf.getStatements(), context)),
                        ifFalse));
            }

            return new IrIf(
                    toExpression(context, node.getExpression()),
                    block(statements(node.getStatements(), context)),
                    ifFalse);
        }

        @Override
        protected IrStatement visitCaseStatement(CaseStatement node, Context context)
        {
            if (node.getExpression().isPresent()) {
                io.trino.sql.ir.Expression valueExpression = toExpression(context, node.getExpression().get());
                IrVariable valueVariable = new IrVariable(allVariables.size(), valueExpression.type(), valueExpression);

                IrStatement statement = node.getElseClause()
                        .map(elseClause -> block(statements(elseClause.getStatements(), context)))
                        .orElseGet(() -> new IrBlock(ImmutableList.of(), ImmutableList.of()));

                for (CaseStatementWhenClause whenClause : node.getWhenClauses().reversed()) {
                    io.trino.sql.ir.Expression conditionValue = toExpression(context, whenClause.getExpression());

                    io.trino.sql.ir.Expression testValue = new Reference(valueVariable.type(), variableReferenceName(valueVariable));
                    if (!testValue.type().equals(conditionValue.type())) {
                        ResolvedFunction castFunction = plannerContext.getMetadata().getCoercion(testValue.type(), conditionValue.type());
                        testValue = call(castFunction, testValue);
                    }

                    ResolvedFunction equals = plannerContext.getMetadata().resolveOperator(OperatorType.EQUAL, ImmutableList.of(testValue.type(), conditionValue.type()));
                    io.trino.sql.ir.Expression condition = call(equals, testValue, conditionValue);

                    IrStatement ifTrue = block(statements(whenClause.getStatements(), context));
                    statement = new IrIf(condition, ifTrue, Optional.of(statement));
                }
                return new IrBlock(ImmutableList.of(valueVariable), ImmutableList.of(statement));
            }

            IrStatement statement = node.getElseClause()
                    .map(elseClause -> block(statements(elseClause.getStatements(), context)))
                    .orElseGet(() -> new IrBlock(ImmutableList.of(), ImmutableList.of()));

            for (CaseStatementWhenClause whenClause : node.getWhenClauses().reversed()) {
                io.trino.sql.ir.Expression condition = toExpression(context, whenClause.getExpression());
                IrStatement ifTrue = block(statements(whenClause.getStatements(), context));
                statement = new IrIf(condition, ifTrue, Optional.of(statement));
            }

            return statement;
        }

        @Override
        protected IrStatement visitWhileStatement(WhileStatement node, Context context)
        {
            Context newContext = context.newScope();
            Optional<IrLabel> label = getSqlLabel(newContext, node.getLabel());
            io.trino.sql.ir.Expression condition = toExpression(newContext, node.getExpression());
            List<IrStatement> statements = statements(node.getStatements(), newContext);
            return new IrWhile(label, condition, block(statements));
        }

        @Override
        protected IrStatement visitRepeatStatement(RepeatStatement node, Context context)
        {
            Context newContext = context.newScope();
            Optional<IrLabel> label = getSqlLabel(newContext, node.getLabel());
            io.trino.sql.ir.Expression condition = toExpression(newContext, node.getCondition());
            List<IrStatement> statements = statements(node.getStatements(), newContext);
            return new IrRepeat(label, condition, block(statements));
        }

        @Override
        protected IrStatement visitLoopStatement(LoopStatement node, Context context)
        {
            Context newContext = context.newScope();
            Optional<IrLabel> label = getSqlLabel(newContext, node.getLabel());
            List<IrStatement> statements = statements(node.getStatements(), newContext);
            return new IrLoop(label, block(statements));
        }

        @Override
        protected IrStatement visitForStatement(ForStatement node, Context context)
        {
            Metadata metadata = plannerContext.getMetadata();

            io.trino.sql.ir.Expression lowerExpression = toExpression(context, node.getLowerBound());
            io.trino.sql.ir.Expression upperExpression = toExpression(context, node.getUpperBound());
            Type type = lowerExpression.type();
            io.trino.sql.ir.Expression stepExpression = node.getStep()
                    .map(step -> toExpression(context, step))
                    .orElseGet(() -> castConstant(1, type));

            IrVariable counterVariable = new IrVariable(allVariables.size(), type, lowerExpression);
            allVariables.add(counterVariable);
            IrVariable upperVariable = new IrVariable(allVariables.size(), type, upperExpression);
            allVariables.add(upperVariable);
            IrVariable stepVariable = new IrVariable(allVariables.size(), type, stepExpression);
            allVariables.add(stepVariable);

            io.trino.sql.ir.Expression counterReference = new Reference(type, variableReferenceName(counterVariable));
            io.trino.sql.ir.Expression upperReference = new Reference(type, variableReferenceName(upperVariable));
            io.trino.sql.ir.Expression stepReference = new Reference(type, variableReferenceName(stepVariable));
            io.trino.sql.ir.Expression zero = castConstant(0, type);

            // Direction is derived from the sign of the step: ascending while counter <= upper if step > 0,
            // descending while counter >= upper if step < 0. A step of zero (or one whose sign contradicts
            // the bounds) satisfies neither branch, so the loop simply runs zero times.
            io.trino.sql.ir.Expression condition = new Logical(Logical.Operator.OR, ImmutableList.of(
                    new Logical(Logical.Operator.AND, ImmutableList.of(
                            comparison(metadata, ComparisonOperator.GREATER_THAN, stepReference, zero),
                            comparison(metadata, ComparisonOperator.LESS_THAN_OR_EQUAL, counterReference, upperReference))),
                    new Logical(Logical.Operator.AND, ImmutableList.of(
                            comparison(metadata, ComparisonOperator.LESS_THAN, stepReference, zero),
                            comparison(metadata, ComparisonOperator.GREATER_THAN_OR_EQUAL, counterReference, upperReference)))));

            Context newContext = context.newScope();
            verify(newContext.variables().put(identifierValue(node.getVariable()), counterVariable) == null, "Variable already declared in scope: %s", node.getVariable());

            Optional<IrLabel> outerLabel = getSqlLabel(newContext, node.getLabel());
            IrLabel innerLabel = new IrLabel("for_body_" + labelCounter.getAndIncrement());
            node.getLabel().ifPresent(name ->
                    verify(newContext.iterateTargets().put(identifierValue(name), innerLabel) == null, "Label already declared in this scope: %s", name));

            List<IrStatement> bodyStatements = statements(node.getStatements(), newContext);
            IrStatement innerLoop = new IrLoop(Optional.of(innerLabel), block(ImmutableList.<IrStatement>builder()
                    .addAll(bodyStatements)
                    .add(new IrBreak(innerLabel))
                    .build()));

            ResolvedFunction add = metadata.resolveOperator(OperatorType.ADD, ImmutableList.of(type, type));
            IrStatement increment = new IrSet(counterVariable, call(add, counterReference, stepReference));

            IrStatement loop = new IrWhile(outerLabel, condition, block(ImmutableList.of(innerLoop, increment)));

            return new IrBlock(ImmutableList.of(counterVariable, upperVariable, stepVariable), ImmutableList.of(loop));
        }

        private io.trino.sql.ir.Expression castConstant(long value, Type type)
        {
            Constant literal = new Constant(BIGINT, value);
            if (type.equals(BIGINT)) {
                return literal;
            }
            ResolvedFunction cast = plannerContext.getMetadata().getCoercion(BIGINT, type);
            return call(cast, literal);
        }

        @Override
        protected IrStatement visitReturnStatement(ReturnStatement node, Context context)
        {
            return new IrReturn(toExpression(context, node.getValue()));
        }

        @Override
        protected IrStatement visitAssignmentStatement(AssignmentStatement node, Context context)
        {
            Identifier name = node.getTarget();
            IrVariable target = context.variables().get(identifierValue(name));
            checkArgument(target != null, "Variable not declared in scope: %s", name);
            return new IrSet(target, toExpression(context, node.getValue()));
        }

        @Override
        protected IrStatement visitIterateStatement(IterateStatement node, Context context)
        {
            // FOR loops are desugared to a WHILE loop wrapping a single-pass inner IrLoop; ITERATE must
            // break out of that inner wrapper (falling through to the increment) rather than continuing
            // the outer WHILE directly, which would skip the increment and loop forever.
            IrLabel iterateTarget = context.iterateTargets().get(identifierValue(node.getLabel()));
            if (iterateTarget != null) {
                return new IrBreak(iterateTarget);
            }
            return new IrContinue(label(context, node.getLabel()));
        }

        @Override
        protected IrStatement visitLeaveStatement(LeaveStatement node, Context context)
        {
            return new IrBreak(label(context, node.getLabel()));
        }

        private Optional<IrLabel> getSqlLabel(Context context, Optional<Identifier> labelName)
        {
            return labelName.map(name -> {
                IrLabel label = new IrLabel(identifierValue(name) + "_" + labelCounter.getAndIncrement());
                verify(context.labels().put(identifierValue(name), label) == null, "Label already declared in this scope: %s", name);
                return label;
            });
        }

        private static IrLabel label(Context context, Identifier name)
        {
            IrLabel label = context.labels().get(identifierValue(name));
            checkArgument(label != null, "Label not defined: %s", name);
            return label;
        }

        private io.trino.sql.ir.Expression toExpression(Context context, Expression expression)
        {
            // build symbol and field indexes for translation
            List<Field> fields = context.variables().entrySet().stream()
                    .map(entry -> Field.newUnqualified(entry.getKey(), entry.getValue().type()))
                    .collect(toImmutableList());

            Scope scope = Scope.builder()
                    .withRelationType(RelationId.of(expression), new RelationType(fields))
                    .build();

            SymbolAllocator symbolAllocator = new SymbolAllocator();
            List<Symbol> fieldSymbols = fields.stream()
                    .map(symbolAllocator::newSymbol)
                    .collect(toImmutableList());

            Map<NodeRef<LambdaArgumentDeclaration>, Symbol> nodeRefSymbolMap = buildLambdaDeclarationToSymbolMap(analysis, symbolAllocator);

            // Apply casts, desugar expression, and perform other rewrites
            TranslationMap translationMap = new TranslationMap(Optional.empty(), scope, analysis, nodeRefSymbolMap, fieldSymbols, session, plannerContext, symbolAllocator);
            io.trino.sql.ir.Expression translated = coerceIfNecessary(analysis, expression, translationMap.rewrite(expression));

            // desugar the lambda captures
            io.trino.sql.ir.Expression lambdaCaptureDesugared = LambdaCaptureDesugaringRewriter.rewrite(translated, symbolAllocator);

            // optimize the expression
            io.trino.sql.ir.Expression optimized = optimizer.process(lambdaCaptureDesugared, session, symbolAllocator, ImmutableMap.of()).orElse(lambdaCaptureDesugared);

            // Replace symbol references with routine variable references
            List<Map.Entry<String, IrVariable>> variableEntries = List.copyOf(context.variables().entrySet());
            ImmutableMap.Builder<String, io.trino.sql.ir.Expression> symbolReplacements = ImmutableMap.builder();
            for (int i = 0; i < fieldSymbols.size(); i++) {
                Symbol symbol = fieldSymbols.get(i);
                IrVariable variable = variableEntries.get(i).getValue();
                symbolReplacements.put(symbol.name(), new Reference(variable.type(), variableReferenceName(variable)));
            }
            Map<String, io.trino.sql.ir.Expression> replacements = symbolReplacements.buildOrThrow();

            return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
            {
                @Override
                public io.trino.sql.ir.Expression rewriteReference(Reference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    io.trino.sql.ir.Expression replacement = replacements.get(node.name());
                    if (replacement != null) {
                        return replacement;
                    }
                    return node;
                }
            }, optimized);
        }

        public static io.trino.sql.ir.Expression coerceIfNecessary(Analysis analysis, Expression original, io.trino.sql.ir.Expression rewritten)
        {
            Type coercion = analysis.getCoercion(original);
            if (coercion == null) {
                return rewritten;
            }
            return new Cast(rewritten, coercion);
        }

        private List<IrStatement> statements(List<ControlStatement> statements, Context context)
        {
            return statements.stream()
                    .map(statement -> process(statement, context))
                    .collect(toImmutableList());
        }

        private static IrBlock block(List<IrStatement> statements)
        {
            return new IrBlock(ImmutableList.of(), statements);
        }

        private static String identifierValue(Identifier name)
        {
            // TODO: this should use getCanonicalValue()
            // stop-gap: lowercasing for now to match what is happening during analysis;
            // otherwise we do not support non-lowercase variables in functions.
            // Rework as part of https://github.com/trinodb/trino/pull/24829
            return name.getValue().toLowerCase(ENGLISH);
        }
    }

    private record Context(Map<String, IrVariable> variables, Map<String, IrLabel> labels, Map<String, IrLabel> iterateTargets)
    {
        public Context
        {
            variables = new LinkedHashMap<>(variables);
            labels = new LinkedHashMap<>(labels);
            iterateTargets = new LinkedHashMap<>(iterateTargets);
        }

        public Context newScope()
        {
            return new Context(variables, labels, iterateTargets);
        }
    }

    static String variableReferenceName(IrVariable variable)
    {
        return variableReferenceName(variable.field());
    }

    static String variableReferenceName(int field)
    {
        return "$sqlvar_" + field;
    }
}
