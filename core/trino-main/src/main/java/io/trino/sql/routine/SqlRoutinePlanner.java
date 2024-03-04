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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.ExpressionAnalyzer;
import io.trino.sql.analyzer.Field;
import io.trino.sql.analyzer.RelationId;
import io.trino.sql.analyzer.RelationType;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.planner.IrExpressionInterpreter;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.NoOpSymbolResolver;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TranslationMap;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.rule.LambdaCaptureDesugaringRewriter;
import io.trino.sql.planner.sanity.SugarFreeChecker;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SqlToRowExpressionTranslator;
import io.trino.sql.relational.StandardFunctionResolution;
import io.trino.sql.relational.optimizer.ExpressionOptimizer;
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
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CompoundStatement;
import io.trino.sql.tree.ControlStatement;
import io.trino.sql.tree.ElseIfClause;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionSpecification;
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
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.VariableDeclaration;
import io.trino.sql.tree.WhileStatement;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.LogicalPlanner.buildLambdaDeclarationToSymbolMap;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constantNull;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static java.util.Objects.requireNonNull;

public final class SqlRoutinePlanner
{
    private final PlannerContext plannerContext;
    private final WarningCollector warningCollector;

    public SqlRoutinePlanner(PlannerContext plannerContext, WarningCollector warningCollector)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    public IrRoutine planSqlFunction(Session session, FunctionSpecification function, SqlRoutineAnalysis routineAnalysis)
    {
        List<IrVariable> allVariables = new ArrayList<>();
        Map<String, IrVariable> scopeVariables = new LinkedHashMap<>();

        ImmutableList.Builder<IrVariable> parameters = ImmutableList.builder();
        routineAnalysis.arguments().forEach((name, type) -> {
            IrVariable variable = new IrVariable(allVariables.size(), type, constantNull(type));
            allVariables.add(variable);
            scopeVariables.put(name, variable);
            parameters.add(variable);
        });

        Analysis analysis = routineAnalysis.analysis();
        StatementVisitor visitor = new StatementVisitor(session, allVariables, analysis);
        IrStatement body = visitor.process(function.getStatement(), new Context(scopeVariables, Map.of()));

        return new IrRoutine(routineAnalysis.returnType(), parameters.build(), body);
    }

    private class StatementVisitor
            extends AstVisitor<IrStatement, Context>
    {
        private final Session session;
        private final List<IrVariable> allVariables;
        private final Analysis analysis;
        private final StandardFunctionResolution resolution;

        public StatementVisitor(
                Session session,
                List<IrVariable> allVariables,
                Analysis analysis)
        {
            this.session = requireNonNull(session, "session is null");
            this.resolution = new StandardFunctionResolution(plannerContext.getMetadata());
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
                RowExpression defaultValue = declaration.getDefaultValue()
                        .map(expression -> toRowExpression(newContext, expression))
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
                        toRowExpression(context, elseIf.getExpression()),
                        block(statements(elseIf.getStatements(), context)),
                        ifFalse));
            }

            return new IrIf(
                    toRowExpression(context, node.getExpression()),
                    block(statements(node.getStatements(), context)),
                    ifFalse);
        }

        @Override
        protected IrStatement visitCaseStatement(CaseStatement node, Context context)
        {
            if (node.getExpression().isPresent()) {
                RowExpression valueExpression = toRowExpression(context, node.getExpression().get());
                IrVariable valueVariable = new IrVariable(allVariables.size(), valueExpression.getType(), valueExpression);

                IrStatement statement = node.getElseClause()
                        .map(elseClause -> block(statements(elseClause.getStatements(), context)))
                        .orElseGet(() -> new IrBlock(ImmutableList.of(), ImmutableList.of()));

                for (CaseStatementWhenClause whenClause : node.getWhenClauses().reversed()) {
                    RowExpression conditionValue = toRowExpression(context, whenClause.getExpression());

                    RowExpression testValue = field(valueVariable.field(), valueVariable.type());
                    if (!testValue.getType().equals(conditionValue.getType())) {
                        ResolvedFunction castFunction = plannerContext.getMetadata().getCoercion(testValue.getType(), conditionValue.getType());
                        testValue = call(castFunction, testValue);
                    }

                    ResolvedFunction equals = resolution.comparisonFunction(EQUAL, testValue.getType(), conditionValue.getType());
                    RowExpression condition = call(equals, testValue, conditionValue);

                    IrStatement ifTrue = block(statements(whenClause.getStatements(), context));
                    statement = new IrIf(condition, ifTrue, Optional.of(statement));
                }
                return new IrBlock(ImmutableList.of(valueVariable), ImmutableList.of(statement));
            }

            IrStatement statement = node.getElseClause()
                    .map(elseClause -> block(statements(elseClause.getStatements(), context)))
                    .orElseGet(() -> new IrBlock(ImmutableList.of(), ImmutableList.of()));

            for (CaseStatementWhenClause whenClause : node.getWhenClauses().reversed()) {
                RowExpression condition = toRowExpression(context, whenClause.getExpression());
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
            RowExpression condition = toRowExpression(newContext, node.getExpression());
            List<IrStatement> statements = statements(node.getStatements(), newContext);
            return new IrWhile(label, condition, block(statements));
        }

        @Override
        protected IrStatement visitRepeatStatement(RepeatStatement node, Context context)
        {
            Context newContext = context.newScope();
            Optional<IrLabel> label = getSqlLabel(newContext, node.getLabel());
            RowExpression condition = toRowExpression(newContext, node.getCondition());
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
        protected IrStatement visitReturnStatement(ReturnStatement node, Context context)
        {
            return new IrReturn(toRowExpression(context, node.getValue()));
        }

        @Override
        protected IrStatement visitAssignmentStatement(AssignmentStatement node, Context context)
        {
            Identifier name = node.getTarget();
            IrVariable target = context.variables().get(identifierValue(name));
            checkArgument(target != null, "Variable not declared in scope: %s", name);
            return new IrSet(target, toRowExpression(context, node.getValue()));
        }

        @Override
        protected IrStatement visitIterateStatement(IterateStatement node, Context context)
        {
            return new IrContinue(label(context, node.getLabel()));
        }

        @Override
        protected IrStatement visitLeaveStatement(LeaveStatement node, Context context)
        {
            return new IrBreak(label(context, node.getLabel()));
        }

        private static Optional<IrLabel> getSqlLabel(Context context, Optional<Identifier> labelName)
        {
            return labelName.map(name -> {
                IrLabel label = new IrLabel(identifierValue(name));
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

        private RowExpression toRowExpression(Context context, Expression expression)
        {
            // build symbol and field indexes for translation
            TypeProvider typeProvider = TypeProvider.viewOf(
                    context.variables().entrySet().stream().collect(toImmutableMap(
                            entry -> new Symbol(entry.getKey()),
                            entry -> entry.getValue().type())));

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

            // Apply casts, desugar expression, and preform other rewrites
            TranslationMap translationMap = new TranslationMap(Optional.empty(), scope, analysis, nodeRefSymbolMap, fieldSymbols, session, plannerContext);
            Expression translated = coerceIfNecessary(analysis, expression, translationMap.rewrite(expression));

            // desugar the lambda captures
            Expression lambdaCaptureDesugared = LambdaCaptureDesugaringRewriter.rewrite(translated, typeProvider, symbolAllocator);

            // The expression tree has been rewritten which breaks all the identity maps, so redo the analysis
            // to re-analyze coercions that might be necessary
            ExpressionAnalyzer analyzer = createExpressionAnalyzer(session, typeProvider);
            analyzer.analyze(lambdaCaptureDesugared, scope);

            // optimize the expression
            IrExpressionInterpreter interpreter = new IrExpressionInterpreter(lambdaCaptureDesugared, plannerContext, session, analyzer.getExpressionTypes());
            Expression optimized = new LiteralEncoder(plannerContext)
                    .toExpression(interpreter.optimize(NoOpSymbolResolver.INSTANCE), analyzer.getExpressionTypes().get(NodeRef.of(lambdaCaptureDesugared)));

            // validate expression
            SugarFreeChecker.validate(optimized);

            // Analyze again after optimization
            analyzer = createExpressionAnalyzer(session, typeProvider);
            analyzer.analyze(optimized, scope);

            // translate to RowExpression
            TranslationVisitor translator = new TranslationVisitor(plannerContext.getMetadata(), plannerContext.getTypeManager(), analyzer.getExpressionTypes(), ImmutableMap.of(), context.variables());
            RowExpression rowExpression = translator.process(optimized, null);

            // optimize RowExpression
            ExpressionOptimizer optimizer = new ExpressionOptimizer(plannerContext.getMetadata(), plannerContext.getFunctionManager(), session);
            rowExpression = optimizer.optimize(rowExpression);

            return rowExpression;
        }

        public static Expression coerceIfNecessary(Analysis analysis, Expression original, Expression rewritten)
        {
            Type coercion = analysis.getCoercion(original);
            if (coercion == null) {
                return rewritten;
            }
            return new Cast(rewritten, toSqlType(coercion), false);
        }

        private ExpressionAnalyzer createExpressionAnalyzer(Session session, TypeProvider typeProvider)
        {
            return ExpressionAnalyzer.createWithoutSubqueries(
                    plannerContext,
                    new AllowAllAccessControl(),
                    session,
                    typeProvider,
                    ImmutableMap.of(),
                    node -> new VerifyException("Unexpected subquery"),
                    warningCollector,
                    false);
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
            return name.getValue();
        }
    }

    private record Context(Map<String, IrVariable> variables, Map<String, IrLabel> labels)
    {
        public Context
        {
            variables = new LinkedHashMap<>(variables);
            labels = new LinkedHashMap<>(labels);
        }

        public Context newScope()
        {
            return new Context(variables, labels);
        }
    }

    private static class TranslationVisitor
            extends SqlToRowExpressionTranslator.Visitor
    {
        private final Map<String, IrVariable> variables;

        public TranslationVisitor(
                Metadata metadata,
                TypeManager typeManager,
                Map<NodeRef<Expression>, Type> types,
                Map<Symbol, Integer> layout,
                Map<String, IrVariable> variables)
        {
            super(metadata, typeManager, types, layout);
            this.variables = requireNonNull(variables, "variables is null");
        }

        @Override
        protected RowExpression visitSymbolReference(SymbolReference node, Void context)
        {
            IrVariable variable = variables.get(node.getName());
            if (variable != null) {
                return field(variable.field(), variable.type());
            }
            return super.visitSymbolReference(node, context);
        }
    }
}
