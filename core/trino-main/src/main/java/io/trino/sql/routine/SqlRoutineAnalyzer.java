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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.ResolvedFunction;
import io.trino.security.AccessControl;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.CorrelationSupport;
import io.trino.sql.analyzer.ExpressionAnalyzer;
import io.trino.sql.analyzer.Field;
import io.trino.sql.analyzer.QueryType;
import io.trino.sql.analyzer.RelationId;
import io.trino.sql.analyzer.RelationType;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.analyzer.TypeSignatureTranslator;
import io.trino.sql.tree.AssignmentStatement;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.CaseStatement;
import io.trino.sql.tree.CaseStatementWhenClause;
import io.trino.sql.tree.CommentCharacteristic;
import io.trino.sql.tree.CompoundStatement;
import io.trino.sql.tree.ControlStatement;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.DeterministicCharacteristic;
import io.trino.sql.tree.ElseClause;
import io.trino.sql.tree.ElseIfClause;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionSpecification;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfStatement;
import io.trino.sql.tree.IterateStatement;
import io.trino.sql.tree.LanguageCharacteristic;
import io.trino.sql.tree.LeaveStatement;
import io.trino.sql.tree.LoopStatement;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NullInputCharacteristic;
import io.trino.sql.tree.ParameterDeclaration;
import io.trino.sql.tree.PropertiesCharacteristic;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.RepeatStatement;
import io.trino.sql.tree.ReturnStatement;
import io.trino.sql.tree.ReturnsClause;
import io.trino.sql.tree.SecurityCharacteristic;
import io.trino.sql.tree.SecurityCharacteristic.Security;
import io.trino.sql.tree.VariableDeclaration;
import io.trino.sql.tree.WhileStatement;
import io.trino.type.TypeCoercion;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getLast;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_PROPERTY;
import static io.trino.spi.StandardErrorCode.MISSING_RETURN;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toTypeSignature;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

public class SqlRoutineAnalyzer
{
    private final PlannerContext plannerContext;
    private final WarningCollector warningCollector;

    public SqlRoutineAnalyzer(PlannerContext plannerContext, WarningCollector warningCollector)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    public static FunctionMetadata extractFunctionMetadata(FunctionId functionId, FunctionSpecification function)
    {
        validateLanguage(function);

        String functionName = getFunctionName(function);
        Signature.Builder signatureBuilder = Signature.builder()
                .returnType(toTypeSignature(function.getReturnsClause().getReturnType()));

        validateArguments(function);
        function.getParameters().stream()
                .map(ParameterDeclaration::getType)
                .map(TypeSignatureTranslator::toTypeSignature)
                .forEach(signatureBuilder::argumentType);
        Signature signature = signatureBuilder.build();

        FunctionMetadata.Builder builder = FunctionMetadata.scalarBuilder(functionName)
                .functionId(functionId)
                .signature(signature)
                .nullable()
                .argumentNullability(nCopies(signature.getArgumentTypes().size(), isCalledOnNull(function)));

        getComment(function)
                .filter(not(String::isBlank))
                .ifPresentOrElse(builder::description, builder::noDescription);

        if (!isDeterministic(function)) {
            builder.nondeterministic();
        }

        validateSecurity(function);

        return builder.build();
    }

    public SqlRoutineAnalysis analyze(Session session, AccessControl accessControl, FunctionSpecification function)
    {
        checkArgument(getLanguageName(function).equalsIgnoreCase("SQL"), "function language must be SQL");
        ControlStatement statement = function.getStatement().orElseThrow();

        String functionName = getFunctionName(function);

        boolean calledOnNull = isCalledOnNull(function);
        Optional<String> comment = getComment(function);

        ReturnsClause returnsClause = function.getReturnsClause();
        Type returnType = getType(returnsClause, returnsClause.getReturnType());

        Map<String, Type> arguments = getArguments(function);

        validateReturn(statement);

        StatementVisitor visitor = new StatementVisitor(session, accessControl, returnType);
        visitor.process(statement, new Context(arguments, Set.of()));

        Analysis analysis = visitor.getAnalysis();

        boolean actuallyDeterministic = analysis.getResolvedFunctions().stream().allMatch(ResolvedFunction::deterministic);

        boolean declaredDeterministic = isDeterministic(function);
        if (!declaredDeterministic && actuallyDeterministic) {
            throw semanticException(INVALID_ARGUMENTS, function, "Deterministic function declared NOT DETERMINISTIC");
        }
        if (declaredDeterministic && !actuallyDeterministic) {
            throw semanticException(INVALID_ARGUMENTS, function, "Non-deterministic function declared DETERMINISTIC");
        }

        return new SqlRoutineAnalysis(
                functionName,
                arguments,
                returnType,
                calledOnNull,
                actuallyDeterministic,
                comment,
                statement,
                visitor.getAnalysis());
    }

    private static String getFunctionName(FunctionSpecification function)
    {
        String name = function.getName().getSuffix();
        if (name.contains("@") || name.contains("$")) {
            throw semanticException(NOT_SUPPORTED, function, "Function name cannot contain '@' or '$'");
        }
        return name;
    }

    private Type getType(Node node, DataType type)
    {
        try {
            return plannerContext.getTypeManager().getType(toTypeSignature(type));
        }
        catch (TypeNotFoundException e) {
            throw semanticException(TYPE_MISMATCH, node, "Unknown type: %s", type);
        }
    }

    private Map<String, Type> getArguments(FunctionSpecification function)
    {
        validateArguments(function);

        Map<String, Type> arguments = new LinkedHashMap<>();
        for (ParameterDeclaration parameter : function.getParameters()) {
            arguments.put(
                    identifierValue(parameter.getName().orElseThrow()),
                    getType(parameter, parameter.getType()));
        }
        return arguments;
    }

    private static void validateArguments(FunctionSpecification function)
    {
        Set<String> argumentNames = new LinkedHashSet<>();
        for (ParameterDeclaration parameter : function.getParameters()) {
            if (parameter.getName().isEmpty()) {
                throw semanticException(INVALID_ARGUMENTS, parameter, "Function parameters must have a name");
            }
            String name = identifierValue(parameter.getName().get());
            if (!argumentNames.add(name)) {
                throw semanticException(INVALID_ARGUMENTS, parameter, "Duplicate function parameter name: %s", name);
            }
        }
    }

    public static Optional<Identifier> getLanguage(FunctionSpecification function)
    {
        List<LanguageCharacteristic> language = function.getRoutineCharacteristics().stream()
                .filter(LanguageCharacteristic.class::isInstance)
                .map(LanguageCharacteristic.class::cast)
                .collect(toImmutableList());

        if (language.size() > 1) {
            throw semanticException(SYNTAX_ERROR, language.get(1), "Multiple language clauses specified");
        }

        return language.stream()
                .map(LanguageCharacteristic::getLanguage)
                .findAny();
    }

    public static String getLanguageName(FunctionSpecification function)
    {
        return getLanguage(function).map(Identifier::getCanonicalValue).orElse("SQL");
    }

    private static void validateLanguage(FunctionSpecification function)
    {
        if (getLanguageName(function).equalsIgnoreCase("SQL")) {
            function.getDefinition().ifPresent(definition -> {
                throw semanticException(SYNTAX_ERROR, definition, "Functions using language 'SQL' must be defined using SQL");
            });
            List<Property> properties = getProperties(function);
            if (!properties.isEmpty()) {
                throw semanticException(INVALID_FUNCTION_PROPERTY, properties.getFirst(), "Function language 'SQL' does not support properties");
            }
        }
        else {
            function.getStatement().ifPresent(statement -> {
                throw semanticException(SYNTAX_ERROR, statement, "Only functions using language 'SQL' may be defined using SQL");
            });
            function.getRoutineCharacteristics().stream()
                    .filter(SecurityCharacteristic.class::isInstance)
                    .findFirst()
                    .ifPresent(security -> {
                        throw semanticException(NOT_SUPPORTED, security, "Only functions using language 'SQL' may declare security");
                    });
        }
    }

    private static boolean isDeterministic(FunctionSpecification function)
    {
        List<DeterministicCharacteristic> deterministic = function.getRoutineCharacteristics().stream()
                .filter(DeterministicCharacteristic.class::isInstance)
                .map(DeterministicCharacteristic.class::cast)
                .collect(toImmutableList());

        if (deterministic.size() > 1) {
            throw semanticException(SYNTAX_ERROR, deterministic.get(1), "Multiple deterministic clauses specified");
        }

        return deterministic.stream()
                .map(DeterministicCharacteristic::isDeterministic)
                .findAny()
                .orElse(true);
    }

    private static boolean isCalledOnNull(FunctionSpecification function)
    {
        List<NullInputCharacteristic> nullInput = function.getRoutineCharacteristics().stream()
                .filter(NullInputCharacteristic.class::isInstance)
                .map(NullInputCharacteristic.class::cast)
                .collect(toImmutableList());

        if (nullInput.size() > 1) {
            throw semanticException(SYNTAX_ERROR, nullInput.get(1), "Multiple null-call clauses specified");
        }

        return nullInput.stream()
                .map(NullInputCharacteristic::isCalledOnNull)
                .findAny()
                .orElse(true);
    }

    public static boolean isRunAsInvoker(FunctionSpecification function)
    {
        List<SecurityCharacteristic> security = function.getRoutineCharacteristics().stream()
                .filter(SecurityCharacteristic.class::isInstance)
                .map(SecurityCharacteristic.class::cast)
                .collect(toImmutableList());

        if (security.size() > 1) {
            throw semanticException(SYNTAX_ERROR, security.get(1), "Multiple security clauses specified");
        }

        return security.stream()
                .map(SecurityCharacteristic::getSecurity)
                .map(Security.INVOKER::equals)
                .findAny()
                .orElse(false);
    }

    private static void validateSecurity(FunctionSpecification function)
    {
        isRunAsInvoker(function);
    }

    private static Optional<String> getComment(FunctionSpecification function)
    {
        List<CommentCharacteristic> comment = function.getRoutineCharacteristics().stream()
                .filter(CommentCharacteristic.class::isInstance)
                .map(CommentCharacteristic.class::cast)
                .collect(toImmutableList());

        if (comment.size() > 1) {
            throw semanticException(SYNTAX_ERROR, comment.get(1), "Multiple comment clauses specified");
        }

        return comment.stream()
                .map(CommentCharacteristic::getComment)
                .findAny();
    }

    public static List<Property> getProperties(FunctionSpecification function)
    {
        List<PropertiesCharacteristic> properties = function.getRoutineCharacteristics().stream()
                .filter(PropertiesCharacteristic.class::isInstance)
                .map(PropertiesCharacteristic.class::cast)
                .toList();

        if (properties.size() > 1) {
            throw semanticException(SYNTAX_ERROR, properties.get(1), "Multiple properties clauses specified");
        }

        return properties.stream()
                .map(PropertiesCharacteristic::getProperties)
                .flatMap(List::stream)
                .collect(toImmutableList());
    }

    private static void validateReturn(ControlStatement statement)
    {
        switch (statement) {
            case ReturnStatement _ -> {}
            case CompoundStatement body -> {
                if (!(getLast(body.getStatements(), null) instanceof ReturnStatement)) {
                    throw semanticException(MISSING_RETURN, body, "Function must end in a RETURN statement");
                }
            }
            default -> throw new IllegalArgumentException("Invalid function statement: " + statement);
        }
    }

    private class StatementVisitor
            extends AstVisitor<Void, Context>
    {
        private final Session session;
        private final AccessControl accessControl;
        private final Type returnType;

        private final Analysis analysis = new Analysis(null, ImmutableMap.of(), QueryType.OTHERS);
        private final TypeCoercion typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);

        public StatementVisitor(Session session, AccessControl accessControl, Type returnType)
        {
            this.session = requireNonNull(session, "session is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.returnType = requireNonNull(returnType, "returnType is null");
        }

        public Analysis getAnalysis()
        {
            return analysis;
        }

        @Override
        protected Void visitNode(Node node, Context context)
        {
            throw new UnsupportedOperationException("Analysis not yet implemented: " + node);
        }

        @Override
        protected Void visitCompoundStatement(CompoundStatement node, Context context)
        {
            Context newContext = context.newScope();

            for (VariableDeclaration declaration : node.getVariableDeclarations()) {
                Type type = getType(declaration, declaration.getType());
                analysis.addType(declaration.getType(), type);
                declaration.getDefaultValue().ifPresent(value ->
                        analyzeExpression(newContext, value, type, "Value of DEFAULT"));

                for (Identifier name : declaration.getNames()) {
                    if (newContext.variables().put(identifierValue(name), type) != null) {
                        throw semanticException(ALREADY_EXISTS, name, "Variable already declared in this scope: %s", name);
                    }
                }
            }

            analyzeNodes(newContext, node.getStatements());

            return null;
        }

        @Override
        protected Void visitIfStatement(IfStatement node, Context context)
        {
            analyzeExpression(context, node.getExpression(), BOOLEAN, "Condition of IF statement");
            analyzeNodes(context, node.getStatements());
            analyzeNodes(context, node.getElseIfClauses());
            node.getElseClause().ifPresent(statement -> process(statement, context));
            return null;
        }

        @Override
        protected Void visitElseIfClause(ElseIfClause node, Context context)
        {
            analyzeExpression(context, node.getExpression(), BOOLEAN, "Condition of ELSEIF clause");
            analyzeNodes(context, node.getStatements());
            return null;
        }

        @Override
        protected Void visitElseClause(ElseClause node, Context context)
        {
            analyzeNodes(context, node.getStatements());
            return null;
        }

        @Override
        protected Void visitCaseStatement(CaseStatement node, Context context)
        {
            // when clause condition
            if (node.getExpression().isPresent()) {
                Type valueType = analyzeExpression(context, node.getExpression().get());
                for (CaseStatementWhenClause whenClause : node.getWhenClauses()) {
                    Type whenType = analyzeExpression(context, whenClause.getExpression());
                    Optional<Type> superType = typeCoercion.getCommonSuperType(valueType, whenType);
                    if (superType.isEmpty()) {
                        throw semanticException(TYPE_MISMATCH, whenClause.getExpression(), "WHEN clause value must evaluate to CASE value type %s (actual: %s)", valueType, whenType);
                    }
                    if (!whenType.equals(superType.get())) {
                        addCoercion(whenClause.getExpression(), superType.get());
                    }
                }
            }
            else {
                for (CaseStatementWhenClause whenClause : node.getWhenClauses()) {
                    analyzeExpression(context, whenClause.getExpression(), BOOLEAN, "Condition of WHEN clause");
                }
            }

            // when clause body
            for (CaseStatementWhenClause whenClause : node.getWhenClauses()) {
                analyzeNodes(context, whenClause.getStatements());
            }

            // else clause body
            if (node.getElseClause().isPresent()) {
                process(node.getElseClause().get(), context);
            }
            return null;
        }

        @Override
        protected Void visitWhileStatement(WhileStatement node, Context context)
        {
            Context newContext = context.newScope();
            node.getLabel().ifPresent(name -> defineLabel(newContext, name));
            analyzeExpression(newContext, node.getExpression(), BOOLEAN, "Condition of WHILE statement");
            analyzeNodes(newContext, node.getStatements());
            return null;
        }

        @Override
        protected Void visitRepeatStatement(RepeatStatement node, Context context)
        {
            Context newContext = context.newScope();
            node.getLabel().ifPresent(name -> defineLabel(newContext, name));
            analyzeExpression(newContext, node.getCondition(), BOOLEAN, "Condition of REPEAT statement");
            analyzeNodes(newContext, node.getStatements());
            return null;
        }

        @Override
        protected Void visitLoopStatement(LoopStatement node, Context context)
        {
            Context newContext = context.newScope();
            node.getLabel().ifPresent(name -> defineLabel(newContext, name));
            analyzeNodes(newContext, node.getStatements());
            return null;
        }

        @Override
        protected Void visitReturnStatement(ReturnStatement node, Context context)
        {
            analyzeExpression(context, node.getValue(), returnType, "Value of RETURN");
            return null;
        }

        @Override
        protected Void visitAssignmentStatement(AssignmentStatement node, Context context)
        {
            Identifier name = node.getTarget();
            Type targetType = context.variables().get(identifierValue(name));
            if (targetType == null) {
                throw semanticException(NOT_FOUND, name, "Variable cannot be resolved: %s", name);
            }
            analyzeExpression(context, node.getValue(), targetType, format("Value of SET '%s'", name));
            return null;
        }

        @Override
        protected Void visitIterateStatement(IterateStatement node, Context context)
        {
            verifyLabelExists(context, node.getLabel());
            return null;
        }

        @Override
        protected Void visitLeaveStatement(LeaveStatement node, Context context)
        {
            verifyLabelExists(context, node.getLabel());
            return null;
        }

        private void analyzeExpression(Context context, Expression expression, Type expectedType, String message)
        {
            Type actualType = analyzeExpression(context, expression);
            if (actualType.equals(expectedType)) {
                return;
            }
            if (!typeCoercion.canCoerce(actualType, expectedType)) {
                throw semanticException(TYPE_MISMATCH, expression, "%s must evaluate to %s (actual: %s)", message, expectedType, actualType);
            }

            addCoercion(expression, expectedType);
        }

        private Type analyzeExpression(Context context, Expression expression)
        {
            List<Field> fields = context.variables().entrySet().stream()
                    .map(entry -> Field.newUnqualified(entry.getKey(), entry.getValue()))
                    .collect(toImmutableList());

            Scope scope = Scope.builder()
                    .withRelationType(RelationId.of(expression), new RelationType(fields))
                    .build();

            ExpressionAnalyzer.analyzeExpressionWithoutSubqueries(
                    session,
                    plannerContext,
                    accessControl,
                    scope,
                    analysis,
                    expression,
                    NOT_SUPPORTED,
                    "Queries are not allowed in functions",
                    warningCollector,
                    CorrelationSupport.DISALLOWED);

            return analysis.getType(expression);
        }

        private void addCoercion(Expression expression, Type expectedType)
        {
            analysis.addCoercion(expression, expectedType);
        }

        private void analyzeNodes(Context context, List<? extends Node> statements)
        {
            for (Node statement : statements) {
                process(statement, context);
            }
        }

        private static void defineLabel(Context context, Identifier name)
        {
            if (!context.labels().add(identifierValue(name))) {
                throw semanticException(ALREADY_EXISTS, name, "Label already declared in this scope: %s", name);
            }
        }

        private static void verifyLabelExists(Context context, Identifier name)
        {
            if (!context.labels().contains(identifierValue(name))) {
                throw semanticException(NOT_FOUND, name, "Label not defined: %s", name);
            }
        }
    }

    private record Context(Map<String, Type> variables, Set<String> labels)
    {
        private Context
        {
            variables = new LinkedHashMap<>(variables);
            labels = new LinkedHashSet<>(labels);
        }

        public Context newScope()
        {
            return new Context(variables, labels);
        }
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
