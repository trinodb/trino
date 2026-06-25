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
package io.trino.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.spi.TrinoException;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.NumericVariableConstraint;
import io.trino.spi.function.Signature;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.function.VariableDeclaration;
import io.trino.spi.type.FunctionType;
import io.trino.spi.type.NumericExpression;
import io.trino.spi.type.NumericExpressions;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TemplateParameter;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeParameter;
import io.trino.spi.type.TypeTemplate;
import io.trino.spi.type.TypeTemplates;
import io.trino.sql.analyzer.TypeDescriptorProvider;
import io.trino.type.TypeCoercion;
import io.trino.type.UnknownType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSortedMap.toImmutableSortedMap;
import static io.trino.metadata.GlobalFunctionCatalog.BUILTIN_SCHEMA;
import static io.trino.metadata.OperatorNameUtil.mangleOperatorName;
import static io.trino.metadata.SignatureBinder.RelationshipType.EXACT;
import static io.trino.metadata.SignatureBinder.RelationshipType.EXPLICIT_COERCION_FROM;
import static io.trino.metadata.SignatureBinder.RelationshipType.EXPLICIT_COERCION_TO;
import static io.trino.metadata.SignatureBinder.RelationshipType.IMPLICIT_COERCION;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.TypeTemplates.toTypeDescriptor;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.type.TypeCoercion.isCovariantTypeBase;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Determines whether, and how, a callsite matches a generic function signature.
 * Which is equivalent to finding assignments for the variables in the generic signature,
 * such that all of the function's declared parameters are super types of the corresponding
 * arguments, and also satisfy the declared constraints (such as a given type parameter must
 * bind to an orderable type)
 * <p>
 * This implementation has made assumptions. When any of the assumptions is not satisfied, it will fail loudly.
 * <ul>
 * <li>A type cannot have both type parameter and literal parameter.
 * <li>A literal parameter cannot be used across types. see {@link #checkNoLiteralVariableUsageAcrossTypes(TypeTemplate, Map)}.
 * </ul>
 * <p>
 * Here are some known implementation limitations:
 * <ul>
 * <li>Binding signature {@code (decimal(x,2))boolean} with arguments {@code decimal(1,0)} fails.
 * It should produce {@code decimal(3,1)}.
 * </ul>
 */
public class SignatureBinder
{
    // 4 is chosen arbitrarily here. This limit is set to avoid having infinite loops in iterative solving.
    private static final int SOLVE_ITERATION_LIMIT = 4;

    private final Metadata metadata;
    private final TypeManager typeManager;
    private final TypeCoercion typeCoercion;
    private final Signature declaredSignature;
    private final boolean allowCoercion;
    private final Map<String, TypeVariableConstraint> typeVariableConstraints;

    // this could use the function resolver instead of Metadata, but Metadata caches coercion resolution
    SignatureBinder(Metadata metadata, TypeManager typeManager, Signature declaredSignature, boolean allowCoercion, boolean legacyVarcharToCharCoercion)
    {
        checkNoLiteralVariableUsageAcrossTypes(declaredSignature);
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.typeCoercion = new TypeCoercion(typeManager::getType, legacyVarcharToCharCoercion);
        this.declaredSignature = requireNonNull(declaredSignature, "declaredSignature is null");
        this.allowCoercion = allowCoercion;

        this.typeVariableConstraints = typeVariableConstraintsByName(declaredSignature);
    }

    private static Map<String, TypeVariableConstraint> typeVariableConstraintsByName(Signature signature)
    {
        return signature.getVariables().stream()
                .filter(VariableDeclaration.TypeVariable.class::isInstance)
                .map(VariableDeclaration.TypeVariable.class::cast)
                .collect(toImmutableSortedMap(CASE_INSENSITIVE_ORDER, VariableDeclaration::name, VariableDeclaration.TypeVariable::constraint));
    }

    public boolean canBind(List<? extends TypeDescriptorProvider> actualArgumentTypes)
    {
        return bindVariables(actualArgumentTypes).isPresent();
    }

    public boolean canBind(List<? extends TypeDescriptorProvider> actualArgumentTypes, TypeDescriptor actualReturnType)
    {
        return bindVariables(actualArgumentTypes, actualReturnType).isPresent();
    }

    Optional<GroundSignature> bind(List<? extends TypeDescriptorProvider> actualArgumentTypes)
    {
        Optional<VariableBindings> boundVariables = bindVariables(actualArgumentTypes);
        if (boundVariables.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(applyBoundVariables(declaredSignature, boundVariables.get(), actualArgumentTypes.size()));
    }

    @VisibleForTesting
    Optional<VariableBindings> bindVariables(List<? extends TypeDescriptorProvider> actualArgumentTypes)
    {
        ImmutableList.Builder<TypeConstraintSolver> constraintSolvers = ImmutableList.builder();
        if (!appendConstraintSolversForArguments(constraintSolvers, actualArgumentTypes)) {
            return Optional.empty();
        }

        return iterativeSolve(constraintSolvers.build());
    }

    @VisibleForTesting
    Optional<VariableBindings> bindVariables(List<? extends TypeDescriptorProvider> actualArgumentTypes, TypeDescriptor actualReturnType)
    {
        ImmutableList.Builder<TypeConstraintSolver> constraintSolvers = ImmutableList.builder();
        if (!appendConstraintSolversForReturnValue(constraintSolvers, new TypeDescriptorProvider(actualReturnType))) {
            return Optional.empty();
        }
        if (!appendConstraintSolversForArguments(constraintSolvers, actualArgumentTypes)) {
            return Optional.empty();
        }

        return iterativeSolve(constraintSolvers.build());
    }

    private static GroundSignature applyBoundVariables(Signature signature, VariableBindings bindings, int arity)
    {
        Map<String, TypeDescriptor> typeBindings = bindings.getTypeDescriptors();
        Map<String, Long> numericBindings = bindings.getNumericVariables();

        List<TypeTemplate> argumentTemplates = signature.getArgumentTypes();
        if (signature.isVariableArity()) {
            argumentTemplates = expandVarargFormalTemplate(argumentTemplates, arity);
        }
        else {
            checkArgument(argumentTemplates.size() == arity);
        }

        return new GroundSignature(
                TypeTemplates.bind(signature.getReturnType(), typeBindings, numericBindings),
                argumentTemplates.stream()
                        .map(argument -> TypeTemplates.bind(argument, typeBindings, numericBindings))
                        .collect(toImmutableList()));
    }

    private static List<TypeTemplate> expandVarargFormalTemplate(List<TypeTemplate> templates, int arity)
    {
        int variableArityArgumentsCount = arity - templates.size() + 1;
        if (variableArityArgumentsCount == 0) {
            return templates.subList(0, templates.size() - 1);
        }
        if (variableArityArgumentsCount == 1) {
            return templates;
        }
        checkArgument(variableArityArgumentsCount > 1 && !templates.isEmpty());

        ImmutableList.Builder<TypeTemplate> builder = ImmutableList.builderWithExpectedSize(templates.size() + variableArityArgumentsCount - 1);
        builder.addAll(templates);
        TypeTemplate variadic = templates.getLast();
        for (int i = 1; i < variableArityArgumentsCount; i++) {
            builder.add(variadic);
        }
        return builder.build();
    }

    /**
     * Grounds a type template against the bound variables, evaluating any calculated numeric expressions.
     */
    public static TypeDescriptor applyBoundVariables(TypeTemplate template, VariableBindings bindings)
    {
        return TypeTemplates.bind(template, bindings.getTypeDescriptors(), bindings.getNumericVariables());
    }

    public static List<TypeDescriptor> applyBoundVariables(List<TypeTemplate> templates, VariableBindings bindings)
    {
        return templates.stream()
                .map(template -> applyBoundVariables(template, bindings))
                .collect(toImmutableList());
    }

    public static FunctionBinding bindFunction(FunctionId functionId, Signature declaredSignature, BoundSignature boundSignature)
    {
        requireNonNull(declaredSignature, "declaredSignature is null");
        requireNonNull(boundSignature, "boundSignature is null");

        checkNoLiteralVariableUsageAcrossTypes(declaredSignature);

        Map<String, TypeVariableConstraint> typeVariableConstraints = typeVariableConstraintsByName(declaredSignature);

        boolean variableArity = declaredSignature.isVariableArity();
        List<TypeTemplate> formalTemplates = declaredSignature.getArgumentTypes();
        if (variableArity) {
            verifyBoundSignature(boundSignature.getArgumentTypes().size() >= formalTemplates.size() - 1, boundSignature, declaredSignature);
            formalTemplates = expandVarargFormalTemplate(formalTemplates, boundSignature.getArgumentTypes().size());
        }

        verifyBoundSignature(formalTemplates.size() == boundSignature.getArgumentTypes().size(), boundSignature, declaredSignature);

        BindingsBuilder bindings = new BindingsBuilder();
        for (int i = 0; i < formalTemplates.size(); i++) {
            extractBoundVariables(boundSignature, declaredSignature, typeVariableConstraints, bindings, boundSignature.getArgumentTypes().get(i), formalTemplates.get(i));
        }
        extractBoundVariables(boundSignature, declaredSignature, typeVariableConstraints, bindings, boundSignature.getReturnType(), declaredSignature.getReturnType());

        verifyBoundSignature(bindings.getTypeVariables().keySet().equals(typeVariableConstraints.keySet()), boundSignature, declaredSignature);

        return new FunctionBinding(
                functionId,
                boundSignature,
                bindings.build());
    }

    private static void extractBoundVariables(
            BoundSignature boundSignature,
            Signature declaredSignature,
            Map<String, TypeVariableConstraint> typeVariableConstraints,
            BindingsBuilder bindings,
            Type actualType,
            TypeTemplate declaredType)
    {
        String base = declaredType.baseName();
        List<TemplateParameter> parameters = parametersOf(declaredType);

        // type without nested type parameters
        if (parameters.isEmpty()) {
            TypeVariableConstraint typeVariableConstraint = typeVariableConstraints.get(base);
            if (typeVariableConstraint == null) {
                return;
            }

            if (bindings.containsTypeVariable(base)) {
                Type existingTypeBinding = bindings.getTypeVariable(base);
                verifyBoundSignature(actualType.equals(existingTypeBinding), boundSignature, declaredSignature);
            }
            else {
                bindings.setTypeVariable(base, actualType);
            }
            return;
        }

        verifyBoundSignature(base.equalsIgnoreCase(actualType.getTypeDescriptor().getBase()), boundSignature, declaredSignature);

        // type with nested literal parameters
        if (isTypeWithLiteralParameters(declaredType)) {
            for (int i = 0; i < parameters.size(); i++) {
                NumericExpression parameter = ((TemplateParameter.NumericArgument) parameters.get(i)).value();
                long actualNumericBinding = ((TypeParameter.Numeric) actualType.getTypeDescriptor().getParameters().get(i)).value();
                switch (parameter) {
                    case NumericExpression.Variable(String variable) -> {
                        if (bindings.containsNumericVariable(variable)) {
                            long existingNumericBinding = bindings.getNumericVariable(variable);
                            verifyBoundSignature(actualNumericBinding == existingNumericBinding, boundSignature, declaredSignature);
                        }
                        else {
                            bindings.setNumericVariable(variable, actualNumericBinding);
                        }
                    }
                    case NumericExpression.Literal(long value) -> verifyBoundSignature(actualNumericBinding == value, boundSignature, declaredSignature);
                    // A calculated parameter is computed from other variables, not matched, so it binds nothing here.
                    case NumericExpression.Operation _ -> {}
                    case NumericExpression.Conditional _ -> {}
                }
            }
            return;
        }

        // type with nested type parameters
        List<Type> actualTypeParameters = actualType.getTypeParameters();

        // unknown types are assumed to have unknown nested types
        if (UNKNOWN.equals(actualType)) {
            actualTypeParameters = Collections.nCopies(parameters.size(), UNKNOWN);
        }

        verifyBoundSignature(parameters.size() == actualTypeParameters.size(), boundSignature, declaredSignature);
        for (int i = 0; i < parameters.size(); i++) {
            TypeTemplate parameterTemplate = ((TemplateParameter.TypeArgument) parameters.get(i)).type();
            Type actualTypeParameter = actualTypeParameters.get(i);
            extractBoundVariables(boundSignature, declaredSignature, typeVariableConstraints, bindings, actualTypeParameter, parameterTemplate);
        }
    }

    private static void verifyBoundSignature(boolean expression, BoundSignature boundSignature, Signature declaredSignature)
    {
        checkArgument(expression, "Bound signature %s does not match declared signature %s", boundSignature, declaredSignature);
    }

    /**
     * Example of not allowed literal variable usages across typeSignatures:
     * <p><ul>
     * <li>x used in different base types: char(x) and varchar(x)
     * <li>x used in different positions of the same base type: decimal(x,y) and decimal(z,x)
     * <li>p used in combination with different literals, types, or literal variables: decimal(p,s1) and decimal(p,s2)
     * </ul>
     */
    private static void checkNoLiteralVariableUsageAcrossTypes(Signature declaredSignature)
    {
        Map<String, TypeTemplate> existingUsages = new HashMap<>();
        for (TypeTemplate parameter : declaredSignature.getArgumentTypes()) {
            checkNoLiteralVariableUsageAcrossTypes(parameter, existingUsages);
        }
    }

    private static void checkNoLiteralVariableUsageAcrossTypes(TypeTemplate template, Map<String, TypeTemplate> existingUsages)
    {
        for (TemplateParameter parameter : parametersOf(template)) {
            switch (parameter) {
                case TemplateParameter.NumericArgument(NumericExpression value) -> {
                    if (value instanceof NumericExpression.Variable(String name)) {
                        TypeTemplate existing = existingUsages.get(name);
                        if (existing != null && !existing.equals(template)) {
                            throw new UnsupportedOperationException("Literal parameters may not be shared across different types");
                        }
                        existingUsages.put(name, template);
                    }
                }
                case TemplateParameter.TypeArgument(_, TypeTemplate type) -> checkNoLiteralVariableUsageAcrossTypes(type, existingUsages);
            }
        }
    }

    private boolean appendConstraintSolversForReturnValue(ImmutableList.Builder<TypeConstraintSolver> resultBuilder, TypeDescriptorProvider actualReturnType)
    {
        TypeTemplate formalReturnType = declaredSignature.getReturnType();
        return appendTypeRelationshipConstraintSolver(resultBuilder, formalReturnType, actualReturnType, EXACT)
                && appendConstraintSolvers(resultBuilder, formalReturnType, actualReturnType, false);
    }

    private boolean appendConstraintSolversForArguments(ImmutableList.Builder<TypeConstraintSolver> resultBuilder, List<? extends TypeDescriptorProvider> actualTypes)
    {
        boolean variableArity = declaredSignature.isVariableArity();
        List<TypeTemplate> formalTemplates = declaredSignature.getArgumentTypes();
        if (variableArity) {
            if (actualTypes.size() < formalTemplates.size() - 1) {
                return false;
            }
            formalTemplates = expandVarargFormalTemplate(formalTemplates, actualTypes.size());
        }

        if (formalTemplates.size() != actualTypes.size()) {
            return false;
        }

        for (int i = 0; i < formalTemplates.size(); i++) {
            if (!appendTypeRelationshipConstraintSolver(resultBuilder, formalTemplates.get(i), actualTypes.get(i), allowCoercion ? IMPLICIT_COERCION : EXACT)) {
                return false;
            }
        }

        return appendConstraintSolvers(resultBuilder, formalTemplates, actualTypes, allowCoercion);
    }

    private boolean appendConstraintSolvers(
            ImmutableList.Builder<TypeConstraintSolver> resultBuilder,
            List<? extends TypeTemplate> formalTemplates,
            List<? extends TypeDescriptorProvider> actualTypes,
            boolean allowCoercion)
    {
        if (formalTemplates.size() != actualTypes.size()) {
            return false;
        }
        for (int i = 0; i < formalTemplates.size(); i++) {
            if (!appendConstraintSolvers(resultBuilder, formalTemplates.get(i), actualTypes.get(i), allowCoercion)) {
                return false;
            }
        }
        return true;
    }

    private boolean appendConstraintSolvers(
            ImmutableList.Builder<TypeConstraintSolver> resultBuilder,
            TypeTemplate formalTemplate,
            TypeDescriptorProvider actualTypeDescriptorProvider,
            boolean allowCoercion)
    {
        // formalTemplate can be categorized into one of the 5 cases below:
        // * function type
        // * type without type parameter
        // * type parameter of type/named_type kind
        // * type with type parameter of literal/variable kind
        // * type with type parameter of type/named_type kind (except function type)

        String base = formalTemplate.baseName();
        List<TemplateParameter> parameters = parametersOf(formalTemplate);

        if (FunctionType.NAME.equalsIgnoreCase(base)) {
            resultBuilder.add(new FunctionSolver(
                    getLambdaArgumentTemplates(formalTemplate),
                    ((TemplateParameter.TypeArgument) parameters.getLast()).type(),
                    actualTypeDescriptorProvider));
            return true;
        }

        if (actualTypeDescriptorProvider.hasDependency()) {
            return false;
        }

        if (parameters.isEmpty()) {
            TypeVariableConstraint typeVariableConstraint = typeVariableConstraints.get(base);
            if (typeVariableConstraint == null) {
                return true;
            }
            Type actualType = typeManager.getType(actualTypeDescriptorProvider.getTypeDescriptor());
            for (TypeTemplate castToTemplate : typeVariableConstraint.getCastableTo()) {
                appendTypeRelationshipConstraintSolver(resultBuilder, castToTemplate, actualTypeDescriptorProvider, EXPLICIT_COERCION_TO);
            }
            for (TypeTemplate castFromTemplate : typeVariableConstraint.getCastableFrom()) {
                appendTypeRelationshipConstraintSolver(resultBuilder, castFromTemplate, actualTypeDescriptorProvider, EXPLICIT_COERCION_FROM);
            }
            if (typeVariableConstraint.isRowType() && !(actualType instanceof RowType)) {
                return actualType == UNKNOWN;
            }
            resultBuilder.add(new TypeParameterSolver(
                    base,
                    actualType,
                    typeVariableConstraint.isComparableRequired(),
                    typeVariableConstraint.isOrderableRequired(),
                    typeVariableConstraint.isRowType()));
            return true;
        }

        Type actualType = typeManager.getType(actualTypeDescriptorProvider.getTypeDescriptor());
        if (isTypeWithLiteralParameters(formalTemplate)) {
            resultBuilder.add(new TypeWithLiteralParametersSolver(formalTemplate, actualType));
            return true;
        }

        List<TypeDescriptorProvider> actualTypeParametersTypeDescriptorProvider;
        if (UNKNOWN.equals(actualType)) {
            actualTypeParametersTypeDescriptorProvider = Collections.nCopies(parameters.size(), new TypeDescriptorProvider(UNKNOWN.getTypeDescriptor()));
        }
        else {
            actualTypeParametersTypeDescriptorProvider = fromTypes(actualType.getTypeParameters());
        }

        ImmutableList.Builder<TypeTemplate> formalTypeParameters = ImmutableList.builder();
        for (TemplateParameter formalTypeParameter : parameters) {
            formalTypeParameters.add(((TemplateParameter.TypeArgument) formalTypeParameter).type());
        }

        return appendConstraintSolvers(
                resultBuilder,
                formalTypeParameters.build(),
                actualTypeParametersTypeDescriptorProvider,
                allowCoercion && isCovariantTypeBase(base));
    }

    private Set<String> typeVariablesOf(TypeTemplate template)
    {
        String base = template.baseName();
        if (typeVariableConstraints.containsKey(base)) {
            return ImmutableSet.of(base);
        }
        ImmutableSet.Builder<String> variables = ImmutableSet.builder();
        for (TemplateParameter parameter : parametersOf(template)) {
            if (parameter instanceof TemplateParameter.TypeArgument(_, TypeTemplate type)) {
                variables.addAll(typeVariablesOf(type));
            }
        }

        return variables.build();
    }

    private static List<TemplateParameter> parametersOf(TypeTemplate template)
    {
        return template instanceof TypeTemplate.TypeApplication application ? application.parameters() : List.of();
    }

    private static Set<String> longVariablesOf(TypeTemplate template)
    {
        ImmutableSet.Builder<String> variables = ImmutableSet.builder();
        for (TemplateParameter parameter : parametersOf(template)) {
            switch (parameter) {
                case TemplateParameter.TypeArgument(_, TypeTemplate type) -> variables.addAll(longVariablesOf(type));
                case TemplateParameter.NumericArgument(NumericExpression value) -> variables.addAll(numericVariablesOf(value));
            }
        }

        return variables.build();
    }

    private static Set<String> numericVariablesOf(NumericExpression expression)
    {
        return switch (expression) {
            case NumericExpression.Literal _ -> ImmutableSet.of();
            case NumericExpression.Variable(String name) -> ImmutableSet.of(name);
            case NumericExpression.Operation(_, NumericExpression left, NumericExpression right) -> ImmutableSet.<String>builder()
                    .addAll(numericVariablesOf(left))
                    .addAll(numericVariablesOf(right))
                    .build();
            case NumericExpression.Conditional(NumericExpression.Comparison condition, NumericExpression ifTrue, NumericExpression ifFalse) -> ImmutableSet.<String>builder()
                    .addAll(numericVariablesOf(condition.left()))
                    .addAll(numericVariablesOf(condition.right()))
                    .addAll(numericVariablesOf(ifTrue))
                    .addAll(numericVariablesOf(ifFalse))
                    .build();
        };
    }

    private static boolean isTypeWithLiteralParameters(TypeTemplate template)
    {
        // True when every parameter is numeric — including a calculated Operation/Conditional. Those only
        // occur in a return type, never in an argument (no current signature inlines a calculated parameter
        // into an argument), so they reach the unification solver only on the return path, where it rejects
        // them loudly. Routing a calculated parameter to the relationship solver instead is the inlining
        // follow-up.
        return parametersOf(template).stream()
                .allMatch(TemplateParameter.NumericArgument.class::isInstance);
    }

    private Optional<VariableBindings> iterativeSolve(List<TypeConstraintSolver> constraints)
    {
        BindingsBuilder boundVariables = new BindingsBuilder();
        for (int i = 0; true; i++) {
            if (i == SOLVE_ITERATION_LIMIT) {
                throw new VerifyException(format("SignatureBinder.iterativeSolve does not converge after %d iterations.", SOLVE_ITERATION_LIMIT));
            }
            SolverReturnStatusMerger statusMerger = new SolverReturnStatusMerger();
            for (TypeConstraintSolver constraint : constraints) {
                statusMerger.add(constraint.update(boundVariables));
                if (statusMerger.getCurrent() == SolverReturnStatus.UNSOLVABLE) {
                    return Optional.empty();
                }
            }
            switch (statusMerger.getCurrent()) {
                case UNCHANGED_SATISFIED -> {}
                case UNCHANGED_NOT_SATISFIED -> {
                    return Optional.empty();
                }
                case CHANGED -> {
                    continue;
                }
                case UNSOLVABLE -> throw new VerifyException();
                default -> throw new UnsupportedOperationException("unknown status");
            }
            break;
        }

        calculateVariableValuesForLongConstraints(boundVariables);

        VariableBindings bindings = boundVariables.build();
        if (!allTypeVariablesBound(bindings)) {
            return Optional.empty();
        }
        return Optional.of(bindings);
    }

    private void calculateVariableValuesForLongConstraints(BindingsBuilder variableBinder)
    {
        for (VariableDeclaration variable : declaredSignature.getVariables()) {
            if (!(variable instanceof VariableDeclaration.NumericVariable(NumericVariableConstraint constraint))) {
                continue;
            }
            NumericExpression calculation = constraint.getExpression();
            String variableName = constraint.getName();
            long calculatedValue = NumericExpressions.evaluate(calculation, variableBinder.getNumericVariables()).longValueExact();
            if (variableBinder.containsNumericVariable(variableName)) {
                Long currentValue = variableBinder.getNumericVariable(variableName);
                checkState(Objects.equals(currentValue, calculatedValue),
                        "variable '%s' is already set to %s when trying to set %s",
                        variableName,
                        currentValue,
                        calculatedValue);
            }
            variableBinder.setNumericVariable(variableName, calculatedValue);
        }
    }

    private boolean allTypeVariablesBound(VariableBindings typeVariables)
    {
        return typeVariableConstraints.keySet().stream()
                .allMatch(typeVariables::containsTypeVariable);
    }

    private boolean satisfiesCoercion(RelationshipType relationshipType, Type actualType, TypeDescriptor constraintTypeDescriptor)
    {
        return switch (relationshipType) {
            case EXACT -> actualType.getTypeDescriptor().equals(constraintTypeDescriptor);
            case IMPLICIT_COERCION -> typeCoercion.canCoerce(actualType, typeManager.getType(constraintTypeDescriptor));
            case EXPLICIT_COERCION_TO -> canCast(actualType, typeManager.getType(constraintTypeDescriptor));
            case EXPLICIT_COERCION_FROM -> canCast(typeManager.getType(constraintTypeDescriptor), actualType);
        };
    }

    private boolean canCast(Type fromType, Type toType)
    {
        // NULL can be cast to any type; avoid re-entering coercion cache.
        if (fromType instanceof UnknownType || toType instanceof UnknownType) {
            return true;
        }
        if (fromType instanceof RowType fromRowType) {
            if (toType instanceof RowType toRowType) {
                List<Type> fromTypeParameters = fromRowType.getFieldTypes();
                List<Type> toTypeParameters = toRowType.getFieldTypes();
                if (fromTypeParameters.size() != toTypeParameters.size()) {
                    return false;
                }
                for (int fieldIndex = 0; fieldIndex < fromTypeParameters.size(); fieldIndex++) {
                    if (!canCast(fromTypeParameters.get(fieldIndex), toTypeParameters.get(fieldIndex))) {
                        return false;
                    }
                }
                return true;
            }
            if (isRecursiveCastFromRow(toType)) {
                return fromType.getTypeParameters().stream()
                        .allMatch(fromTypeParameter -> canCast(fromTypeParameter, toType));
            }
            return false;
        }
        if (toType instanceof RowType toRowType) {
            if (isRecursiveCastToRow(fromType)) {
                return toRowType.getFieldTypes().stream()
                        .allMatch(toTypeParameter -> canCast(fromType, toTypeParameter));
            }
        }
        try {
            metadata.getCoercion(fromType, toType);
            return true;
        }
        catch (TrinoException e) {
            return false;
        }
    }

    /// Check if there is a recursive variadic CAST from ROW.
    /// This needs special handling because the cast is applied to each field of ROW individually.
    private boolean isRecursiveCastFromRow(Type toType)
    {
        return metadata.getFunctions(null, new CatalogSchemaFunctionName(GlobalSystemConnector.NAME, BUILTIN_SCHEMA, mangleOperatorName(CAST))).stream()
                .map(cast -> cast.functionMetadata().getSignature())
                .anyMatch(signature -> isRecursiveCastFromRow(toType, signature));
    }

    private static boolean isRecursiveCastFromRow(Type toType, Signature signature)
    {
        // the return type must match toType
        if (!signature.getReturnType().equals(TypeTemplates.fromTypeDescriptor(toType.getTypeDescriptor()))) {
            return false;
        }

        // there must be exactly one variable, a type variable
        if (signature.getVariables().size() != 1 || !(signature.getVariables().getFirst() instanceof VariableDeclaration.TypeVariable(TypeVariableConstraint typeVariableConstraint))) {
            return false;
        }

        // The argument type must be a type variable with variadic bound of "row"
        return signature.getArgumentTypes().size() == 1 &&
                signature.getArgumentTypes().getFirst().baseName().equals(typeVariableConstraint.getName()) &&
                typeVariableConstraint.isRowType();
    }

    /// Check if there is a recursive variadic CAST to ROW.
    /// This needs special handling because the cast is applied to each field of ROW individually.
    private boolean isRecursiveCastToRow(Type fromType)
    {
        return metadata.getFunctions(null, new CatalogSchemaFunctionName(GlobalSystemConnector.NAME, BUILTIN_SCHEMA, mangleOperatorName(CAST))).stream()
                .map(cast -> cast.functionMetadata().getSignature())
                .anyMatch(signature -> isRecursiveCastToRow(fromType, signature));
    }

    private static boolean isRecursiveCastToRow(Type fromType, Signature signature)
    {
        // the argument type must match fromType
        if (signature.getArgumentTypes().size() != 1 || !signature.getArgumentTypes().getFirst().equals(TypeTemplates.fromTypeDescriptor(fromType.getTypeDescriptor()))) {
            return false;
        }

        // there must be exactly one variable, a type variable
        if (signature.getVariables().size() != 1 || !(signature.getVariables().getFirst() instanceof VariableDeclaration.TypeVariable(TypeVariableConstraint typeVariableConstraint))) {
            return false;
        }

        // The return type must be a type variable with variadic bound of "row"
        return signature.getReturnType().baseName().equals(typeVariableConstraint.getName()) &&
                typeVariableConstraint.isRowType();
    }

    private static List<TypeDescriptor> getLambdaArgumentTypeDescriptors(TypeDescriptor lambdaTypeDescriptor)
    {
        List<TypeParameter> parameters = lambdaTypeDescriptor.getParameters();
        return parameters.subList(0, parameters.size() - 1).stream()
                .map(parameter -> ((TypeParameter.Type) parameter).type())
                .collect(toImmutableList());
    }

    private static List<TypeTemplate> getLambdaArgumentTemplates(TypeTemplate lambdaTemplate)
    {
        List<TemplateParameter> parameters = parametersOf(lambdaTemplate);
        return parameters.subList(0, parameters.size() - 1).stream()
                .map(parameter -> ((TemplateParameter.TypeArgument) parameter).type())
                .collect(toImmutableList());
    }

    private interface TypeConstraintSolver
    {
        SolverReturnStatus update(BindingsBuilder bindings);
    }

    private enum SolverReturnStatus
    {
        UNCHANGED_SATISFIED,
        UNCHANGED_NOT_SATISFIED,
        CHANGED,
        UNSOLVABLE,
    }

    private static class SolverReturnStatusMerger
    {
        // This class gives the overall status when multiple status are seen from different parts.
        // The logic is simple and can be summarized as finding the right most item (based on the list below) seen so far:
        //   UNCHANGED_SATISFIED, UNCHANGED_NOT_SATISFIED, CHANGED, UNSOLVABLE
        // If no item was seen ever, it provides UNCHANGED_SATISFIED.

        private SolverReturnStatus current = SolverReturnStatus.UNCHANGED_SATISFIED;

        public void add(SolverReturnStatus newStatus)
        {
            switch (newStatus) {
                case UNCHANGED_SATISFIED -> {}
                case UNCHANGED_NOT_SATISFIED -> {
                    if (current == SolverReturnStatus.UNCHANGED_SATISFIED) {
                        current = SolverReturnStatus.UNCHANGED_NOT_SATISFIED;
                    }
                }
                case CHANGED -> {
                    if (current == SolverReturnStatus.UNCHANGED_SATISFIED || current == SolverReturnStatus.UNCHANGED_NOT_SATISFIED) {
                        current = SolverReturnStatus.CHANGED;
                    }
                }
                case UNSOLVABLE -> current = SolverReturnStatus.UNSOLVABLE;
            }
        }

        public SolverReturnStatus getCurrent()
        {
            return current;
        }
    }

    private class TypeParameterSolver
            implements TypeConstraintSolver
    {
        private final String typeParameter;
        private final Type actualType;
        private final boolean comparableRequired;
        private final boolean orderableRequired;
        private final boolean rowTypeRequired;

        public TypeParameterSolver(String typeParameter, Type actualType, boolean comparableRequired, boolean orderableRequired, boolean rowTypeRequired)
        {
            this.typeParameter = typeParameter;
            this.actualType = actualType;
            this.comparableRequired = comparableRequired;
            this.orderableRequired = orderableRequired;
            this.rowTypeRequired = rowTypeRequired;
        }

        @Override
        public SolverReturnStatus update(BindingsBuilder bindings)
        {
            if (!bindings.containsTypeVariable(typeParameter)) {
                if (!satisfiesConstraints(actualType)) {
                    return SolverReturnStatus.UNSOLVABLE;
                }
                bindings.setTypeVariable(typeParameter, actualType);
                return SolverReturnStatus.CHANGED;
            }
            Type originalType = bindings.getTypeVariable(typeParameter);
            Optional<Type> commonSuperType = typeCoercion.getCommonSuperType(originalType, actualType);
            if (commonSuperType.isEmpty()) {
                return SolverReturnStatus.UNSOLVABLE;
            }
            if (!satisfiesConstraints(commonSuperType.get())) {
                // This check must not be skipped even if commonSuperType is equal to originalType
                return SolverReturnStatus.UNSOLVABLE;
            }
            if (commonSuperType.get().equals(originalType)) {
                return SolverReturnStatus.UNCHANGED_SATISFIED;
            }
            bindings.setTypeVariable(typeParameter, commonSuperType.get());
            return SolverReturnStatus.CHANGED;
        }

        private boolean satisfiesConstraints(Type type)
        {
            if (comparableRequired && !type.isComparable()) {
                return false;
            }
            if (orderableRequired && !type.isOrderable()) {
                return false;
            }
            return UNKNOWN.equals(type) || !rowTypeRequired || type instanceof RowType;
        }
    }

    private class TypeWithLiteralParametersSolver
            implements TypeConstraintSolver
    {
        private final TypeTemplate formalTemplate;
        private final Type actualType;

        public TypeWithLiteralParametersSolver(TypeTemplate formalTemplate, Type actualType)
        {
            this.formalTemplate = formalTemplate;
            this.actualType = actualType;
        }

        @Override
        public SolverReturnStatus update(BindingsBuilder bindings)
        {
            String base = formalTemplate.baseName();
            List<TemplateParameter> parameters = parametersOf(formalTemplate);
            ImmutableList.Builder<TypeParameter> originalTypeTypeParametersBuilder = ImmutableList.builder();
            for (int i = 0; i < parameters.size(); i++) {
                NumericExpression parameter = ((TemplateParameter.NumericArgument) parameters.get(i)).value();

                switch (parameter) {
                    case NumericExpression.Variable(String variable) -> {
                        if (bindings.containsNumericVariable(variable)) {
                            originalTypeTypeParametersBuilder.add(TypeParameter.numericParameter(bindings.getNumericVariable(variable)));
                        }
                        else {
                            // if an existing value doesn't exist for the given variable name, use the value that comes from the actual type.
                            Optional<Type> type = typeCoercion.coerceTypeBase(actualType, base);
                            if (type.isEmpty()) {
                                return SolverReturnStatus.UNSOLVABLE;
                            }
                            verify(type.get().getBaseName().equals(base),
                                    "Unexpected coerce result for %s and %s: %s",
                                    actualType,
                                    base,
                                    type.get());
                            TypeDescriptor typeSignature = type.get().getTypeDescriptor();
                            originalTypeTypeParametersBuilder.add(TypeParameter.numericParameter(((TypeParameter.Numeric) typeSignature.getParameters().get(i)).value()));
                        }
                    }
                    case NumericExpression.Literal(long value) -> originalTypeTypeParametersBuilder.add(TypeParameter.numericParameter(value));
                    case NumericExpression.Operation _, NumericExpression.Conditional _ -> throw new UnsupportedOperationException("Calculated type parameter is not supported in a unification position: " + parameter);
                }
            }
            Type originalType = typeManager.getType(new TypeDescriptor(base, originalTypeTypeParametersBuilder.build()));
            Optional<Type> commonSuperType = typeCoercion.getCommonSuperType(originalType, actualType);
            if (commonSuperType.isEmpty()) {
                return SolverReturnStatus.UNSOLVABLE;
            }
            TypeDescriptor commonSuperTypeDescriptor = commonSuperType.get().getTypeDescriptor();
            if (!commonSuperTypeDescriptor.getBase().equals(base)) {
                return SolverReturnStatus.UNSOLVABLE;
            }
            SolverReturnStatus result = SolverReturnStatus.UNCHANGED_SATISFIED;
            for (int i = 0; i < parameters.size(); i++) {
                NumericExpression parameter = ((TemplateParameter.NumericArgument) parameters.get(i)).value();
                long commonSuperLongLiteral = ((TypeParameter.Numeric) commonSuperTypeDescriptor.getParameters().get(i)).value();

                switch (parameter) {
                    case NumericExpression.Variable(String name) -> {
                        if (!bindings.containsNumericVariable(name) || bindings.getNumericVariable(name) != commonSuperLongLiteral) {
                            bindings.setNumericVariable(name, commonSuperLongLiteral);
                            result = SolverReturnStatus.CHANGED;
                        }
                    }
                    case NumericExpression.Literal(long value) -> {
                        if (commonSuperLongLiteral != value) {
                            return SolverReturnStatus.UNSOLVABLE;
                        }
                    }
                    case NumericExpression.Operation _, NumericExpression.Conditional _ -> throw new UnsupportedOperationException("Calculated type parameter is not supported in a unification position: " + parameter);
                }
            }
            return result;
        }
    }

    private class FunctionSolver
            implements TypeConstraintSolver
    {
        private final List<TypeTemplate> formalLambdaArgumentTemplates;
        private final TypeTemplate formalLambdaReturnTemplate;
        private final TypeDescriptorProvider typeSignatureProvider;

        public FunctionSolver(
                List<TypeTemplate> formalLambdaArgumentTemplates,
                TypeTemplate formalLambdaReturnTemplate,
                TypeDescriptorProvider typeSignatureProvider)
        {
            this.formalLambdaArgumentTemplates = formalLambdaArgumentTemplates;
            this.formalLambdaReturnTemplate = formalLambdaReturnTemplate;
            this.typeSignatureProvider = typeSignatureProvider;
        }

        @Override
        public SolverReturnStatus update(BindingsBuilder bindings)
        {
            Optional<List<Type>> lambdaArgumentTypes = synthesizeLambdaArgumentTypes(bindings, formalLambdaArgumentTemplates);
            if (lambdaArgumentTypes.isEmpty()) {
                return SolverReturnStatus.UNCHANGED_NOT_SATISFIED;
            }
            TypeDescriptor actualLambdaTypeDescriptor;
            if (!typeSignatureProvider.hasDependency()) {
                actualLambdaTypeDescriptor = typeSignatureProvider.getTypeDescriptor();
                if (!FunctionType.NAME.equals(actualLambdaTypeDescriptor.getBase()) || !getLambdaArgumentTypeDescriptors(actualLambdaTypeDescriptor).equals(toTypeDescriptors(lambdaArgumentTypes.get()))) {
                    return SolverReturnStatus.UNSOLVABLE;
                }
            }
            else {
                actualLambdaTypeDescriptor = typeSignatureProvider.getTypeDescriptor(lambdaArgumentTypes.get());
                if (!FunctionType.NAME.equals(actualLambdaTypeDescriptor.getBase())) {
                    return SolverReturnStatus.UNSOLVABLE;
                }
                verify(getLambdaArgumentTypeDescriptors(actualLambdaTypeDescriptor).equals(toTypeDescriptors(lambdaArgumentTypes.get())));
            }

            Type actualLambdaType = typeManager.getType(actualLambdaTypeDescriptor);
            Type actualReturnType = ((FunctionType) actualLambdaType).getReturnType();

            ImmutableList.Builder<TypeConstraintSolver> constraintsBuilder = ImmutableList.builder();
            // Coercion on function type is not supported yet.
            if (!appendTypeRelationshipConstraintSolver(constraintsBuilder, formalLambdaReturnTemplate, new TypeDescriptorProvider(actualReturnType.getTypeDescriptor()), EXACT)) {
                return SolverReturnStatus.UNSOLVABLE;
            }
            if (!appendConstraintSolvers(constraintsBuilder, formalLambdaReturnTemplate, new TypeDescriptorProvider(actualReturnType.getTypeDescriptor()), allowCoercion)) {
                return SolverReturnStatus.UNSOLVABLE;
            }
            SolverReturnStatusMerger statusMerger = new SolverReturnStatusMerger();
            for (TypeConstraintSolver constraint : constraintsBuilder.build()) {
                statusMerger.add(constraint.update(bindings));
                if (statusMerger.getCurrent() == SolverReturnStatus.UNSOLVABLE) {
                    return SolverReturnStatus.UNSOLVABLE;
                }
            }
            return statusMerger.getCurrent();
        }

        private Optional<List<Type>> synthesizeLambdaArgumentTypes(
                BindingsBuilder bindings,
                List<TypeTemplate> formalLambdaArgumentTemplates)
        {
            ImmutableList.Builder<Type> lambdaArgumentTypesBuilder = ImmutableList.builder();
            for (TypeTemplate lambdaArgument : formalLambdaArgumentTemplates) {
                String base = lambdaArgument.baseName();
                if (typeVariableConstraints.containsKey(base)) {
                    if (!bindings.containsTypeVariable(base)) {
                        return Optional.empty();
                    }
                    Type typeVariable = bindings.getTypeVariable(base);
                    lambdaArgumentTypesBuilder.add(typeVariable);
                }
                else {
                    lambdaArgumentTypesBuilder.add(typeManager.getType(toTypeDescriptor(lambdaArgument)));
                }
            }
            return Optional.of(lambdaArgumentTypesBuilder.build());
        }

        private List<TypeDescriptor> toTypeDescriptors(List<Type> types)
        {
            return types.stream()
                    .map(Type::getTypeDescriptor)
                    .collect(toImmutableList());
        }
    }

    private boolean appendTypeRelationshipConstraintSolver(
            ImmutableList.Builder<TypeConstraintSolver> resultBuilder,
            TypeTemplate formalTemplate,
            TypeDescriptorProvider actualTypeDescriptorProvider,
            RelationshipType relationshipType)
    {
        if (actualTypeDescriptorProvider.hasDependency()) {
            // Fail if the formal type is not function.
            // Otherwise do nothing because FunctionConstraintSolver will handle type relationship constraint directly
            return FunctionType.NAME.equals(formalTemplate.baseName());
        }
        Set<String> typeVariables = typeVariablesOf(formalTemplate);
        Set<String> longVariables = longVariablesOf(formalTemplate);
        resultBuilder.add(new TypeRelationshipConstraintSolver(
                formalTemplate,
                typeVariables,
                longVariables,
                typeManager.getType(actualTypeDescriptorProvider.getTypeDescriptor()),
                relationshipType));
        return true;
    }

    private final class TypeRelationshipConstraintSolver
            implements TypeConstraintSolver
    {
        private final TypeTemplate formalTemplate;
        private final Set<String> typeVariables;
        private final Set<String> longVariables;
        private final Type actualType;
        private final RelationshipType relationshipType;

        public TypeRelationshipConstraintSolver(TypeTemplate formalTemplate, Set<String> typeVariables, Set<String> longVariables, Type actualType, RelationshipType relationshipType)
        {
            this.formalTemplate = requireNonNull(formalTemplate, "formalTemplate is null");
            this.typeVariables = ImmutableSet.copyOf(typeVariables);
            this.longVariables = ImmutableSet.copyOf(longVariables);
            this.actualType = requireNonNull(actualType, "actualType is null");
            this.relationshipType = requireNonNull(relationshipType, "relationshipType is null");
        }

        @Override
        public SolverReturnStatus update(BindingsBuilder bindings)
        {
            for (String variable : typeVariables) {
                if (!bindings.containsTypeVariable(variable)) {
                    return SolverReturnStatus.UNCHANGED_NOT_SATISFIED;
                }
            }
            for (String variable : longVariables) {
                if (!bindings.containsNumericVariable(variable)) {
                    return SolverReturnStatus.UNCHANGED_NOT_SATISFIED;
                }
            }

            VariableBindings variableBindings = bindings.build();
            TypeDescriptor constraintTypeDescriptor = TypeTemplates.bind(formalTemplate, variableBindings.getTypeDescriptors(), variableBindings.getNumericVariables());

            return satisfiesCoercion(relationshipType, actualType, constraintTypeDescriptor) ? SolverReturnStatus.UNCHANGED_SATISFIED : SolverReturnStatus.UNCHANGED_NOT_SATISFIED;
        }
    }

    public enum RelationshipType
    {
        EXACT, IMPLICIT_COERCION, EXPLICIT_COERCION_TO, EXPLICIT_COERCION_FROM
    }

    /// A function signature with all of its variables bound: the ground argument and return types of one
    /// applicable binding. The types stay unresolved signatures rather than [Type]s because function
    /// resolution keeps candidates whose bound types are illegal (e.g. `char(2147483647)`) so they can be
    /// reported in error messages.
    record GroundSignature(TypeDescriptor returnType, List<TypeDescriptor> argumentTypes)
    {
        public GroundSignature
        {
            requireNonNull(returnType, "returnType is null");
            argumentTypes = ImmutableList.copyOf(argumentTypes);
        }

        /// The rendering participates in function resolution: ambiguous-candidate selection orders
        /// by it, and error messages include it.
        @Override
        public String toString()
        {
            return "%s:%s".formatted(
                    argumentTypes.stream()
                            .map(TypeDescriptor::toString)
                            .collect(joining(",", "(", ")")),
                    returnType);
        }
    }
}
