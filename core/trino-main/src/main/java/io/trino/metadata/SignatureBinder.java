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
import io.trino.Session;
import io.trino.spi.TrinoException;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.LongVariableConstraint;
import io.trino.spi.function.Signature;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.type.NamedTypeSignature;
import io.trino.spi.type.ParameterKind;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.type.FunctionType;
import io.trino.type.JsonType;
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
import static io.trino.metadata.SignatureBinder.RelationshipType.EXACT;
import static io.trino.metadata.SignatureBinder.RelationshipType.EXPLICIT_COERCION_FROM;
import static io.trino.metadata.SignatureBinder.RelationshipType.EXPLICIT_COERCION_TO;
import static io.trino.metadata.SignatureBinder.RelationshipType.IMPLICIT_COERCION;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.type.TypeCalculation.calculateLiteralValue;
import static io.trino.type.TypeCoercion.isCovariantTypeBase;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;

/**
 * Determines whether, and how, a callsite matches a generic function signature.
 * Which is equivalent to finding assignments for the variables in the generic signature,
 * such that all of the function's declared parameters are super types of the corresponding
 * arguments, and also satisfy the declared constraints (such as a given type parameter must
 * bind to an orderable type)
 * <p>
 * This implementation has made assumptions. When any of the assumptions is not satisfied, it will fail loudly.
 * <p><ul>
 * <li>A type cannot have both type parameter and literal parameter.
 * <li>A literal parameter cannot be used across types. see {@link #checkNoLiteralVariableUsageAcrossTypes(TypeSignature, Map)}.
 * </ul><p>
 * Here are some known implementation limitations:
 * <p><ul>
 * <li>Binding signature {@code (decimal(x,2))boolean} with arguments {@code decimal(1,0)} fails.
 * It should produce {@code decimal(3,1)}.
 * </ul>
 */
public class SignatureBinder
{
    // 4 is chosen arbitrarily here. This limit is set to avoid having infinite loops in iterative solving.
    private static final int SOLVE_ITERATION_LIMIT = 4;

    private final Session session;
    private final Metadata metadata;
    private final TypeManager typeManager;
    private final TypeCoercion typeCoercion;
    private final Signature declaredSignature;
    private final boolean allowCoercion;
    private final Map<String, TypeVariableConstraint> typeVariableConstraints;

    SignatureBinder(Session session, Metadata metadata, TypeManager typeManager, Signature declaredSignature, boolean allowCoercion)
    {
        checkNoLiteralVariableUsageAcrossTypes(declaredSignature);
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.typeCoercion = new TypeCoercion(typeManager::getType);
        this.declaredSignature = requireNonNull(declaredSignature, "declaredSignature is null");
        this.allowCoercion = allowCoercion;

        this.typeVariableConstraints = declaredSignature.getTypeVariableConstraints().stream()
                .collect(toImmutableSortedMap(CASE_INSENSITIVE_ORDER, TypeVariableConstraint::getName, identity()));
    }

    public Optional<Signature> bind(List<? extends TypeSignatureProvider> actualArgumentTypes)
    {
        Optional<TypeVariables> boundVariables = bindVariables(actualArgumentTypes);
        if (boundVariables.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(applyBoundVariables(declaredSignature, boundVariables.get(), actualArgumentTypes.size()));
    }

    public Optional<Signature> bind(List<? extends TypeSignatureProvider> actualArgumentTypes, Type actualReturnType)
    {
        Optional<TypeVariables> boundVariables = bindVariables(actualArgumentTypes, actualReturnType.getTypeSignature());
        if (boundVariables.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(applyBoundVariables(declaredSignature, boundVariables.get(), actualArgumentTypes.size()));
    }

    public boolean canBind(List<? extends TypeSignatureProvider> actualArgumentTypes)
    {
        return bindVariables(actualArgumentTypes).isPresent();
    }

    @VisibleForTesting
    Optional<TypeVariables> bindVariables(List<? extends TypeSignatureProvider> actualArgumentTypes)
    {
        ImmutableList.Builder<TypeConstraintSolver> constraintSolvers = ImmutableList.builder();
        if (!appendConstraintSolversForArguments(constraintSolvers, actualArgumentTypes)) {
            return Optional.empty();
        }

        return iterativeSolve(constraintSolvers.build());
    }

    public boolean canBind(List<? extends TypeSignatureProvider> actualArgumentTypes, TypeSignature actualReturnType)
    {
        return bindVariables(actualArgumentTypes, actualReturnType).isPresent();
    }

    @VisibleForTesting
    Optional<TypeVariables> bindVariables(List<? extends TypeSignatureProvider> actualArgumentTypes, TypeSignature actualReturnType)
    {
        ImmutableList.Builder<TypeConstraintSolver> constraintSolvers = ImmutableList.builder();
        if (!appendConstraintSolversForReturnValue(constraintSolvers, new TypeSignatureProvider(actualReturnType))) {
            return Optional.empty();
        }
        if (!appendConstraintSolversForArguments(constraintSolvers, actualArgumentTypes)) {
            return Optional.empty();
        }

        return iterativeSolve(constraintSolvers.build());
    }

    public static Signature applyBoundVariables(Signature signature, FunctionBinding functionBinding, int arity)
    {
        return applyBoundVariables(signature, new FunctionTypeVariables(functionBinding), arity);
    }

    private static Signature applyBoundVariables(Signature signature, TypeVariables typeVariables, int arity)
    {
        List<TypeSignature> argumentSignatures;
        if (signature.isVariableArity()) {
            argumentSignatures = expandVarargFormalTypeSignature(signature.getArgumentTypes(), arity);
        }
        else {
            checkArgument(signature.getArgumentTypes().size() == arity);
            argumentSignatures = signature.getArgumentTypes();
        }
        List<TypeSignature> boundArgumentSignatures = applyBoundVariables(argumentSignatures, typeVariables);
        TypeSignature boundReturnTypeSignature = applyBoundVariables(signature.getReturnType(), typeVariables);

        return Signature.builder()
                .name(signature.getName())
                .returnType(boundReturnTypeSignature)
                .argumentTypes(boundArgumentSignatures)
                .build();
    }

    public static List<TypeSignature> applyBoundVariables(List<TypeSignature> typeSignatures, FunctionBinding functionBinding)
    {
        return applyBoundVariables(typeSignatures, new FunctionTypeVariables(functionBinding));
    }

    private static List<TypeSignature> applyBoundVariables(List<TypeSignature> typeSignatures, TypeVariables typeVariables)
    {
        ImmutableList.Builder<TypeSignature> builder = ImmutableList.builder();
        for (TypeSignature typeSignature : typeSignatures) {
            builder.add(applyBoundVariables(typeSignature, typeVariables));
        }
        return builder.build();
    }

    public static TypeSignature applyBoundVariables(TypeSignature typeSignature, FunctionBinding functionBinding)
    {
        return applyBoundVariables(typeSignature, new FunctionTypeVariables(functionBinding));
    }

    static TypeSignature applyBoundVariables(TypeSignature typeSignature, TypeVariables typeVariables)
    {
        String baseType = typeSignature.getBase();
        if (typeVariables.containsTypeVariable(baseType)) {
            checkState(typeSignature.getParameters().isEmpty(), "Type parameters cannot have parameters");
            return typeVariables.getTypeVariable(baseType).getTypeSignature();
        }

        List<TypeSignatureParameter> parameters = typeSignature.getParameters().stream()
                .map(typeSignatureParameter -> applyBoundVariables(typeSignatureParameter, typeVariables))
                .collect(toList());

        return new TypeSignature(baseType, parameters);
    }

    public static FunctionBinding bindFunction(FunctionId functionId, Signature declaredSignature, BoundSignature boundSignature)
    {
        requireNonNull(declaredSignature, "declaredSignature is null");
        requireNonNull(boundSignature, "boundSignature is null");

        checkNoLiteralVariableUsageAcrossTypes(declaredSignature);

        Map<String, TypeVariableConstraint> typeVariableConstraints = declaredSignature.getTypeVariableConstraints().stream()
                .collect(toImmutableSortedMap(CASE_INSENSITIVE_ORDER, TypeVariableConstraint::getName, identity()));

        boolean variableArity = declaredSignature.isVariableArity();
        List<TypeSignature> formalTypeSignatures = declaredSignature.getArgumentTypes();
        if (variableArity) {
            verifyBoundSignature(boundSignature.getArgumentTypes().size() >= formalTypeSignatures.size() - 1, boundSignature, declaredSignature);
            formalTypeSignatures = expandVarargFormalTypeSignature(formalTypeSignatures, boundSignature.getArgumentTypes().size());
        }

        verifyBoundSignature(formalTypeSignatures.size() == boundSignature.getArgumentTypes().size(), boundSignature, declaredSignature);

        BoundVariables bindings = new BoundVariables();
        for (int i = 0; i < formalTypeSignatures.size(); i++) {
            extractBoundVariables(boundSignature, declaredSignature, typeVariableConstraints, bindings, boundSignature.getArgumentTypes().get(i), formalTypeSignatures.get(i));
        }
        extractBoundVariables(boundSignature, declaredSignature, typeVariableConstraints, bindings, boundSignature.getReturnType(), declaredSignature.getReturnType());

        verifyBoundSignature(bindings.getTypeVariables().keySet().equals(typeVariableConstraints.keySet()), boundSignature, declaredSignature);

        return new FunctionBinding(
                functionId,
                boundSignature,
                bindings.getTypeVariables(),
                bindings.getLongVariables());
    }

    private static void extractBoundVariables(
            BoundSignature boundSignature,
            Signature declaredSignature,
            Map<String, TypeVariableConstraint> typeVariableConstraints,
            BoundVariables bindings,
            Type actualType,
            TypeSignature declaredTypeSignature)
    {
        // type without nested type parameters
        if (declaredTypeSignature.getParameters().isEmpty()) {
            String typeVariable = declaredTypeSignature.getBase();
            TypeVariableConstraint typeVariableConstraint = typeVariableConstraints.get(typeVariable);
            if (typeVariableConstraint == null) {
                return;
            }

            if (bindings.containsTypeVariable(typeVariable)) {
                Type existingTypeBinding = bindings.getTypeVariable(typeVariable);
                verifyBoundSignature(actualType.equals(existingTypeBinding), boundSignature, declaredSignature);
            }
            else {
                bindings.setTypeVariable(typeVariable, actualType);
            }
            return;
        }

        verifyBoundSignature(declaredTypeSignature.getBase().equalsIgnoreCase(actualType.getTypeSignature().getBase()), boundSignature, declaredSignature);

        // type with nested literal parameters
        if (isTypeWithLiteralParameters(declaredTypeSignature)) {
            for (int i = 0; i < declaredTypeSignature.getParameters().size(); i++) {
                TypeSignatureParameter typeSignatureParameter = declaredTypeSignature.getParameters().get(i);
                Long actualLongBinding = actualType.getTypeSignature().getParameters().get(i).getLongLiteral();
                if (typeSignatureParameter.getKind() == ParameterKind.VARIABLE) {
                    if (bindings.containsLongVariable(typeSignatureParameter.getVariable())) {
                        Long existingLongBinding = bindings.getLongVariable(typeSignatureParameter.getVariable());
                        verifyBoundSignature(actualLongBinding.equals(existingLongBinding), boundSignature, declaredSignature);
                    }
                    else {
                        bindings.setLongVariable(typeSignatureParameter.getVariable(), actualLongBinding);
                    }
                }
                else {
                    verify(typeSignatureParameter.getKind() == ParameterKind.LONG);
                    verifyBoundSignature(actualLongBinding.equals(typeSignatureParameter.getLongLiteral()), boundSignature, declaredSignature);
                }
            }
            return;
        }

        // type with nested type parameters
        List<Type> actualTypeParameters = actualType.getTypeParameters();

        // unknown types are assumed to have unknown nested types
        if (UNKNOWN.equals(actualType)) {
            actualTypeParameters = Collections.nCopies(declaredTypeSignature.getParameters().size(), UNKNOWN);
        }

        verifyBoundSignature(declaredTypeSignature.getParameters().size() == actualTypeParameters.size(), boundSignature, declaredSignature);
        for (int i = 0; i < declaredTypeSignature.getParameters().size(); i++) {
            TypeSignatureParameter typeSignatureParameter = declaredTypeSignature.getParameters().get(i);
            TypeSignature typeSignature = typeSignatureParameter.getTypeSignatureOrNamedTypeSignature()
                    .orElseThrow(() -> new UnsupportedOperationException("Types with both type parameters and literal parameters at the same time are not supported"));
            Type actualTypeParameter = actualTypeParameters.get(i);
            extractBoundVariables(boundSignature, declaredSignature, typeVariableConstraints, bindings, actualTypeParameter, typeSignature);
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
        Map<String, TypeSignature> existingUsages = new HashMap<>();
        for (TypeSignature parameter : declaredSignature.getArgumentTypes()) {
            checkNoLiteralVariableUsageAcrossTypes(parameter, existingUsages);
        }
    }

    private static void checkNoLiteralVariableUsageAcrossTypes(TypeSignature typeSignature, Map<String, TypeSignature> existingUsages)
    {
        List<TypeSignatureParameter> variables = typeSignature.getParameters().stream()
                .filter(TypeSignatureParameter::isVariable)
                .collect(toList());
        for (TypeSignatureParameter variable : variables) {
            TypeSignature existing = existingUsages.get(variable.getVariable());
            if (existing != null && !existing.equals(typeSignature)) {
                throw new UnsupportedOperationException("Literal parameters may not be shared across different types");
            }
            existingUsages.put(variable.getVariable(), typeSignature);
        }

        for (TypeSignatureParameter parameter : typeSignature.getParameters()) {
            if (parameter.isLongLiteral() || parameter.isVariable()) {
                continue;
            }
            checkNoLiteralVariableUsageAcrossTypes(parameter.getTypeSignatureOrNamedTypeSignature().get(), existingUsages);
        }
    }

    private boolean appendConstraintSolversForReturnValue(ImmutableList.Builder<TypeConstraintSolver> resultBuilder, TypeSignatureProvider actualReturnType)
    {
        TypeSignature formalReturnTypeSignature = declaredSignature.getReturnType();
        return appendTypeRelationshipConstraintSolver(resultBuilder, formalReturnTypeSignature, actualReturnType, EXACT)
                && appendConstraintSolvers(resultBuilder, formalReturnTypeSignature, actualReturnType, false);
    }

    private boolean appendConstraintSolversForArguments(ImmutableList.Builder<TypeConstraintSolver> resultBuilder, List<? extends TypeSignatureProvider> actualTypes)
    {
        boolean variableArity = declaredSignature.isVariableArity();
        List<TypeSignature> formalTypeSignatures = declaredSignature.getArgumentTypes();
        if (variableArity) {
            if (actualTypes.size() < formalTypeSignatures.size() - 1) {
                return false;
            }
            formalTypeSignatures = expandVarargFormalTypeSignature(formalTypeSignatures, actualTypes.size());
        }

        if (formalTypeSignatures.size() != actualTypes.size()) {
            return false;
        }

        for (int i = 0; i < formalTypeSignatures.size(); i++) {
            if (!appendTypeRelationshipConstraintSolver(resultBuilder, formalTypeSignatures.get(i), actualTypes.get(i), allowCoercion ? IMPLICIT_COERCION : EXACT)) {
                return false;
            }
        }

        return appendConstraintSolvers(resultBuilder, formalTypeSignatures, actualTypes, allowCoercion);
    }

    private boolean appendConstraintSolvers(
            ImmutableList.Builder<TypeConstraintSolver> resultBuilder,
            List<? extends TypeSignature> formalTypeSignatures,
            List<? extends TypeSignatureProvider> actualTypes,
            boolean allowCoercion)
    {
        if (formalTypeSignatures.size() != actualTypes.size()) {
            return false;
        }
        for (int i = 0; i < formalTypeSignatures.size(); i++) {
            if (!appendConstraintSolvers(resultBuilder, formalTypeSignatures.get(i), actualTypes.get(i), allowCoercion)) {
                return false;
            }
        }
        return true;
    }

    private boolean appendConstraintSolvers(
            ImmutableList.Builder<TypeConstraintSolver> resultBuilder,
            TypeSignature formalTypeSignature,
            TypeSignatureProvider actualTypeSignatureProvider,
            boolean allowCoercion)
    {
        // formalTypeSignature can be categorized into one of the 5 cases below:
        // * function type
        // * type without type parameter
        // * type parameter of type/named_type kind
        // * type with type parameter of literal/variable kind
        // * type with type parameter of type/named_type kind (except function type)

        if (FunctionType.NAME.equalsIgnoreCase(formalTypeSignature.getBase())) {
            List<TypeSignature> formalTypeParameterTypeSignatures = formalTypeSignature.getTypeParametersAsTypeSignatures();
            resultBuilder.add(new FunctionSolver(
                    getLambdaArgumentTypeSignatures(formalTypeSignature),
                    formalTypeParameterTypeSignatures.get(formalTypeParameterTypeSignatures.size() - 1),
                    actualTypeSignatureProvider));
            return true;
        }

        if (actualTypeSignatureProvider.hasDependency()) {
            return false;
        }

        if (formalTypeSignature.getParameters().isEmpty()) {
            TypeVariableConstraint typeVariableConstraint = typeVariableConstraints.get(formalTypeSignature.getBase());
            if (typeVariableConstraint == null) {
                return true;
            }
            Type actualType = typeManager.getType(actualTypeSignatureProvider.getTypeSignature());
            for (TypeSignature castToSignature : typeVariableConstraint.getCastableTo()) {
                appendTypeRelationshipConstraintSolver(resultBuilder, castToSignature, actualTypeSignatureProvider, EXPLICIT_COERCION_TO);
            }
            for (TypeSignature castFromSignature : typeVariableConstraint.getCastableFrom()) {
                appendTypeRelationshipConstraintSolver(resultBuilder, castFromSignature, actualTypeSignatureProvider, EXPLICIT_COERCION_FROM);
            }
            if (typeVariableConstraint.getVariadicBound().isPresent() && !typeVariableConstraint.getVariadicBound().get().equalsIgnoreCase(actualType.getTypeSignature().getBase())) {
                return actualType == UNKNOWN;
            }
            resultBuilder.add(new TypeParameterSolver(
                    formalTypeSignature.getBase(),
                    actualType,
                    typeVariableConstraint.isComparableRequired(),
                    typeVariableConstraint.isOrderableRequired(),
                    typeVariableConstraint.getVariadicBound()));
            return true;
        }

        Type actualType = typeManager.getType(actualTypeSignatureProvider.getTypeSignature());
        if (isTypeWithLiteralParameters(formalTypeSignature)) {
            resultBuilder.add(new TypeWithLiteralParametersSolver(formalTypeSignature, actualType));
            return true;
        }

        List<TypeSignatureProvider> actualTypeParametersTypeSignatureProvider;
        if (UNKNOWN.equals(actualType)) {
            actualTypeParametersTypeSignatureProvider = Collections.nCopies(formalTypeSignature.getParameters().size(), new TypeSignatureProvider(UNKNOWN.getTypeSignature()));
        }
        else {
            actualTypeParametersTypeSignatureProvider = fromTypes(actualType.getTypeParameters());
        }

        ImmutableList.Builder<TypeSignature> formalTypeParameterTypeSignatures = ImmutableList.builder();
        for (TypeSignatureParameter formalTypeParameter : formalTypeSignature.getParameters()) {
            TypeSignature typeSignature = formalTypeParameter.getTypeSignatureOrNamedTypeSignature()
                    .orElseThrow(() -> new UnsupportedOperationException("Types with both type parameters and literal parameters at the same time are not supported"));
            formalTypeParameterTypeSignatures.add(typeSignature);
        }

        return appendConstraintSolvers(
                resultBuilder,
                formalTypeParameterTypeSignatures.build(),
                actualTypeParametersTypeSignatureProvider,
                allowCoercion && isCovariantTypeBase(formalTypeSignature.getBase()));
    }

    private Set<String> typeVariablesOf(TypeSignature typeSignature)
    {
        if (typeVariableConstraints.containsKey(typeSignature.getBase())) {
            return ImmutableSet.of(typeSignature.getBase());
        }
        ImmutableSet.Builder<String> variables = ImmutableSet.builder();
        for (TypeSignatureParameter parameter : typeSignature.getParameters()) {
            switch (parameter.getKind()) {
                case TYPE:
                    variables.addAll(typeVariablesOf(parameter.getTypeSignature()));
                    break;
                case NAMED_TYPE:
                    variables.addAll(typeVariablesOf(parameter.getNamedTypeSignature().getTypeSignature()));
                    break;
                case LONG:
                    break;
                case VARIABLE:
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown parameter kind: " + parameter.getKind());
            }
        }

        return variables.build();
    }

    private static Set<String> longVariablesOf(TypeSignature typeSignature)
    {
        ImmutableSet.Builder<String> variables = ImmutableSet.builder();
        for (TypeSignatureParameter parameter : typeSignature.getParameters()) {
            switch (parameter.getKind()) {
                case TYPE:
                    variables.addAll(longVariablesOf(parameter.getTypeSignature()));
                    break;
                case NAMED_TYPE:
                    variables.addAll(longVariablesOf(parameter.getNamedTypeSignature().getTypeSignature()));
                    break;
                case LONG:
                    break;
                case VARIABLE:
                    variables.add(parameter.getVariable());
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown parameter kind: " + parameter.getKind());
            }
        }

        return variables.build();
    }

    private static boolean isTypeWithLiteralParameters(TypeSignature typeSignature)
    {
        return typeSignature.getParameters().stream()
                .map(TypeSignatureParameter::getKind)
                .allMatch(kind -> kind == ParameterKind.LONG || kind == ParameterKind.VARIABLE);
    }

    private Optional<TypeVariables> iterativeSolve(List<TypeConstraintSolver> constraints)
    {
        BoundVariables boundVariables = new BoundVariables();
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
                case UNCHANGED_SATISFIED:
                    break;
                case UNCHANGED_NOT_SATISFIED:
                    return Optional.empty();
                case CHANGED:
                    continue;
                case UNSOLVABLE:
                    throw new VerifyException();
                default:
                    throw new UnsupportedOperationException("unknown status");
            }
            break;
        }

        calculateVariableValuesForLongConstraints(boundVariables);

        if (!allTypeVariablesBound(boundVariables)) {
            return Optional.empty();
        }
        return Optional.of(boundVariables);
    }

    private void calculateVariableValuesForLongConstraints(BoundVariables variableBinder)
    {
        for (LongVariableConstraint longVariableConstraint : declaredSignature.getLongVariableConstraints()) {
            String calculation = longVariableConstraint.getExpression();
            String variableName = longVariableConstraint.getName();
            Long calculatedValue = calculateLiteralValue(calculation, variableBinder.getLongVariables());
            if (variableBinder.containsLongVariable(variableName)) {
                Long currentValue = variableBinder.getLongVariable(variableName);
                checkState(Objects.equals(currentValue, calculatedValue),
                        "variable '%s' is already set to %s when trying to set %s", variableName, currentValue, calculatedValue);
            }
            variableBinder.setLongVariable(variableName, calculatedValue);
        }
    }

    private boolean allTypeVariablesBound(TypeVariables typeVariables)
    {
        return typeVariableConstraints.keySet().stream()
                .allMatch(typeVariables::containsTypeVariable);
    }

    private static TypeSignatureParameter applyBoundVariables(TypeSignatureParameter parameter, TypeVariables typeVariables)
    {
        ParameterKind parameterKind = parameter.getKind();
        switch (parameterKind) {
            case TYPE: {
                TypeSignature typeSignature = parameter.getTypeSignature();
                return TypeSignatureParameter.typeParameter(applyBoundVariables(typeSignature, typeVariables));
            }
            case NAMED_TYPE: {
                NamedTypeSignature namedTypeSignature = parameter.getNamedTypeSignature();
                TypeSignature typeSignature = namedTypeSignature.getTypeSignature();
                return TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(
                        namedTypeSignature.getFieldName(),
                        applyBoundVariables(typeSignature, typeVariables)));
            }
            case VARIABLE: {
                String variableName = parameter.getVariable();
                checkState(
                        typeVariables.containsLongVariable(variableName),
                        "Variable is not bound: %s", variableName);
                Long variableValue = typeVariables.getLongVariable(variableName);
                return TypeSignatureParameter.numericParameter(variableValue);
            }
            case LONG: {
                return parameter;
            }
        }
        throw new IllegalStateException("Unknown parameter kind: " + parameter.getKind());
    }

    private static List<TypeSignature> expandVarargFormalTypeSignature(List<TypeSignature> formalTypeSignatures, int actualArity)
    {
        int variableArityArgumentsCount = actualArity - formalTypeSignatures.size() + 1;
        if (variableArityArgumentsCount == 0) {
            return formalTypeSignatures.subList(0, formalTypeSignatures.size() - 1);
        }
        if (variableArityArgumentsCount == 1) {
            return formalTypeSignatures;
        }
        checkArgument(variableArityArgumentsCount > 1 && !formalTypeSignatures.isEmpty());

        ImmutableList.Builder<TypeSignature> builder = ImmutableList.builder();
        builder.addAll(formalTypeSignatures);
        TypeSignature lastTypeSignature = formalTypeSignatures.get(formalTypeSignatures.size() - 1);
        for (int i = 1; i < variableArityArgumentsCount; i++) {
            builder.add(lastTypeSignature);
        }
        return builder.build();
    }

    private boolean satisfiesCoercion(RelationshipType relationshipType, Type actualType, TypeSignature constraintTypeSignature)
    {
        switch (relationshipType) {
            case EXACT:
                return actualType.getTypeSignature().equals(constraintTypeSignature);
            case IMPLICIT_COERCION:
                return typeCoercion.canCoerce(actualType, typeManager.getType(constraintTypeSignature));
            case EXPLICIT_COERCION_TO:
                return canCast(actualType, typeManager.getType(constraintTypeSignature));
            case EXPLICIT_COERCION_FROM:
                return canCast(typeManager.getType(constraintTypeSignature), actualType);
        }
        throw new IllegalArgumentException("Unsupported relationshipType " + relationshipType);
    }

    private boolean canCast(Type fromType, Type toType)
    {
        if (toType instanceof UnknownType) {
            return true;
        }
        if (fromType instanceof RowType) {
            if (toType instanceof RowType) {
                List<Type> fromTypeParameters = fromType.getTypeParameters();
                List<Type> toTypeParameters = toType.getTypeParameters();
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
            if (toType instanceof JsonType) {
                return fromType.getTypeParameters().stream()
                        .allMatch(fromTypeParameter -> canCast(fromTypeParameter, toType));
            }
            return false;
        }
        if (fromType instanceof JsonType) {
            if (toType instanceof RowType) {
                return toType.getTypeParameters().stream()
                        .allMatch(toTypeParameter -> canCast(fromType, toTypeParameter));
            }
        }
        try {
            metadata.getCoercion(session, fromType, toType);
            return true;
        }
        catch (TrinoException e) {
            return false;
        }
    }

    private static List<TypeSignature> getLambdaArgumentTypeSignatures(TypeSignature lambdaTypeSignature)
    {
        List<TypeSignature> typeParameters = lambdaTypeSignature.getTypeParametersAsTypeSignatures();
        return typeParameters.subList(0, typeParameters.size() - 1);
    }

    private interface TypeConstraintSolver
    {
        SolverReturnStatus update(BoundVariables bindings);
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
                case UNCHANGED_SATISFIED:
                    break;
                case UNCHANGED_NOT_SATISFIED:
                    if (current == SolverReturnStatus.UNCHANGED_SATISFIED) {
                        current = SolverReturnStatus.UNCHANGED_NOT_SATISFIED;
                    }
                    break;
                case CHANGED:
                    if (current == SolverReturnStatus.UNCHANGED_SATISFIED || current == SolverReturnStatus.UNCHANGED_NOT_SATISFIED) {
                        current = SolverReturnStatus.CHANGED;
                    }
                    break;
                case UNSOLVABLE:
                    current = SolverReturnStatus.UNSOLVABLE;
                    break;
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
        private final Optional<String> requiredBaseName;

        public TypeParameterSolver(String typeParameter, Type actualType, boolean comparableRequired, boolean orderableRequired, Optional<String> requiredBaseName)
        {
            this.typeParameter = typeParameter;
            this.actualType = actualType;
            this.comparableRequired = comparableRequired;
            this.orderableRequired = orderableRequired;
            this.requiredBaseName = requiredBaseName;
        }

        @Override
        public SolverReturnStatus update(BoundVariables bindings)
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
            if (requiredBaseName.isPresent() && !UNKNOWN.equals(type) && !requiredBaseName.get().equalsIgnoreCase(type.getBaseName())) {
                // TODO: the case below should be properly handled:
                // * `type` does not have the `requiredBaseName` but can be coerced to some type that has the `requiredBaseName`.
                return false;
            }
            return true;
        }
    }

    private class TypeWithLiteralParametersSolver
            implements TypeConstraintSolver
    {
        private final TypeSignature formalTypeSignature;
        private final Type actualType;

        public TypeWithLiteralParametersSolver(TypeSignature formalTypeSignature, Type actualType)
        {
            this.formalTypeSignature = formalTypeSignature;
            this.actualType = actualType;
        }

        @Override
        public SolverReturnStatus update(BoundVariables bindings)
        {
            ImmutableList.Builder<TypeSignatureParameter> originalTypeTypeParametersBuilder = ImmutableList.builder();
            List<TypeSignatureParameter> parameters = formalTypeSignature.getParameters();
            for (int i = 0; i < parameters.size(); i++) {
                TypeSignatureParameter typeSignatureParameter = parameters.get(i);
                if (typeSignatureParameter.getKind() == ParameterKind.VARIABLE) {
                    if (bindings.containsLongVariable(typeSignatureParameter.getVariable())) {
                        originalTypeTypeParametersBuilder.add(TypeSignatureParameter.numericParameter(bindings.getLongVariable(typeSignatureParameter.getVariable())));
                    }
                    else {
                        // if an existing value doesn't exist for the given variable name, use the value that comes from the actual type.
                        Optional<Type> type = typeCoercion.coerceTypeBase(actualType, formalTypeSignature.getBase());
                        if (type.isEmpty()) {
                            return SolverReturnStatus.UNSOLVABLE;
                        }
                        verify(
                                type.get().getBaseName().equals(formalTypeSignature.getBase()),
                                "Unexpected coerce result for %s and %s: %s",
                                actualType,
                                formalTypeSignature.getBase(),
                                type.get());
                        TypeSignature typeSignature = type.get().getTypeSignature();
                        originalTypeTypeParametersBuilder.add(TypeSignatureParameter.numericParameter(typeSignature.getParameters().get(i).getLongLiteral()));
                    }
                }
                else {
                    verify(typeSignatureParameter.getKind() == ParameterKind.LONG);
                    originalTypeTypeParametersBuilder.add(typeSignatureParameter);
                }
            }
            Type originalType = typeManager.getType(new TypeSignature(formalTypeSignature.getBase(), originalTypeTypeParametersBuilder.build()));
            Optional<Type> commonSuperType = typeCoercion.getCommonSuperType(originalType, actualType);
            if (commonSuperType.isEmpty()) {
                return SolverReturnStatus.UNSOLVABLE;
            }
            TypeSignature commonSuperTypeSignature = commonSuperType.get().getTypeSignature();
            if (!commonSuperTypeSignature.getBase().equals(formalTypeSignature.getBase())) {
                return SolverReturnStatus.UNSOLVABLE;
            }
            SolverReturnStatus result = SolverReturnStatus.UNCHANGED_SATISFIED;
            for (int i = 0; i < parameters.size(); i++) {
                TypeSignatureParameter typeSignatureParameter = parameters.get(i);
                long commonSuperLongLiteral = commonSuperTypeSignature.getParameters().get(i).getLongLiteral();
                if (typeSignatureParameter.getKind() == ParameterKind.VARIABLE) {
                    String variableName = typeSignatureParameter.getVariable();
                    if (!bindings.containsLongVariable(variableName) || !bindings.getLongVariable(variableName).equals(commonSuperLongLiteral)) {
                        bindings.setLongVariable(variableName, commonSuperLongLiteral);
                        result = SolverReturnStatus.CHANGED;
                    }
                }
                else {
                    verify(typeSignatureParameter.getKind() == ParameterKind.LONG);
                    if (commonSuperLongLiteral != typeSignatureParameter.getLongLiteral()) {
                        return SolverReturnStatus.UNSOLVABLE;
                    }
                }
            }
            return result;
        }
    }

    private class FunctionSolver
            implements TypeConstraintSolver
    {
        private final List<TypeSignature> formalLambdaArgumentsTypeSignature;
        private final TypeSignature formalLambdaReturnTypeSignature;
        private final TypeSignatureProvider typeSignatureProvider;

        public FunctionSolver(
                List<TypeSignature> formalLambdaArgumentsTypeSignature,
                TypeSignature formalLambdaReturnTypeSignature,
                TypeSignatureProvider typeSignatureProvider)
        {
            this.formalLambdaArgumentsTypeSignature = formalLambdaArgumentsTypeSignature;
            this.formalLambdaReturnTypeSignature = formalLambdaReturnTypeSignature;
            this.typeSignatureProvider = typeSignatureProvider;
        }

        @Override
        public SolverReturnStatus update(BoundVariables bindings)
        {
            Optional<List<Type>> lambdaArgumentTypes = synthesizeLambdaArgumentTypes(bindings, formalLambdaArgumentsTypeSignature);
            if (lambdaArgumentTypes.isEmpty()) {
                return SolverReturnStatus.UNCHANGED_NOT_SATISFIED;
            }
            TypeSignature actualLambdaTypeSignature;
            if (!typeSignatureProvider.hasDependency()) {
                actualLambdaTypeSignature = typeSignatureProvider.getTypeSignature();
                if (!FunctionType.NAME.equals(actualLambdaTypeSignature.getBase()) || !getLambdaArgumentTypeSignatures(actualLambdaTypeSignature).equals(toTypeSignatures(lambdaArgumentTypes.get()))) {
                    return SolverReturnStatus.UNSOLVABLE;
                }
            }
            else {
                actualLambdaTypeSignature = typeSignatureProvider.getTypeSignature(lambdaArgumentTypes.get());
                if (!FunctionType.NAME.equals(actualLambdaTypeSignature.getBase())) {
                    return SolverReturnStatus.UNSOLVABLE;
                }
                verify(getLambdaArgumentTypeSignatures(actualLambdaTypeSignature).equals(toTypeSignatures(lambdaArgumentTypes.get())));
            }

            Type actualLambdaType = typeManager.getType(actualLambdaTypeSignature);
            Type actualReturnType = ((FunctionType) actualLambdaType).getReturnType();

            ImmutableList.Builder<TypeConstraintSolver> constraintsBuilder = ImmutableList.builder();
            // Coercion on function type is not supported yet.
            if (!appendTypeRelationshipConstraintSolver(constraintsBuilder, formalLambdaReturnTypeSignature, new TypeSignatureProvider(actualReturnType.getTypeSignature()), EXACT)) {
                return SolverReturnStatus.UNSOLVABLE;
            }
            if (!appendConstraintSolvers(constraintsBuilder, formalLambdaReturnTypeSignature, new TypeSignatureProvider(actualReturnType.getTypeSignature()), allowCoercion)) {
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
                BoundVariables bindings,
                List<TypeSignature> formalLambdaArgumentTypeSignatures)
        {
            ImmutableList.Builder<Type> lambdaArgumentTypesBuilder = ImmutableList.builder();
            for (TypeSignature lambdaArgument : formalLambdaArgumentTypeSignatures) {
                if (typeVariableConstraints.containsKey(lambdaArgument.getBase())) {
                    if (!bindings.containsTypeVariable(lambdaArgument.getBase())) {
                        return Optional.empty();
                    }
                    Type typeVariable = bindings.getTypeVariable(lambdaArgument.getBase());
                    lambdaArgumentTypesBuilder.add(typeVariable);
                }
                else {
                    lambdaArgumentTypesBuilder.add(typeManager.getType(lambdaArgument));
                }
            }
            return Optional.of(lambdaArgumentTypesBuilder.build());
        }

        private List<TypeSignature> toTypeSignatures(List<Type> types)
        {
            return types.stream()
                    .map(Type::getTypeSignature)
                    .collect(toImmutableList());
        }
    }

    private boolean appendTypeRelationshipConstraintSolver(
            ImmutableList.Builder<TypeConstraintSolver> resultBuilder,
            TypeSignature formalTypeSignature,
            TypeSignatureProvider actualTypeSignatureProvider,
            RelationshipType relationshipType)
    {
        if (actualTypeSignatureProvider.hasDependency()) {
            // Fail if the formal type is not function.
            // Otherwise do nothing because FunctionConstraintSolver will handle type relationship constraint directly
            return FunctionType.NAME.equals(formalTypeSignature.getBase());
        }
        Set<String> typeVariables = typeVariablesOf(formalTypeSignature);
        Set<String> longVariables = longVariablesOf(formalTypeSignature);
        resultBuilder.add(new TypeRelationshipConstraintSolver(
                formalTypeSignature,
                typeVariables,
                longVariables,
                typeManager.getType(actualTypeSignatureProvider.getTypeSignature()),
                relationshipType));
        return true;
    }

    private final class TypeRelationshipConstraintSolver
            implements TypeConstraintSolver
    {
        private final TypeSignature formalTypeSignature;
        private final Set<String> typeVariables;
        private final Set<String> longVariables;
        private final Type actualType;
        private final RelationshipType relationshipType;

        public TypeRelationshipConstraintSolver(TypeSignature formalTypeSignature, Set<String> typeVariables, Set<String> longVariables, Type actualType, RelationshipType relationshipType)
        {
            this.formalTypeSignature = requireNonNull(formalTypeSignature, "formalTypeSignature is null");
            this.typeVariables = ImmutableSet.copyOf(typeVariables);
            this.longVariables = ImmutableSet.copyOf(longVariables);
            this.actualType = requireNonNull(actualType, "actualType is null");
            this.relationshipType = requireNonNull(relationshipType, "relationshipType is null");
        }

        @Override
        public SolverReturnStatus update(BoundVariables bindings)
        {
            for (String variable : typeVariables) {
                if (!bindings.containsTypeVariable(variable)) {
                    return SolverReturnStatus.UNCHANGED_NOT_SATISFIED;
                }
            }
            for (String variable : longVariables) {
                if (!bindings.containsLongVariable(variable)) {
                    return SolverReturnStatus.UNCHANGED_NOT_SATISFIED;
                }
            }

            TypeSignature constraintTypeSignature = applyBoundVariables(formalTypeSignature, bindings);

            return satisfiesCoercion(relationshipType, actualType, constraintTypeSignature) ? SolverReturnStatus.UNCHANGED_SATISFIED : SolverReturnStatus.UNCHANGED_NOT_SATISFIED;
        }
    }

    public enum RelationshipType
    {
        EXACT, IMPLICIT_COERCION, EXPLICIT_COERCION_TO, EXPLICIT_COERCION_FROM
    }

    private static final class FunctionTypeVariables
            implements TypeVariables
    {
        private final FunctionBinding functionBinding;

        public FunctionTypeVariables(FunctionBinding functionBinding)
        {
            this.functionBinding = requireNonNull(functionBinding, "functionBinding is null");
        }

        @Override
        public Type getTypeVariable(String variableName)
        {
            return functionBinding.getTypeVariable(variableName);
        }

        @Override
        public boolean containsTypeVariable(String variableName)
        {
            return functionBinding.containsTypeVariable(variableName);
        }

        @Override
        public Long getLongVariable(String variableName)
        {
            return functionBinding.getLongVariable(variableName);
        }

        @Override
        public boolean containsLongVariable(String variableName)
        {
            return functionBinding.containsLongVariable(variableName);
        }
    }
}
