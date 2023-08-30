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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.analyzer.TypeSignatureProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_CALL;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

/**
 * Binds an actual call site signature to a function.
 */
class FunctionBinder
{
    private final Metadata metadata;
    private final TypeManager typeManager;

    public FunctionBinder(Metadata metadata, TypeManager typeManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    CatalogFunctionBinding bindFunction(List<TypeSignatureProvider> parameterTypes, Collection<CatalogFunctionMetadata> candidates, String displayName)
    {
        return tryBindFunction(parameterTypes, candidates).orElseThrow(() -> functionNotFound(displayName, parameterTypes, candidates));
    }

    Optional<CatalogFunctionBinding> tryBindFunction(List<TypeSignatureProvider> parameterTypes, Collection<CatalogFunctionMetadata> candidates)
    {
        if (candidates.isEmpty()) {
            return Optional.empty();
        }

        List<CatalogFunctionMetadata> exactCandidates = candidates.stream()
                .filter(function -> function.functionMetadata().getSignature().getTypeVariableConstraints().isEmpty())
                .collect(toImmutableList());

        Optional<CatalogFunctionBinding> match = matchFunctionExact(exactCandidates, parameterTypes);
        if (match.isPresent()) {
            return match;
        }

        List<CatalogFunctionMetadata> genericCandidates = candidates.stream()
                .filter(function -> !function.functionMetadata().getSignature().getTypeVariableConstraints().isEmpty())
                .collect(toImmutableList());

        match = matchFunctionExact(genericCandidates, parameterTypes);
        if (match.isPresent()) {
            return match;
        }

        return matchFunctionWithCoercion(candidates, parameterTypes);
    }

    CatalogFunctionBinding bindCoercion(Signature signature, Collection<CatalogFunctionMetadata> candidates)
    {
        // coercions are much more common and much simpler than function calls, so we use a custom algorithm
        List<CatalogFunctionMetadata> exactCandidates = candidates.stream()
                .filter(function -> possibleExactCastMatch(signature, function.functionMetadata().getSignature()))
                .collect(toImmutableList());
        for (CatalogFunctionMetadata candidate : exactCandidates) {
            if (canBindSignature(candidate.functionMetadata().getSignature(), signature)) {
                return toFunctionBinding(candidate, signature);
            }
        }

        // only consider generic genericCandidates
        List<CatalogFunctionMetadata> genericCandidates = candidates.stream()
                .filter(function -> !function.functionMetadata().getSignature().getTypeVariableConstraints().isEmpty())
                .collect(toImmutableList());
        for (CatalogFunctionMetadata candidate : genericCandidates) {
            if (canBindSignature(candidate.functionMetadata().getSignature(), signature)) {
                return toFunctionBinding(candidate, signature);
            }
        }

        throw new TrinoException(FUNCTION_IMPLEMENTATION_MISSING, format("%s not found", signature));
    }

    private boolean canBindSignature(Signature declaredSignature, Signature actualSignature)
    {
        return new SignatureBinder(metadata, typeManager, declaredSignature, false)
                .canBind(fromTypeSignatures(actualSignature.getArgumentTypes()), actualSignature.getReturnType());
    }

    private static boolean possibleExactCastMatch(Signature signature, Signature declaredSignature)
    {
        if (!declaredSignature.getTypeVariableConstraints().isEmpty()) {
            return false;
        }
        if (!declaredSignature.getReturnType().getBase().equalsIgnoreCase(signature.getReturnType().getBase())) {
            return false;
        }
        if (!declaredSignature.getArgumentTypes().get(0).getBase().equalsIgnoreCase(signature.getArgumentTypes().get(0).getBase())) {
            return false;
        }
        return true;
    }

    private Optional<CatalogFunctionBinding> matchFunctionExact(List<CatalogFunctionMetadata> candidates, List<TypeSignatureProvider> actualParameters)
    {
        return matchFunction(candidates, actualParameters, false);
    }

    private Optional<CatalogFunctionBinding> matchFunctionWithCoercion(Collection<CatalogFunctionMetadata> candidates, List<TypeSignatureProvider> actualParameters)
    {
        return matchFunction(candidates, actualParameters, true);
    }

    private Optional<CatalogFunctionBinding> matchFunction(Collection<CatalogFunctionMetadata> candidates, List<TypeSignatureProvider> parameters, boolean coercionAllowed)
    {
        List<ApplicableFunction> applicableFunctions = identifyApplicableFunctions(candidates, parameters, coercionAllowed);
        if (applicableFunctions.isEmpty()) {
            return Optional.empty();
        }

        if (coercionAllowed) {
            applicableFunctions = selectMostSpecificFunctions(applicableFunctions, parameters);
            checkState(!applicableFunctions.isEmpty(), "at least single function must be left");
        }

        if (applicableFunctions.size() == 1) {
            ApplicableFunction applicableFunction = getOnlyElement(applicableFunctions);
            return Optional.of(toFunctionBinding(applicableFunction.function(), applicableFunction.boundSignature()));
        }

        StringBuilder errorMessageBuilder = new StringBuilder();
        errorMessageBuilder.append("Could not choose a best candidate operator. Explicit type casts must be added.\n");
        errorMessageBuilder.append("Candidates are:\n");
        for (ApplicableFunction function : applicableFunctions) {
            errorMessageBuilder.append("\t * ");
            errorMessageBuilder.append(function.boundSignature());
            errorMessageBuilder.append("\n");
        }
        throw new TrinoException(AMBIGUOUS_FUNCTION_CALL, errorMessageBuilder.toString());
    }

    private List<ApplicableFunction> identifyApplicableFunctions(Collection<CatalogFunctionMetadata> candidates, List<TypeSignatureProvider> actualParameters, boolean allowCoercion)
    {
        ImmutableList.Builder<ApplicableFunction> applicableFunctions = ImmutableList.builder();
        for (CatalogFunctionMetadata function : candidates) {
            new SignatureBinder(metadata, typeManager, function.functionMetadata().getSignature(), allowCoercion)
                    .bind(actualParameters)
                    .ifPresent(signature -> applicableFunctions.add(new ApplicableFunction(function, signature)));
        }
        return applicableFunctions.build();
    }

    private List<ApplicableFunction> selectMostSpecificFunctions(List<ApplicableFunction> applicableFunctions, List<TypeSignatureProvider> parameters)
    {
        checkArgument(!applicableFunctions.isEmpty());

        List<ApplicableFunction> mostSpecificFunctions = selectMostSpecificFunctions(applicableFunctions);
        if (mostSpecificFunctions.size() <= 1) {
            return mostSpecificFunctions;
        }

        Optional<List<Type>> optionalParameterTypes = toTypes(parameters);
        if (optionalParameterTypes.isEmpty()) {
            // give up and return all remaining matches
            return mostSpecificFunctions;
        }

        List<Type> parameterTypes = optionalParameterTypes.get();
        if (!someParameterIsUnknown(parameterTypes)) {
            // give up and return all remaining matches
            return mostSpecificFunctions;
        }

        // look for functions that only cast the unknown arguments
        List<ApplicableFunction> unknownOnlyCastFunctions = getUnknownOnlyCastFunctions(applicableFunctions, parameterTypes);
        if (!unknownOnlyCastFunctions.isEmpty()) {
            mostSpecificFunctions = unknownOnlyCastFunctions;
            if (mostSpecificFunctions.size() == 1) {
                return mostSpecificFunctions;
            }
        }

        // If the return type for all the selected function is the same, and the parameters are declared as RETURN_NULL_ON_NULL, then
        // all the functions are semantically the same. We can return just any of those.
        if (returnTypeIsTheSame(mostSpecificFunctions) && allReturnNullOnGivenInputTypes(mostSpecificFunctions, parameterTypes)) {
            // make it deterministic
            ApplicableFunction selectedFunction = Ordering.usingToString()
                    .reverse()
                    .sortedCopy(mostSpecificFunctions)
                    .get(0);
            return ImmutableList.of(selectedFunction);
        }

        return mostSpecificFunctions;
    }

    private List<ApplicableFunction> selectMostSpecificFunctions(List<ApplicableFunction> candidates)
    {
        List<ApplicableFunction> representatives = new ArrayList<>();

        for (ApplicableFunction current : candidates) {
            boolean found = false;
            for (int i = 0; i < representatives.size(); i++) {
                ApplicableFunction representative = representatives.get(i);
                if (isMoreSpecificThan(current, representative)) {
                    representatives.set(i, current);
                }
                if (isMoreSpecificThan(current, representative) || isMoreSpecificThan(representative, current)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                representatives.add(current);
            }
        }

        return representatives;
    }

    private static boolean someParameterIsUnknown(List<Type> parameters)
    {
        return parameters.stream().anyMatch(type -> type.equals(UNKNOWN));
    }

    private List<ApplicableFunction> getUnknownOnlyCastFunctions(List<ApplicableFunction> applicableFunction, List<Type> actualParameters)
    {
        return applicableFunction.stream()
                .filter(function -> onlyCastsUnknown(function, actualParameters))
                .collect(toImmutableList());
    }

    private boolean onlyCastsUnknown(ApplicableFunction applicableFunction, List<Type> actualParameters)
    {
        List<Type> boundTypes = applicableFunction.boundSignature().getArgumentTypes().stream()
                .map(typeManager::getType)
                .collect(toImmutableList());
        checkState(actualParameters.size() == boundTypes.size(), "type lists are of different lengths");
        for (int i = 0; i < actualParameters.size(); i++) {
            if (!boundTypes.get(i).equals(actualParameters.get(i)) && actualParameters.get(i) != UNKNOWN) {
                return false;
            }
        }
        return true;
    }

    private boolean returnTypeIsTheSame(List<ApplicableFunction> applicableFunctions)
    {
        Set<Type> returnTypes = applicableFunctions.stream()
                .map(function -> typeManager.getType(function.boundSignature().getReturnType()))
                .collect(Collectors.toSet());
        return returnTypes.size() == 1;
    }

    private static boolean allReturnNullOnGivenInputTypes(List<ApplicableFunction> applicableFunctions, List<Type> parameters)
    {
        return applicableFunctions.stream().allMatch(x -> returnsNullOnGivenInputTypes(x, parameters));
    }

    private static boolean returnsNullOnGivenInputTypes(ApplicableFunction applicableFunction, List<Type> parameterTypes)
    {
        FunctionMetadata function = applicableFunction.functionMetadata();

        // Window and Aggregation functions have fixed semantic where NULL values are always skipped
        if (function.getKind() != SCALAR) {
            return true;
        }

        FunctionNullability functionNullability = function.getFunctionNullability();
        for (int i = 0; i < parameterTypes.size(); i++) {
            // if the argument value is always null and the function argument is not nullable, the function will always return null
            if (parameterTypes.get(i).equals(UNKNOWN) && !functionNullability.isArgumentNullable(i)) {
                return true;
            }
        }
        return false;
    }

    private Optional<List<Type>> toTypes(List<TypeSignatureProvider> typeSignatureProviders)
    {
        ImmutableList.Builder<Type> resultBuilder = ImmutableList.builder();
        for (TypeSignatureProvider typeSignatureProvider : typeSignatureProviders) {
            if (typeSignatureProvider.hasDependency()) {
                return Optional.empty();
            }
            resultBuilder.add(typeManager.getType(typeSignatureProvider.getTypeSignature()));
        }
        return Optional.of(resultBuilder.build());
    }

    /**
     * One method is more specific than another if invocation handled by the first method could be passed on to the other one
     */
    private boolean isMoreSpecificThan(ApplicableFunction left, ApplicableFunction right)
    {
        List<TypeSignatureProvider> resolvedTypes = fromTypeSignatures(left.boundSignature().getArgumentTypes());
        return new SignatureBinder(metadata, typeManager, right.declaredSignature(), true)
                .canBind(resolvedTypes);
    }

    private CatalogFunctionBinding toFunctionBinding(CatalogFunctionMetadata functionMetadata, Signature signature)
    {
        BoundSignature boundSignature = new BoundSignature(
                new CatalogSchemaFunctionName(
                        functionMetadata.catalogHandle().getCatalogName(),
                        functionMetadata.schemaName(),
                        functionMetadata.functionMetadata().getCanonicalName()),
                typeManager.getType(signature.getReturnType()),
                signature.getArgumentTypes().stream()
                        .map(typeManager::getType)
                        .collect(toImmutableList()));
        return new CatalogFunctionBinding(
                functionMetadata.catalogHandle(),
                bindFunctionMetadata(boundSignature, functionMetadata.functionMetadata()),
                SignatureBinder.bindFunction(
                        functionMetadata.functionMetadata().getFunctionId(),
                        functionMetadata.functionMetadata().getSignature(),
                        boundSignature));
    }

    private static FunctionMetadata bindFunctionMetadata(BoundSignature signature, FunctionMetadata functionMetadata)
    {
        FunctionMetadata.Builder newMetadata = FunctionMetadata.builder(functionMetadata.getCanonicalName(), functionMetadata.getKind())
                .functionId(functionMetadata.getFunctionId())
                .signature(signature.toSignature());

        functionMetadata.getNames().forEach(newMetadata::alias);

        if (functionMetadata.getDescription().isEmpty()) {
            newMetadata.noDescription();
        }
        else {
            newMetadata.description(functionMetadata.getDescription());
        }

        if (functionMetadata.isHidden()) {
            newMetadata.hidden();
        }
        if (!functionMetadata.isDeterministic()) {
            newMetadata.nondeterministic();
        }
        if (functionMetadata.isDeprecated()) {
            newMetadata.deprecated();
        }
        if (functionMetadata.getFunctionNullability().isReturnNullable()) {
            newMetadata.nullable();
        }

        // specialize function metadata to resolvedFunction
        List<Boolean> argumentNullability = functionMetadata.getFunctionNullability().getArgumentNullable();
        if (functionMetadata.getSignature().isVariableArity()) {
            List<Boolean> fixedArgumentNullability = argumentNullability.subList(0, argumentNullability.size() - 1);
            int variableArgumentCount = signature.getArgumentTypes().size() - fixedArgumentNullability.size();
            argumentNullability = ImmutableList.<Boolean>builder()
                    .addAll(fixedArgumentNullability)
                    .addAll(nCopies(variableArgumentCount, argumentNullability.get(argumentNullability.size() - 1)))
                    .build();
        }
        newMetadata.argumentNullability(argumentNullability);

        return newMetadata.build();
    }

    static TrinoException functionNotFound(String name, List<TypeSignatureProvider> parameterTypes, Collection<CatalogFunctionMetadata> candidates)
    {
        if (candidates.isEmpty()) {
            return new TrinoException(FUNCTION_NOT_FOUND, format("Function '%s' not registered", name));
        }

        Set<String> expectedParameters = new TreeSet<>();
        for (CatalogFunctionMetadata function : candidates) {
            String arguments = Joiner.on(", ").join(function.functionMetadata().getSignature().getArgumentTypes());
            String constraints = Joiner.on(", ").join(function.functionMetadata().getSignature().getTypeVariableConstraints());
            expectedParameters.add(format("%s(%s) %s", name, arguments, constraints).stripTrailing());
        }

        String parameters = Joiner.on(", ").join(parameterTypes);
        String expected = Joiner.on(", ").join(expectedParameters);
        String message = format("Unexpected parameters (%s) for function %s. Expected: %s", parameters, name, expected);
        return new TrinoException(FUNCTION_NOT_FOUND, message);
    }

    /**
     * @param boundSignature Ideally this would be a real bound signature,
     * but the resolver algorithm considers functions with illegal types (e.g., char(large_number))
     * We could just not consider these applicable functions, but there are tests that depend on
     * the specific error messages for these failures.
     */
    private record ApplicableFunction(CatalogFunctionMetadata function, Signature boundSignature)
    {
        public FunctionMetadata functionMetadata()
        {
            return function.functionMetadata();
        }

        public Signature declaredSignature()
        {
            return function.functionMetadata().getSignature();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("declaredSignature", function.functionMetadata().getSignature())
                    .add("boundSignature", boundSignature)
                    .toString();
        }
    }

    record CatalogFunctionBinding(CatalogHandle catalogHandle, FunctionMetadata functionMetadata, FunctionBinding functionBinding)
    {
        CatalogFunctionBinding
        {
            requireNonNull(catalogHandle, "catalogHandle is null");
            requireNonNull(functionMetadata, "functionMetadata is null");
            requireNonNull(functionBinding, "functionBinding is null");
        }
    }
}
