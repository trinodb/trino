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
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.QualifiedFunctionName;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.SqlPathElement;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.tree.Identifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metadata.GlobalFunctionCatalog.BUILTIN_SCHEMA;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_CALL;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.function.FunctionKind.AGGREGATE;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static io.trino.spi.function.FunctionKind.WINDOW;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FunctionResolver
{
    private final Metadata metadata;
    private final TypeManager typeManager;

    public FunctionResolver(Metadata metadata, TypeManager typeManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    boolean isAggregationFunction(Session session, QualifiedFunctionName name, Function<CatalogSchemaFunctionName, Collection<CatalogFunctionMetadata>> candidateLoader)
    {
        for (CatalogSchemaFunctionName catalogSchemaFunctionName : toPath(session, name)) {
            Collection<CatalogFunctionMetadata> candidates = candidateLoader.apply(catalogSchemaFunctionName);
            if (!candidates.isEmpty()) {
                return candidates.stream()
                        .map(CatalogFunctionMetadata::getFunctionMetadata)
                        .map(FunctionMetadata::getKind)
                        .anyMatch(AGGREGATE::equals);
            }
        }
        return false;
    }

    boolean isWindowFunction(Session session, QualifiedFunctionName name, Function<CatalogSchemaFunctionName, Collection<CatalogFunctionMetadata>> candidateLoader)
    {
        for (CatalogSchemaFunctionName catalogSchemaFunctionName : toPath(session, name)) {
            Collection<CatalogFunctionMetadata> candidates = candidateLoader.apply(catalogSchemaFunctionName);
            if (!candidates.isEmpty()) {
                return candidates.stream()
                        .map(CatalogFunctionMetadata::getFunctionMetadata)
                        .map(FunctionMetadata::getKind)
                        .anyMatch(WINDOW::equals);
            }
        }
        return false;
    }

    CatalogFunctionBinding resolveCoercion(Session session, QualifiedFunctionName name, Signature signature, Function<CatalogSchemaFunctionName, Collection<CatalogFunctionMetadata>> candidateLoader)
    {
        for (CatalogSchemaFunctionName catalogSchemaFunctionName : toPath(session, name)) {
            Collection<CatalogFunctionMetadata> candidates = candidateLoader.apply(catalogSchemaFunctionName);
            List<CatalogFunctionMetadata> exactCandidates = candidates.stream()
                    .filter(function -> possibleExactCastMatch(signature, function.getFunctionMetadata().getSignature()))
                    .collect(toImmutableList());
            for (CatalogFunctionMetadata candidate : exactCandidates) {
                if (canBindSignature(session, candidate.getFunctionMetadata().getSignature(), signature)) {
                    return toFunctionBinding(candidate, signature);
                }
            }

            // only consider generic genericCandidates
            List<CatalogFunctionMetadata> genericCandidates = candidates.stream()
                    .filter(function -> !function.getFunctionMetadata().getSignature().getTypeVariableConstraints().isEmpty())
                    .collect(toImmutableList());
            for (CatalogFunctionMetadata candidate : genericCandidates) {
                if (canBindSignature(session, candidate.getFunctionMetadata().getSignature(), signature)) {
                    return toFunctionBinding(candidate, signature);
                }
            }
        }

        throw new TrinoException(FUNCTION_IMPLEMENTATION_MISSING, format("%s not found", signature));
    }

    private boolean canBindSignature(Session session, Signature declaredSignature, Signature actualSignature)
    {
        return new SignatureBinder(session, metadata, typeManager, declaredSignature, false)
                .canBind(fromTypeSignatures(actualSignature.getArgumentTypes()), actualSignature.getReturnType());
    }

    private CatalogFunctionBinding toFunctionBinding(CatalogFunctionMetadata functionMetadata, Signature signature)
    {
        BoundSignature boundSignature = new BoundSignature(
                signature.getName(),
                typeManager.getType(signature.getReturnType()),
                signature.getArgumentTypes().stream()
                        .map(typeManager::getType)
                        .collect(toImmutableList()));
        return new CatalogFunctionBinding(
                functionMetadata.getCatalogHandle(),
                SignatureBinder.bindFunction(
                        functionMetadata.getFunctionMetadata().getFunctionId(),
                        functionMetadata.getFunctionMetadata().getSignature(),
                        boundSignature));
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

    CatalogFunctionBinding resolveFunction(
            Session session,
            QualifiedFunctionName name,
            List<TypeSignatureProvider> parameterTypes,
            Function<CatalogSchemaFunctionName, Collection<CatalogFunctionMetadata>> candidateLoader)
    {
        ImmutableList.Builder<CatalogFunctionMetadata> allCandidates = ImmutableList.builder();
        for (CatalogSchemaFunctionName catalogSchemaFunctionName : toPath(session, name)) {
            Collection<CatalogFunctionMetadata> candidates = candidateLoader.apply(catalogSchemaFunctionName);
            List<CatalogFunctionMetadata> exactCandidates = candidates.stream()
                    .filter(function -> function.getFunctionMetadata().getSignature().getTypeVariableConstraints().isEmpty())
                    .collect(toImmutableList());

            Optional<CatalogFunctionBinding> match = matchFunctionExact(session, exactCandidates, parameterTypes);
            if (match.isPresent()) {
                return match.get();
            }

            List<CatalogFunctionMetadata> genericCandidates = candidates.stream()
                    .filter(function -> !function.getFunctionMetadata().getSignature().getTypeVariableConstraints().isEmpty())
                    .collect(toImmutableList());

            match = matchFunctionExact(session, genericCandidates, parameterTypes);
            if (match.isPresent()) {
                return match.get();
            }

            match = matchFunctionWithCoercion(session, candidates, parameterTypes);
            if (match.isPresent()) {
                return match.get();
            }

            allCandidates.addAll(candidates);
        }

        List<CatalogFunctionMetadata> candidates = allCandidates.build();
        if (candidates.isEmpty()) {
            throw new TrinoException(FUNCTION_NOT_FOUND, format("Function '%s' not registered", name));
        }

        List<String> expectedParameters = new ArrayList<>();
        for (CatalogFunctionMetadata function : candidates) {
            String arguments = Joiner.on(", ").join(function.getFunctionMetadata().getSignature().getArgumentTypes());
            String constraints = Joiner.on(", ").join(function.getFunctionMetadata().getSignature().getTypeVariableConstraints());
            expectedParameters.add(format("%s(%s) %s", name, arguments, constraints).stripTrailing());
        }

        String parameters = Joiner.on(", ").join(parameterTypes);
        String expected = Joiner.on(", ").join(expectedParameters);
        String message = format("Unexpected parameters (%s) for function %s. Expected: %s", parameters, name, expected);
        throw new TrinoException(FUNCTION_NOT_FOUND, message);
    }

    public static List<CatalogSchemaFunctionName> toPath(Session session, QualifiedFunctionName name)
    {
        if (name.getCatalogName().isPresent()) {
            return ImmutableList.of(new CatalogSchemaFunctionName(name.getCatalogName().orElseThrow(), name.getSchemaName().orElseThrow(), name.getFunctionName()));
        }

        if (name.getSchemaName().isPresent()) {
            String currentCatalog = session.getCatalog()
                    .orElseThrow(() -> new IllegalArgumentException("Session default catalog must be set to resolve a partial function name: " + name));
            return ImmutableList.of(new CatalogSchemaFunctionName(currentCatalog, name.getSchemaName().orElseThrow(), name.getFunctionName()));
        }

        ImmutableList.Builder<CatalogSchemaFunctionName> names = ImmutableList.builder();

        // global namespace
        names.add(new CatalogSchemaFunctionName(GlobalSystemConnector.NAME, BUILTIN_SCHEMA, name.getFunctionName()));

        // add resolved path items
        for (SqlPathElement sqlPathElement : session.getPath().getParsedPath()) {
            String catalog = sqlPathElement.getCatalog().map(Identifier::getCanonicalValue).or(session::getCatalog)
                    .orElseThrow(() -> new IllegalArgumentException("Session default catalog must be set to resolve a partial function name: " + name));
            names.add(new CatalogSchemaFunctionName(catalog, sqlPathElement.getSchema().getCanonicalValue(), name.getFunctionName()));
        }
        return names.build();
    }

    private Optional<CatalogFunctionBinding> matchFunctionExact(Session session, List<CatalogFunctionMetadata> candidates, List<TypeSignatureProvider> actualParameters)
    {
        return matchFunction(session, candidates, actualParameters, false);
    }

    private Optional<CatalogFunctionBinding> matchFunctionWithCoercion(Session session, Collection<CatalogFunctionMetadata> candidates, List<TypeSignatureProvider> actualParameters)
    {
        return matchFunction(session, candidates, actualParameters, true);
    }

    private Optional<CatalogFunctionBinding> matchFunction(Session session, Collection<CatalogFunctionMetadata> candidates, List<TypeSignatureProvider> parameters, boolean coercionAllowed)
    {
        List<ApplicableFunction> applicableFunctions = identifyApplicableFunctions(session, candidates, parameters, coercionAllowed);
        if (applicableFunctions.isEmpty()) {
            return Optional.empty();
        }

        if (coercionAllowed) {
            applicableFunctions = selectMostSpecificFunctions(session, applicableFunctions, parameters);
            checkState(!applicableFunctions.isEmpty(), "at least single function must be left");
        }

        if (applicableFunctions.size() == 1) {
            ApplicableFunction applicableFunction = getOnlyElement(applicableFunctions);
            return Optional.of(toFunctionBinding(applicableFunction.getFunction(), applicableFunction.getBoundSignature()));
        }

        StringBuilder errorMessageBuilder = new StringBuilder();
        errorMessageBuilder.append("Could not choose a best candidate operator. Explicit type casts must be added.\n");
        errorMessageBuilder.append("Candidates are:\n");
        for (ApplicableFunction function : applicableFunctions) {
            errorMessageBuilder.append("\t * ");
            errorMessageBuilder.append(function.getBoundSignature());
            errorMessageBuilder.append("\n");
        }
        throw new TrinoException(AMBIGUOUS_FUNCTION_CALL, errorMessageBuilder.toString());
    }

    private List<ApplicableFunction> identifyApplicableFunctions(Session session, Collection<CatalogFunctionMetadata> candidates, List<TypeSignatureProvider> actualParameters, boolean allowCoercion)
    {
        ImmutableList.Builder<ApplicableFunction> applicableFunctions = ImmutableList.builder();
        for (CatalogFunctionMetadata function : candidates) {
            new SignatureBinder(session, metadata, typeManager, function.getFunctionMetadata().getSignature(), allowCoercion)
                    .bind(actualParameters)
                    .ifPresent(signature -> applicableFunctions.add(new ApplicableFunction(function, signature)));
        }
        return applicableFunctions.build();
    }

    private List<ApplicableFunction> selectMostSpecificFunctions(Session session, List<ApplicableFunction> applicableFunctions, List<TypeSignatureProvider> parameters)
    {
        checkArgument(!applicableFunctions.isEmpty());

        List<ApplicableFunction> mostSpecificFunctions = selectMostSpecificFunctions(session, applicableFunctions);
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

        // If the return type for all the selected function is the same, and the parameters are declared as RETURN_NULL_ON_NULL
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

    private List<ApplicableFunction> selectMostSpecificFunctions(Session session, List<ApplicableFunction> candidates)
    {
        List<ApplicableFunction> representatives = new ArrayList<>();

        for (ApplicableFunction current : candidates) {
            boolean found = false;
            for (int i = 0; i < representatives.size(); i++) {
                ApplicableFunction representative = representatives.get(i);
                if (isMoreSpecificThan(session, current, representative)) {
                    representatives.set(i, current);
                }
                if (isMoreSpecificThan(session, current, representative) || isMoreSpecificThan(session, representative, current)) {
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
        List<Type> boundTypes = applicableFunction.getBoundSignature().getArgumentTypes().stream()
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
                .map(function -> typeManager.getType(function.getBoundSignature().getReturnType()))
                .collect(Collectors.toSet());
        return returnTypes.size() == 1;
    }

    private static boolean allReturnNullOnGivenInputTypes(List<ApplicableFunction> applicableFunctions, List<Type> parameters)
    {
        return applicableFunctions.stream().allMatch(x -> returnsNullOnGivenInputTypes(x, parameters));
    }

    private static boolean returnsNullOnGivenInputTypes(ApplicableFunction applicableFunction, List<Type> parameterTypes)
    {
        FunctionMetadata function = applicableFunction.getFunctionMetadata();

        // Window and Aggregation functions have fixed semantic where NULL values are always skipped
        if (function.getKind() != SCALAR) {
            return true;
        }

        FunctionNullability functionNullability = function.getFunctionNullability();
        for (int i = 0; i < parameterTypes.size(); i++) {
            // if the argument value will always be null and the function argument is not nullable, the function will always return null
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
    private boolean isMoreSpecificThan(Session session, ApplicableFunction left, ApplicableFunction right)
    {
        List<TypeSignatureProvider> resolvedTypes = fromTypeSignatures(left.getBoundSignature().getArgumentTypes());
        return new SignatureBinder(session, metadata, typeManager, right.getDeclaredSignature(), true)
                .canBind(resolvedTypes);
    }

    private static class ApplicableFunction
    {
        private final CatalogFunctionMetadata function;
        // Ideally this would be a real bound signature, but the resolver algorithm considers functions with illegal types (e.g., char(large_number))
        // We could just not consider these applicable functions, but there are tests that depend on the specific error messages for these failures.
        private final Signature boundSignature;

        private ApplicableFunction(CatalogFunctionMetadata function, Signature boundSignature)
        {
            this.function = function;
            this.boundSignature = boundSignature;
        }

        public CatalogFunctionMetadata getFunction()
        {
            return function;
        }

        public FunctionMetadata getFunctionMetadata()
        {
            return function.getFunctionMetadata();
        }

        public Signature getDeclaredSignature()
        {
            return function.getFunctionMetadata().getSignature();
        }

        public Signature getBoundSignature()
        {
            return boundSignature;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("declaredSignature", function.getFunctionMetadata().getSignature())
                    .add("boundSignature", boundSignature)
                    .toString();
        }
    }

    static class CatalogFunctionMetadata
    {
        private final CatalogHandle catalogHandle;
        private final FunctionMetadata functionMetadata;

        public CatalogFunctionMetadata(CatalogHandle catalogHandle, FunctionMetadata functionMetadata)
        {
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
            this.functionMetadata = requireNonNull(functionMetadata, "functionMetadata is null");
        }

        public CatalogHandle getCatalogHandle()
        {
            return catalogHandle;
        }

        public FunctionMetadata getFunctionMetadata()
        {
            return functionMetadata;
        }
    }

    static class CatalogFunctionBinding
    {
        private final CatalogHandle catalogHandle;
        private final FunctionBinding functionBinding;

        private CatalogFunctionBinding(CatalogHandle catalogHandle, FunctionBinding functionBinding)
        {
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
            this.functionBinding = requireNonNull(functionBinding, "functionBinding is null");
        }

        public CatalogHandle getCatalogHandle()
        {
            return catalogHandle;
        }

        public FunctionBinding getFunctionBinding()
        {
            return functionBinding;
        }
    }
}
