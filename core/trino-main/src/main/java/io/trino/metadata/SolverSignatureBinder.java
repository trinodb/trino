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

import io.trino.metadata.SignatureBinder.GroundSignature;
import io.trino.spi.function.Signature;
import io.trino.spi.function.TypeVariableConstraint;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeTemplate;
import io.trino.spi.type.TypeTemplates;
import io.trino.sql.analyzer.TypeDescriptorProvider;
import io.trino.type.TypeCoercion;
import io.trino.type.TypeResolutionPolicy;
import io.trino.typesolver.CompiledSignature;
import io.trino.typesolver.Expression;
import io.trino.typesolver.SignatureCompiler;
import io.trino.typesolver.TrinoPreset;
import io.trino.typesolver.TypeCompiler;
import io.trino.typesolver.TypeSystem;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static java.lang.String.CASE_INSENSITIVE_ORDER;

/// Infers a candidate's ground signature with the constraint solver. Catalog lookup,
/// overload ordering, and specialization remain the responsibility of [FunctionBinder].
final class SolverSignatureBinder
{
    private final Metadata metadata;
    private final TypeManager typeManager;
    private final Signature signature;
    private final boolean allowCoercion;
    private final TypeResolutionPolicy policy;
    private final TypeSystem typeSystem;
    private final TypeCoercion typeCoercion;

    SolverSignatureBinder(Metadata metadata, TypeManager typeManager, Signature signature, boolean allowCoercion, TypeResolutionPolicy policy)
    {
        this.metadata = metadata;
        this.typeManager = typeManager;
        this.signature = signature;
        this.allowCoercion = allowCoercion;
        this.policy = policy;
        this.typeCoercion = new TypeCoercion(typeManager::getType, policy);
        this.typeSystem = new TypeSystem(TrinoPreset.typeConstructors(), TrinoPreset.coercionRules(TrinoPreset.CharVarcharCoercion.valueOf(policy.charVarcharCoercion().name())), TrinoPreset.castRules())
        {
            @Override
            public boolean isComparable(Expression type)
            {
                return !Expression.isGround(type) || typeManager.getType(TypeCompiler.toTypeDescriptor(type)).isComparable();
            }

            @Override
            public boolean isOrderable(Expression type)
            {
                return !Expression.isGround(type) || typeManager.getType(TypeCompiler.toTypeDescriptor(type)).isOrderable();
            }
        };
    }

    Optional<GroundSignature> bind(List<? extends TypeDescriptorProvider> parameters)
    {
        return bind(parameters, Optional.empty());
    }

    boolean canBind(List<? extends TypeDescriptorProvider> parameters)
    {
        return bind(parameters).isPresent();
    }

    boolean canBind(List<? extends TypeDescriptorProvider> parameters, TypeDescriptor returnType)
    {
        return bind(parameters, Optional.of(returnType)).isPresent();
    }

    private Optional<GroundSignature> bind(List<? extends TypeDescriptorProvider> parameters, Optional<TypeDescriptor> expectedReturn)
    {
        List<TypeTemplate> formals = new ArrayList<>(signature.getArgumentTypes());
        if (signature.isVariableArity() && !formals.isEmpty() && parameters.size() >= formals.size() - 1) {
            TypeTemplate repeated = formals.removeLast();
            while (formals.size() < parameters.size()) {
                formals.add(repeated);
            }
        }
        if (parameters.size() != formals.size()) {
            return Optional.empty();
        }

        CompiledSignature compiled = SignatureCompiler.compile(signature);
        Expression.FunctionType function = (Expression.FunctionType) compiled.type();
        List<Expression> parameterTemplates = new ArrayList<>(function.parameterTypes());
        while (parameterTemplates.size() < parameters.size()) {
            parameterTemplates.add(function.variadicParameterType().orElseThrow());
        }
        if (expectedReturn.isPresent()) {
            parameterTemplates = new ArrayList<>(parameterTemplates);
            parameterTemplates.add(function.returnType());
            compiled = new CompiledSignature(compiled.parameters(), compiled.constraints(), Expression.function(parameterTemplates, function.returnType()));
        }

        List<Expression> arguments = new ArrayList<>();
        for (int index = 0; index < parameters.size(); index++) {
            TypeDescriptorProvider parameter = parameters.get(index);
            if (!parameter.hasDependency()) {
                arguments.add(TypeCompiler.compile(parameter.getTypeDescriptor()));
                continue;
            }
            Expression formal = TypeCompiler.compile(formals.get(index));
            if (!(formal instanceof Expression.FunctionType lambda)) {
                return Optional.empty();
            }
            List<Expression> inputs = new ArrayList<>();
            for (int input = 0; input < lambda.parameterTypes().size(); input++) {
                inputs.add(Expression.variable("lambda_" + index + "_" + input));
            }
            arguments.add(Expression.function(inputs, Expression.variable("lambda_" + index + "_return")));
        }
        expectedReturn.ifPresent(type -> arguments.add(TypeCompiler.compile(type)));

        Set<List<Expression>> visited = new HashSet<>();
        while (visited.add(List.copyOf(arguments))) {
            if (!(compiled.matchFunctionCallOutcome(arguments, typeSystem, allowCoercion) instanceof CompiledSignature.Satisfied satisfied)) {
                return Optional.empty();
            }
            CompiledSignature.MatchResult match = satisfied.result();
            boolean changed = false;
            for (int index = 0; index < parameters.size(); index++) {
                if (!parameters.get(index).hasDependency()) {
                    continue;
                }
                if (!(match.parameterTypes().get(index) instanceof Expression.FunctionType lambda)) {
                    return Optional.empty();
                }
                if (!lambda.parameterTypes().stream().allMatch(Expression::isGround)) {
                    // Another lambda may supply these inputs on the next solve.
                    continue;
                }
                List<Type> inputs = lambda.parameterTypes().stream().map(TypeCompiler::toTypeDescriptor).map(typeManager::getType).toList();
                Expression actual = TypeCompiler.compile(parameters.get(index).getTypeDescriptor(inputs));
                changed |= !actual.equals(arguments.set(index, actual));
            }
            if (changed) {
                continue;
            }
            if (!Expression.isGround(match.returnType()) || !match.parameterTypes().stream().allMatch(Expression::isGround)) {
                return Optional.empty();
            }
            TypeDescriptor returnType = TypeCompiler.toTypeDescriptor(match.returnType());
            if (expectedReturn.isPresent() && !typeManager.getType(returnType).equals(typeManager.getType(expectedReturn.orElseThrow()))) {
                return Optional.empty();
            }
            List<TypeDescriptor> argumentTypes = match.parameterTypes().subList(0, parameters.size()).stream()
                    .map(TypeCompiler::toTypeDescriptor)
                    .toList();
            for (int index = 0; index < parameters.size(); index++) {
                Type actual = typeManager.getType(TypeCompiler.toTypeDescriptor(arguments.get(index)));
                Type formal = typeManager.getType(argumentTypes.get(index));
                if (allowCoercion ? !typeCoercion.canCoerce(actual, formal) : !actual.equals(formal)) {
                    return Optional.empty();
                }
            }
            if (!satisfiesConstraints(match)) {
                return Optional.empty();
            }
            return Optional.of(new GroundSignature(returnType, argumentTypes));
        }
        return Optional.empty();
    }

    private boolean satisfiesConstraints(CompiledSignature.MatchResult match)
    {
        Map<String, TypeDescriptor> types = new TreeMap<>(CASE_INSENSITIVE_ORDER);
        match.typeBindings().forEach((name, type) -> types.put(name, TypeCompiler.toTypeDescriptor(type)));
        Map<String, Long> numbers = new TreeMap<>(CASE_INSENSITIVE_ORDER);
        match.numericBindings().forEach((name, value) -> numbers.put(name, value.longValue()));
        for (TypeVariableConstraint constraint : signature.getTypeVariableConstraints()) {
            TypeDescriptor descriptor = types.get(constraint.getName());
            if (descriptor == null) {
                return false;
            }
            Type type = typeManager.getType(descriptor);
            if ((constraint.isComparableRequired() && !type.isComparable()) ||
                    (constraint.isOrderableRequired() && !type.isOrderable()) ||
                    (constraint.isRowType() && !(type instanceof RowType))) {
                return false;
            }
            for (TypeTemplate target : constraint.getCastableTo()) {
                if (!canCast(type, typeManager.getType(TypeTemplates.bind(target, types, numbers)))) {
                    return false;
                }
            }
            for (TypeTemplate source : constraint.getCastableFrom()) {
                if (!canCast(typeManager.getType(TypeTemplates.bind(source, types, numbers)), type)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean canCast(Type from, Type to)
    {
        return new TypeCastability(metadata, policy).canCast(from, to);
    }
}
