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
package io.trino.operator.scalar;

import io.trino.operator.scalar.ChoicesSpecializedSqlScalarFunction.ScalarImplementationChoice;
import io.trino.operator.scalar.annotations.ParametricScalarImplementation.ConstantImplementationChoice;
import io.trino.spi.expression.Constant;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.ConstantSpecializedImplementation;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.ScalarFunctionSpecializationContext;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class ConstantSpecializedSqlScalarFunction
        implements SpecializedSqlScalarFunction
{
    private final SpecializedSqlScalarFunction rowImplementation;
    private final List<Specialization> specializations;

    public ConstantSpecializedSqlScalarFunction(SpecializedSqlScalarFunction rowImplementation, List<Specialization> specializations)
    {
        this.rowImplementation = requireNonNull(rowImplementation, "rowImplementation is null");
        this.specializations = specializations.stream()
                .sorted(Comparator.comparingInt((Specialization specialization) -> specialization.consumedArguments().size()).reversed()
                        .thenComparing(specialization -> specialization.consumedArguments().stream().sorted().toList().toString()))
                .toList();
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(InvocationConvention invocationConvention)
    {
        return rowImplementation.getScalarFunctionImplementation(invocationConvention);
    }

    @Override
    public Optional<ConstantSpecializedImplementation> getConstantSpecializedScalarFunctionImplementation(ScalarFunctionSpecializationContext context)
    {
        for (Specialization specialization : specializations) {
            if (!specialization.consumedArguments().stream().allMatch(argument -> context.constantArguments().get(argument).isPresent())) {
                continue;
            }

            List<InvocationArgumentConvention> residualArgumentConventions = new ArrayList<>();
            for (int index = 0; index < context.invocationConvention().getArgumentConventions().size(); index++) {
                if (!specialization.consumedArguments().contains(index)) {
                    residualArgumentConventions.add(context.invocationConvention().getArgumentConvention(index));
                }
            }
            InvocationConvention residualConvention = new InvocationConvention(
                    residualArgumentConventions,
                    context.invocationConvention().getReturnConvention(),
                    context.invocationConvention().supportsSession(),
                    context.invocationConvention().supportsInstanceFactory());

            Object[] constantValues = specialization.constructorConstantArguments().stream()
                    .map(argument -> context.constantArguments().get(argument).orElseThrow())
                    .map(Constant::getValue)
                    .toArray();

            List<ScalarImplementationChoice> choices = specialization.choices().stream()
                    .map(choice -> bindConstants(choice, constantValues))
                    .toList();
            ScalarFunctionImplementation implementation = new ChoicesSpecializedSqlScalarFunction(specialization.boundSignature(), choices)
                    .getScalarFunctionImplementation(residualConvention);

            return Optional.of(new ConstantSpecializedImplementation(
                    implementation,
                    specialization.consumedArguments()));
        }
        return Optional.empty();
    }

    private static ScalarImplementationChoice bindConstants(ConstantImplementationChoice choice, Object[] constantValues)
    {
        MethodHandle boundInstanceFactory = MethodHandles.insertArguments(choice.instanceFactory(), 0, constantValues);
        return new ScalarImplementationChoice(
                choice.returnConvention(),
                choice.argumentConventions(),
                choice.lambdaInterfaces(),
                choice.methodHandle(),
                Optional.of(boundInstanceFactory));
    }

    public record Specialization(
            Set<Integer> consumedArguments,
            BoundSignature boundSignature,
            List<ConstantImplementationChoice> choices,
            List<Integer> constructorConstantArguments)
    {
        public Specialization
        {
            consumedArguments = Set.copyOf(requireNonNull(consumedArguments, "consumedArguments is null"));
            requireNonNull(boundSignature, "boundSignature is null");
            choices = List.copyOf(requireNonNull(choices, "choices is null"));
            constructorConstantArguments = List.copyOf(requireNonNull(constructorConstantArguments, "constructorConstantArguments is null"));
        }
    }
}
