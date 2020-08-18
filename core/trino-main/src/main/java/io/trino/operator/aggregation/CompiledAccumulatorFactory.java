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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class CompiledAccumulatorFactory
        implements AccumulatorFactory
{
    private final Constructor<? extends Accumulator> accumulatorConstructor;
    private final Constructor<? extends GroupedAccumulator> groupedAccumulatorConstructor;
    private final List<Supplier<Object>> lambdaProviders;

    public CompiledAccumulatorFactory(
            Constructor<? extends Accumulator> accumulatorConstructor,
            Constructor<? extends GroupedAccumulator> groupedAccumulatorConstructor,
            List<Supplier<Object>> lambdaProviders)
    {
        this.accumulatorConstructor = requireNonNull(accumulatorConstructor, "accumulatorConstructor is null");
        this.groupedAccumulatorConstructor = requireNonNull(groupedAccumulatorConstructor, "groupedAccumulatorConstructor is null");
        this.lambdaProviders = ImmutableList.copyOf(requireNonNull(lambdaProviders, "lambdaProviders is null"));
    }

    @Override
    public Accumulator createAccumulator()
    {
        try {
            return accumulatorConstructor.newInstance(lambdaProviders);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Accumulator createIntermediateAccumulator()
    {
        try {
            return accumulatorConstructor.newInstance(lambdaProviders);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public GroupedAccumulator createGroupedAccumulator()
    {
        try {
            return groupedAccumulatorConstructor.newInstance(lambdaProviders);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAccumulator()
    {
        try {
            return groupedAccumulatorConstructor.newInstance(lambdaProviders);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
