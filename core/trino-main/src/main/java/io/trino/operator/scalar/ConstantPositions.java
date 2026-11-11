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

import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.InvocationConvention;

import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * Derives the signature and invocation convention that remain after the
 * arguments at the given positions are supplied as constants.
 */
public final class ConstantPositions
{
    private ConstantPositions() {}

    public static <T> List<T> removeConstants(List<T> arguments, Set<Integer> constantPositions)
    {
        return IntStream.range(0, arguments.size())
                .filter(position -> !constantPositions.contains(position))
                .mapToObj(arguments::get)
                .toList(); // arguments may contain nulls, e.g. lambda arguments that are compiled separately
    }

    public static BoundSignature remainingSignature(BoundSignature signature, Set<Integer> constantPositions)
    {
        return new BoundSignature(signature.getName(), signature.getReturnType(), removeConstants(signature.getArgumentTypes(), constantPositions));
    }

    public static InvocationConvention remainingConvention(InvocationConvention convention, Set<Integer> constantPositions)
    {
        return new InvocationConvention(
                removeConstants(convention.getArgumentConventions(), constantPositions),
                convention.getReturnConvention(),
                convention.supportsSession(),
                convention.supportsInstanceFactory());
    }
}
