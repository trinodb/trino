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
package io.trino.spi.function;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.PreimageResult.Exactness;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/// Computes which values of one input argument produce a requested set of scalar-function
/// results. All other arguments have fixed, non-null values supplied by [Context].
/// For example, a provider for `year(date)` can return the dates in 2025 when asked for the
/// preimage of the result 2025.
///
/// Implementations must check the concrete argument and result types, the selected
/// [Context#inputArgument], and the fixed values before computing a result.
/// A provider can support any subset of calls to its function. Return an empty optional
/// when a call is unsupported, including when a known boundary conversion cannot be performed.
/// Unrelated implementation errors must propagate rather than being treated as unsupported calls.
///
/// The function must return null exactly when the selected input is null, with the other
/// arguments fixed. On every input where the function succeeds, the provider's answer must
/// preserve the result and must not introduce a failure. Inputs on which the function fails
/// do not constrain the answer.
public interface DomainPreimage
{
    /// Returns the input values whose function results belong to `resultDomain`.
    /// For each input on which the function succeeds, membership in an `EXACT` result must
    /// agree with membership of the function result in `resultDomain`. A `CONSERVATIVE`
    /// result may contain additional inputs, but must not omit any matching input.
    /// Inputs on which the function fails may be included or excluded in either case.
    /// Honor [Context#requiredExactness]:
    /// return an empty optional if the requested accuracy cannot be provided.
    ///
    /// Null belongs to the exact input set if and only if it belongs to the requested result
    /// set. A conservative answer must also include null when the requested set includes it.
    /// An empty optional means unsupported; a result containing [Domain#none] means
    /// that no input matches. These have different meanings for callers.
    Optional<PreimageResult> preimage(Context context, Domain resultDomain);

    /// Returns an input constant that can replace `constant` when removing the function
    /// from a comparison. If the selected input is `x` and the fixed arguments are `a`,
    /// a returned value `c` promises that `f(x, a) OP constant` and `x OP c` have the same
    /// Boolean or null result for every supported comparison operator `OP`, including
    /// `IS NOT DISTINCT FROM`, on every input where the original comparison succeeds.
    /// The replacement must also succeed on those inputs.
    ///
    /// For example, a cast from `array(integer)` to `array(bigint)` can map an array constant
    /// back to integers when all elements fit. Keeping the array comparison preserves its
    /// null behavior, including unknown results caused by null elements.
    ///
    /// The returned constant must have the selected input's type and the same nullness as
    /// `constant`. Return an empty optional when these comparison guarantees cannot be met.
    /// This operation does not promise that [#preimage] can represent the matching input set.
    default Optional<NullableValue> comparisonConstant(Context context, NullableValue constant)
    {
        return Optional.empty();
    }

    /// Returns whether the function can be removed from comparisons with any second operand
    /// of its result type. A true answer promises that `f(x, a) OP y` and `x OP y` have the
    /// same Boolean or null result for every comparison operator and every value of `y`,
    /// wherever the original comparison succeeds. The replacement must succeed there too.
    /// Return false if the selected input, fixed arguments, or types do not satisfy this contract.
    default boolean isComparisonIdentity(Context context)
    {
        return false;
    }

    /// Describes a function call in which one input argument may vary and every other argument
    /// has a fixed value. For `date_trunc('month', timestamp)`, `inputArgument` is 1,
    /// argument 0 contains the string `month`, and argument 1 has no constant value.
    /// A provider must check that it supports the selected input before reading the other
    /// arguments as parameters or calculating a preimage.
    ///
    /// @param session session settings used by the function, such as the time zone
    /// @param signature the function name and concrete SQL argument and result types, including
    ///         precisions and scales; injected Java parameters are not part of this signature
    /// @param inputArgument zero-based SQL argument index whose possible values are requested
    /// @param arguments typed constant values in SQL argument order; the selected input has an
    ///         empty optional and every other entry contains a non-null constant
    /// @param requiredExactness `EXACT` requires exact membership on inputs where the function
    ///         succeeds; `CONSERVATIVE` also permits a superset of the matching inputs
    /// @param functions operations for evaluating this function on constant inputs, obtaining
    ///         casts, comparing result values, and checking implicit coercions
    record Context(
            ConnectorSession session,
            BoundSignature signature,
            int inputArgument,
            List<Optional<NullableValue>> arguments,
            Exactness requiredExactness,
            PreimageFunctionDependencies functions)
    {
        public Context
        {
            requireNonNull(session, "session is null");
            requireNonNull(signature, "signature is null");
            arguments = List.copyOf(arguments);
            requireNonNull(requiredExactness, "requiredExactness is null");
            requireNonNull(functions, "functions is null");
            if (inputArgument < 0 || inputArgument >= signature.getArgumentTypes().size()) {
                throw new IllegalArgumentException("input argument is outside the bound signature");
            }
            if (arguments.size() != signature.getArgumentTypes().size()) {
                throw new IllegalArgumentException("arguments must match the bound signature");
            }
            for (int index = 0; index < arguments.size(); index++) {
                Optional<NullableValue> argument = arguments.get(index);
                if (index == inputArgument) {
                    if (argument.isPresent()) {
                        throw new IllegalArgumentException("input argument must not have a constant value");
                    }
                }
                else if (argument.isEmpty() || argument.get().isNull() || !argument.get().getType().equals(signature.getArgumentTypes().get(index))) {
                    throw new IllegalArgumentException("parameters must be non-null constants of their bound types");
                }
            }
        }
    }
}
