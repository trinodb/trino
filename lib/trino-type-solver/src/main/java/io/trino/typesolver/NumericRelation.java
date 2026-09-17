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
package io.trino.typesolver;

import io.trino.typesolver.Expression.BinaryOperation;

import java.util.Map;

/// A numeric relation (comparison) over numeric-kind variables and literals.
///
/// Handled by [NumericPropagator]. Typical shapes:
///
/// - `p - s >= 10` — pattern guard for a `decimal(p,s) → bigint` coercion.
/// - `n1 <= n2` — varchar-widening guard.
/// - `p == 6` — equality from calculated-return-type arithmetic.
///
/// The solver first tries to evaluate the relation against current bounds; if that
/// succeeds it's either discharged or a contradiction. Otherwise bounds are propagated
/// and the relation parked as pending.
public record NumericRelation(BinaryOperation operation)
        implements Constraint
{
    @Override
    public Constraint apply(Map<String, Expression> substitutions)
    {
        return new NumericRelation((BinaryOperation) Expression.substitute(operation, substitutions));
    }

    @Override
    public String toString()
    {
        return operation.toString();
    }
}
