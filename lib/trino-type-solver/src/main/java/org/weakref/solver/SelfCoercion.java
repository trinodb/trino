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
package org.weakref.solver;

import java.util.Optional;
import java.util.Set;

/**
 * Reflexivity rule: any type is a subtype of itself.
 * <p>
 * Also handles the degenerate variable cases: when either side is a lone {@link Expression.Variable},
 * the rule produces an {@link ExactType} constraint binding the variable to the other side.
 * Always registered first so the solver can short-circuit trivial matches before scanning the
 * full rule list.
 */
public class SelfCoercion
        implements CoercionRule
{
    @Override
    public String ruleId()
    {
        return "self";
    }

    @Override
    public Optional<Match> matches(VariableAllocator allocator, Expression from, Expression to)
    {
        if (from.equals(to)) {
            return Optional.of(new Match(Set.of(), CoercionPlan.exact(from, to)));
        }

        if (from instanceof Expression.Variable(String variable)) {
            return Optional.of(new Match(
                    Set.of(new ExactType(variable, to)),
                    CoercionPlan.exact(from, to)));
        }

        if (to instanceof Expression.Variable(String variable)) {
            return Optional.of(new Match(
                    Set.of(new ExactType(variable, from)),
                    CoercionPlan.exact(from, to)));
        }

        return Optional.empty();
    }
}
