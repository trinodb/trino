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
package io.prestosql.sql.planner.iterative.rule.test;

import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.PlanNode;

public class FakRules
{
    public static class FakeRule1
            implements Rule<PlanNode>
    {
        @Override
        public Pattern<PlanNode> getPattern()
        {
            return null;
        }

        @Override
        public Result apply(PlanNode node, Captures captures, Context context)
        {
            return null;
        }
    }

    public static class FakeRule2
            implements Rule<PlanNode>
    {
        @Override
        public Pattern<PlanNode> getPattern()
        {
            return null;
        }

        @Override
        public Result apply(PlanNode node, Captures captures, Context context)
        {
            return null;
        }
    }

    public static class FakeRule3
            implements Rule<PlanNode>
    {
        @Override
        public Pattern<PlanNode> getPattern()
        {
            return null;
        }

        @Override
        public Result apply(PlanNode node, Captures captures, Context context)
        {
            return null;
        }
    }
}
