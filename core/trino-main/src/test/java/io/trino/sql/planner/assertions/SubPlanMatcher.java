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
package io.trino.sql.planner.assertions;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.cost.StatsCalculator;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.SubPlan;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class SubPlanMatcher
{
    private final PlanFragmentMatcher fragmentMatcher;
    private final List<SubPlanMatcher> children;

    public static Builder builder()
    {
        return new Builder();
    }

    public SubPlanMatcher(
            PlanFragmentMatcher fragmentMatcher,
            List<SubPlanMatcher> children)
    {
        this.fragmentMatcher = requireNonNull(fragmentMatcher, "fragmentMatcher is null");
        this.children = requireNonNull(children, "children is null");
    }

    public boolean matches(SubPlan subPlan, StatsCalculator statsCalculator, Session session, Metadata metadata)
    {
        if (subPlan.getChildren().size() != children.size()) {
            // Shape of the plan does not match
            return false;
        }

        for (int i = 0; i < children.size(); i++) {
            if (!children.get(i).matches(subPlan.getChildren().get(i), statsCalculator, session, metadata)) {
                return false;
            }
        }

        return fragmentMatcher.matches(subPlan.getFragment(), statsCalculator, session, metadata);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(fragmentMatcher.toString()).append("\n");
        for (SubPlanMatcher child : children) {
            builder.append(child.toString());
        }
        return builder.toString();
    }

    public static class Builder
    {
        private PlanFragmentMatcher fragmentMatcher;
        private List<SubPlanMatcher> children = ImmutableList.of();

        public Builder fragmentMatcher(Function<PlanFragmentMatcher.Builder, PlanFragmentMatcher.Builder> fragmentBuilder)
        {
            this.fragmentMatcher = fragmentBuilder.apply(PlanFragmentMatcher.builder()).build();
            return this;
        }

        @SafeVarargs
        public final Builder children(Function<Builder, Builder>... children)
        {
            this.children = Arrays.stream(children)
                    .map(child -> child.apply(new Builder()).build())
                    .collect(toImmutableList());
            return this;
        }

        public SubPlanMatcher build()
        {
            return new SubPlanMatcher(fragmentMatcher, children);
        }
    }
}
