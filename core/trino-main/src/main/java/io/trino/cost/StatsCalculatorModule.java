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
package io.trino.cost;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.trino.sql.PlannerContext;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.List;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.Objects.requireNonNull;

public class StatsCalculatorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(StatsNormalizer.class).in(Scopes.SINGLETON);
        binder.bind(ScalarStatsCalculator.class).in(Scopes.SINGLETON);
        binder.bind(FilterStatsCalculator.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, new TypeLiteral<List<ComposableStatsCalculator.Rule<?>>>() {})
                .setDefault().toProvider(StatsRulesProvider.class).in(Scopes.SINGLETON);
        binder.bind(StatsCalculator.class).to(ComposableStatsCalculator.class).in(Scopes.SINGLETON);
    }

    public static class StatsRulesProvider
            implements Provider<List<ComposableStatsCalculator.Rule<?>>>
    {
        private final PlannerContext plannerContext;
        private final ScalarStatsCalculator scalarStatsCalculator;
        private final FilterStatsCalculator filterStatsCalculator;
        private final StatsNormalizer normalizer;

        @Inject
        public StatsRulesProvider(PlannerContext plannerContext, ScalarStatsCalculator scalarStatsCalculator, FilterStatsCalculator filterStatsCalculator, StatsNormalizer normalizer)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.scalarStatsCalculator = requireNonNull(scalarStatsCalculator, "scalarStatsCalculator is null");
            this.filterStatsCalculator = requireNonNull(filterStatsCalculator, "filterStatsCalculator is null");
            this.normalizer = requireNonNull(normalizer, "normalizer is null");
        }

        @Override
        public List<ComposableStatsCalculator.Rule<?>> get()
        {
            ImmutableList.Builder<ComposableStatsCalculator.Rule<?>> rules = ImmutableList.builder();

            rules.add(new OutputStatsRule());
            rules.add(new TableScanStatsRule(plannerContext.getMetadata(), normalizer));
            rules.add(new SimpleFilterProjectSemiJoinStatsRule(plannerContext.getMetadata(), normalizer, filterStatsCalculator)); // this must be before FilterStatsRule
            rules.add(new FilterProjectAggregationStatsRule(normalizer, filterStatsCalculator)); // this must be before FilterStatsRule
            rules.add(new FilterStatsRule(normalizer, filterStatsCalculator));
            rules.add(new ValuesStatsRule(plannerContext));
            rules.add(new LimitStatsRule(normalizer));
            rules.add(new DistinctLimitStatsRule(normalizer));
            rules.add(new TopNStatsRule(normalizer));
            rules.add(new EnforceSingleRowStatsRule(normalizer));
            rules.add(new ProjectStatsRule(scalarStatsCalculator, normalizer));
            rules.add(new ExchangeStatsRule(normalizer));
            rules.add(new JoinStatsRule(filterStatsCalculator, normalizer));
            rules.add(new SpatialJoinStatsRule(filterStatsCalculator, normalizer));
            rules.add(new AggregationStatsRule(normalizer));
            rules.add(new UnionStatsRule(normalizer));
            rules.add(new AssignUniqueIdStatsRule());
            rules.add(new SemiJoinStatsRule());
            rules.add(new RowNumberStatsRule(normalizer));
            rules.add(new SampleStatsRule(normalizer));
            rules.add(new SortStatsRule());

            return rules.build();
        }
    }
}
