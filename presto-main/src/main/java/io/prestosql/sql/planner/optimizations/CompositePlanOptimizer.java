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
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.PlanNode;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class CompositePlanOptimizer
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        for (PlanOptimizer optimizer : getOptimizers()) {
            plan = optimizer.optimize(plan, session, symbolAllocator.getTypes(), symbolAllocator, idAllocator, warningCollector);
            requireNonNull(plan, format("%s returned a null plan", optimizer.getClass().getName()));
        }
        return plan;
    }

    protected abstract Iterable<? extends PlanOptimizer> getOptimizers();

    public static PlanOptimizer of(PlanOptimizer... optimizers)
    {
        return of(ImmutableList.copyOf(optimizers));
    }

    public static PlanOptimizer of(Iterable<PlanOptimizer> optimizers)
    {
        List<PlanOptimizer> list = ImmutableList.copyOf(optimizers);
        return new CompositePlanOptimizer()
        {
            @Override
            protected Iterable<? extends PlanOptimizer> getOptimizers()
            {
                return list;
            }
        };
    }
}
