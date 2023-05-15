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
package io.trino.sql.rewrite;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Statement;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class StatementRewrite
{
    private final Set<Rewrite> rewrites;

    @Inject
    public StatementRewrite(Set<Rewrite> rewrites)
    {
        this.rewrites = ImmutableSet.copyOf(requireNonNull(rewrites, "rewrites is null"));
    }

    public Statement rewrite(
            AnalyzerFactory analyzerFactory,
            Session session,
            Statement node,
            List<Expression> parameters,
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector)
    {
        for (Rewrite rewrite : rewrites) {
            node = requireNonNull(
                    rewrite.rewrite(
                            analyzerFactory,
                            session,
                            node,
                            parameters,
                            parameterLookup,
                            warningCollector,
                            planOptimizersStatsCollector),
                    "Statement rewrite returned null");
        }
        return node;
    }

    public interface Rewrite
    {
        Statement rewrite(
                AnalyzerFactory analyzerFactory,
                Session session,
                Statement node,
                List<Expression> parameters,
                Map<NodeRef<Parameter>, Expression> parameterLookup,
                WarningCollector warningCollector,
                PlanOptimizersStatsCollector planOptimizersStatsCollector);
    }
}
