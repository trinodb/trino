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
package io.trino.testing;

import io.trino.Session;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.planprinter.PlanPrinter;
import org.intellij.lang.annotations.Language;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class PlanDeterminismChecker
{
    private static final int MINIMUM_SUBSEQUENT_SAME_PLANS = 10;

    private final QueryRunner queryRunner;
    private final Function<String, String> planEquivalenceFunction;

    public PlanDeterminismChecker(QueryRunner queryRunner)
    {
        this.queryRunner = queryRunner;
        this.planEquivalenceFunction = Function.identity();
    }

    public void checkPlanIsDeterministic(@Language("SQL") String sql)
    {
        checkPlanIsDeterministic(queryRunner.getDefaultSession(), sql);
    }

    public void checkPlanIsDeterministic(Session session, @Language("SQL") String sql)
    {
        String previous = planEquivalenceFunction.apply(getPlanText(session, sql));
        for (int attempt = 1; attempt < MINIMUM_SUBSEQUENT_SAME_PLANS; attempt++) {
            String current = planEquivalenceFunction.apply(getPlanText(session, sql));
            assertThat(previous).isEqualTo(current);
        }
    }

    private String getPlanText(Session session, @Language("SQL") String sql)
    {
        return queryRunner.inTransaction(session, transactionSession -> {
            Plan plan = queryRunner.createPlan(transactionSession, sql);
            return PlanPrinter.textLogicalPlan(
                    plan.getRoot(),
                    plan.getTypes(),
                    queryRunner.getPlannerContext().getMetadata(),
                    queryRunner.getPlannerContext().getFunctionManager(),
                    plan.getStatsAndCosts(),
                    transactionSession,
                    0,
                    false);
        });
    }
}
