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
package io.trino.sql.planner.runtimeconstraint;

import io.trino.Session;
import io.trino.spi.predicate.Domain;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.FUNCTION_PREIMAGES_ENABLED;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.ORDINARY;
import static io.trino.type.Reals.toReal;
import static org.assertj.core.api.Assertions.assertThat;

final class TestRuntimeConstraintTransform
{
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCastSelectsCoercionThroughFunctionPreimages(boolean preimagesEnabled)
    {
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(FUNCTION_PREIMAGES_ENABLED, Boolean.toString(preimagesEnabled))
                .build();
        RuntimeConstraintTransform.Context context = new RuntimeConstraintTransform.Context(
                PLANNER_CONTEXT.getMetadata(),
                PLANNER_CONTEXT.getFunctionManager(),
                PLANNER_CONTEXT.getTypeManager(),
                session);
        RuntimeMembershipPayload input = new RuntimeMembershipPayload(List.of(Domain.multipleValues(DOUBLE, List.of(Double.NaN, 2.0))), ORDINARY);

        RuntimeMembershipPayload result = RuntimeConstraintTransform.cast(REAL).apply(input, context);

        // Saturated floor casts cannot represent NaN, so the legacy coercion widens to every non-null value
        Domain expected = preimagesEnabled
                ? Domain.multipleValues(REAL, List.of(toReal(Float.NaN), toReal(2)))
                : Domain.notNull(REAL);
        assertThat(result.scalarDomains()).containsExactly(expected);
    }
}
