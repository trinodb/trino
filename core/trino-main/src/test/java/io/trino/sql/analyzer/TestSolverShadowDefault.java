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
package io.trino.sql.analyzer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// Pins the experiment's wiring: every test JVM in this branch runs with the solver shadows
/// enabled (the root pom passes -Dtrino.solver-shadow=true and
/// -Dtrino.solver-expression-shadow=true to surefire), so a green build IS a full differential
/// run of solver resolution and expression analysis against the engine.
class TestSolverShadowDefault
{
    @Test
    void testShadowEnabledByDefault()
    {
        assertThat(SolverShadow.isEnabled())
                .as("trino.solver-shadow must reach test JVMs (root pom air.test.jvm.additional-arguments)")
                .isTrue();
    }

    @Test
    void testExpressionShadowEnabledByDefault()
    {
        assertThat(SolverExpressionShadow.isEnabled())
                .as("trino.solver-expression-shadow must reach test JVMs (root pom air.test.jvm.additional-arguments)")
                .isTrue();
    }
}
