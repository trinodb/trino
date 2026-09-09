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
package io.trino.execution.executor.dedicated;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestConcurrencyController
{
    @Test
    public void testHoldsSteadyInsideDeadband()
    {
        ConcurrencyController controller = new ConcurrencyController(4);

        // a task running slightly under the target utilization must not have its concurrency
        // walked up on every tick
        for (int i = 0; i < 100; i++) {
            controller.update(0.45, controller.targetConcurrency());
        }
        assertThat(controller.targetConcurrency()).isEqualTo(4);

        // and slightly over must not walk it down
        for (int i = 0; i < 100; i++) {
            controller.update(0.55, controller.targetConcurrency());
        }
        assertThat(controller.targetConcurrency()).isEqualTo(4);
    }

    @Test
    public void testIncreasesWhenUnderutilized()
    {
        ConcurrencyController controller = new ConcurrencyController(1);

        controller.update(0.1, 1);

        assertThat(controller.targetConcurrency()).isEqualTo(2);
    }

    @Test
    public void testDoesNotIncreaseWhenBelowTarget()
    {
        // the task is not even running as many drivers as it is already allowed, so raising
        // the target cannot help
        ConcurrencyController controller = new ConcurrencyController(4);

        controller.update(0.1, 2);

        assertThat(controller.targetConcurrency()).isEqualTo(4);
    }

    @Test
    public void testDecreasesWhenOverutilized()
    {
        ConcurrencyController controller = new ConcurrencyController(4);

        controller.update(0.9, 4);

        assertThat(controller.targetConcurrency()).isEqualTo(3);
    }

    @Test
    public void testNeverDropsBelowOne()
    {
        ConcurrencyController controller = new ConcurrencyController(1);

        for (int i = 0; i < 10; i++) {
            controller.update(1.0, 1);
        }

        assertThat(controller.targetConcurrency()).isEqualTo(1);
    }

    @Test
    public void testRejectsInvalidInput()
    {
        assertThatThrownBy(() -> new ConcurrencyController(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("initial concurrency must be positive");

        ConcurrencyController controller = new ConcurrencyController(1);

        assertThatThrownBy(() -> controller.update(Double.NaN, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("utilization must be finite");

        assertThatThrownBy(() -> controller.update(-1, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("utilization is negative");

        assertThatThrownBy(() -> controller.update(0.5, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("currentConcurrency is negative");
    }
}
