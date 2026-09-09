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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.isFinite;

class ConcurrencyController
{
    private static final double TARGET_UTILIZATION = 0.5;

    /**
     * Half-width of the band around the target in which the concurrency is left alone. Output
     * buffer utilization essentially never lands exactly on the target, so without a deadband
     * every adjustment moves the target by one in one direction or the other and the controller
     * hunts around the setpoint instead of settling on it.
     */
    private static final double DEADBAND = 0.1;

    private static final double LOWER_BOUND = TARGET_UTILIZATION - DEADBAND;
    private static final double UPPER_BOUND = TARGET_UTILIZATION + DEADBAND;

    private int targetConcurrency;

    public ConcurrencyController(int initialConcurrency)
    {
        checkArgument(initialConcurrency > 0, "initial concurrency must be positive");
        this.targetConcurrency = initialConcurrency;
    }

    public void update(double utilization, int currentConcurrency)
    {
        checkArgument(isFinite(utilization), "utilization must be finite");
        checkArgument(utilization >= 0, "utilization is negative");
        checkArgument(currentConcurrency >= 0, "currentConcurrency is negative");

        if (utilization > UPPER_BOUND && targetConcurrency > 1) {
            targetConcurrency--;
        }
        else if (utilization < LOWER_BOUND && currentConcurrency >= targetConcurrency) {
            targetConcurrency++;
        }
    }

    public int targetConcurrency()
    {
        return targetConcurrency;
    }
}
