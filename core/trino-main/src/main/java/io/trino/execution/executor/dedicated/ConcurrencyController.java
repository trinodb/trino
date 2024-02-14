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

class ConcurrencyController
{
    private static final double TARGET_UTILIZATION = 0.5;

    private int targetConcurrency;

    public ConcurrencyController(int initialConcurrency)
    {
        checkArgument(initialConcurrency > 0, "initial concurrency must be positive");
        this.targetConcurrency = initialConcurrency;
    }

    public void update(double utilization, int currentConcurrency)
    {
        if (utilization > TARGET_UTILIZATION && targetConcurrency > 1) {
            targetConcurrency--;
        }
        else if (utilization < TARGET_UTILIZATION && currentConcurrency >= targetConcurrency) {
            targetConcurrency++;
        }
    }

    public int targetConcurrency()
    {
        return targetConcurrency;
    }
}
