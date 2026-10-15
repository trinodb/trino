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
package io.trino.sql.planner;

import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public final class PlanningInterruption
{
    private PlanningInterruption() {}

    /**
     * Planning thread is interrupted when the query fails, e.g. for exceeding the planning time limit.
     * CPU-bound planning never blocks, so it has to poll the flag to stop.
     */
    public static void checkPlanningNotInterrupted()
    {
        if (Thread.currentThread().isInterrupted()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Planning was interrupted");
        }
    }
}
