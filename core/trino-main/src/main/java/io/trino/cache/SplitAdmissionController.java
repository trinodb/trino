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
package io.trino.cache;

import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.cache.CacheSplitId;

import java.util.List;

public interface SplitAdmissionController
{
    /**
     * Determines if a split can be scheduled on a worker.
     *
     * @param splitId the split to schedule
     * @param address the worker to schedule the split on
     * @return true if the split can be schedule; false otherwise
     */
    boolean canScheduleSplit(CacheSplitId splitId, HostAddress address);

    /**
     * Notifies the manager that a list of splits have been scheduled.
     */
    void splitsScheduled(List<Split> splits);
}
