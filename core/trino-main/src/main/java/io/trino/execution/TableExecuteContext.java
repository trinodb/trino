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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TableExecuteContext
{
    @GuardedBy("this")
    private List<Object> splitsInfo;
    @GuardedBy("this")
    private Map<String, Long> metrics;

    public synchronized void setSplitsInfo(List<Object> splitsInfo)
    {
        requireNonNull(splitsInfo, "splitsInfo is null");
        if (this.splitsInfo != null) {
            throw new IllegalStateException("splitsInfo already set to " + this.splitsInfo);
        }
        this.splitsInfo = ImmutableList.copyOf(splitsInfo);
    }

    public synchronized List<Object> getSplitsInfo()
    {
        if (splitsInfo == null) {
            throw new IllegalStateException("splitsInfo not set yet");
        }
        return splitsInfo;
    }

    public synchronized Optional<Map<String, Long>> getMetrics()
    {
        return Optional.ofNullable(metrics);
    }

    public synchronized void setMetrics(Map<String, Long> newMetrics)
    {
        checkState(metrics == null, "metrics already set to %s", metrics);
        metrics = ImmutableMap.copyOf(newMetrics);
    }
}
