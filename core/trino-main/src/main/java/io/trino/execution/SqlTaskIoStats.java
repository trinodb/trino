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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import static java.lang.Math.max;

public final class SqlTaskIoStats
{
    private final CounterStat inputDataSize = new CounterStat();
    private final CounterStat inputPositions = new CounterStat();
    private final CounterStat outputDataSize = new CounterStat();
    private final CounterStat outputPositions = new CounterStat();

    @GuardedBy("this")
    private SqlTaskIoTotals recordedTotals = SqlTaskIoTotals.EMPTY;

    @Managed
    @Nested
    public CounterStat getInputDataSize()
    {
        return inputDataSize;
    }

    @Managed
    @Nested
    public CounterStat getInputPositions()
    {
        return inputPositions;
    }

    @Managed
    @Nested
    public CounterStat getOutputDataSize()
    {
        return outputDataSize;
    }

    @Managed
    @Nested
    public CounterStat getOutputPositions()
    {
        return outputPositions;
    }

    /**
     * Records the growth of the cumulative totals since the highest totals seen so far.
     */
    public synchronized void update(SqlTaskIoTotals totals)
    {
        inputDataSize.update(max(0, totals.inputDataSize() - recordedTotals.inputDataSize()));
        inputPositions.update(max(0, totals.inputPositions() - recordedTotals.inputPositions()));
        outputDataSize.update(max(0, totals.outputDataSize() - recordedTotals.outputDataSize()));
        outputPositions.update(max(0, totals.outputPositions() - recordedTotals.outputPositions()));
        recordedTotals = new SqlTaskIoTotals(
                max(totals.inputDataSize(), recordedTotals.inputDataSize()),
                max(totals.inputPositions(), recordedTotals.inputPositions()),
                max(totals.outputDataSize(), recordedTotals.outputDataSize()),
                max(totals.outputPositions(), recordedTotals.outputPositions()));
    }
}
