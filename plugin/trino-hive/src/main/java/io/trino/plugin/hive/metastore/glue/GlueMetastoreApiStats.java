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
package io.trino.plugin.hive.metastore.glue;

import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class GlueMetastoreApiStats
{
    private final TimeStat time = new TimeStat(MILLISECONDS);
    private final CounterStat totalFailures = new CounterStat();

    public <V, E extends Exception> V call(ThrowingCallable<V, E> callable)
            throws E
    {
        try (TimeStat.BlockTimer ignored = time.time()) {
            return callable.call();
        }
        catch (Exception e) {
            totalFailures.update(1);
            throw e;
        }
    }

    @Managed
    @Nested
    public TimeStat getTime()
    {
        return time;
    }

    @Managed
    @Nested
    public CounterStat getTotalFailures()
    {
        return totalFailures;
    }

    public void recordCall(long executionTimeInMillis, boolean failure)
    {
        time.add(executionTimeInMillis, MILLISECONDS);
        if (failure) {
            totalFailures.update(1);
        }
    }

    public interface ThrowingCallable<V, E extends Exception>
    {
        V call()
                throws E;
    }
}
