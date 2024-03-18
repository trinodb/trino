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
package io.varada.tools.util;

public class StopWatch
{
    private final com.google.common.base.Stopwatch stopwatch;

    public StopWatch()
    {
        stopwatch = com.google.common.base.Stopwatch.createUnstarted();
    }

    public void start()
    {
        stopwatch.start();
    }

    public void stop()
    {
        stopwatch.stop();
    }

    public void reset()
    {
        stopwatch.reset();
    }

    public long getNanoTime()
    {
        return stopwatch.elapsed().toNanos();
    }

    public long getTime()
    {
        return this.getNanoTime() / 1000000L;
    }

    public boolean isStarted()
    {
        return stopwatch.isRunning();
    }
}
