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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class FailingScheduledExecutorService
        extends ForwardingScheduledExecutorService
{
    public static final ScheduledExecutorService INSTANCE = new FailingScheduledExecutorService();

    @Override
    protected ScheduledExecutorService getDelegate()
    {
        throw new UnsupportedOperationException("This ScheduledExecutorService should not be used");
    }

    @Override
    public void shutdown() {}

    @Override
    public List<Runnable> shutdownNow()
    {
        return ImmutableList.of();
    }

    @Override
    public boolean isShutdown()
    {
        return true;
    }

    @Override
    public boolean isTerminated()
    {
        return true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
    {
        return true;
    }
}
