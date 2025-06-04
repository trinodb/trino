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
package io.trino.plugin.base.classloader;

import com.google.inject.Inject;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;

import static java.util.Objects.requireNonNull;

public class ClassLoaderSafeEventListener
        implements EventListener
{
    private final EventListener delegate;
    private final ClassLoader classLoader;

    @Inject
    public ClassLoaderSafeEventListener(@ForClassLoaderSafe EventListener delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            delegate.queryCreated(queryCreatedEvent);
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            delegate.queryCompleted(queryCompletedEvent);
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            delegate.splitCompleted(splitCompletedEvent);
        }
    }

    @Override
    public boolean requiresAnonymizedPlan()
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            return delegate.requiresAnonymizedPlan();
        }
    }

    @Override
    public void shutdown()
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            delegate.shutdown();
        }
    }
}
