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
package io.trino.plugin.base;

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.plugin.base.ClosingBinder.closingBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

public class TestClosingBinder
{
    @Test
    public void testExecutorShutdown()
    {
        Module module = binder -> {
            binder.bind(ExecutorService.class).toInstance(newCachedThreadPool());
            closingBinder(binder).registerExecutor(ExecutorService.class);
        };
        Injector injector = getInjector(module);

        ExecutorService executorService = injector.getInstance(ExecutorService.class);
        assertThat(executorService.isShutdown()).isFalse();
        stop(injector);
        assertThat(executorService.isShutdown()).isTrue();
    }

    @Test
    public void testCloseableShutdown()
    {
        AtomicBoolean closed = new AtomicBoolean();
        Injector injector = getInjector(binder -> {
            binder.bind(Closeable.class).toInstance(() -> closed.set(true));
            closingBinder(binder).registerCloseable(Closeable.class);
        });

        assertThat(closed.get()).isFalse();
        stop(injector);
        assertThat(closed.get()).isTrue();
    }

    private static Injector getInjector(Module module)
    {
        Bootstrap app = new Bootstrap(module);

        return app.doNotInitializeLogging()
                .quiet()
                .initialize();
    }

    private static void stop(Injector injector)
    {
        LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        lifeCycleManager.stop();
    }
}
