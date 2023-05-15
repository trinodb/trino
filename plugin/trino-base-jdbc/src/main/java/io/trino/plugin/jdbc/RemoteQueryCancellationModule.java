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
package io.trino.plugin.jdbc;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.CatalogName;

import java.util.concurrent.ExecutorService;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class RemoteQueryCancellationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(conditionalModule(
                RemoteQueryCancellationConfig.class,
                RemoteQueryCancellationConfig::isRemoteQueryCancellationEnabled,
                bindForRecordCursor()));
    }

    private static Module bindForRecordCursor()
    {
        return binder -> {
            newOptionalBinder(binder, Key.get(ExecutorService.class, ForRecordCursor.class))
                    .setBinding()
                    .toProvider(RecordCursorExecutorServiceProvider.class)
                    .in(Scopes.SINGLETON);
        };
    }

    private static class RecordCursorExecutorServiceProvider
            implements Provider<ExecutorService>
    {
        private final CatalogName catalogName;

        @Inject
        public RecordCursorExecutorServiceProvider(CatalogName catalogName)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
        }

        @Override
        public ExecutorService get()
        {
            return newCachedThreadPool(daemonThreadsNamed(format("%s-record-cursor-%%d", catalogName)));
        }
    }
}
