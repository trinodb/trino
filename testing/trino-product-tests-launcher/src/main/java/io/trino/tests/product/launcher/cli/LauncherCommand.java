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
package io.trino.tests.product.launcher.cli;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.trino.tests.product.launcher.Extensions;
import io.trino.tests.product.launcher.LauncherModule;

import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;

public abstract class LauncherCommand
        implements Callable<Integer>
{
    private final Class<? extends Callable<Integer>> commandClass;
    private final OutputStream outputStream;
    protected final Extensions extensions;

    public LauncherCommand(Class<? extends Callable<Integer>> commandClass, OutputStream outputStream, Extensions extensions)
    {
        this.commandClass = requireNonNull(commandClass, "commandClass is null");
        this.outputStream = requireNonNull(outputStream, "outputStream is null");
        this.extensions = requireNonNull(extensions, "extensions is null");
    }

    abstract List<Module> getCommandModules();

    @Override
    public Integer call()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                ImmutableList.<Module>builder()
                        .add(new LauncherModule(outputStream))
                        .addAll(getCommandModules())
                        .add(binder -> binder.bind(commandClass))
                        .build());

        Injector injector = app
                .initialize();

        try {
            return injector.getInstance(commandClass)
                    .call();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
