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

import java.util.List;
import java.util.concurrent.Callable;

final class Commands
{
    private Commands() {}

    public static int runCommand(List<Module> modules, Class<? extends Callable<Integer>> commandExecution)
    {
        Bootstrap app = new Bootstrap(
                ImmutableList.<Module>builder()
                        .addAll(modules)
                        .add(binder -> binder.bind(commandExecution))
                        .build());

        Injector injector = app
                .initialize();

        try {
            return injector.getInstance(commandExecution)
                    .call();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
