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

import io.trino.spi.Page;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorState;

import java.util.List;
import java.util.Optional;

public class ClassLoaderSafeTableFunctionDataProcessor
        implements TableFunctionDataProcessor
{
    private final TableFunctionDataProcessor delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeTableFunctionDataProcessor(TableFunctionDataProcessor delegate, ClassLoader classLoader)
    {
        this.delegate = delegate;
        this.classLoader = classLoader;
    }

    @Override
    public TableFunctionProcessorState process(List<Optional<Page>> input)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.process(input);
        }
    }
}
