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

import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.ptf.Argument;
import io.trino.spi.ptf.ArgumentSpecification;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.ptf.ReturnTypeSpecification;
import io.trino.spi.ptf.TableFunctionAnalysis;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ClassLoaderSafeConnectorTableFunction
        implements ConnectorTableFunction
{
    private final ConnectorTableFunction delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeConnectorTableFunction(@ForClassLoaderSafe ConnectorTableFunction delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public String getSchema()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getSchema();
        }
    }

    @Override
    public String getName()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getName();
        }
    }

    @Override
    public List<ArgumentSpecification> getArguments()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getArguments();
        }
    }

    @Override
    public ReturnTypeSpecification getReturnTypeSpecification()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getReturnTypeSpecification();
        }
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.analyze(session, transaction, arguments);
        }
    }
}
