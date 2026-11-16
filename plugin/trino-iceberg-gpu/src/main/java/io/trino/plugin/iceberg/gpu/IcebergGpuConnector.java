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
package io.trino.plugin.iceberg.gpu;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static java.util.Objects.requireNonNull;

/**
 * The Iceberg connector with its metadata, split manager and page sources wrapped; every other
 * part, such as writes, procedures and system tables, is the Iceberg connector's.
 */
final class IcebergGpuConnector
        implements InvocationHandler
{
    private final Connector delegate;
    private final boolean deviceAvailable;

    private IcebergGpuConnector(Connector delegate, boolean deviceAvailable)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.deviceAvailable = deviceAvailable;
    }

    static Connector wrap(Connector delegate, boolean deviceAvailable)
    {
        return (Connector) Proxy.newProxyInstance(
                IcebergGpuConnector.class.getClassLoader(),
                new Class<?>[] {Connector.class},
                new IcebergGpuConnector(delegate, deviceAvailable));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable
    {
        if (method.getDeclaringClass() == Object.class) {
            return switch (method.getName()) {
                case "equals" -> proxy == args[0];
                case "hashCode" -> System.identityHashCode(proxy);
                default -> "IcebergGpuConnector(" + delegate + ")";
            };
        }
        int arity = args == null ? 0 : args.length;
        switch (method.getName()) {
            case "getMetadata" -> {
                if (arity == 2) {
                    ConnectorTransactionHandle transaction = (ConnectorTransactionHandle) args[1];
                    return IcebergGpuMetadata.wrap(delegate.getMetadata((ConnectorSession) args[0], transaction), delegate.getSplitManager(), transaction, deviceAvailable);
                }
            }
            case "getSplitManager" -> {
                return new IcebergGpuSplitManager(delegate.getSplitManager());
            }
            case "getPageSourceProviderFactory" -> {
                ConnectorPageSourceProviderFactory factory = delegate.getPageSourceProviderFactory();
                return (ConnectorPageSourceProviderFactory) memory -> new IcebergGpuPageSourceProvider(factory.createPageSourceProvider(memory));
            }
            default -> {
                // the Iceberg connector's
            }
        }
        try {
            return method.invoke(delegate, args);
        }
        catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
