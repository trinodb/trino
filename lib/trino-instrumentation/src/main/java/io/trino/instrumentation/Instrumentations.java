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
package io.trino.instrumentation;

import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemAccessControlFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Optional;

import static java.lang.System.getenv;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Arrays.stream;

public class Instrumentations
{
    private static final MethodHandle CONNECTOR_INSTRUMENTED = getMethodHandle(Connector.class, String.class, Connector.class); // (catalogName, delegate)Connector
    private static final MethodHandle SYSTEM_ACCESS_CONTROL_INSTRUMENTED = getMethodHandle(SystemAccessControl.class, SystemAccessControl.class); // (delegate)SystemAccessControl
    private static final MethodHandle CONNECTOR_ACCESS_CONTROL_INSTRUMENTED = getMethodHandle(ConnectorAccessControl.class, String.class, ConnectorAccessControl.class); // (catalogName, delegate)ConnectorAccessControl

    // TODO: Improve that poor man's on/off switch
    private static final boolean ENABLED = Optional.ofNullable(getenv("INSTRUMENTATION_ENABLED"))
            .map(Boolean::parseBoolean)
            .orElse(true);

    private Instrumentations() {}

    public static ConnectorFactory instrumentedConnectorFactory(ConnectorFactory delegate)
    {
        if (!ENABLED) {
            return delegate;
        }

        return new ConnectorFactory() {
            @Override
            public String getName()
            {
                return delegate.getName();
            }

            @Override
            public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
            {
                // We need to use the delegate class loader as it comes from the plugin
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(delegate.getClass().getClassLoader())) {
                    return (Connector) CONNECTOR_INSTRUMENTED.invoke(catalogName, delegate.create(catalogName, config, context));
                }
                catch (RuntimeException e) {
                    throw e;
                }
                catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static SystemAccessControlFactory instrumentedSystemAccessControlFactory(SystemAccessControlFactory delegate)
    {
        if (!ENABLED) {
            return delegate;
        }

        return new SystemAccessControlFactory() {
            @Override
            public String getName()
            {
                return delegate.getName();
            }

            @Override
            public SystemAccessControl create(Map<String, String> config)
            {
                // We need to use the delegate class loader as it comes from the plugin
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(delegate.getClass().getClassLoader())) {
                    return (SystemAccessControl) SYSTEM_ACCESS_CONTROL_INSTRUMENTED.invoke(delegate.create(config));
                }
                catch (RuntimeException e) {
                    throw e;
                }
                catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static ConnectorAccessControl instrumentedConnectorAccessControl(String catalogName, ConnectorAccessControl delegate)
    {
        if (!ENABLED) {
            return delegate;
        }

        // We need to use the delegate class loader as it comes from the plugin
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(delegate.getClass().getClassLoader())) {
            return (ConnectorAccessControl) CONNECTOR_ACCESS_CONTROL_INSTRUMENTED.invoke(catalogName, delegate);
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> Class<T> instrumentationClass(Class<T> interfaceClazz)
    {
        try {
            return (Class<T>) MethodHandles.lookup().findClass(delegatedClassName(interfaceClazz));
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static String delegatedClassName(Class<?> interfaceClazz)
    {
        return interfaceClazz.getPackageName() + ".Instrumented" + interfaceClazz.getSimpleName();
    }

    private static MethodHandle getMethodHandle(Class<?> interfaceClass, Class<?>... arguments)
    {
        try {
            return MethodHandles.lookup().findStatic(instrumentationClass(interfaceClass), "instrument", methodType(interfaceClass, stream(arguments).toList()));
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
