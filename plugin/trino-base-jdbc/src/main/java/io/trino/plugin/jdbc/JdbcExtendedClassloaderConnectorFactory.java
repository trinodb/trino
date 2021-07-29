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

import com.google.inject.Module;
import io.airlift.log.Logger;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

public class JdbcExtendedClassloaderConnectorFactory
        extends JdbcConnectorFactory
{
    private static final Logger log = Logger.get(JdbcConnectorFactory.class);

    private static final String CLASSPATH_EXTENSION_PROPERTY_NAME = "classpath-extension";

    public JdbcExtendedClassloaderConnectorFactory(String name, Module module)
    {
        super(name, module);
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        if (!requiredConfig.containsKey(CLASSPATH_EXTENSION_PROPERTY_NAME)) {
            return super.create(catalogName, requiredConfig, context);
        }

        ClassLoader classLoader = getExtendedClassLoader(requiredConfig, context);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return super.create(catalogName, requiredConfig, context);
        }
    }

    private static ClassLoader getExtendedClassLoader(Map<String, String> requiredConfig, ConnectorContext context)
    {
        String extension = requiredConfig.remove(CLASSPATH_EXTENSION_PROPERTY_NAME);
        try {
            log.info("Extending current Jdbc Connector class-path with: '{}'.", extension);
            URL url = new URL(extension);
            return new URLClassLoader(new URL[]{url}, context.duplicatePluginClassLoader());
        }
        catch (MalformedURLException e) {
            log.error(e, "Failed to extend Jdbc Connector class-path.");
            // TODO (question to the reviewer) What more suitable runtime exception to use here?
            throw new RuntimeException(e);
        }
    }
}
