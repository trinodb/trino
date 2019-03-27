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
package io.prestosql.ranger;

import io.airlift.log.Logger;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorSystemAccessControl;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemAccessControlFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class RangerSystemAccessControlFactory
        implements SystemAccessControlFactory
{
    private static final Logger log = Logger.get(RangerSystemAccessControlFactory.class);
    protected static final String RANGER_ACCESS_CONTROL = "ranger-access-control";

    private final ClassLoader classLoader;

    public RangerSystemAccessControlFactory(ClassLoader classLoader)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return RANGER_ACCESS_CONTROL;
    }

    @Override
    public SystemAccessControl create(Map<String, String> config)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return new ClassLoaderSafeConnectorSystemAccessControl(new RangerPluginInitializer().initRanger(config), classLoader);
        }
    }
}
