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
package io.prestosql.server.extension;

import io.airlift.log.Logger;

import java.util.Optional;
import java.util.Properties;

public class ExtensionFactory
{
    private static final Logger LOG = Logger.get(ExtensionFactory.class);

    public static final ExtensionFactory INSTANCE = new ExtensionFactory();

    /**
     * Create extension.
     *
     * @param extensionImplClass The class of the extension implementation.
     * @param props Properties to initialize the extension
     * @param <T> Type parameter
     * @return An Optional of the extension, empty if failed to create it.
     */
    public <T extends Extension> Optional<T> createExtension(Class<T> extensionImplClass, Properties props)
    {
        try {
            T extension = extensionImplClass.getConstructor().newInstance();
            extension.init(props);
            LOG.info("Created extension " + extensionImplClass.getName());
            return Optional.of(extension);
        }
        catch (Exception e) {
            LOG.error("Failed to create extension " + extensionImplClass.getName(), e);
            return Optional.empty();
        }
    }

    /**
     *
     * @param extensionImplClass The extension implementation class name.
     * @param props Properties to initialize the extension.
     * @param extensionProtoType The super-class/interface that the extension should implement.
     * @param <T>
     * @return An Optional of the extension, empty if failed to create it.
     */
    public <T extends Extension> Optional<? extends T> createExtension(String extensionImplClass, Properties props, Class<T> extensionProtoType)
    {
        try {
            Class<? extends T> extensionClass = Class.forName(extensionImplClass).asSubclass(extensionProtoType);
            return createExtension(extensionClass, props);
        }
        catch (Exception e) {
            LOG.error("Failed to create extension " + extensionImplClass, e);
            return Optional.empty();
        }
    }
}
