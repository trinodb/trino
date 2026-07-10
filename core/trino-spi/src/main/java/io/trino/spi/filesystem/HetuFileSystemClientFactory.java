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
package io.trino.spi.filesystem;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

/**
 * HetuFileSystemClientFactory provides HetuFileSystemClient objects
 *
 * @since 2020-03-30
 */
public interface HetuFileSystemClientFactory
{
    /**
     * Get a {@link HetuFileSystemClient} from a configuration Properties object.
     * This object can either be constructed manually or read through a config file.
     *
     * <p>
     * In the config object, {@code fs.client.type} must be specified. For different filesystems, different
     * configurations will be required. If any delegation is needed setup them here as well.
     * </p>
     *
     * @param properties A property object to generate filesystem client from.
     * @return A {@link HetuFileSystemClient} object.
     * @throws IOException If any IOException occur reading property files or setup filesystem client.
     */
    HetuFileSystemClient getFileSystemClient(Properties properties)
            throws IOException;

    HetuFileSystemClient getFileSystemClient(Properties properties, Path root)
            throws IOException;

    String getName();
}
