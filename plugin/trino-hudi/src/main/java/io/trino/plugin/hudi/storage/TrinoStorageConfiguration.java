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
package io.trino.plugin.hudi.storage;

import io.trino.plugin.hudi.io.TrinoHudiIoFactory;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StorageConfiguration;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS;
import static org.apache.hudi.common.config.HoodieStorageConfig.HOODIE_STORAGE_CLASS;

/**
 * {@link StorageConfiguration} implementation based on a config map
 */
public class TrinoStorageConfiguration
        extends StorageConfiguration<Map<String, String>>
{
    private final Map<String, String> config;

    public TrinoStorageConfiguration()
    {
        this(getDefaultConfigs());
    }

    TrinoStorageConfiguration(Map<String, String> config)
    {
        this.config = getDefaultConfigs();
        this.config.putAll(config);
    }

    private static Map<String, String> getDefaultConfigs()
    {
        Map<String, String> config = new HashMap<>();
        config.put(HOODIE_IO_FACTORY_CLASS.key(), TrinoHudiIoFactory.class.getName());
        config.put(HOODIE_STORAGE_CLASS.key(), TrinoHudiStorage.class.getName());
        return config;
    }

    @Override
    public TrinoStorageConfiguration newInstance()
    {
        return new TrinoStorageConfiguration(new HashMap<>(config));
    }

    @Override
    public Map<String, String> unwrap()
    {
        return config;
    }

    @Override
    public Map<String, String> unwrapCopy()
    {
        return new HashMap<>(config);
    }

    @Override
    public void set(String key, String value)
    {
        config.put(key, value);
    }

    @Override
    public Option<String> getString(String key)
    {
        return Option.ofNullable(config.get(key));
    }

    @Override
    public TrinoStorageConfiguration getInline()
    {
        return newInstance();
    }
}
