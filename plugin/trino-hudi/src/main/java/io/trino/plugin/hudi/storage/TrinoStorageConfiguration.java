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

import io.trino.plugin.hudi.io.HudiTrinoIOFactory;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StorageConfiguration;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS;
import static org.apache.hudi.common.config.HoodieStorageConfig.HOODIE_STORAGE_CLASS;

public class TrinoStorageConfiguration
        extends StorageConfiguration
{
    private final Map<String, String> configMap;

    public TrinoStorageConfiguration()
    {
        this(getDefaultConfigs());
    }

    public TrinoStorageConfiguration(Map<String, String> configMap)
    {
        this.configMap = configMap;
    }

    public static Map<String, String> getDefaultConfigs()
    {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(HOODIE_IO_FACTORY_CLASS.key(), HudiTrinoIOFactory.class.getName());
        configMap.put(HOODIE_STORAGE_CLASS.key(), HudiTrinoStorage.class.getName());
        return configMap;
    }

    @Override
    public StorageConfiguration newInstance()
    {
        return new TrinoStorageConfiguration(new HashMap<>(configMap));
    }

    @Override
    public Object unwrap()
    {
        return configMap;
    }

    @Override
    public Object unwrapCopy()
    {
        return new HashMap<>(configMap);
    }

    @Override
    public void set(String key, String value)
    {
        configMap.put(key, value);
    }

    @Override
    public Option<String> getString(String key)
    {
        return Option.ofNullable(configMap.get(key));
    }

    @Override
    public StorageConfiguration getInline()
    {
        return newInstance();
    }
}
