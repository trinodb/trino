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

import org.apache.hudi.io.storage.BaseTestStorageConfiguration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS;
import static org.apache.hudi.common.config.HoodieStorageConfig.HOODIE_STORAGE_CLASS;
import static org.assertj.core.api.Assertions.assertThat;

final class TestTrinoStorageConfiguration
        extends BaseTestStorageConfiguration<Map<String, String>>
{
    @Override
    protected TrinoStorageConfiguration getStorageConfiguration(Map<String, String> config)
    {
        return new TrinoStorageConfiguration(config);
    }

    @Override
    protected Map<String, String> getConf(Map<String, String> config)
    {
        return config;
    }

    @Test
    void testConfigOverrides()
    {
        String overriddenClassName = "NewIoFactoryClass";
        Map<String, String> providedConfig = new HashMap<>();
        providedConfig.put(HOODIE_IO_FACTORY_CLASS.key(), overriddenClassName);
        TrinoStorageConfiguration config = getStorageConfiguration(providedConfig);
        assertThat(config.getString(HOODIE_IO_FACTORY_CLASS.key()).get()).isEqualTo(overriddenClassName);
        assertThat(config.getString(HOODIE_STORAGE_CLASS.key()).get()).isEqualTo(TrinoHudiStorage.class.getName());
    }
}
