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
package io.varada.tools.configuration;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class MultiPrefixConfigurationWrapper
        implements Map<String, String>
{
    public static final String WARP_SPEED = "warp-speed";
    public static final String LOCAL_DATA_STORAGE = "local-data-storage";
    public static final String WARP_SPEED_PREFIX = WARP_SPEED + ".";
    public static final String LOCAL_DATA_STORAGE_PREFIX = LOCAL_DATA_STORAGE + ".";
    private final Map<String, String> originalConfig;

    public MultiPrefixConfigurationWrapper(Map<String, String> originalConfig)
    {
        this.originalConfig = originalConfig;
    }

    @Override
    public String getOrDefault(Object key, String defaultValue)
    {
        return originalConfig.getOrDefault(WARP_SPEED_PREFIX + key, originalConfig.getOrDefault(LOCAL_DATA_STORAGE_PREFIX + key, originalConfig.getOrDefault((String) key, defaultValue)));
    }

    @Override
    public int size()
    {
        return originalConfig.size();
    }

    @Override
    public boolean isEmpty()
    {
        return originalConfig.isEmpty();
    }

    @Override
    public boolean containsKey(Object key)
    {
        return originalConfig.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value)
    {
        return originalConfig.containsValue(value);
    }

    @Override
    public String put(String key, String value)
    {
        return originalConfig.put(key, value);
    }

    @Override
    public String remove(Object key)
    {
        return originalConfig.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m)
    {
        originalConfig.putAll(m);
    }

    @Override
    public void clear()
    {
        originalConfig.clear();
    }

    @Override
    public Set<String> keySet()
    {
        return originalConfig.keySet();
    }

    @Override
    public Collection<String> values()
    {
        return originalConfig.values();
    }

    @Override
    public Set<Entry<String, String>> entrySet()
    {
        return originalConfig.entrySet();
    }

    @Override
    public String get(Object key)
    {
        return originalConfig.getOrDefault(WARP_SPEED_PREFIX + key, originalConfig.getOrDefault(LOCAL_DATA_STORAGE_PREFIX + key, originalConfig.get(key)));
    }
}
