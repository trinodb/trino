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
package io.trino.util;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.fromProperties;

public final class ConfigurationLoader
{
    private ConfigurationLoader() {}

    public static Map<String, String> loadProperties()
            throws IOException
    {
        Map<String, String> result = new TreeMap();
        String configFile = System.getProperty("config");
        if (configFile != null) {
            result.putAll(loadPropertiesFrom(configFile));
        }

        result.putAll(getSystemProperties());
        return ImmutableSortedMap.copyOf(result);
    }

    public static Map<String, String> loadPropertiesFrom(String path)
            throws IOException
    {
        Properties properties = new Properties();
        try (Reader inputStream = new BufferedReader(new InputStreamReader(new FileInputStream(path),
                StandardCharsets.UTF_8))) {
            properties.load(inputStream);
        }

        return fromProperties(properties).entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> entry.getValue().trim()));
    }

    public static Map<String, String> getSystemProperties()
    {
        return Maps.fromProperties(System.getProperties());
    }
}
