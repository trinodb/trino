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
package io.trino.plugin.warp.proxiedconnector.utils;

import io.trino.plugin.iceberg.CatalogType;

import java.util.Map;
import java.util.stream.Collectors;

public class ConfigurationUtils
{
    private ConfigurationUtils() {}

    public static Map<String, String> getDeltaLakeFilteredConfig(Map<String, String> config)
    {
        return getProxiedConnectorFilteredConfig(config);
    }

    public static Map<String, String> getHiveFilteredConfig(Map<String, String> config)
    {
        return getProxiedConnectorFilteredConfig(config);
    }

    public static Map<String, String> getIcebergFilteredConfig(Map<String, String> config)
    {
        if (config.getOrDefault("iceberg.catalog.type", CatalogType.HIVE_METASTORE.name()).equals("GLUE")) {
            return getProxiedConnectorFilteredConfig(config).entrySet()
                    .stream()
                    .filter(e -> !e.getKey().startsWith("hive.metastore"))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        else {
            return getProxiedConnectorFilteredConfig(config).entrySet()
                    .stream()
                    .filter(e -> !e.getKey().equals("hive.metastore"))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

    public static Map<String, String> getProxiedConnectorFilteredConfig(Map<String, String> config)
    {
        return config.entrySet().stream()
                .filter(e -> !e.getKey().startsWith("warp-speed"))
                .filter(e -> !e.getKey().startsWith("http"))
                .filter(e -> !e.getKey().equals("node.environment"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
