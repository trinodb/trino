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
package io.trino.tests.product.iceberg;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class IcebergCatalogPropertiesBuilder
{
    private static final Map<String, String> HADOOP_FILESYSTEM_PROPERTIES = Map.of(
            "fs.hadoop.enabled", "true",
            "hive.config.resources", "/etc/trino/hdfs-site.xml");

    private final Map<String, String> properties = new LinkedHashMap<>();

    private IcebergCatalogPropertiesBuilder() {}

    public static IcebergCatalogPropertiesBuilder icebergCatalog(String metastoreUri)
    {
        return new IcebergCatalogPropertiesBuilder()
                .put("connector.name", "iceberg")
                .put("hive.metastore.uri", metastoreUri);
    }

    public IcebergCatalogPropertiesBuilder withHadoopFileSystem()
    {
        return putAll(HADOOP_FILESYSTEM_PROPERTIES);
    }

    public IcebergCatalogPropertiesBuilder put(String key, String value)
    {
        properties.put(key, value);
        return this;
    }

    public IcebergCatalogPropertiesBuilder putAll(Map<String, String> additionalProperties)
    {
        properties.putAll(additionalProperties);
        return this;
    }

    public Map<String, String> build()
    {
        return Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }
}
