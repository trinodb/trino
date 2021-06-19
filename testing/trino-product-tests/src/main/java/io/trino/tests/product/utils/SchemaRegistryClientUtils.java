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
package io.trino.tests.product.utils;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.trino.tempto.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContext;

public final class SchemaRegistryClientUtils
{
    private static final int CACHE_SIZE = 100;

    private SchemaRegistryClientUtils() {}

    public static SchemaRegistryClient getSchemaRegistryClient()
    {
        Map<String, Object> schemaRegistryProperties = new HashMap<>(
                testContext().getDependency(Configuration.class)
                        .getSubconfiguration("schema-registry")
                        .asMap());

        checkArgument(schemaRegistryProperties.containsKey("url"), "schemaRegistry url is missing");

        return new CachedSchemaRegistryClient(
                (String) schemaRegistryProperties.remove("url"),
                CACHE_SIZE,
                schemaRegistryProperties);
    }
}
