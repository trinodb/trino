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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.stringProperty;

public final class IcebergSchemaProperties
{
    public static final String LOCATION_PROPERTY = "location";

    public final List<PropertyMetadata<?>> schemaProperties;

    @Inject
    public IcebergSchemaProperties()
    {
        this.schemaProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(stringProperty(
                        LOCATION_PROPERTY,
                        "Base file system location URI",
                        null,
                        false))
                .build();
    }

    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return schemaProperties;
    }
}
