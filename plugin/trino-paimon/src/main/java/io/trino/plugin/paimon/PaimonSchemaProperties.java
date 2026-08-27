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
package io.trino.plugin.paimon;

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static org.apache.paimon.catalog.Catalog.COMMENT_PROP;
import static org.apache.paimon.catalog.Catalog.DB_LOCATION_PROP;
import static org.apache.paimon.catalog.Catalog.OWNER_PROP;

public class PaimonSchemaProperties
{
    public static final String LOCATION_PROPERTY = DB_LOCATION_PROP;
    public static final String COMMENT_PROPERTY = COMMENT_PROP;
    public static final String OWNER_PROPERTY = OWNER_PROP;

    private final List<PropertyMetadata<?>> schemaProperties;

    public PaimonSchemaProperties()
    {
        schemaProperties = ImmutableList.of(
                stringProperty(LOCATION_PROPERTY, "Schema location.", null, false),
                stringProperty(COMMENT_PROPERTY, "Schema comment.", null, false));
    }

    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return schemaProperties;
    }
}
