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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.EmptyFieldStrategy;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.enumProperty;

public class ConfluentSessionProperties
        implements SessionPropertiesProvider
{
    public static final String EMPTY_FIELD_STRATEGY = "empty_field_strategy";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public ConfluentSessionProperties(ConfluentSchemaRegistryConfig config)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(enumProperty(
                        EMPTY_FIELD_STRATEGY,
                        "Strategy for handling struct types with no fields: IGNORE (default), FAIL, and ADD_DUMMY to add a boolean field named 'dummy'",
                        EmptyFieldStrategy.class,
                        config.getEmptyFieldStrategy(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static EmptyFieldStrategy getEmptyFieldStrategy(ConnectorSession session)
    {
        return session.getProperty(EMPTY_FIELD_STRATEGY, EmptyFieldStrategy.class);
    }
}
