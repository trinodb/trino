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
package io.prestosql.plugin.kafka.confluent;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.kafka.confluent.AvroSchemaConverter.EmptyFieldStrategy;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.List;

import static io.prestosql.plugin.kafka.KafkaSessionProperties.EMPTY_FIELD_STRATEGY;
import static java.util.Objects.requireNonNull;

public class ConfluentSessionProperties
        implements Provider<List<PropertyMetadata<?>>>
{
    List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public ConfluentSessionProperties(ConfluentSchemaRegistryConfig config)
    {
        requireNonNull(config, "config is null");
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(PropertyMetadata.enumProperty(EMPTY_FIELD_STRATEGY,
                        "Strategy for handling struct types with no fields: IGNORE (default), FAIL, and ADD_DUMMY to add a boolean field named 'dummy'",
                        EmptyFieldStrategy.class, config.getEmptyFieldStrategy(), false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> get()
    {
        return sessionProperties;
    }

    public static EmptyFieldStrategy getEmptyFieldStrategy(ConnectorSession session)
    {
        return session.getProperty(EMPTY_FIELD_STRATEGY, EmptyFieldStrategy.class);
    }
}
