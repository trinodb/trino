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
package io.prestosql.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.kafka.KafkaInternalFieldManager.InternalField;

import javax.inject.Provider;

import java.util.List;

import static io.prestosql.spi.type.BigintType.BIGINT;

public class ConfluentInternalFields
        implements Provider<List<InternalField>>
{
    public static final String KEY_SCHEMA_ID = "_key_schema_id";
    public static final String MESSAGE_SCHEMA_ID = "_message_schema_id";

    private final List<InternalField> internalFields;

    public ConfluentInternalFields()
    {
        internalFields = new ImmutableList.Builder<InternalField>()
                .add(new InternalField(KEY_SCHEMA_ID, "Confluent schema registry id for key subject", BIGINT))
                .add(new InternalField(MESSAGE_SCHEMA_ID, "Confluent schema registry id for message subject", BIGINT))
                .build();
    }

    @Override
    public List<InternalField> get()
    {
        return internalFields;
    }
}
