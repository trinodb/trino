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
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.trino.decoder.avro.AvroRowDecoderFactory;
import io.trino.plugin.kafka.KafkaTopicFieldDescription;
import io.trino.plugin.kafka.KafkaTopicFieldGroup;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.avro.Schema;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.EmptyFieldStrategy.IGNORE;
import static io.trino.plugin.kafka.schema.confluent.ConfluentSessionProperties.getEmptyFieldStrategy;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

public class AvroSchemaParser
        implements SchemaParser
{
    private final TypeManager typeManager;

    @Inject
    public AvroSchemaParser(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public KafkaTopicFieldGroup parse(ConnectorSession session, String subject, ParsedSchema parsedSchema)
    {
        checkArgument(parsedSchema instanceof AvroSchema, "parsedSchema should be an instance of AvroSchema");
        Schema schema = ((AvroSchema) parsedSchema).rawSchema();
        AvroSchemaConverter schemaConverter = new AvroSchemaConverter(typeManager, getEmptyFieldStrategy(session));
        List<Type> types = schemaConverter.convertAvroSchema(schema);
        ImmutableList.Builder<KafkaTopicFieldDescription> fieldsBuilder = ImmutableList.builder();
        if (schema.getType() != Schema.Type.RECORD) {
            checkState(types.size() == 1, "incompatible schema");
            fieldsBuilder.add(new KafkaTopicFieldDescription(
                    subject,
                    getOnlyElement(types),
                    subject,
                    null,
                    null,
                    null,
                    false));
        }
        else {
            List<Schema.Field> avroFields = getFields(session, schema);
            checkState(avroFields.size() == types.size(), "incompatible schema");

            for (int i = 0; i < types.size(); i++) {
                Schema.Field field = avroFields.get(i);
                fieldsBuilder.add(new KafkaTopicFieldDescription(
                        field.name(),
                        types.get(i),
                        field.name(),
                        null,
                        null,
                        null,
                        false));
            }
        }
        return new KafkaTopicFieldGroup(AvroRowDecoderFactory.NAME, Optional.empty(), Optional.of(subject), fieldsBuilder.build());
    }

    static List<Schema.Field> getFields(ConnectorSession session, Schema schema)
    {
        checkState(schema.getType() == Schema.Type.RECORD, "Unexpected type '%s' for record schema", schema.getType());
        if (getEmptyFieldStrategy(session) != IGNORE) {
            return schema.getFields();
        }
        return schema.getFields().stream()
                .filter(not(AvroSchemaParser::hasEmptyStruct))
                .collect(toImmutableList());
    }

    private static boolean hasEmptyStruct(Schema.Field field)
    {
        Schema schema = field.schema();
        if (schema.isUnion()) {
            return schema.getTypes().stream()
                    .anyMatch(memberSchema -> memberSchema.getType() == Schema.Type.RECORD && memberSchema.getFields().isEmpty());
        }
        return schema.getType() == Schema.Type.RECORD && schema.getFields().isEmpty();
    }
}
