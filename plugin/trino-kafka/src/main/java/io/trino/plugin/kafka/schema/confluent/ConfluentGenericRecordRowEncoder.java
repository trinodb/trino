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

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.kafka.schema.confluent.AvroSchemaParser.getFields;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;

public class ConfluentGenericRecordRowEncoder
        extends AbstractConfluentRowEncoder
{
    private GenericRecordBuilder recordBuilder;
    private final List<Schema.Field> fields;

    public ConfluentGenericRecordRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles, Schema schema, KafkaAvroSerializer kafkaAvroSerializer, String topic)
    {
        super(session, columnHandles, schema, kafkaAvroSerializer, topic);
        this.recordBuilder = new GenericRecordBuilder(schema);
        checkState(schema.getType() == Schema.Type.RECORD, "Unexpected schema type: '%s' should be RECORD", schema.getType());
        Map<String, Schema.Field> fieldMap = getFields(session, schema).stream()
                .collect(toImmutableMap(field -> field.name().toLowerCase(ENGLISH), identity()));
        checkState(columnHandles.size() == fieldMap.size(), "Mismatch between trino and avro schema field count");
        fields = columnHandles.stream().map(handle -> fieldMap.get(handle.getName().toLowerCase(ENGLISH))).collect(toImmutableList());
    }

    @Override
    protected Object buildRow()
    {
        GenericRecord record = recordBuilder.build();
        recordBuilder = new GenericRecordBuilder(schema);
        return record;
    }

    @Override
    protected void addValue(Block block, int position)
    {
        Type type = columnHandles.get(getCurrentColumnIndex()).getType();
        Schema.Field field = requireNonNull(fields.get(getCurrentColumnIndex()), format("Field not found in schema for column: '%s'", columnHandles.get(getCurrentColumnIndex())));
        if (!block.isNull(position)) {
            recordBuilder.set(field, getValue(type, block, position, field.schema()));
        }
    }
}
