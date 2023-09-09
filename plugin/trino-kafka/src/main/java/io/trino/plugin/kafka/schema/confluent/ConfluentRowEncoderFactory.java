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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.trino.plugin.kafka.encoder.KafkaFieldType;
import io.trino.plugin.kafka.encoder.RowEncoder;
import io.trino.plugin.kafka.encoder.RowEncoderFactory;
import io.trino.plugin.kafka.encoder.RowEncoderSpec;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSession;
import org.apache.avro.Schema;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Objects.requireNonNull;
import static org.apache.avro.Schema.Type.RECORD;

public class ConfluentRowEncoderFactory
        implements RowEncoderFactory
{
    private final SchemaRegistryClient schemaRegistryClient;
    private final List<String> schemaRegistryUrls;

    @Inject
    public ConfluentRowEncoderFactory(SchemaRegistryClient schemaRegistryClient, ConfluentSchemaRegistryConfig config)
    {
        this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
        requireNonNull(config, "config is null");
        this.schemaRegistryUrls = config.getConfluentSchemaRegistryUrls().stream().map(HostAddress::getHostText)
                .collect(toImmutableList());
    }

    @Override
    public RowEncoder create(ConnectorSession session, RowEncoderSpec rowEncoderSpec)
    {
        requireNonNull(rowEncoderSpec, "rowEncoderSpec is null");
        checkState(rowEncoderSpec.dataSchema().isPresent(), "dataSchema is empty");
        Schema parsedSchema = new Schema.Parser().parse(rowEncoderSpec.dataSchema().get());
        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
        boolean isKey = rowEncoderSpec.kafkaFieldType() == KafkaFieldType.KEY;
        kafkaAvroSerializer.configure(ImmutableMap.of(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrls), isKey);
        if (parsedSchema.getType() == RECORD) {
            return new ConfluentGenericRecordRowEncoder(session, rowEncoderSpec.columnHandles(), parsedSchema, kafkaAvroSerializer, rowEncoderSpec.topic());
        }
        return new ConfluentSingleValueRowEncoder(session, rowEncoderSpec.columnHandles(), parsedSchema, kafkaAvroSerializer, rowEncoderSpec.topic());
    }
}
