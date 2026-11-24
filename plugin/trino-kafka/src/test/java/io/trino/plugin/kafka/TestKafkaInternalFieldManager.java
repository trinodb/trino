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
package io.trino.plugin.kafka;

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.KEY_FIELD;
import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.MESSAGE_FIELD;
import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.PARTITION_ID_FIELD;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestKafkaInternalFieldManager
{
    @Test
    public void testInternalField()
    {
        KafkaInternalFieldManager.InternalField internalField =
                new KafkaInternalFieldManager.InternalField(
                        PARTITION_ID_FIELD,
                        "internal_field_name",
                        "Partition Id",
                        BigintType.BIGINT);

        KafkaColumnHandle kafkaColumnHandle =
                new KafkaColumnHandle(
                        "internal_field_name",
                        BigintType.BIGINT,
                        null,
                        null,
                        null,
                        false,
                        false,
                        true);

        ColumnMetadata columnMetadata =
                ColumnMetadata.builder()
                        .setName("internal_field_name")
                        .setType(BigintType.BIGINT)
                        .setComment(Optional.of("Partition Id"))
                        .setHidden(false)
                        .build();

        assertThat(internalField.getInternalFieldId()).isEqualTo(PARTITION_ID_FIELD);
        assertThat(internalField.getColumnName()).isEqualTo("internal_field_name");
        assertThat(internalField.getColumnHandle(false)).isEqualTo(kafkaColumnHandle);
        assertThat(internalField.getColumnMetadata(false)).isEqualTo(columnMetadata);
    }

    @Test
    public void testKeyFieldIsVarbinary()
    {
        KafkaInternalFieldManager kafkaInternalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, new KafkaConfig());

        KafkaInternalFieldManager.InternalField keyField = kafkaInternalFieldManager.getFieldById(KEY_FIELD);

        assertThat(keyField.getColumnHandle(false).getType()).isEqualTo(VARBINARY);
        assertThat(keyField.getColumnMetadata(false).getType()).isEqualTo(VARBINARY);
    }

    @Test
    public void testMessageFieldIsVarbinary()
    {
        KafkaInternalFieldManager kafkaInternalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, new KafkaConfig());

        KafkaInternalFieldManager.InternalField messageField = kafkaInternalFieldManager.getFieldById(MESSAGE_FIELD);

        assertThat(messageField.getColumnHandle(false).getType()).isEqualTo(VARBINARY);
        assertThat(messageField.getColumnMetadata(false).getType()).isEqualTo(VARBINARY);
    }
}
