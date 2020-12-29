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
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.kafka.KafkaInternalFieldManager.PARTITION_ID_FIELD;
import static org.assertj.core.api.Assertions.assertThat;

public class TestKafkaInternalFieldManager
{
    @Test
    public void testInternalField()
    {
        KafkaInternalFieldManager.InternalField internalField =
                new KafkaInternalFieldManager.InternalField(
                        PARTITION_ID_FIELD,
                        "Partition Id",
                        BigintType.BIGINT);

        KafkaColumnHandle kafkaColumnHandle =
                new KafkaColumnHandle(
                        PARTITION_ID_FIELD,
                        BigintType.BIGINT,
                        null,
                        null,
                        null,
                        false,
                        false,
                        true);

        ColumnMetadata columnMetadata =
                ColumnMetadata.builder()
                        .setName(PARTITION_ID_FIELD)
                        .setType(BigintType.BIGINT)
                        .setComment(Optional.of("Partition Id"))
                        .setHidden(false)
                        .build();

        assertThat(internalField.getColumnName()).isEqualTo(PARTITION_ID_FIELD);
        assertThat(internalField.getColumnHandle(0, false)).isEqualTo(kafkaColumnHandle);
        assertThat(internalField.getColumnMetadata(false)).isEqualTo(columnMetadata);
    }
}
