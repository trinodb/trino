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
package io.trino.plugin.kafka.encoder;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record RowEncoderSpec(String dataFormat, Optional<String> dataSchema, List<EncoderColumnHandle> columnHandles, String topic, KafkaFieldType kafkaFieldType)
{
    public RowEncoderSpec
    {
        requireNonNull(dataFormat, "dataFormat is null");
        requireNonNull(dataSchema, "dataSchema is null");
        requireNonNull(columnHandles, "columnHandles is null");
        requireNonNull(topic, "topic is null");
        requireNonNull(kafkaFieldType, "kafkaFieldType is null");
    }
}
