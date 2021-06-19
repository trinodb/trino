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
package io.trino.plugin.kafka.encoder.avro;

import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.plugin.kafka.encoder.RowEncoder;
import io.trino.plugin.kafka.encoder.RowEncoderFactory;
import io.trino.spi.connector.ConnectorSession;
import org.apache.avro.Schema;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class AvroRowEncoderFactory
        implements RowEncoderFactory
{
    @Override
    public RowEncoder create(ConnectorSession session, Optional<String> dataSchema, List<EncoderColumnHandle> columnHandles)
    {
        checkArgument(dataSchema.isPresent(), "dataSchema for Avro format is not present");
        Schema parsedSchema = new Schema.Parser().parse(dataSchema.get());
        return new AvroRowEncoder(session, columnHandles, parsedSchema);
    }
}
