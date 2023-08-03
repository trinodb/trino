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
package io.trino.plugin.kafka.encoder.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.trino.plugin.kafka.encoder.RowEncoder;
import io.trino.plugin.kafka.encoder.RowEncoderFactory;
import io.trino.plugin.kafka.encoder.RowEncoderSpec;
import io.trino.spi.connector.ConnectorSession;

import static java.util.Objects.requireNonNull;

public class JsonRowEncoderFactory
        implements RowEncoderFactory
{
    private final ObjectMapper objectMapper;

    @Inject
    public JsonRowEncoderFactory(ObjectMapper objectMapper)
    {
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    @Override
    public RowEncoder create(ConnectorSession session, RowEncoderSpec rowEncoderSpec)
    {
        return new JsonRowEncoder(session, rowEncoderSpec.columnHandles(), objectMapper);
    }
}
