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
package io.trino.plugin.kafka.encoder.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import io.trino.plugin.kafka.encoder.RowEncoder;
import io.trino.plugin.kafka.encoder.RowEncoderFactory;
import io.trino.plugin.kafka.encoder.RowEncoderSpec;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.decoder.protobuf.ProtobufErrorCode.INVALID_PROTO_FILE;
import static io.trino.decoder.protobuf.ProtobufErrorCode.MESSAGE_NOT_FOUND;
import static io.trino.decoder.protobuf.ProtobufRowDecoderFactory.DEFAULT_MESSAGE;
import static io.trino.decoder.protobuf.ProtobufUtils.getFileDescriptor;
import static java.lang.String.format;

public class ProtobufRowEncoderFactory
        implements RowEncoderFactory
{
    @Override
    public RowEncoder create(ConnectorSession session, RowEncoderSpec rowEncoderSpec)
    {
        checkArgument(rowEncoderSpec.dataSchema().isPresent(), "dataSchema for Protobuf format is not present");

        try {
            Descriptor descriptor = getFileDescriptor(rowEncoderSpec.dataSchema().get()).findMessageTypeByName(DEFAULT_MESSAGE);
            if (descriptor != null) {
                return new ProtobufRowEncoder(descriptor, session, rowEncoderSpec.columnHandles());
            }
        }
        catch (DescriptorValidationException descriptorValidationException) {
            throw new TrinoException(INVALID_PROTO_FILE, "Unable to parse protobuf schema", descriptorValidationException);
        }
        throw new TrinoException(MESSAGE_NOT_FOUND, format("Message %s not found", DEFAULT_MESSAGE));
    }
}
