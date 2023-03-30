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
package io.trino.decoder.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.trino.spi.TrinoException;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.decoder.protobuf.ProtobufRowDecoderFactory.DEFAULT_MESSAGE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FixedSchemaDynamicMessageProvider
        implements DynamicMessageProvider
{
    private final Descriptor descriptor;

    public FixedSchemaDynamicMessageProvider(Descriptor descriptor)
    {
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
    }

    @Override
    public DynamicMessage parseDynamicMessage(byte[] data)
    {
        try {
            return DynamicMessage.parseFrom(descriptor, data);
        }
        catch (InvalidProtocolBufferException e) {
            throw new TrinoException(ProtobufErrorCode.INVALID_PROTOBUF_MESSAGE, "Decoding Protobuf record failed.", e);
        }
    }

    public static class Factory
            implements DynamicMessageProvider.Factory
    {
        @Override
        public DynamicMessageProvider create(Optional<String> protoFile)
        {
            checkState(protoFile.isPresent(), "proto file is missing");
            try {
                Descriptor descriptor = ProtobufUtils.getFileDescriptor(protoFile.orElseThrow()).findMessageTypeByName(DEFAULT_MESSAGE);
                checkState(descriptor != null, format("Message %s not found", DEFAULT_MESSAGE));
                return new FixedSchemaDynamicMessageProvider(descriptor);
            }
            catch (DescriptorValidationException descriptorValidationException) {
                throw new TrinoException(ProtobufErrorCode.INVALID_PROTO_FILE, "Unable to parse protobuf schema", descriptorValidationException);
            }
        }
    }
}
