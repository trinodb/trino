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

import com.google.inject.Inject;
import io.trino.decoder.RowDecoder;
import io.trino.decoder.RowDecoderFactory;
import io.trino.decoder.RowDecoderSpec;
import io.trino.decoder.protobuf.DynamicMessageProvider.Factory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeManager;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ProtobufRowDecoderFactory
        implements RowDecoderFactory
{
    public static final String DEFAULT_MESSAGE = "schema";

    private final Factory dynamicMessageProviderFactory;
    private final TypeManager typeManager;
    private final DescriptorProvider descriptorProvider;

    @Inject
    public ProtobufRowDecoderFactory(Factory dynamicMessageProviderFactory, TypeManager typeManager, DescriptorProvider descriptorProvider)
    {
        this.dynamicMessageProviderFactory = requireNonNull(dynamicMessageProviderFactory, "dynamicMessageProviderFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.descriptorProvider = requireNonNull(descriptorProvider, "descriptorProvider is null");
    }

    @Override
    public RowDecoder create(ConnectorSession session, RowDecoderSpec rowDecoderSpec)
    {
        return new ProtobufRowDecoder(
                dynamicMessageProviderFactory.create(Optional.ofNullable(rowDecoderSpec.decoderParams().get("dataSchema"))),
                rowDecoderSpec.columns(),
                typeManager,
                descriptorProvider);
    }
}
