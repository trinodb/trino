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

import com.google.protobuf.DynamicMessage;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.RowDecoder;
import io.trino.spi.type.TypeManager;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;

public class ProtobufRowDecoder
        implements RowDecoder
{
    public static final String NAME = "protobuf";

    private final DynamicMessageProvider dynamicMessageProvider;
    private final Map<DecoderColumnHandle, ProtobufColumnDecoder> columnDecoders;

    public ProtobufRowDecoder(DynamicMessageProvider dynamicMessageProvider, Set<DecoderColumnHandle> columns, TypeManager typeManager)
    {
        this.dynamicMessageProvider = requireNonNull(dynamicMessageProvider, "dynamicMessageSupplier is null");
        this.columnDecoders = columns.stream()
                .collect(toImmutableMap(
                        identity(),
                        column -> new ProtobufColumnDecoder(column, typeManager)));
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data)
    {
        DynamicMessage message = dynamicMessageProvider.parseDynamicMessage(data);
        return Optional.of(columnDecoders.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().decodeField(message))));
    }
}
