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
package io.trino.lance.file.v2.metadata;

import com.google.common.collect.ImmutableList;
import io.trino.lance.file.v2.encoding.LanceEncoding;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record MiniBlockLayout(
        LanceEncoding valueEncoding,
        Optional<LanceEncoding> repetitionEncoding,
        Optional<LanceEncoding> definitionEncoding,
        Optional<LanceEncoding> dictionaryEncoding,
        Optional<Long> numDictionaryItems,
        List<RepDefLayer> layers,
        long numBuffers,
        int repIndexDepth,
        long numItems)
        implements PageLayout
{
    public MiniBlockLayout
    {
        requireNonNull(valueEncoding, "valueEncoding is null");
        requireNonNull(repetitionEncoding, "repetitionEncoding is null");
        requireNonNull(definitionEncoding, "definitionEncoding is null");
        requireNonNull(dictionaryEncoding, "dictionaryEncoding is null");
        requireNonNull(numDictionaryItems, "numDictionaryItems is null");
        layers = ImmutableList.copyOf(layers);
    }

    public static MiniBlockLayout fromProto(build.buf.gen.lance.encodings21.MiniBlockLayout proto)
    {
        checkArgument(proto.hasValueCompression());
        return new MiniBlockLayout(
                LanceEncoding.fromProto(proto.getValueCompression()),
                Optional.ofNullable(proto.hasRepCompression() ? LanceEncoding.fromProto(proto.getRepCompression()) : null),
                Optional.ofNullable(proto.hasDefCompression() ? LanceEncoding.fromProto(proto.getDefCompression()) : null),
                Optional.ofNullable(proto.hasDictionary() ? LanceEncoding.fromProto(proto.getDictionary()) : null),
                Optional.ofNullable(proto.hasDictionary() ? proto.getNumDictionaryItems() : null),
                RepDefLayer.fromProtoList(proto.getLayersList()),
                proto.getNumBuffers(),
                proto.getRepetitionIndexDepth(),
                proto.getNumItems());
    }
}
