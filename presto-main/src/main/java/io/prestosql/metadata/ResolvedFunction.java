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
package io.prestosql.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.type.TypeDeserializer;
import io.prestosql.type.TypeSignatureDeserializer;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.BaseEncoding.base32Hex;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ResolvedFunction
{
    private static final JsonCodec<ResolvedFunction> JSON_CODEC;

    static {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(
                Type.class, new TypeDeserializer(createTestMetadataManager()),
                TypeSignature.class, new TypeSignatureDeserializer()));
        JSON_CODEC = new JsonCodecFactory(objectMapperProvider).jsonCodec(ResolvedFunction.class);
    }

    private static final String PREFIX = "@";
    private final Signature signature;
    private final FunctionId functionId;

    @JsonCreator
    public ResolvedFunction(
            @JsonProperty("signature") Signature signature,
            @JsonProperty("id") FunctionId functionId)
    {
        this.signature = requireNonNull(signature, "signature is null");
        this.functionId = requireNonNull(functionId, "functionId is null");
        checkArgument(signature.getTypeVariableConstraints().isEmpty() && signature.getLongVariableConstraints().isEmpty(), "%s has unbound type parameters", signature);
        checkArgument(!signature.isVariableArity(), "%s has variable arity", signature);
    }

    @JsonProperty
    public Signature getSignature()
    {
        return signature;
    }

    @JsonProperty("id")
    public FunctionId getFunctionId()
    {
        return functionId;
    }

    public QualifiedName toQualifiedName()
    {
        byte[] json = JSON_CODEC.toJsonBytes(this);

        // json can be large so use zstd to compress
        ZstdCompressor compressor = new ZstdCompressor();
        byte[] compressed = new byte[toIntExact(compressor.maxCompressedLength(json.length))];
        int outputSize = compressor.compress(json, 0, json.length, compressed, 0, compressed.length);

        // names are case insensitive, so use base32 instead of base64
        String base32 = base32Hex().encode(compressed, 0, outputSize);
        // add name so expressions are still readable
        return QualifiedName.of(PREFIX + signature.getName() + PREFIX + base32);
    }

    public static Optional<ResolvedFunction> fromQualifiedName(QualifiedName qualifiedName)
    {
        String data = qualifiedName.getSuffix();
        if (!data.startsWith(PREFIX)) {
            return Optional.empty();
        }
        List<String> parts = Splitter.on(PREFIX).splitToList(data.subSequence(1, data.length()));
        checkArgument(parts.size() == 2, "Expected encoded resolved function to contain two parts: %s", qualifiedName);
        String name = parts.get(0);

        String base32 = parts.get(1);
        // name may have been lower cased, but base32 decoder requires upper case
        base32 = base32.toUpperCase(ENGLISH);
        byte[] compressed = base32Hex().decode(base32);

        byte[] json = new byte[toIntExact(ZstdDecompressor.getDecompressedSize(compressed, 0, compressed.length))];
        new ZstdDecompressor().decompress(compressed, 0, compressed.length, json, 0, json.length);

        ResolvedFunction resolvedFunction = JSON_CODEC.fromJson(json);
        checkArgument(resolvedFunction.getSignature().getName().equalsIgnoreCase(name),
                "Expected decoded function to have name %s, but name is %s", resolvedFunction.getSignature().getName(), name);
        return Optional.of(resolvedFunction);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResolvedFunction that = (ResolvedFunction) o;
        return Objects.equals(signature, that.signature) &&
                Objects.equals(functionId, that.functionId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(signature, functionId);
    }

    @Override
    public String toString()
    {
        return signature.toString();
    }
}
