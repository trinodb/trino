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
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.prestosql.operator.TypeSignatureParser;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.tree.QualifiedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ResolvedFunction
{
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
        return QualifiedName.of(PREFIX + encodeSimpleSignature(signature) + PREFIX + functionId);
    }

    public static Optional<ResolvedFunction> fromQualifiedName(QualifiedName name)
    {
        String data = name.getSuffix();
        if (!data.startsWith(PREFIX)) {
            return Optional.empty();
        }
        List<String> parts = Splitter.on(PREFIX).splitToList(data.subSequence(1, data.length()));
        checkArgument(parts.size() == 2, "Expected encoded resolved function to contain two parts: %s", name);
        Signature signature = decodeSimpleSignature(parts.get(0));
        FunctionId functionId = new FunctionId(parts.get(1));
        return Optional.of(new ResolvedFunction(signature, functionId));
    }

    private static String encodeSimpleSignature(Signature signature)
    {
        List<Object> parts = new ArrayList<>(3 + signature.getArgumentTypes().size());
        parts.add(signature.getName());
        parts.add(signature.getKind());
        parts.add(signature.getReturnType());
        parts.addAll(signature.getArgumentTypes());
        // TODO: this needs to be canonicalized elsewhere
        return Joiner.on('|').join(parts).toLowerCase(Locale.US);
    }

    private static Signature decodeSimpleSignature(String encodedSignature)
    {
        List<String> parts = Splitter.on('|').splitToList(encodedSignature);
        checkArgument(parts.size() >= 3, "Expected encoded signature to contain at least 3 parts: %s", encodedSignature);
        String name = parts.get(0);
        FunctionKind kind = FunctionKind.valueOf(parts.get(1).toUpperCase(Locale.US));
        TypeSignature returnType = TypeSignatureParser.parseTypeSignature(parts.get(2), ImmutableSet.of());
        List<TypeSignature> argumentTypes = parts.subList(3, parts.size()).stream()
                .map(part -> TypeSignatureParser.parseTypeSignature(part, ImmutableSet.of()))
                .collect(toImmutableList());
        return new Signature(name, kind, returnType, argumentTypes);
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
