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
package io.trino.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.errorprone.annotations.Immutable;

import java.io.IOException;
import java.util.Objects;

import static java.lang.String.format;

@Immutable
@JsonDeserialize(using = ClientTypeSignatureParameter.ClientTypeSignatureParameterDeserializer.class)
public class ClientTypeSignatureParameter
{
    public enum ParameterKind
    {
        TYPE, NAMED_TYPE, LONG, VARIABLE;

        @JsonCreator
        public static ParameterKind fromJsonValue(String value)
        {
            // deserialize old names for compatibility for pre 321 servers
            switch (value) {
                case "TYPE_SIGNATURE":
                    return TYPE;
                case "NAMED_TYPE_SIGNATURE":
                    return NAMED_TYPE;
                case "LONG_LITERAL":
                    return LONG;
            }
            return valueOf(value);
        }
    }

    private final ParameterKind kind;
    private final Object value;

    public static ClientTypeSignatureParameter ofType(ClientTypeSignature typeSignature)
    {
        return new ClientTypeSignatureParameter(ParameterKind.TYPE, typeSignature);
    }

    public static ClientTypeSignatureParameter ofNamedType(NamedClientTypeSignature namedTypeSignature)
    {
        return new ClientTypeSignatureParameter(ParameterKind.NAMED_TYPE, namedTypeSignature);
    }

    public static ClientTypeSignatureParameter ofLong(long longLiteral)
    {
        return new ClientTypeSignatureParameter(ParameterKind.LONG, longLiteral);
    }

    @JsonCreator
    public ClientTypeSignatureParameter(
            @JsonProperty("kind") ParameterKind kind,
            @JsonProperty("value") Object value)
    {
        this.kind = kind;
        this.value = value;
    }

    @JsonProperty
    public ParameterKind getKind()
    {
        return kind;
    }

    @JsonProperty
    public Object getValue()
    {
        return value;
    }

    private <A> A getValue(ParameterKind expectedParameterKind, Class<A> target)
    {
        if (kind != expectedParameterKind) {
            throw new IllegalArgumentException(format("ParameterKind is [%s] but expected [%s]", kind, expectedParameterKind));
        }
        return target.cast(value);
    }

    public ClientTypeSignature getTypeSignature()
    {
        return getValue(ParameterKind.TYPE, ClientTypeSignature.class);
    }

    public Long getLongLiteral()
    {
        return getValue(ParameterKind.LONG, Long.class);
    }

    public NamedClientTypeSignature getNamedTypeSignature()
    {
        return getValue(ParameterKind.NAMED_TYPE, NamedClientTypeSignature.class);
    }

    @Override
    public String toString()
    {
        return value.toString();
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

        ClientTypeSignatureParameter other = (ClientTypeSignatureParameter) o;

        return this.kind == other.kind &&
                Objects.equals(this.value, other.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind, value);
    }

    public static class ClientTypeSignatureParameterDeserializer
            extends JsonDeserializer<ClientTypeSignatureParameter>
    {
        private static final ObjectMapper MAPPER = JsonCodec.OBJECT_MAPPER_SUPPLIER.get();

        @Override
        public ClientTypeSignatureParameter deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException
        {
            JsonNode node = jp.getCodec().readTree(jp);
            ParameterKind kind = MAPPER.readValue(MAPPER.treeAsTokens(node.get("kind")), ParameterKind.class);
            JsonParser jsonValue = MAPPER.treeAsTokens(node.get("value"));
            Object value;
            switch (kind) {
                case TYPE:
                    value = MAPPER.readValue(jsonValue, ClientTypeSignature.class);
                    break;
                case NAMED_TYPE:
                    value = MAPPER.readValue(jsonValue, NamedClientTypeSignature.class);
                    break;
                case LONG:
                    value = MAPPER.readValue(jsonValue, Long.class);
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unsupported kind [%s]", kind));
            }
            return new ClientTypeSignatureParameter(kind, value);
        }
    }
}
