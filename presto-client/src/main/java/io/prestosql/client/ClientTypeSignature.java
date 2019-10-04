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
package io.prestosql.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.client.ClientTypeSignatureParameter.ParameterKind;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.client.ClientStandardTypes.ROW;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

@Immutable
public class ClientTypeSignature
{
    private static final Pattern PATTERN = Pattern.compile(".*[<>,].*");
    private final String rawType;
    private final List<ClientTypeSignatureParameter> arguments;

    public ClientTypeSignature(String rawType)
    {
        this(rawType, ImmutableList.of());
    }

    @JsonCreator
    public ClientTypeSignature(
            @JsonProperty("rawType") String rawType,
            @JsonProperty("arguments") List<ClientTypeSignatureParameter> arguments)
    {
        requireNonNull(rawType, "rawType is null");
        this.rawType = rawType;
        checkArgument(!rawType.isEmpty(), "rawType is empty");
        checkArgument(!PATTERN.matcher(rawType).matches(), "Bad characters in rawType type: %s", rawType);
        this.arguments = unmodifiableList(new ArrayList<>(requireNonNull(arguments, "arguments is null")));
    }

    @JsonProperty
    public String getRawType()
    {
        return rawType;
    }

    @JsonProperty
    public List<ClientTypeSignatureParameter> getArguments()
    {
        return arguments;
    }

    public List<ClientTypeSignature> getArgumentsAsTypeSignatures()
    {
        return arguments.stream()
                .peek(parameter -> checkState(parameter.getKind() == ParameterKind.TYPE,
                        "Expected all parameters to be TypeSignatures but [%s] was found", parameter))
                .map(ClientTypeSignatureParameter::getTypeSignature)
                .collect(toImmutableList());
    }

    @Override
    public String toString()
    {
        if (rawType.equals(ROW)) {
            return rowToString();
        }

        if (arguments.isEmpty()) {
            return rawType;
        }
        return rawType + arguments.stream()
                .map(ClientTypeSignatureParameter::toString)
                .collect(joining(",", "(", ")"));
    }

    @Deprecated
    private String rowToString()
    {
        String fields = arguments.stream()
                .map(ClientTypeSignatureParameter::getNamedTypeSignature)
                .map(parameter -> {
                    if (parameter.getName().isPresent()) {
                        return format("%s %s", parameter.getName().get(), parameter.getTypeSignature().toString());
                    }
                    return parameter.getTypeSignature().toString();
                })
                .collect(joining(","));

        return format("row(%s)", fields);
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

        ClientTypeSignature other = (ClientTypeSignature) o;

        return Objects.equals(this.rawType.toLowerCase(Locale.ENGLISH), other.rawType.toLowerCase(Locale.ENGLISH)) &&
                Objects.equals(this.arguments, other.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rawType.toLowerCase(Locale.ENGLISH), arguments);
    }
}
