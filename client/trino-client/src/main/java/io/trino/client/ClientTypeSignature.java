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
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.trino.client.ClientTypeSignatureParameter.ParameterKind;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.client.ClientStandardTypes.ROW;
import static io.trino.client.ClientStandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.client.ClientStandardTypes.TIME_WITH_TIME_ZONE;
import static io.trino.client.ClientStandardTypes.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

@Immutable
public class ClientTypeSignature
{
    private static final Pattern PATTERN = Pattern.compile(".*[<>,].*");
    private final String rawType;
    private final List<ClientTypeSignatureParameter> arguments;
    public static final int VARCHAR_UNBOUNDED_LENGTH = Integer.MAX_VALUE;

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
        this.arguments = ImmutableList.copyOf(arguments);
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

        if (rawType.equals(VARCHAR) && arguments.get(0).getKind() == ParameterKind.LONG && arguments.get(0).getLongLiteral() == VARCHAR_UNBOUNDED_LENGTH) {
            return "varchar";
        }

        if (arguments.isEmpty()) {
            return rawType;
        }

        if (rawType.equals(TIME_WITH_TIME_ZONE)) {
            return "time(" + arguments.get(0) + ") with time zone";
        }

        if (rawType.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return "timestamp(" + arguments.get(0) + ") with time zone";
        }

        return rawType + arguments.stream()
                .map(ClientTypeSignatureParameter::toString)
                .collect(joining(",", "(", ")"));
    }

    @Deprecated
    private String rowToString()
    {
        if (arguments.isEmpty()) {
            return "row";
        }
        return arguments.stream()
                .map(ClientTypeSignatureParameter::getNamedTypeSignature)
                .map(parameter -> {
                    if (parameter.getName().isPresent()) {
                        return parameter.getName().get() + " " + parameter.getTypeSignature();
                    }
                    return parameter.getTypeSignature().toString();
                })
                .collect(joining(",", "row(", ")"));
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
