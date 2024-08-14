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
package io.trino.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static io.trino.spi.ErrorType.USER_ERROR;
import static java.util.Objects.requireNonNull;

public final class ErrorCode
{
    private final int code;
    private final String name;
    private final ErrorType type;
    private final boolean fatal;

    public ErrorCode(
            @JsonProperty("code") int code,
            @JsonProperty("name") String name,
            @JsonProperty("type") ErrorType type)
    {
        this(code, name, type, type == USER_ERROR); // by default USER_ERRORs are fatal
    }

    @JsonCreator
    public ErrorCode(
            @JsonProperty("code") int code,
            @JsonProperty("name") String name,
            @JsonProperty("type") ErrorType type,
            @JsonProperty("fatal") boolean fatal)
    {
        if (code < 0) {
            throw new IllegalArgumentException("code is negative");
        }
        this.code = code;
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.fatal = fatal;
    }

    @JsonProperty
    public int getCode()
    {
        return code;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public ErrorType getType()
    {
        return type;
    }

    @JsonProperty
    public boolean isFatal()
    {
        return fatal;
    }

    @Override
    public String toString()
    {
        return name + ":" + code;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ErrorCode that = (ErrorCode) obj;
        return this.code == that.code;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(code);
    }
}
