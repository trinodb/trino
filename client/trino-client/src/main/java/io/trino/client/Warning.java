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

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class Warning
{
    private final Code warningCode;
    private final String message;

    @JsonCreator
    public Warning(
            @JsonProperty("warningCode") Code warningCode,
            @JsonProperty("message") String message)
    {
        this.warningCode = requireNonNull(warningCode, "warningCode is null");
        this.message = requireNonNull(message, "message is null");
    }

    @JsonProperty
    public Code getWarningCode()
    {
        return warningCode;
    }

    @JsonProperty
    public String getMessage()
    {
        return message;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Warning that = (Warning) obj;
        return Objects.equals(warningCode, that.warningCode) &&
                Objects.equals(message, that.message);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(warningCode, message);
    }

    @Override
    public String toString()
    {
        return warningCode + ", " + message;
    }

    public static final class Code
    {
        private final int code;
        private final String name;

        @JsonCreator
        public Code(
                @JsonProperty("code") int code,
                @JsonProperty("name") String name)
        {
            this.code = code;
            this.name = requireNonNull(name, "name is null");
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

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            Code other = (Code) obj;
            return (code == other.code) &&
                    Objects.equals(name, other.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(code, name);
        }

        @Override
        public String toString()
        {
            return name + ":" + code;
        }
    }
}
