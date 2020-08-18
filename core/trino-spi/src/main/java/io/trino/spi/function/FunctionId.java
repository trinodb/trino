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
package io.trino.spi.function;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.trino.spi.Experimental;

import java.util.Locale;

import static java.util.Objects.requireNonNull;

@Experimental(eta = "2022-10-31")
public class FunctionId
{
    private final String id;

    @JsonCreator
    public FunctionId(String id)
    {
        requireNonNull(id, "id is null");
        if (id.isEmpty()) {
            throw new IllegalArgumentException("id must not be empty");
        }
        if (!id.toLowerCase(Locale.US).equals(id)) {
            throw new IllegalArgumentException("id must be lowercase");
        }
        if (id.contains("@")) {
            throw new IllegalArgumentException("id must not contain '@'");
        }
        this.id = id;
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
        FunctionId that = (FunctionId) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    @JsonValue
    @Override
    public String toString()
    {
        return id;
    }

    public static FunctionId toFunctionId(Signature signature)
    {
        return new FunctionId(signature.toString().toLowerCase(Locale.US));
    }
}
