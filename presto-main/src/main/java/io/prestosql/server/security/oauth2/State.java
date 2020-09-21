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
package io.prestosql.server.security.oauth2;

import java.util.Base64;
import java.util.Objects;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

class State
{
    private final String value;

    State(String value)
    {
        this.value = requireNonNull(value, "values is null");
    }

    String get()
    {
        return value;
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
        State state = (State) o;
        return value.equals(state.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
    }

    static State randomState()
    {
        return new State(
                new String(
                        Base64.getEncoder()
                                .encode(UUID.randomUUID().toString().getBytes(UTF_8)), UTF_8));
    }
}
