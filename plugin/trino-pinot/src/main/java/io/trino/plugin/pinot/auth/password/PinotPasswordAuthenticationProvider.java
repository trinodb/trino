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
package io.trino.plugin.pinot.auth.password;

import io.trino.plugin.pinot.auth.PinotAuthenticationProvider;
import okhttp3.Credentials;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PinotPasswordAuthenticationProvider
        implements PinotAuthenticationProvider
{
    private final Optional<String> authToken;

    public PinotPasswordAuthenticationProvider(String user, String password)
    {
        requireNonNull(user, "user is null");
        requireNonNull(password, "password is null");
        this.authToken = Optional.of(encode(user, password));
    }

    @Override
    public Optional<String> getAuthenticationToken()
    {
        return authToken;
    }

    private String encode(String username, String password)
    {
        return Credentials.basic(username, password, StandardCharsets.ISO_8859_1);
    }
}
