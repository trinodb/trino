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
package io.trino.spi.security;

import java.util.Map;

/**
 * Resolves additional extra credentials for an authenticated user, server-side.
 * The returned {@code {name -> value}} pairs are merged into the session identity's
 * extra credentials and are consumed by connectors that read named extra credentials
 * (for example the JDBC {@code user-credential-name}/{@code password-credential-name}
 * pass-through), letting a shared catalog connect as the real per-user account.
 */
public interface ExtraCredentialsProvider
{
    Map<String, String> getExtraCredentials(String user);
}
