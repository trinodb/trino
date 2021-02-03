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
package io.trino.server;

import java.util.Optional;

public final class TrinoServer
{
    private TrinoServer() {}

    public static void main(String[] args)
    {
        Runtime.Version expectedVersion = Runtime.Version.parse("11.0.7");
        Runtime.Version currentVersion = Runtime.version();

        if (currentVersion.compareTo(expectedVersion) < 0) {
            System.err.printf("ERROR: Trino requires Java %s (found %s)%n", expectedVersion, currentVersion);
            System.exit(100);
        }

        String version = TrinoServer.class.getPackage().getImplementationVersion();
        new Server().start(Optional.ofNullable(version).orElse("unknown"));
    }
}
