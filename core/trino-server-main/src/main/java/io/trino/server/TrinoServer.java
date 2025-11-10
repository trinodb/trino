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

import static com.google.common.base.MoreObjects.firstNonNull;

public final class TrinoServer
{
    private TrinoServer() {}

    public static void main(String[] args)
    {
        Runtime.Version javaVersion = Runtime.version();
        if (javaVersion.feature() < 22) {
            System.err.printf("ERROR: Trino requires Java 22+ (found %s)%n", javaVersion);
            System.exit(100);
        }

        String trinoVersion = TrinoServer.class.getPackage().getImplementationVersion();
        new Server().start(firstNonNull(trinoVersion, "unknown"));
    }
}
