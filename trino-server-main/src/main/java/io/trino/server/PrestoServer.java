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
package io.prestosql.server;

import com.google.common.base.StandardSystemProperty;
import com.google.common.primitives.Ints;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;

public final class PrestoServer
{
    private PrestoServer() {}

    public static void main(String[] args)
    {
        String javaVersion = nullToEmpty(StandardSystemProperty.JAVA_VERSION.value());
        String majorVersion = javaVersion.split("[^\\d]", 2)[0];
        Integer major = Ints.tryParse(majorVersion);
        if (major == null || major < 11) {
            System.err.println(format("ERROR: Presto requires Java 11+ (found %s)", javaVersion));
            System.exit(100);
        }

        String version = PrestoServer.class.getPackage().getImplementationVersion();
        new Server().start(firstNonNull(version, "unknown"));
    }
}
