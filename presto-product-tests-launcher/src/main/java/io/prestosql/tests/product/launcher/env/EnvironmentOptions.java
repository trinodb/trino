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
package io.prestosql.tests.product.launcher.env;

import java.io.File;
import java.util.Locale;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Objects.requireNonNull;
import static picocli.CommandLine.Option;

public final class EnvironmentOptions
{
    private static final String DEFAULT_VALUE = "(default: ${DEFAULT-VALUE})";

    @Option(names = "--config", paramLabel = "<config>", description = "Environment config to use")
    public String config = "config-default";

    @Option(names = "--server-package", paramLabel = "<package>", description = "Path to Presto server package " + DEFAULT_VALUE, defaultValue = "presto-server/target/presto-server-${project.version}.tar.gz")
    public File serverPackage;

    @Option(names = "--without-presto", description = "Do not start presto-master")
    public boolean withoutPrestoMaster;

    @Option(names = "--bind", description = "Bind ports on localhost")
    public boolean bindPorts = toBoolean(firstNonNull(System.getenv("PTL_BIND_PORTS"), "true"));

    @Option(names = "--debug", description = "Open Java debug ports")
    public boolean debug;

    public EnvironmentOptions copyOf()
    {
        EnvironmentOptions copy = new EnvironmentOptions();
        copy.bindPorts = bindPorts;
        copy.debug = debug;
        copy.withoutPrestoMaster = withoutPrestoMaster;
        copy.serverPackage = serverPackage;
        copy.config = config;
        return copy;
    }

    private static boolean toBoolean(String value)
    {
        requireNonNull(value, "value is null");
        switch (value.toLowerCase(Locale.ENGLISH)) {
            case "true":
                return true;
            case "false":
                return false;
        }
        throw new IllegalArgumentException("Cannot convert to boolean: " + value);
    }

    public static EnvironmentOptions empty()
    {
        return new EnvironmentOptions();
    }
}
