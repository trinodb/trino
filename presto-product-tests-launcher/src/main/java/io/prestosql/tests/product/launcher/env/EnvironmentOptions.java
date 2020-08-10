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

import com.google.inject.Module;
import io.airlift.airline.Option;
import io.prestosql.tests.product.launcher.PathResolver;
import io.prestosql.tests.product.launcher.suite.SuiteConfig;

import java.io.File;
import java.util.Locale;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Objects.requireNonNull;

public final class EnvironmentOptions
{
    @Option(name = "--hadoop-base-image", title = "image", description = "Hadoop base image")
    public String hadoopBaseImage = EnvironmentDefaults.getHadoopBaseImage();

    @Option(name = "--image-version", title = "version", description = "docker images version")
    public String imagesVersion = EnvironmentDefaults.getImagesVersion();

    @Option(name = "--hadoop-image-version", title = "version", description = "docker images version")
    public String hadoopImagesVersion = EnvironmentDefaults.getHadoopImagesVersion();

    @Option(name = "--tempto-environment-config-file", title = "tempto-config-file", description = "tempto environment config file")
    public String temptoEnvironmentConfigFile = EnvironmentDefaults.getTemptoConfigurationFile();

    @Option(name = "--server-package", title = "server-package", description = "path to Presto server package")
    public File serverPackage = new File("presto-server/target/presto-server-${project.version}.tar.gz");

    @Option(name = "--without-presto", title = "without Presto", description = "do not start presto-master")
    public boolean withoutPrestoMaster;

    @Option(name = "--bind", description = "bind ports on localhost")
    public boolean bindPorts = toBoolean(firstNonNull(System.getenv("PTL_BIND_PORTS"), "true"));

    @Option(name = "--debug", description = "open Java debug ports")
    public boolean debug;

    @Option(name = "--hadoop-init-script", description = "Hadoop init script")
    public String hadoopInitScript = "";

    public Module toModule()
    {
        return binder -> {
            binder.bind(EnvironmentOptions.class).toInstance(this);
        };
    }

    public static EnvironmentOptions copy(EnvironmentOptions options)
    {
        EnvironmentOptions copy = new EnvironmentOptions();
        copy.hadoopBaseImage = options.hadoopBaseImage;
        copy.imagesVersion = options.imagesVersion;
        copy.hadoopImagesVersion = options.hadoopImagesVersion;
        copy.temptoEnvironmentConfigFile = options.temptoEnvironmentConfigFile;
        copy.serverPackage = options.serverPackage;
        copy.withoutPrestoMaster = options.withoutPrestoMaster;
        copy.bindPorts = options.bindPorts;
        copy.debug = options.debug;
        return copy;
    }

    public static EnvironmentOptions applySuiteConfig(EnvironmentOptions source, SuiteConfig suiteConfig)
    {
        // Override environment options using SuiteConfig values
        EnvironmentOptions copy = copy(source);
        copy.imagesVersion = suiteConfig.getImagesVersion();
        copy.hadoopBaseImage = suiteConfig.getHadoopBaseImage();
        copy.temptoEnvironmentConfigFile = suiteConfig.getTemptoEnvironmentConfigFile();
        copy.hadoopImagesVersion = suiteConfig.getHadoopImagesVersion();
        copy.serverPackage = new PathResolver().resolvePlaceholders(copy.serverPackage);
        suiteConfig.getHadoopInitScript().ifPresent(initScript -> copy.hadoopInitScript = initScript);
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
}
