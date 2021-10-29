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
package io.trino.tests.product.launcher.env.configs;

import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentDefaults;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.tests.product.launcher.Configurations.nameForConfigClass;

public class ConfigDefault
        implements EnvironmentConfig
{
    @Override
    public String getHadoopBaseImage()
    {
        return EnvironmentDefaults.HADOOP_BASE_IMAGE;
    }

    @Override
    public String getImagesVersion()
    {
        return EnvironmentDefaults.DOCKER_IMAGES_VERSION;
    }

    @Override
    public String getHadoopImagesVersion()
    {
        return EnvironmentDefaults.HADOOP_IMAGES_VERSION;
    }

    @Override
    public String getTemptoEnvironmentConfigFile()
    {
        return EnvironmentDefaults.TEMPTO_ENVIRONMENT_CONFIG;
    }

    @Override
    public String getConfigName()
    {
        return nameForConfigClass(getClass());
    }

    @Override
    public List<String> getExcludedGroups()
    {
        return List.of(
                // 'default' config's Hive is slower than e.g. HDP3. Exclude some test groups to shorten product test execution time.
                // The groups excluded here do not explicitly depend on Hive version, so it's not important to run them for all
                // Hive versions.
                "aggregate",
                "group-by",
                "join",
                "orderby",
                "union",
                "window",
                "with_clause",
                // This test group doesn't exercise hive connector, so it's enough to run it with one config. It's retained for ConfigHdp3.
                "no_from",
                // This test group doesn't exercise hive connector, so it's enough to run it with one config. It's retained for ConfigHdp3.
                "tpch_connector");
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hadoopBaseImage", getHadoopBaseImage())
                .add("hadoopImagesVersion", getHadoopImagesVersion())
                .add("imagesVersion", getImagesVersion())
                .add("excludedGroups", getExcludedGroups())
                .add("excludedTests", getExcludedTests())
                .add("temptoEnvironmentConfigFile", getTemptoEnvironmentConfigFile())
                .toString();
    }
}
