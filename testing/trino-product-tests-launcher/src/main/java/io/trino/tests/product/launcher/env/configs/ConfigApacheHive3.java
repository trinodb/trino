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

import io.trino.tests.product.launcher.env.Environment;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;

public class ConfigApacheHive3
        extends ConfigDefault
{
    @Override
    public String getHadoopBaseImage()
    {
        return "ghcr.io/trinodb/testing/hive3.1-hive";
    }

    @Override
    public String getTemptoEnvironmentConfigFile()
    {
        return "/docker/presto-product-tests/conf/tempto/tempto-configuration-for-hive3.yaml,/docker/presto-product-tests/conf/tempto/tempto-configuration-for-hms-only.yaml";
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        // TODO: Remove when https://github.com/trinodb/docker-images/pull/115 is merged and released
        checkState(getImagesVersion().equals("49"), "Override is unnecessary when version is moved beyond 49");

        builder.configureContainer(HADOOP, container -> {
            container.withEnv("JAVA_HOME", "/usr/lib/jvm/zulu-8");
        });
    }
}
