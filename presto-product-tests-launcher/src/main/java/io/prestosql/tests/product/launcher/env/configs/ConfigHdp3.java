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
package io.prestosql.tests.product.launcher.env.configs;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class ConfigHdp3
        extends ConfigDefault
{
    /**
     * export HADOOP_BASE_IMAGE="prestodev/hdp3.1-hive"
     * export TEMPTO_ENVIRONMENT_CONFIG_FILE="/docker/presto-product-tests/conf/tempto/tempto-configuration-for-hive3.yaml"
     * export DISTRO_SKIP_GROUP=iceberg
     */
    @Override
    public String getHadoopBaseImage()
    {
        return "prestodev/hdp3.1-hive";
    }

    @Override
    public List<String> getExcludedGroups()
    {
        return ImmutableList.of("iceberg");
    }

    @Override
    public String getTemptoEnvironmentConfigFile()
    {
        return "/docker/presto-product-tests/conf/tempto/tempto-configuration-for-hive3.yaml";
    }
}
