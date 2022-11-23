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
package io.trino.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.hadoop.ConfigurationInstantiator.newConfigurationWithDefaultResources;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;

public final class ConfigurationUtils
{
    private static final Configuration INITIAL_CONFIGURATION;

    static {
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");

        // must not be transitively reloaded during the future loading of various Hadoop modules
        // all the required default resources must be declared above
        INITIAL_CONFIGURATION = newEmptyConfiguration();
        Configuration defaultConfiguration = newConfigurationWithDefaultResources();
        copy(defaultConfiguration, INITIAL_CONFIGURATION);
    }

    private ConfigurationUtils() {}

    public static Configuration getInitialConfiguration()
    {
        return copy(INITIAL_CONFIGURATION);
    }

    public static Configuration copy(Configuration configuration)
    {
        Configuration copy = newEmptyConfiguration();
        copy(configuration, copy);
        return copy;
    }

    public static void copy(Configuration from, Configuration to)
    {
        for (Map.Entry<String, String> entry : from) {
            to.set(entry.getKey(), entry.getValue());
        }
    }

    public static JobConf toJobConf(Configuration conf)
    {
        if (conf instanceof JobConf) {
            return (JobConf) conf;
        }
        return new JobConf(conf);
    }

    public static Configuration readConfiguration(List<File> resourcePaths)
    {
        Configuration result = newEmptyConfiguration();

        for (File resourcePath : resourcePaths) {
            checkArgument(resourcePath.exists(), "File does not exist: %s", resourcePath);

            Configuration resourceProperties = newEmptyConfiguration();
            // We need to call `getPath` instead of `toURI` because Hadoop library can not handle a configuration file with relative Xinclude files in case of passing URI.
            // In details, see https://issues.apache.org/jira/browse/HADOOP-17088.
            resourceProperties.addResource(new Path(resourcePath.getPath()));
            copy(resourceProperties, result);
        }

        return result;
    }
}
