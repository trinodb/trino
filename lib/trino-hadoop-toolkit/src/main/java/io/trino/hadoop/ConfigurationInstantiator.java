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
package io.trino.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import static com.google.common.base.Preconditions.checkState;

public final class ConfigurationInstantiator
{
    private ConfigurationInstantiator() {}

    public static Configuration newEmptyConfiguration()
    {
        return newConfiguration(false);
    }

    /**
     * @see Configuration#Configuration(boolean)
     */
    public static Configuration newConfigurationWithDefaultResources()
    {
        return newConfiguration(true);
    }

    private static Configuration newConfiguration(boolean loadDefaults)
    {
        // Configuration captures TCCL and it may used later e.g. to load filesystem implementation class
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        ClassLoader expectedClassLoader = ConfigurationInstantiator.class.getClassLoader();
        checkState(
                tccl == expectedClassLoader,
                "During instantiation, the Configuration object captures the TCCL and uses it to resolve classes by name. " +
                        "For this reason, the current TCCL %s should be same as this class's classloader %s. " +
                        "Otherwise the constructed Configuration will use *some* classloader to resolve classes",
                tccl,
                expectedClassLoader);
        return newConfigurationWithTccl(loadDefaults);
    }

    // Usage of `new Configuration(boolean)` is not allowed. Only ConfigurationInstantiator
    // can instantiate Configuration directly. Suppress the violation so that we can use it here.
    @SuppressModernizer
    private static Configuration newConfigurationWithTccl(boolean loadDefaults)
    {
        // Note: the Configuration captures current thread context class loader (TCCL), so it may or may not be generally usable.
        return new Configuration(loadDefaults);
    }
}
