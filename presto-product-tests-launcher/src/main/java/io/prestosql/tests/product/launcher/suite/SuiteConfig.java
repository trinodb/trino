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
package io.prestosql.tests.product.launcher.suite;

import com.google.common.collect.ImmutableList;
import io.prestosql.tests.product.launcher.env.EnvironmentProvider;
import io.prestosql.tests.product.launcher.env.common.EnvironmentExtender;

import java.util.List;
import java.util.Optional;

import static io.prestosql.tests.product.launcher.suite.Suites.nameForConfigClass;

public interface SuiteConfig
{
    String getImagesVersion();

    String getHadoopBaseImage();

    String getHadoopImagesVersion();

    default List<String> getExcludedGroups()
    {
        return ImmutableList.of();
    }

    default List<String> getExcludedTests()
    {
        return ImmutableList.of();
    }

    String getTemptoEnvironmentConfigFile();

    default String getConfigName()
    {
        return nameForConfigClass(getClass());
    }

    default Optional<EnvironmentExtender> extendEnvironment(Class<? extends EnvironmentProvider> environment)
    {
        return Optional.empty();
    }
}
