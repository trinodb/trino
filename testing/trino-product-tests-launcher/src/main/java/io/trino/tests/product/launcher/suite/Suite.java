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
package io.trino.tests.product.launcher.suite;

import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.configs.ConfigDefault;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.tests.product.launcher.Configurations.nameForSuiteClass;

public abstract class Suite
{
    public abstract List<SuiteTestRun> getTestRuns(EnvironmentConfig config);

    @Override
    public String toString()
    {
        return toStringHelper(getSuiteName())
                .add("testRuns", getTestRuns(new ConfigDefault()))
                .toString();
    }

    public String getSuiteName()
    {
        return nameForSuiteClass(getClass());
    }
}
