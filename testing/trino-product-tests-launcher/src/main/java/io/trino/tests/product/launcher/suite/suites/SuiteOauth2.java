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
package io.trino.tests.product.launcher.suite.suites;

import com.google.common.collect.ImmutableList;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeOauth2;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeOauth2HttpProxy;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeOauth2HttpsProxy;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeOauth2Refresh;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeOidc;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeOidcRefresh;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static io.trino.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class SuiteOauth2
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        return ImmutableList.of(
                testOnEnvironment(EnvSinglenodeOauth2.class)
                        .withGroups("oauth2")
                        .build(),
                testOnEnvironment(EnvSinglenodeOauth2HttpProxy.class)
                        .withGroups("oauth2")
                        .build(),
                testOnEnvironment(EnvSinglenodeOauth2HttpsProxy.class)
                        .withGroups("oauth2")
                        .build(),
                testOnEnvironment(EnvSinglenodeOidc.class)
                        .withGroups("oauth2")
                        .build(),
                testOnEnvironment(EnvSinglenodeOauth2Refresh.class)
                        .withGroups("oauth2_refresh")
                        .build(),
                testOnEnvironment(EnvSinglenodeOidcRefresh.class)
                        .withGroups("oauth2_refresh")
                        .build());
    }
}
