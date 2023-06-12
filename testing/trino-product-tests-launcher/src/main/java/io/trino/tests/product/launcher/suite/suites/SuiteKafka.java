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
import io.trino.tests.product.launcher.env.environment.EnvMultinodeConfluentKafka;
import io.trino.tests.product.launcher.env.environment.EnvMultinodeKafka;
import io.trino.tests.product.launcher.env.environment.EnvMultinodeKafkaSaslPlaintext;
import io.trino.tests.product.launcher.env.environment.EnvMultinodeKafkaSsl;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static io.trino.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class SuiteKafka
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        return ImmutableList.of(
                testOnEnvironment(EnvMultinodeKafka.class)
                        .withGroups("configured_features", "kafka")
                        .build(),
                testOnEnvironment(EnvMultinodeConfluentKafka.class)
                        // testing kafka group with this env is slightly redundant but helps verify that copying confluent libraries doesn't break non-confluent functionality
                        .withGroups("configured_features", "kafka", "kafka_confluent_license")
                        .build(),
                testOnEnvironment(EnvMultinodeKafkaSsl.class)
                        .withGroups("configured_features", "kafka")
                        .build(),
                testOnEnvironment(EnvMultinodeKafkaSaslPlaintext.class)
                        .withGroups("configured_features", "kafka")
                        .build());
    }
}
