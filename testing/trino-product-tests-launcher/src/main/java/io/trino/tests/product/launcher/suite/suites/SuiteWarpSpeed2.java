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
import io.trino.tests.product.launcher.env.environment.EnvMultinodeWarpGlueDeltaLake;
import io.trino.tests.product.launcher.env.environment.EnvMultinodeWarpGlueHive;
import io.trino.tests.product.launcher.env.environment.EnvMultinodeWarpGlueIceberg;
import io.trino.tests.product.launcher.env.environment.EnvMultinodeWarpHiveCache;
import io.trino.tests.product.launcher.env.environment.EnvMultinodeWarpSpeedMinio;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static io.trino.tests.product.TestGroups.WARP_SPEED_CACHE;
import static io.trino.tests.product.TestGroups.WARP_SPEED_DELTA_LAKE;
import static io.trino.tests.product.TestGroups.WARP_SPEED_HIVE_2;
import static io.trino.tests.product.TestGroups.WARP_SPEED_ICEBERG;
import static io.trino.tests.product.TestGroups.WARP_SPEED_MINIO;
import static io.trino.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class SuiteWarpSpeed2
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        return ImmutableList.of(
                testOnEnvironment(EnvMultinodeWarpGlueHive.class)
                        .withGroups(WARP_SPEED_HIVE_2)
                        .build(),
                testOnEnvironment(EnvMultinodeWarpGlueDeltaLake.class)
                        .withGroups(WARP_SPEED_DELTA_LAKE)
                        .build(),
                testOnEnvironment(EnvMultinodeWarpGlueIceberg.class)
                        .withGroups(WARP_SPEED_ICEBERG)
                        .build(),
                testOnEnvironment(EnvMultinodeWarpHiveCache.class)
                        .withGroups(WARP_SPEED_CACHE)
                        .build(),
                testOnEnvironment(EnvMultinodeWarpSpeedMinio.class)
                        .withGroups(WARP_SPEED_MINIO)
                        .build());
    }
}
