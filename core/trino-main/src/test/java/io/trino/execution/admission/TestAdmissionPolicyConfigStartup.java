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
package io.trino.execution.admission;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.trino.spi.admission.AdmissionPolicy;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end coverage for the
 * {@code query-manager.admission-policy.name=does-not-exist} startup scenario.
 *
 * <p>Boots an {@link AdmissionPolicyModule} the same way {@code CoordinatorModule}
 * does and asserts startup fails with a clear error naming the unknown policy
 * name and the registered set, which always includes the bundled
 * {@code min-workers} default factory.
 *
 * <p>A full {@code DistributedQueryRunner} or coordinator boot is unnecessary
 * here: the SPI resolution lives entirely inside {@link AdmissionPolicyModule}'s
 * provider, and the surrounding coordinator infrastructure adds no behaviour
 * relevant to this acceptance criterion. Booting only the module keeps the test
 * fast while exercising the same code path the coordinator would.
 *
 * <p>Validates: Requirements 3.7, 8.6
 */
public class TestAdmissionPolicyConfigStartup
{
    private static final String UNKNOWN_NAME = "does-not-exist";

    @Test
    public void startupFailsWithUnknownPolicyNameAndRegisteredSet()
    {
        assertThatThrownBy(() -> bootCoordinator(UNKNOWN_NAME))
                .hasMessageContaining(UNKNOWN_NAME)
                .hasMessageContaining(MinWorkersAdmissionPolicy.NAME);
    }

    @Test
    public void startupSucceedsWithDefaultPolicyName()
    {
        AdmissionPolicy policy = bootCoordinator(MinWorkersAdmissionPolicy.NAME);
        assertThat(policy).isInstanceOf(MinWorkersAdmissionPolicy.class);
    }

    @Test
    public void startupSucceedsWithoutExplicitPolicyName()
    {
        // No configuration override — the AdmissionPolicyConfig default must
        // resolve to the bundled min-workers factory.
        Bootstrap app = new Bootstrap(new AdmissionPolicyModule(), _ -> {});

        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();

        AdmissionPolicy policy = injector.getInstance(AdmissionPolicy.class);
        assertThat(policy).isInstanceOf(MinWorkersAdmissionPolicy.class);
    }

    private static AdmissionPolicy bootCoordinator(String policyName)
    {
        Bootstrap app = new Bootstrap(new AdmissionPolicyModule(), _ -> {});

        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .setRequiredConfigurationProperties(ImmutableMap.of("query-manager.admission-policy.name", policyName))
                .initialize();

        return injector.getInstance(AdmissionPolicy.class);
    }
}
