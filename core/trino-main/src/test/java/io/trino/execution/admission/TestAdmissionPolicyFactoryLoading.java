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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.units.Duration;
import io.trino.spi.QueryId;
import io.trino.spi.admission.AdmissionPolicy;
import io.trino.spi.admission.AdmissionPolicyFactory;
import io.trino.spi.admission.QueryAdmissionContext;
import io.trino.spi.admission.WaitDecision;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property-style coverage for plugin discovery and factory selection.
 *
 * <p>Feature: admission-policy-spi, Property 5: Plugin discovery and binding correctness.
 *
 * <p>For a registered set of factories with random names, asserts:
 * (a) for every {@code name ∈ registered}, setting
 * {@code query-manager.admission-policy.name=name} resolves to that factory's policy;
 * (b) for any {@code name ∉ registered}, startup fails with both the requested name and
 * the registered set in the error message.
 *
 * <p>Test factories are contributed through a test module that adds them to the same
 * {@code Multibinder<AdmissionPolicyFactory>} that {@link AdmissionPolicyModule} owns.
 * That keeps factory registration synchronous with module installation, which is the
 * shape Guice requires when the {@code AdmissionPolicy} provider is an eager
 * {@code @Singleton} resolved during {@link Bootstrap#initialize()}.
 *
 * <p>Validates: Requirements 3.3, 3.4, 3.5, 3.7, 8.6
 */
public class TestAdmissionPolicyFactoryLoading
{
    /**
     * Number of factory generations exercised. The design calls for jqwik
     * {@code @Property} runs of {@code >=100} iterations, with a documented
     * exception for slow cases. Each iteration here boots one
     * {@link AdmissionPolicyModule} per registered factory plus one for the
     * unregistered-name case, so the effective Guice bootstrap count per
     * iteration is up to {@code n + 1}. 30 iterations keeps wall-clock cost
     * bounded while still covering 100+ distinct boot scenarios.
     */
    private static final int ITERATIONS = 30;

    private static final long SEED = 0xA17_F1A6L;

    private static Stream<List<String>> randomFactoryNameSets()
    {
        Random random = new Random(SEED);
        List<List<String>> generated = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            int n = 1 + random.nextInt(5); // 1..5 factories
            ImmutableList.Builder<String> names = ImmutableList.builder();
            int produced = 0;
            int attempts = 0;
            // De-duplicate within a single set to avoid exercising the duplicate-name
            // failure path in the success cases.
            List<String> seen = new ArrayList<>();
            while (produced < n && attempts < 50) {
                attempts++;
                String candidate = randomName(random);
                if (candidate.equals(MinWorkersAdmissionPolicy.NAME)) {
                    continue;
                }
                if (seen.contains(candidate)) {
                    continue;
                }
                seen.add(candidate);
                names.add(candidate);
                produced++;
            }
            generated.add(names.build());
        }
        return generated.stream();
    }

    @ParameterizedTest
    @MethodSource("randomFactoryNameSets")
    public void registeredNameResolvesToCorrectFactory(List<String> names)
    {
        List<StubAdmissionPolicyFactory> factories = makeStubFactories(names);

        for (StubAdmissionPolicyFactory factory : factories) {
            Injector injector = boot(factory.getName(), factories);
            AdmissionPolicy resolved = injector.getInstance(AdmissionPolicy.class);
            WaitDecision decision = resolved.shouldQueryWait(sampleContext());
            assertThat(decision).isInstanceOf(WaitDecision.Wait.class);
            assertThat(decision.reason())
                    .as("policy resolved for %s should be the matching stub", factory.getName())
                    .isEqualTo(factory.expectedReason());
        }
    }

    @ParameterizedTest
    @MethodSource("randomFactoryNameSets")
    public void unregisteredNameFailsStartup(List<String> names)
    {
        List<StubAdmissionPolicyFactory> factories = makeStubFactories(names);
        String unregistered = unregisteredName(names);

        assertThatThrownBy(() -> boot(unregistered, factories))
                .hasMessageContaining(unregistered)
                .hasMessageContaining(MinWorkersAdmissionPolicy.NAME);
    }

    @Test
    public void defaultFactoryIsAlwaysAvailable()
    {
        Injector injector = boot(MinWorkersAdmissionPolicy.NAME, ImmutableList.of());
        AdmissionPolicy policy = injector.getInstance(AdmissionPolicy.class);
        assertThat(policy).isInstanceOf(MinWorkersAdmissionPolicy.class);
    }

    private static String randomName(Random random)
    {
        int length = 1 + random.nextInt(32);
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int kind = random.nextInt(3);
            char c;
            if (kind == 0) {
                c = (char) ('a' + random.nextInt(26));
            }
            else if (kind == 1) {
                c = (char) ('A' + random.nextInt(26));
            }
            else {
                c = (char) ('0' + random.nextInt(10));
            }
            sb.append(c);
        }
        return sb.toString();
    }

    private static String unregisteredName(List<String> names)
    {
        Random random = new Random(SEED ^ 0x77L);
        for (int i = 0; i < 100; i++) {
            String candidate = "missing-" + randomName(random);
            if (!names.contains(candidate) && !candidate.equals(MinWorkersAdmissionPolicy.NAME)) {
                return candidate;
            }
        }
        // Falls through only on pathological random sequences; the loop bound is
        // deliberately generous, so this is unreachable in practice.
        throw new IllegalStateException("could not generate an unregistered name");
    }

    private static List<StubAdmissionPolicyFactory> makeStubFactories(List<String> names)
    {
        ImmutableList.Builder<StubAdmissionPolicyFactory> builder = ImmutableList.builder();
        for (String name : names) {
            builder.add(new StubAdmissionPolicyFactory(name));
        }
        return builder.build();
    }

    private static QueryAdmissionContext sampleContext()
    {
        return new QueryAdmissionContext(
                new QueryId("test_query"),
                "test_user",
                Optional.empty(),
                ImmutableMap.of());
    }

    /**
     * Builds and initializes a Bootstrap injector wiring the production
     * {@link AdmissionPolicyModule} plus a test module that contributes the
     * supplied stub factories through the same multibinder.
     */
    static Injector boot(String configuredName, List<? extends AdmissionPolicyFactory> factories)
    {
        Module testModule = binder -> {
            Multibinder<AdmissionPolicyFactory> multibinder = Multibinder.newSetBinder(binder, AdmissionPolicyFactory.class);
            for (AdmissionPolicyFactory factory : factories) {
                multibinder.addBinding().toInstance(factory);
            }
        };

        Bootstrap app = new Bootstrap(new AdmissionPolicyModule(), testModule);

        return app
                .doNotInitializeLogging()
                .quiet()
                .setRequiredConfigurationProperties(ImmutableMap.of("query-manager.admission-policy.name", configuredName))
                .initialize();
    }

    /**
     * Stub factory whose policy returns a {@link WaitDecision.Wait} carrying a
     * reason string derived from the factory name. The reason is what the
     * tests inspect to confirm the right factory was bound.
     */
    static final class StubAdmissionPolicyFactory
            implements AdmissionPolicyFactory
    {
        private final String name;

        StubAdmissionPolicyFactory(String name)
        {
            this.name = name;
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public AdmissionPolicy create(Map<String, String> config)
        {
            String reason = expectedReason();
            return _ -> new WaitDecision.Wait(Duration.valueOf("0s"), reason);
        }

        String expectedReason()
        {
            return "stub:" + name;
        }
    }
}
