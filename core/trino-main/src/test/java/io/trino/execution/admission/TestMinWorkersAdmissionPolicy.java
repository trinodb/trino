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
import io.airlift.units.Duration;
import io.trino.spi.QueryId;
import io.trino.spi.admission.AdmissionPolicy;
import io.trino.spi.admission.AdmissionPolicyFactory;
import io.trino.spi.admission.QueryAdmissionContext;
import io.trino.spi.admission.WaitDecision;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the default OSS {@link MinWorkersAdmissionPolicy} returns a
 * {@link WaitDecision.Wait} for any input context and ignores all
 * {@link QueryAdmissionContext} fields.
 *
 * <p>Validates: Requirements 2.1, 2.6, 2.7 / Design: §Default OSS implementation,
 * §Correctness Properties — Property 1 (default-policy equivalence).
 */
public class TestMinWorkersAdmissionPolicy
{
    private static final int RANDOMIZED_CASES = 100;

    @Test
    @DisplayName("Feature: admission-policy-spi, default policy returns Wait('wait for required workers') for any context")
    public void testReturnsWaitDecisionWithExpectedReason()
    {
        MinWorkersAdmissionPolicy policy = new MinWorkersAdmissionPolicy();
        QueryAdmissionContext context = new QueryAdmissionContext(
                new QueryId("query_under_test"),
                "alice",
                Optional.of("global.user"),
                ImmutableMap.of("required_workers_count", "4"));

        WaitDecision decision = policy.shouldQueryWait(context);

        assertThat(decision).isInstanceOf(WaitDecision.Wait.class);
        WaitDecision.Wait wait = (WaitDecision.Wait) decision;
        assertThat(wait.reason()).isEqualTo("wait for required workers");
        // Sentinel "0s" — engine re-reads getRequiredWorkersMaxWait(session) at the call site.
        assertThat(wait.maxWait().toMillis()).isEqualTo(Duration.valueOf("0s").toMillis());
    }

    /**
     * Generates {@value #RANDOMIZED_CASES} pseudo-random {@link QueryAdmissionContext}
     * values covering the field space (random user names, random session property
     * maps, random resource group strings, randomly empty/non-empty resource group).
     *
     * <p>Per task 1.10 we use deterministic generators (seed-fixed) instead of
     * adding {@code net.jqwik:jqwik} as a new dependency — jqwik is not currently
     * used in {@code core/trino-main} and adopting it for one test exceeds the
     * scope of this task. The 100-case generator below covers the input space
     * intelligently (see {@code generateContexts}).
     */
    @ParameterizedTest(name = "[{index}] context={0}")
    @MethodSource("randomContexts")
    @DisplayName("Feature: admission-policy-spi, default policy ignores QueryAdmissionContext fields")
    public void testIgnoresAllContextFields(QueryAdmissionContext context)
    {
        MinWorkersAdmissionPolicy policy = new MinWorkersAdmissionPolicy();

        WaitDecision decision = policy.shouldQueryWait(context);

        assertThat(decision).isInstanceOf(WaitDecision.Wait.class);
        WaitDecision.Wait wait = (WaitDecision.Wait) decision;
        assertThat(wait.reason()).isEqualTo("wait for required workers");
        assertThat(wait.maxWait().toMillis()).isGreaterThanOrEqualTo(0L);
    }

    static Stream<QueryAdmissionContext> randomContexts()
    {
        // Deterministic seed so failures are reproducible.
        Random random = new Random(0xA110_1C_5EEDL);
        List<QueryAdmissionContext> contexts = new ArrayList<>(RANDOMIZED_CASES);
        for (int i = 0; i < RANDOMIZED_CASES; i++) {
            contexts.add(generateContext(random, i));
        }
        return contexts.stream();
    }

    private static QueryAdmissionContext generateContext(Random random, int index)
    {
        QueryId queryId = new QueryId("query_" + Integer.toHexString(random.nextInt()).replace('-', '_'));
        String userName = "user_" + Integer.toHexString(random.nextInt()).replace('-', '_');
        Optional<String> resourceGroupId = (index % 5 == 0)
                ? Optional.empty()
                : Optional.of("rg_" + random.nextInt(1024) + "." + random.nextInt(64));

        int propertyCount = random.nextInt(8);
        ImmutableMap.Builder<String, String> sessionProperties = ImmutableMap.builder();
        for (int i = 0; i < propertyCount; i++) {
            sessionProperties.put(
                    "prop_" + random.nextInt(64),
                    "val_" + random.nextInt(1024));
        }
        return new QueryAdmissionContext(queryId, userName, resourceGroupId, sessionProperties.buildKeepingLast());
    }

    @Test
    public void testReturnedWaitIsWellFormed()
    {
        MinWorkersAdmissionPolicy policy = new MinWorkersAdmissionPolicy();
        QueryAdmissionContext context = new QueryAdmissionContext(
                new QueryId("q"),
                "u",
                Optional.empty(),
                Map.of());

        WaitDecision decision = policy.shouldQueryWait(context);

        assertThat(decision).isInstanceOf(WaitDecision.Wait.class);
        WaitDecision.Wait wait = (WaitDecision.Wait) decision;
        assertThat(wait.reason()).isNotNull();
        assertThat(wait.reason().length()).isBetween(1, 256);
        assertThat(wait.maxWait()).isNotNull();
        assertThat(wait.maxWait().convertTo(MILLISECONDS).getValue()).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    @DisplayName("Feature: admission-policy-spi, MinWorkersAdmissionPolicy.NAME equals AdmissionPolicyConfig.DEFAULT_NAME ('min-workers')")
    public void testNameConstantMatchesConfigDefault()
    {
        assertThat(MinWorkersAdmissionPolicy.NAME)
                .as("MinWorkersAdmissionPolicy.NAME must equal AdmissionPolicyConfig.DEFAULT_NAME")
                .isEqualTo(AdmissionPolicyConfig.DEFAULT_NAME)
                .isEqualTo("min-workers");
    }

    @Test
    @DisplayName("Feature: admission-policy-spi, MinWorkersAdmissionPolicy.Factory.getName() equals AdmissionPolicyConfig.DEFAULT_NAME")
    public void testFactoryNameMatchesConfigDefault()
    {
        AdmissionPolicyFactory factory = new MinWorkersAdmissionPolicy.Factory();
        assertThat(factory.getName())
                .as("Factory.getName() must equal the configured default name")
                .isEqualTo(AdmissionPolicyConfig.DEFAULT_NAME)
                .isEqualTo("min-workers");
    }

    @ParameterizedTest(name = "[{index}] config={0}")
    @MethodSource("factoryConfigs")
    @DisplayName("Feature: admission-policy-spi, MinWorkersAdmissionPolicy.Factory.create(...) returns non-null and ignores the config map")
    public void testFactoryCreateReturnsNonNullAndIgnoresConfig(Map<String, String> factoryConfig)
    {
        AdmissionPolicyFactory factory = new MinWorkersAdmissionPolicy.Factory();

        AdmissionPolicy policy = factory.create(factoryConfig);

        assertThat(policy)
                .as("Factory.create(...) must return a non-null AdmissionPolicy regardless of the config map")
                .isNotNull()
                .isInstanceOf(MinWorkersAdmissionPolicy.class);

        // Ignoring the config means the produced policy still emits the same Wait
        // decision shape regardless of which properties were passed in.
        QueryAdmissionContext context = new QueryAdmissionContext(
                new QueryId("q"),
                "u",
                Optional.empty(),
                Map.of());
        WaitDecision decision = policy.shouldQueryWait(context);
        assertThat(decision).isInstanceOf(WaitDecision.Wait.class);
        assertThat(((WaitDecision.Wait) decision).reason()).isEqualTo("wait for required workers");
    }

    static Stream<Map<String, String>> factoryConfigs()
    {
        return Stream.of(
                Map.of(),
                Map.of("foo", "bar"),
                Map.of("query-manager.admission-policy.name", "should-be-ignored"),
                ImmutableMap.<String, String>builder()
                        .put("a", "1")
                        .put("b", "2")
                        .put("c", "3")
                        .buildOrThrow());
    }
}
