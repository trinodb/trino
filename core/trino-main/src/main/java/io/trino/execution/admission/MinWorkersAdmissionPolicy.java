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

import io.airlift.units.Duration;
import io.trino.spi.admission.AdmissionPolicy;
import io.trino.spi.admission.AdmissionPolicyFactory;
import io.trino.spi.admission.QueryAdmissionContext;
import io.trino.spi.admission.WaitDecision;

import java.util.Map;

/**
 * Default OSS {@link AdmissionPolicy} implementation. Emits a fixed
 * {@link WaitDecision.Wait} for every query so the engine routes admission
 * through {@code ClusterSizeMonitor.waitForMinimumWorkers(...)}, preserving
 * today's behavior byte-for-byte.
 *
 * <p>The policy itself does not call {@code ClusterSizeMonitor}; that call is
 * made by the engine in {@code LocalDispatchQuery} based on the returned
 * decision. Keeping the call out of the SPI lets the SPI live in
 * {@code trino-spi} without reaching into {@code io.trino.execution}.
 */
public class MinWorkersAdmissionPolicy
        implements AdmissionPolicy
{
    static final String NAME = AdmissionPolicyConfig.DEFAULT_NAME;

    @Override
    public WaitDecision shouldQueryWait(QueryAdmissionContext query)
    {
        // The "0s" maxWait carried on this decision is a sentinel — the engine
        // (LocalDispatchQuery) re-reads getRequiredWorkersMaxWait(session) at the
        // call site when the bound policy is the default MinWorkersAdmissionPolicy,
        // preserving today's wait timeout byte-for-byte (Req 2.3, Req 8.1). For
        // non-default policies the engine honors WaitDecision.Wait.maxWait() as
        // returned by the policy. See design.md §Default OSS implementation.
        return new WaitDecision.Wait(Duration.valueOf("0s"), "wait for required workers");
    }

    public static class Factory
            implements AdmissionPolicyFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public AdmissionPolicy create(Map<String, String> config)
        {
            // Default factory ignores the properties map; vanilla operators do not
            // need an etc/admission-policy.properties file.
            return new MinWorkersAdmissionPolicy();
        }
    }
}
