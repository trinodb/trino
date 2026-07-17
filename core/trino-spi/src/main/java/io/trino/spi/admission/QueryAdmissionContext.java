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
package io.trino.spi.admission;

import io.trino.spi.QueryId;

import java.util.Map;
import java.util.Optional;

/**
 * Engine-supplied query context handed to
 * {@link AdmissionPolicy#shouldQueryWait(QueryAdmissionContext)}.
 *
 * <p>Intentionally narrow. Does NOT expose the engine's query execution, the
 * session, the plan, or any coordinator-internal type. Vendor policies needing
 * cluster-state signals (e.g. observed memory, plan-cost-derived thresholds)
 * maintain their own observers; those observers are an implementation detail of
 * the policy plugin, not part of the SPI surface.
 *
 * @param queryId non-null query identifier
 * @param userName non-null submitting user name
 * @param resourceGroupId optional resource group identifier (string form)
 * @param sessionProperties immutable view of the session property map at the
 *         moment of admission; never {@code null}
 */
public record QueryAdmissionContext(
        QueryId queryId,
        String userName,
        Optional<String> resourceGroupId,
        Map<String, String> sessionProperties) {}
