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
package io.trino.sql.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.errorprone.annotations.DoNotCall;

import static java.util.Objects.requireNonNull;

/**
 * A plan fragment that {@link PlanFragmentRedactor} prepared for query reporting.
 */
public final class RedactedPlanFragment
{
    private final PlanFragment fragment;

    RedactedPlanFragment(PlanFragment fragment)
    {
        this.fragment = requireNonNull(fragment, "fragment is null");
    }

    @JsonCreator
    @DoNotCall // For JSON deserialization only
    public static RedactedPlanFragment fromJson(PlanFragment fragment)
    {
        return new RedactedPlanFragment(fragment);
    }

    @JsonValue
    public PlanFragment fragment()
    {
        return fragment;
    }
}
