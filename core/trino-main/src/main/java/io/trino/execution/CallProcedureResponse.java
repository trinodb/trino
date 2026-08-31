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
package io.trino.execution;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record CallProcedureResponse(Optional<Map<String, Long>> metrics, Optional<ExecutionFailureInfo> failure, boolean catalogNotLoaded)
{
    public CallProcedureResponse
    {
        requireNonNull(metrics, "metrics is null");
        requireNonNull(failure, "failure is null");
    }

    public static CallProcedureResponse success(Optional<Map<String, Long>> metrics)
    {
        return new CallProcedureResponse(metrics, Optional.empty(), false);
    }

    public static CallProcedureResponse failure(ExecutionFailureInfo failure)
    {
        return new CallProcedureResponse(Optional.empty(), Optional.of(failure), false);
    }

    public static CallProcedureResponse catalogNotLoadedResponse()
    {
        return new CallProcedureResponse(Optional.empty(), Optional.empty(), true);
    }
}
