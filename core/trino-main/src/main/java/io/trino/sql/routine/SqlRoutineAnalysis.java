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
package io.trino.sql.routine;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.Analysis;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record SqlRoutineAnalysis(
        String name,
        Map<String, Type> arguments,
        Type returnType,
        boolean calledOnNull,
        boolean deterministic,
        Optional<String> comment,
        Analysis analysis)
{
    public SqlRoutineAnalysis
    {
        requireNonNull(name, "name is null");
        arguments = ImmutableMap.copyOf(requireNonNull(arguments, "arguments is null"));
        requireNonNull(returnType, "returnType is null");
        requireNonNull(comment, "comment is null");
        requireNonNull(analysis, "analysis is null");
    }
}
