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

package io.prestosql.execution.warnings;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.WarningCode;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultQueryPhaseWarningCollector
        implements QueryPhaseWarningCollector
{
    @GuardedBy("this")
    private final Map<WarningCode, Set<PrestoWarning>> phaseWarnings = new LinkedHashMap<>();

    public synchronized void add(PrestoWarning warning)
    {
        WarningCode warningCode = warning.getWarningCode();
        if (phaseWarnings.get(warningCode) == null) {
            phaseWarnings.put(warningCode, new HashSet<>());
        }
        phaseWarnings.get(warningCode).add(warning);
    }

    public synchronized List<PrestoWarning> getWarnings()
    {
        ImmutableList.Builder<PrestoWarning> warnings = new ImmutableList.Builder<>();
        for (WarningCode warningCode : phaseWarnings.keySet()) {
            warnings.addAll(phaseWarnings.get(warningCode));
        }
        return warnings.build();
    }
}
