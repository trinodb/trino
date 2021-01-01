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
package io.trino.execution.warnings;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoWarning;

import java.util.List;

public interface WarningCollector
{
    WarningCollector NOOP =
            new WarningCollector()
            {
                @Override
                public void add(TrinoWarning warning) {}

                @Override
                public List<TrinoWarning> getWarnings()
                {
                    return ImmutableList.of();
                }
            };

    void add(TrinoWarning warning);

    List<TrinoWarning> getWarnings();
}
