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
package io.trino.split;

import com.clearspring.analytics.util.Pair;
import io.trino.Session;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;
import java.util.Optional;

public interface PageSourceProvider
{
    @Deprecated
    ConnectorPageSource createPageSource(
            Session session,
            Split split,
            TableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter);

    // Old comment - How to indicate that one and only one of planToColumns and columns should present and if planToColumns is present it should contain at least one entry?
    default ConnectorPageSource createPageSource(
            Session session,
            Split split,
            List<Pair<TableHandle, List<ColumnHandle>>> planToColumns,
            DynamicFilter dynamicFilter)
    {
        return createPageSource(
                session,
                split,
                planToColumns.get(0).left,
                planToColumns.get(0).right,
                dynamicFilter);
    }

    default Optional<TableHandle> getChosenMicroPlan()
    {
        return Optional.empty();
    }
}
