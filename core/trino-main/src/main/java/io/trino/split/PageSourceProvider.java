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

    // TODO: How to indicate that one and only one of planToColumns and columns should present and if planToColumns is present it should contain at least one entry?
//    default ConnectorPageSource createPageSource(
//            Session session,
//            Split split,
//            TableHandle table,
//            Optional<List<Pair<MicroPlanHandle, List<ColumnHandle>>>> planToColumns,
//            Optional<List<ColumnHandle>> columns,
//            DynamicFilter dynamicFilter)
//    {
//        List<ColumnHandle> transferredColumns = columns.orElse(planToColumns.get().get(0).right);
//        return createPageSource(
//                session,
//                split,
//                table,
//                transferredColumns,
//                dynamicFilter);
//    }

    default Optional<Integer> getChosenMicroPlan()
    {
        return Optional.empty();
    }
}
