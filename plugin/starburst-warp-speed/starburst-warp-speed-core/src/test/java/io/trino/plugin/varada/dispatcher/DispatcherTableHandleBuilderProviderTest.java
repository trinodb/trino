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
package io.trino.plugin.varada.dispatcher;

import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.rewrite.WarpExpression;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DispatcherTableHandleBuilderProviderTest
{
    /**
     * This test is to make sure that every new member of {@code DispatcherTableHandle}
     * is also supported by {@code DispatcherTableHandleBuilderProvider}
     */
    @Test
    public void testClone()
    {
        SimplifiedColumns simplifiedColumns = new SimplifiedColumns(Set.of(new RegularColumn("c1")));
        DispatcherProxiedConnectorTransformer transformer = mock(DispatcherProxiedConnectorTransformer.class);
        when(transformer.getSimplifiedColumns(any(), any(), anyInt())).thenReturn(simplifiedColumns);

        // IMPORTANT: initiate with non-default values only
        DispatcherTableHandle handle = new DispatcherTableHandle(
                "schema",
                "table",
                OptionalLong.of(1),
                TupleDomain.none(),
                simplifiedColumns,
                mock(ConnectorTableHandle.class),
                Optional.of(new WarpExpression(new VaradaCall("func", emptyList(), IntegerType.INTEGER), emptyList())),
                List.of(new CustomStat("stat", 1)),
                true);

        DispatcherTableHandleBuilderProvider builderProvider = new DispatcherTableHandleBuilderProvider(transformer);
        DispatcherTableHandle clonedHandle = builderProvider.builder(handle, 100).build();

        assertThat(clonedHandle).isEqualTo(handle);
    }
}
