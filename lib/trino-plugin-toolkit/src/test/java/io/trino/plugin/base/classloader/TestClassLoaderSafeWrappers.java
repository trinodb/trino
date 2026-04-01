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
package io.trino.plugin.base.classloader;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static java.lang.String.format;
import static org.assertj.core.api.Fail.fail;

public class TestClassLoaderSafeWrappers
{
    @Test
    public void test()
            throws Exception
    {
        testClassLoaderSafe(ConnectorAccessControl.class, ClassLoaderSafeConnectorAccessControl.class);
        testClassLoaderSafe(ConnectorMetadata.class, ClassLoaderSafeConnectorMetadata.class);
        testClassLoaderSafe(ConnectorMergeSink.class, ClassLoaderSafeConnectorMergeSink.class);
        testClassLoaderSafe(ConnectorPageSink.class, ClassLoaderSafeConnectorPageSink.class);
        testClassLoaderSafe(ConnectorPageSinkProvider.class, ClassLoaderSafeConnectorPageSinkProvider.class, ImmutableSet.of(
                ClassLoaderSafeConnectorPageSinkProvider.class.getMethod("createPageSink", ConnectorTransactionHandle.class, ConnectorSession.class, ConnectorOutputTableHandle.class, ConnectorPageSinkId.class),
                ClassLoaderSafeConnectorPageSinkProvider.class.getMethod("createPageSink", ConnectorTransactionHandle.class, ConnectorSession.class, ConnectorInsertTableHandle.class, ConnectorPageSinkId.class),
                ClassLoaderSafeConnectorPageSinkProvider.class.getMethod("createPageSink", ConnectorTransactionHandle.class, ConnectorSession.class, ConnectorTableExecuteHandle.class, ConnectorPageSinkId.class),
                ClassLoaderSafeConnectorPageSinkProvider.class.getMethod("createMergeSink", ConnectorTransactionHandle.class, ConnectorSession.class, ConnectorMergeTableHandle.class, ConnectorPageSinkId.class)));
        testClassLoaderSafe(ConnectorPageSourceProvider.class, ClassLoaderSafeConnectorPageSourceProvider.class, ImmutableSet.of(
                ClassLoaderSafeConnectorPageSourceProvider.class.getMethod("createPageSource", ConnectorTransactionHandle.class, ConnectorSession.class, ConnectorSplit.class, ConnectorTableHandle.class, List.class, DynamicFilter.class)));
        testClassLoaderSafe(ConnectorSplitManager.class, ClassLoaderSafeConnectorSplitManager.class);
        testClassLoaderSafe(ConnectorNodePartitioningProvider.class, ClassLoaderSafeNodePartitioningProvider.class);
        testClassLoaderSafe(ConnectorSplitSource.class, ClassLoaderSafeConnectorSplitSource.class);
        testClassLoaderSafe(SystemTable.class, ClassLoaderSafeSystemTable.class);
        testClassLoaderSafe(ConnectorRecordSetProvider.class, ClassLoaderSafeConnectorRecordSetProvider.class);
        testClassLoaderSafe(RecordSet.class, ClassLoaderSafeRecordSet.class);
        testClassLoaderSafe(EventListener.class, ClassLoaderSafeEventListener.class);
        testClassLoaderSafe(ConnectorTableFunction.class, ClassLoaderSafeConnectorTableFunction.class);
        testClassLoaderSafe(TableFunctionSplitProcessor.class, ClassLoaderSafeTableFunctionSplitProcessor.class);
        testClassLoaderSafe(TableFunctionDataProcessor.class, ClassLoaderSafeTableFunctionDataProcessor.class);
        testClassLoaderSafe(TableFunctionProcessorProvider.class, ClassLoaderSafeTableFunctionProcessorProvider.class);
    }

    private static <I, C extends I> void testClassLoaderSafe(Class<I> iface, Class<C> clazz)
            throws Exception
    {
        testClassLoaderSafe(iface, clazz, Set.of());
    }

    private static <I, C extends I> void testClassLoaderSafe(Class<I> iface, Class<C> clazz, Set<Method> exclusions)
            throws Exception
    {
        assertAllMethodsOverridden(iface, clazz, exclusions);

        for (Method method : iface.getMethods()) {
            if (exclusions.contains(method)) {
                continue;
            }
            Method implementation = clazz.getDeclaredMethod(method.getName(), method.getParameterTypes());
            if (Stream.class.isAssignableFrom(implementation.getReturnType())) {
                fail(format("Method %s returns a Stream, breaks class-loader safety", method));
            }
            if (Iterator.class.isAssignableFrom(implementation.getReturnType()) && !ClassLoaderSafeIterator.class.isAssignableFrom(implementation.getReturnType())) {
                fail(format("Method %s returns an Iterator (but not ClassLoaderSafeIterator), breaks class-loader safety", method));
            }
        }
    }
}
