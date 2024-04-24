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
package io.trino.execution.resourcegroups;

import com.google.errorprone.annotations.ThreadSafe;
import io.trino.execution.ManagedQueryExecution;
import io.trino.spi.resourcegroups.ResourceGroupConfigurationManagerFactory;
import io.trino.spi.resourcegroups.SelectionContext;
import io.trino.spi.resourcegroups.SelectionCriteria;

import java.util.concurrent.Executor;

/**
 * Classes implementing this interface must be thread safe. That is, all the methods listed below
 * may be called concurrently from any thread.
 */
@ThreadSafe
public interface ResourceGroupManager<C>
        extends ResourceGroupInfoProvider
{
    void submit(ManagedQueryExecution queryExecution, SelectionContext<C> selectionContext, Executor executor);

    SelectionContext<C> selectGroup(SelectionCriteria criteria);

    void addConfigurationManagerFactory(ResourceGroupConfigurationManagerFactory factory);

    void loadConfigurationManager()
            throws Exception;
}
