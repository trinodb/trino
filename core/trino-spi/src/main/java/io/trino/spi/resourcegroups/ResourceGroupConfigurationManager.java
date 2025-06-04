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
package io.trino.spi.resourcegroups;

import java.util.Optional;

/**
 * The engine calls the {@link #match(SelectionCriteria)} method whenever a query is submitted,
 * and receives a {@link io.trino.spi.resourcegroups.SelectionContext} in return which
 * contains a fully-qualified {@link io.trino.spi.resourcegroups.ResourceGroupId},
 * and a manager-specific data structure of type {@code C}.
 * <p>
 * At a later time, the engine may decide to construct a resource group with that ID. To do so,
 * it will walk the tree to find the right position for the group, and then create it. It also
 * creates any necessary parent groups. Every time the engine creates a group it will
 * immediately call the configure method with the original context, allowing the manager to
 * set the required properties on the group.
 *
 * @param <C> the type of the manager-specific data structure
 */
public interface ResourceGroupConfigurationManager<C>
{
    /**
     * Implementations may retain a reference to the group, and re-configure it asynchronously.
     * This method is called in two cases: when the group is created, and when it was previously
     * disabled and is now reactivated. In the latter case, the reference passed to this method
     * is the same as during creation, so implementations don’t need to invalidate retained
     * references.
     */
    void configure(ResourceGroup group, SelectionContext<C> criteria);

    /**
     * This method is called for every query that is submitted, so it should be fast.
     */
    Optional<SelectionContext<C>> match(SelectionCriteria criteria);

    /**
     * This method is called when parent group of the one specified by {@link #match(SelectionCriteria)}'s
     * {@link SelectionContext#getResourceGroupId()} does not exist yet. It should return a {@link SelectionContext}
     * appropriate for {@link #configure(ResourceGroup, SelectionContext) configuration} of the parent group.
     *
     * @param context a selection context returned from {@link #match(SelectionCriteria)}
     * @return a selection context suitable for {@link #configure(ResourceGroup, SelectionContext)} for the parent group
     */
    SelectionContext<C> parentGroupContext(SelectionContext<C> context);

    void shutdown();
}
