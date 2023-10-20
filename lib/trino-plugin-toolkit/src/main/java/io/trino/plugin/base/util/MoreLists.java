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
package io.trino.plugin.base.util;

import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;

public final class MoreLists
{
    private MoreLists() {}

    public static <T> boolean containsAll(List<T> container, Collection<? extends T> searchedElements)
    {
        // A heuristic where it probably does not make sense to instantiate a set
        if (container.size() <= 3 || searchedElements.size() <= 3) {
            return container.containsAll(searchedElements);
        }
        return ImmutableSet.copyOf(container).containsAll(searchedElements);
    }
}
