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
package io.trino.spi;

import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;

import java.util.Iterator;
import java.util.List;
import java.util.function.LongConsumer;

public interface PageSorter
{
    /**
     * Sorts the pages, reporting the memory retained by the sorter — including transient
     * working memory allocated while sorting — to the given consumer. The consumer receives
     * the current total in bytes whenever it changes.
     *
     * @return Iterator of sorted pages.
     */
    Iterator<Page> sort(List<Type> types, List<Page> pages, List<Integer> sortChannels, List<SortOrder> sortOrders, int expectedPositions, LongConsumer memoryUsage);
}
