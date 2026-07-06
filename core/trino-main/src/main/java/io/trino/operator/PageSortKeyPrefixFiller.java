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
package io.trino.operator;

import io.trino.spi.Page;

/**
 * Fills the bits of one sort channel into the composite sort key prefixes of all positions of a
 * {@link Page}. Unlike {@link SortKeyPrefixFiller}, prefix ties are never decided by the prefix,
 * so nulls colliding with extreme values need no special handling.
 */
public interface PageSortKeyPrefixFiller
{
    void fill(Page page, long[] prefixes);
}
