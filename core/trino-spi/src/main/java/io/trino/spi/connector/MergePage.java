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
package io.trino.spi.connector;

import io.trino.spi.Page;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class MergePage
{
    private final Optional<Page> deletionsPage;
    private final Optional<Page> insertionsPage;

    public MergePage(Optional<Page> deletionsPage, Optional<Page> insertionsPage)
    {
        this.deletionsPage = canonicalizePage(requireNonNull(deletionsPage, "deletionsPage is null"));
        this.insertionsPage = canonicalizePage(requireNonNull(insertionsPage, "insertionsPage is null"));
    }

    private Optional<Page> canonicalizePage(Optional<Page> page)
    {
        return page.isPresent() && page.get().getPositionCount() > 0 ? page : Optional.empty();
    }

    public Optional<Page> getDeletionsPage()
    {
        return deletionsPage;
    }

    public Optional<Page> getInsertionsPage()
    {
        return insertionsPage;
    }
}
