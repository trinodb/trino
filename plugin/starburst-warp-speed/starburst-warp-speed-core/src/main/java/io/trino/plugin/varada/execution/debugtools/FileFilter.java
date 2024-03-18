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
package io.trino.plugin.varada.execution.debugtools;

import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.warmup.demoter.TupleFilter;

import java.util.Set;

public class FileFilter
        implements TupleFilter
{
    private final Set<String> filePaths;

    public FileFilter(Set<String> filePaths)
    {
        this.filePaths = filePaths;
    }

    @Override
    public boolean shouldHandle(WarmUpElement warmUpElement, RowGroupKey rowGroupKey)
    {
        return filePaths.contains(rowGroupKey.filePath());
    }

    @Override
    public String toString()
    {
        return "FileFilter{" + "filePaths=" + filePaths + '}';
    }
}
