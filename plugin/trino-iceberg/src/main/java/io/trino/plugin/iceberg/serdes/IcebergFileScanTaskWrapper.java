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
package io.trino.plugin.iceberg.serdes;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.iceberg.FileScanTask;

import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.util.SerializationUtil.deserializeFromBase64;
import static org.apache.iceberg.util.SerializationUtil.serializeToBase64;

public class IcebergFileScanTaskWrapper
{
    private final FileScanTask task;

    @JsonCreator
    public IcebergFileScanTaskWrapper(String serializedTask)
    {
        this.task = deserializeFromBase64(requireNonNull(serializedTask, "serializedTask is null"));
    }

    private IcebergFileScanTaskWrapper(FileScanTask task)
    {
        this.task = requireNonNull(task, "task is null");
    }

    public static IcebergFileScanTaskWrapper wrap(FileScanTask task)
    {
        return new IcebergFileScanTaskWrapper(task);
    }

    public FileScanTask getTask()
    {
        return task;
    }

    @JsonValue
    public String serialize()
    {
        return serializeToBase64(task);
    }
}
