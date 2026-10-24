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
package io.trino.execution.executor;

// Memory figures that leaf split admission reads for a task.
public interface TaskMemory
{
    // User memory held by the task's leaf pipelines.
    long getLeafPipelineBytes();

    // Largest user memory held by one leaf driver.
    long getLeafDriverMaxBytes();

    // Bytes the query can still reserve on this node.
    long getQueryFreeBytes();

    // Bytes the query may reserve on this node in total.
    long getQueryMaxBytes();

    // Free bytes in the node memory pool.
    long getPoolFreeBytes();
}
