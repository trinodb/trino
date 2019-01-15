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
package com.qubole.presto.kinesis;

/**
 * Interface representing a support class in the connector that needs to be called
 * from the Presto shutdown hook.
 * <p>
 * To use: implement the interface, and add the instance to the list inside the connector
 * so it can be shut down.
 */
public interface ConnectorShutdown
{
    /**
     * Perform any required shutdown activities.
     * <p>
     * Note that exceptions will be caught in the connector but it's better if
     * most exceptions are handled internally.
     */
    public void shutdown();
}
