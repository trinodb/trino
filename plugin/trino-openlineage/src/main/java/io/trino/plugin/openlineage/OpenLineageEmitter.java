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
package io.trino.plugin.openlineage;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.openlineage.client.OpenLineage;

/**
 * An interface to emit runEvent information into OpenLineage compatible sink - such as REST API or Kafka Topic.
 */
public interface OpenLineageEmitter
{
    void emit(OpenLineage.RunEvent runEvent, String queryId)
            throws JsonProcessingException;

    void close();
}
