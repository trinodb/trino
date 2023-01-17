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
package io.trino.operator.window;

import io.trino.metadata.TableHandle;
import io.trino.operator.OperatorFactory;

import java.util.Map;

public class MPOperator
    // Should implement WorkProcessorSourceOperator? So we'll be able to choose MP per split
{
    Map<TableHandle, OperatorFactory> options;  // PhysicalOperation instead of OperatorFactory?
}
