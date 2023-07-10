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
package io.trino.metadata;

import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionMetadata;

import static io.trino.spi.function.FunctionDependencyDeclaration.NO_DEPENDENCIES;

public interface SqlFunction
{
    FunctionMetadata getFunctionMetadata();

    default FunctionDependencyDeclaration getFunctionDependencies(BoundSignature boundSignature)
    {
        return getFunctionDependencies();
    }

    default FunctionDependencyDeclaration getFunctionDependencies()
    {
        return NO_DEPENDENCIES;
    }
}
