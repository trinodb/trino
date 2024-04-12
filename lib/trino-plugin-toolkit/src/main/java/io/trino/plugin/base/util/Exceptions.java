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
package io.trino.plugin.base.util;

import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;

import java.util.Optional;

import static com.google.common.base.Throwables.getCausalChain;

public final class Exceptions
{
    private Exceptions() {}

    public static Optional<ErrorCodeSupplier> findErrorCode(Throwable throwable)
    {
        return getCausalChain(throwable).stream()
                .filter(TrinoException.class::isInstance)
                .map(TrinoException.class::cast)
                .findFirst()
                .map(e -> e::getErrorCode);
    }
}
