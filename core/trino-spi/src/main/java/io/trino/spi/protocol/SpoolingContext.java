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
package io.trino.spi.protocol;

import io.trino.spi.Experimental;
import io.trino.spi.QueryId;

import static java.util.Objects.requireNonNull;

@Experimental(eta = "2025-05-31")
public record SpoolingContext(String encoding, QueryId queryId, long rows, long size)
{
    public SpoolingContext
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(encoding, "encoding is null");
        if (rows < 0) {
            throw new IllegalArgumentException("rows is negative");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size is negative");
        }
    }
}
