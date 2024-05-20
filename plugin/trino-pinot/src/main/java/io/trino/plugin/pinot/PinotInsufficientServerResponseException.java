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
package io.trino.plugin.pinot;

import io.trino.plugin.pinot.query.PinotQueryInfo;

import java.util.Optional;

import static io.trino.plugin.pinot.PinotErrorCode.PINOT_INSUFFICIENT_SERVER_RESPONSE;
import static java.lang.String.format;

public class PinotInsufficientServerResponseException
        extends PinotException
{
    public PinotInsufficientServerResponseException(PinotQueryInfo query, int numberOfServersResponded, int numberOfServersQueried)
    {
        this(query, format("Only %s out of %s servers responded for query %s", numberOfServersResponded, numberOfServersQueried, query.query()));
    }

    public PinotInsufficientServerResponseException(PinotQueryInfo query, String message)
    {
        super(PINOT_INSUFFICIENT_SERVER_RESPONSE, Optional.of(query.query()), message, true);
    }
}
