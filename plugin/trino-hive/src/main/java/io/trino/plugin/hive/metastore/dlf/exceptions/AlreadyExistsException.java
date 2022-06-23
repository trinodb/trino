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
package io.trino.plugin.hive.metastore.dlf.exceptions;

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;

public class AlreadyExistsException
        extends TrinoException
{
    private static final long serialVersionUID = 1L;

    public AlreadyExistsException(String message)
    {
        super(StandardErrorCode.ALREADY_EXISTS, message);
    }

    public AlreadyExistsException(String message, Throwable e)
    {
        super(StandardErrorCode.ALREADY_EXISTS, message, e);
    }
}
