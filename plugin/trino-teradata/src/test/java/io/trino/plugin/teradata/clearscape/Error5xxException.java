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

package io.trino.plugin.teradata.clearscape;

public class Error5xxException
        extends BaseException
{
    public Error5xxException(int statusCode, String body, String reason)
    {
        super(statusCode, body, reason);
    }

    public Error5xxException(int statusCode, String body)
    {
        super(statusCode, body);
    }
}
