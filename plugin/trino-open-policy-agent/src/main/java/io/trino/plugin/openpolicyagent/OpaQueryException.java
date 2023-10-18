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
package io.trino.plugin.openpolicyagent;

public abstract class OpaQueryException
        extends RuntimeException
{
    public OpaQueryException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public static final class QueryFailed
            extends OpaQueryException
    {
        public QueryFailed(Throwable cause)
        {
            super("Failed to query OPA backend", cause);
        }
    }

    public static final class SerializeFailed
            extends OpaQueryException
    {
        public SerializeFailed(Throwable cause)
        {
            super("Failed to serialize OPA query context", cause);
        }
    }

    public static final class DeserializeFailed
            extends OpaQueryException
    {
        public DeserializeFailed(Throwable cause)
        {
            super("Failed to deserialize OPA policy response", cause);
        }
    }

    public static final class PolicyNotFound
            extends OpaQueryException
    {
        public PolicyNotFound(String policyName)
        {
            super("OPA policy named " + policyName + " did not return a value (or does not exist)", null);
        }
    }

    public static final class OpaServerError
            extends OpaQueryException
    {
        public OpaServerError(String policyName, int statusCode, String extra)
        {
            super(String.format("OPA server returned status %d when processing policy %s: %s", statusCode, policyName, extra), null);
        }
    }

    public static final class OpaInternalPluginError
            extends OpaQueryException
    {
        public OpaInternalPluginError(String message)
        {
            super(message, null);
        }
    }
}
