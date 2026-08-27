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
package io.trino.plugin.paimon;

final class PaimonHandleJsonUtils
{
    private static final String JACKSON_TYPE_FIELD = "@type";
    private static final String PAIMON_HANDLE_PACKAGE = "io.trino.plugin.paimon.";

    private PaimonHandleJsonUtils() {}

    static void rejectUnknownHandleJsonField(String handleName, String fieldName, Object value)
    {
        if (JACKSON_TYPE_FIELD.equals(fieldName)) {
            if (value instanceof String typedHandleId && isExpectedPaimonTypedHandleId(handleName, typedHandleId)) {
                return;
            }
            throw new IllegalArgumentException("Invalid " + handleName + " JSON @type field");
        }
        throw new IllegalArgumentException("Unknown " + handleName + " JSON field: " + fieldName);
    }

    private static boolean isExpectedPaimonTypedHandleId(String handleName, String typedHandleId)
    {
        int splitPoint = typedHandleId.lastIndexOf(':');
        if (splitPoint < 1) {
            return false;
        }
        return typedHandleId.substring(splitPoint + 1).equals(PAIMON_HANDLE_PACKAGE + handleName);
    }
}
