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

public enum OpenLineageTrinoFacet
{
    TRINO_METADATA("trino_metadata"),
    TRINO_QUERY_STATISTICS("trino_query_statistics"),
    TRINO_QUERY_CONTEXT("trino_query_context");

    final String text;

    OpenLineageTrinoFacet(String text)
    {
        this.text = text;
    }

    public String getText()
    {
        return this.text;
    }

    public static OpenLineageTrinoFacet fromText(String text)
            throws IllegalArgumentException
    {
        for (OpenLineageTrinoFacet facet : OpenLineageTrinoFacet.values()) {
            if (facet.text.equals(text)) {
                return facet;
            }
        }

        throw new IllegalArgumentException(text);
    }
}
