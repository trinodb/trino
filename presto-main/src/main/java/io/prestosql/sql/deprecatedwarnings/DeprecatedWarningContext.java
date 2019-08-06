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
package io.prestosql.sql.deprecatedwarnings;

import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.Signature;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class DeprecatedWarningContext
{
    private final List<QualifiedObjectName> tables;
    private final List<Signature> functionSignatures;
    private final Map<String, String> systemProperties;
    private final Set<QualifiedObjectName> views;

    public DeprecatedWarningContext(
            List<QualifiedObjectName> tables,
            List<Signature> functionSignatures,
            Map<String, String> systemProperties,
            Set<QualifiedObjectName> views)
    {
        this.tables = requireNonNull(tables, "tables is null");
        this.functionSignatures = requireNonNull(functionSignatures, "functionSignatures is null");
        this.systemProperties = requireNonNull(systemProperties, "systemProperties is null");
        this.views = requireNonNull(views, "views is null");
    }

    public List<QualifiedObjectName> getTables()
    {
        return tables;
    }

    public List<Signature> getFunctionSignatures()
    {
        return functionSignatures;
    }

    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    public Set<QualifiedObjectName> getViews()
    {
        return views;
    }
}
