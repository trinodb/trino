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
package io.trino.sql;

import com.google.inject.Inject;
import io.trino.connector.ConnectorSensitivePropertiesRegistry;
import io.trino.spi.connector.ConnectorName;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class SensitiveStatementRedactor
{
    private static final String REDACTED_VALUE = "***";
    private static final Pattern PROPERTY_ASSIGNMENT_PATTERN = Pattern.compile("(\s*=\s*)('.*')");
    private static final String PROPERTY_ASSIGNMENT_REPLACEMENT = "$1'%s'".formatted(REDACTED_VALUE);

    private final ConnectorSensitivePropertiesRegistry connectorSensitivePropertiesRegistry;

    @Inject
    public SensitiveStatementRedactor(ConnectorSensitivePropertiesRegistry connectorSensitivePropertiesRegistry)
    {
        this.connectorSensitivePropertiesRegistry = requireNonNull(connectorSensitivePropertiesRegistry, "connectorSensitivePropertiesRegistry is null");
    }

    public String redact(String rawQuery, Statement statement)
    {
        RedactingVisitor visitor = new RedactingVisitor();
        Node redactedStatement = visitor.process(statement);
        if (!redactedStatement.equals(statement)) {
            return SqlFormatter.formatSql(redactedStatement);
        }
        return rawQuery;
    }

    public String redact(String rawQuery)
    {
        if (rawQuery.toLowerCase(Locale.ROOT).contains("catalog")) {
            return PROPERTY_ASSIGNMENT_PATTERN.matcher(rawQuery).replaceAll(PROPERTY_ASSIGNMENT_REPLACEMENT);
        }
        return rawQuery;
    }

    private class RedactingVisitor
            extends AstVisitor<Node, Void>
    {
        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }

        @Override
        protected Node visitExplain(Explain explain, Void context)
        {
            Statement statement = (Statement) process(explain.getStatement());
            return new Explain(explain.getLocation(), statement, explain.getOptions());
        }

        @Override
        protected Node visitExplainAnalyze(ExplainAnalyze explainAnalyze, Void context)
        {
            Statement statement = (Statement) process(explainAnalyze.getStatement());
            return new ExplainAnalyze(explainAnalyze.getLocation(), statement, explainAnalyze.isVerbose());
        }

        @Override
        protected Node visitCreateCatalog(CreateCatalog createCatalog, Void context)
        {
            ConnectorName connectorName = new ConnectorName(createCatalog.getConnectorName().getValue());
            List<Property> redactedProperties = redact(connectorName, createCatalog.getProperties());
            return createCatalog.withProperties(redactedProperties);
        }

        private List<Property> redact(ConnectorName connectorName, List<Property> properties)
        {
            return connectorSensitivePropertiesRegistry.getSensitivePropertyNames(connectorName)
                    .map(sensitiveProperties -> redactSensitiveProperties(properties, sensitiveProperties))
                    // If someone tries to use a non-existent connector, we assume they
                    // misspelled the name and, for safety, we redact all the properties.
                    .orElseGet(() -> redactAllProperties(properties));
        }

        private List<Property> redactSensitiveProperties(List<Property> properties, Set<String> sensitiveProperties)
        {
            return properties.stream()
                    .map(property -> {
                        if (sensitiveProperties.contains(property.getName().getValue())) {
                            return new Property(property.getName(), new StringLiteral(REDACTED_VALUE));
                        }
                        return property;
                    })
                    .collect(toImmutableList());
        }

        private List<Property> redactAllProperties(List<Property> properties)
        {
            return properties.stream()
                    .map(property -> new Property(property.getName(), new StringLiteral(REDACTED_VALUE)))
                    .collect(toImmutableList());
        }
    }
}
