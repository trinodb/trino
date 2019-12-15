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
package io.prestosql.plugin.salesforce.driver.statement;

import com.sforce.soap.partner.QueryResult;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.bind.XmlObject;
import io.prestosql.plugin.salesforce.driver.delegates.ForceResultField;
import io.prestosql.plugin.salesforce.driver.delegates.PartnerResultToCartesianTable;
import io.prestosql.plugin.salesforce.driver.delegates.PartnerService;
import io.prestosql.plugin.salesforce.driver.metadata.ColumnMap;
import org.apache.commons.collections4.IteratorUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class QueryWrapper
{
    private static final List<String> SOAP_RESPONSE_SERVICE_OBJECT_TYPES = Arrays.asList("type", "done", "queryLocator", "size");
    private final PartnerService service;
    private List<FieldDef> expectedSchema;
    private String soql;
    private Optional<QueryResult> queryResult = Optional.empty();

    private QueryWrapper(List<FieldDef> expectedSchema, PartnerService service)
    {
        this.expectedSchema = expectedSchema;
        this.service = service;
    }

    QueryWrapper(String soql, List<FieldDef> expectedSchema, PartnerService service)
    {
        this(expectedSchema, service);
        this.soql = soql;
    }

    public List<ColumnMap<String, Object>> next()
            throws ConnectionException
    {
        try {
            if (!queryResult.isPresent()) {
                queryResult = Optional.of(service.query(soql));
            }
            else {
                queryResult = Optional.of(service.queryMore(queryResult.get()));
            }
        }
        catch (ConnectionException e) {
            ForcePreparedStatement.rethrowAsNonChecked(e);
        }

        List<List> resultRows = Collections.synchronizedList(new ArrayList<>());
        resultRows.addAll(removeServiceInfo(Arrays.asList(queryResult.get().getRecords())));
        List<List> cartesianTable = PartnerResultToCartesianTable.expand(resultRows, expectedSchema);

        List<ColumnMap<String, Object>> maps = Collections.synchronizedList(new ArrayList<>());
        cartesianTable.forEach(record -> maps.add(convertToColumnMap(record)));
        return maps;
    }

    private ColumnMap<String, Object> convertToColumnMap(List<ForceResultField> record)
    {
        ColumnMap<String, Object> columnMap = new ColumnMap<>();
        record.stream().map(field -> field == null ? new ForceResultField(null, null, null, null) : field).forEach(field -> {
            columnMap.put(field.getFullName(), field.getValue());
        });
        return columnMap;
    }

    private List<List> removeServiceInfo(Iterator<XmlObject> rows)
    {
        return removeServiceInfo(IteratorUtils.toList(rows));
    }

    private List<List> removeServiceInfo(List<XmlObject> rows)
    {
        return rows.stream().filter(this::isDataObjectType).map(this::removeServiceInfo).collect(Collectors.toList());
    }

    private List removeServiceInfo(XmlObject row)
    {
        return IteratorUtils.toList(row.getChildren()).stream().filter(this::isDataObjectType).skip(1) // Removes duplicate Id from SF Partner API response
                // (https://developer.salesforce.com/forums/?id=906F00000008kciIAA)
                .map(field -> isNestedResultset(field) ? removeServiceInfo(field.getChildren()) : toForceResultField(field)).collect(Collectors.toList());
    }

    private boolean isNestedResultset(XmlObject object)
    {
        return object.getXmlType() != null && "QueryResult".equals(object.getXmlType().getLocalPart());
    }

    private ForceResultField toForceResultField(XmlObject field)
    {
        String fieldType = field.getXmlType() != null ? field.getXmlType().getLocalPart() : null;
        if ("sObject".equalsIgnoreCase(fieldType)) {
            List<XmlObject> children = new ArrayList<>();
            field.getChildren().forEachRemaining(children::add);
            field = children.get(2);
        }
        String name = field.getName().getLocalPart();
        Object value = field.getValue();
        return new ForceResultField(null, fieldType, name, value);
    }

    private boolean isDataObjectType(XmlObject object)
    {
        return !SOAP_RESPONSE_SERVICE_OBJECT_TYPES.contains(object.getName().getLocalPart());
    }
}
