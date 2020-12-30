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
package io.prestosql.plugin.salesforce.driver.delegates;

import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.bind.XmlObject;
import io.prestosql.plugin.salesforce.driver.metadata.Column;
import io.prestosql.plugin.salesforce.driver.metadata.Table;
import io.prestosql.plugin.salesforce.driver.statement.FieldDef;
import org.apache.commons.collections4.IteratorUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class PartnerService
{
    private static final List<String> SOAP_RESPONSE_SERVICE_OBJECT_TYPES = Arrays.asList("type", "done", "queryLocator", "size");
    private PartnerConnection partnerConnection;
    private List<String> sObjectTypesCache;

    public PartnerService(PartnerConnection partnerConnection)
    {
        this.partnerConnection = partnerConnection;
    }

    public List<Table> getTables()
    {
        List<DescribeSObjectResult> sObjects = getSObjectsDescription();
        return sObjects.stream().map(this::convertToTable).collect(Collectors.toList());
    }

    public Table getTable(String tableName)
            throws ConnectionException
    {
        DescribeSObjectResult describeSObjectResult = partnerConnection.describeSObject(tableName);
        return convertToTable(describeSObjectResult);
    }

    public DescribeSObjectResult describeSObject(String sObjectType)
            throws ConnectionException
    {
        return partnerConnection.describeSObject(sObjectType);
    }

    private Table convertToTable(DescribeSObjectResult so)
    {
        List<Field> fields = Arrays.asList(so.getFields());
        List<Column> columns = fields.stream().map(this::convertToColumn).collect(Collectors.toList());
        return new Table(so.getName(), null, columns);
    }

    private Column convertToColumn(Field field)
    {
        try {
            Column column = new Column(field.getName(), getType(field));
            column.setLength(field.getLength());
            column.setNillable(field.getNillable());
            column.setCalculated(field.isCalculated() || field.isAutoNumber());
            String[] referenceTos = field.getReferenceTo();
            if (referenceTos != null) {
                for (String referenceTo : referenceTos) {
                    if (getSObjectTypes().contains(referenceTo)) {
                        column.setReferencedTable(referenceTo);
                        column.setReferencedColumn("Id");
                    }
                }
            }
            return column;
        }
        catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    private String getType(Field field)
    {
        String s = field.getType().toString();
        if (s.startsWith("_")) {
            s = s.substring("_".length());
        }
        return s.equalsIgnoreCase("double") ? "decimal" : s;
    }

    private List<String> getSObjectTypes()
            throws ConnectionException
    {
        if (sObjectTypesCache == null) {
            DescribeGlobalSObjectResult[] sobs = partnerConnection.describeGlobal().getSobjects();
            sObjectTypesCache = Arrays.stream(sobs).map(DescribeGlobalSObjectResult::getName).collect(Collectors.toList());
        }
        return sObjectTypesCache;
    }

    private List<DescribeSObjectResult> getSObjectsDescription()
    {
        DescribeGlobalResult describeGlobals = describeGlobal();
        List<String> tableNames = Arrays.stream(describeGlobals.getSobjects()).parallel().map(DescribeGlobalSObjectResult::getName).collect(Collectors.toList());
        List<List<String>> tableNamesBatched = toBatches(tableNames, 100);
        return tableNamesBatched.parallelStream().flatMap(batch -> describeSObjects(batch).parallelStream()).collect(Collectors.toList());
    }

    private DescribeGlobalResult describeGlobal()
    {
        try {
            return partnerConnection.describeGlobal();
        }
        catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    private List<DescribeSObjectResult> describeSObjects(List<String> batch)
    {
        DescribeSObjectResult[] result;
        try {
            result = partnerConnection.describeSObjects(batch.toArray(new String[0]));
            return Arrays.asList(result);
        }
        catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> List<List<T>> toBatches(List<T> objects, int batchSize)
    {
        List<List<T>> result = new ArrayList<>();
        for (int fromIndex = 0; fromIndex < objects.size(); fromIndex += batchSize) {
            int toIndex = Math.min(fromIndex + batchSize, objects.size());
            result.add(objects.subList(fromIndex, toIndex));
        }
        return result;
    }

    public List<List> queryAll(String soql, List<FieldDef> expectedSchema)
            throws ConnectionException
    {
        List<List> resultRows = Collections.synchronizedList(new ArrayList<>());
        QueryResult queryResult = null;
        do {
            queryResult = queryResult == null ? partnerConnection.query(soql) : partnerConnection.queryMore(queryResult.getQueryLocator());
            resultRows.addAll(removeServiceInfo(Arrays.asList(queryResult.getRecords())));
        }
        while (!queryResult.isDone());

        return PartnerResultToCartesianTable.expand(resultRows, expectedSchema);
    }

    public QueryResult query(String soql)
            throws ConnectionException
    {
        return partnerConnection.query(soql);
    }

    public QueryResult queryMore(QueryResult queryResult)
            throws ConnectionException, NoSuchElementException
    {
        if (queryResult.isDone()) {
            throw new NoSuchElementException();
        }

        return partnerConnection.queryMore(queryResult.getQueryLocator());
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

    private boolean isNestedResultset(XmlObject object)
    {
        return object.getXmlType() != null && "QueryResult".equals(object.getXmlType().getLocalPart());
    }

    private boolean isDataObjectType(XmlObject object)
    {
        return !SOAP_RESPONSE_SERVICE_OBJECT_TYPES.contains(object.getName().getLocalPart());
    }
}
