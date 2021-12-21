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
package io.trino.sql.parser.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.hivesql.sql.parser.SqlBaseLexer;
import io.hivesql.sql.parser.SqlBaseParser;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.Cube;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.DataTypeParameter;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.DropSchema;
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.DropView;
import io.trino.sql.tree.Except;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.GroupingElement;
import io.trino.sql.tree.GroupingSets;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.Insert;
import io.trino.sql.tree.Intersect;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.JoinUsing;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NaturalJoin;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QueryBody;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.RenameTable;
import io.trino.sql.tree.Rollup;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.SampledRelation;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SetSession;
import io.trino.sql.tree.ShowColumns;
import io.trino.sql.tree.ShowCreate;
import io.trino.sql.tree.ShowFunctions;
import io.trino.sql.tree.ShowSchemas;
import io.trino.sql.tree.ShowTables;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableElement;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.TimeLiteral;
import io.trino.sql.tree.TimestampLiteral;
import io.trino.sql.tree.TypeParameter;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Use;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.Window;
import io.trino.sql.tree.WindowFrame;
import io.trino.sql.tree.WindowSpecification;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HiveAstBuilder
        extends io.hivesql.sql.parser.SqlBaseBaseVisitor<Node>
{
    private final ParsingOptions parsingOptions;

    public HiveAstBuilder(ParsingOptions parsingOptions)
    {
        this.parsingOptions = requireNonNull(parsingOptions, "parsingOptions is null");
    }

    @Override
    protected Node aggregateResult(Node currentResult, Node nextResult)
    {
        if (currentResult == null) {
            return nextResult;
        }
        if (nextResult == null) {
            return currentResult;
        }

        throw new RuntimeException("please check, how should we merge them? " +
            "most possible reason is executing a syntax that hive not support");
    }

    //////////////////
    //
    //////////////////
    @Override
    public Node visitSetConfiguration(SqlBaseParser.SetConfigurationContext ctx)
    {
        return new SetHiveConfiguration(getLocation(ctx), ctx.getText());
    }

    @Override
    public Node visitShowPartitions(SqlBaseParser.ShowPartitionsContext ctx)
    {
        Table table = (Table) visit(ctx.tableIdentifier());
        QualifiedName qualifiedName = table.getName();
        String[] parts = new String[qualifiedName.getParts().size()];
        for (int i = 0; i < qualifiedName.getParts().size(); ++i) {
            parts[i] = qualifiedName.getParts().get(i);
        }
        parts[qualifiedName.getParts().size() - 1] = qualifiedName.getParts().get(qualifiedName.getParts().size() - 1) + "$partitions";
        String[] newPart = new String[parts.length - 1];
        for (int i = 0; i < parts.length - 1; ++i) {
            newPart[i] = parts[i + 1];
        }
        QualifiedName newQualifiedName;
        if (parts.length > 1) {
            newQualifiedName = QualifiedName.of(parts[0], newPart);
        }
        else {
            newQualifiedName = QualifiedName.of(parts[0]);
        }
        Table newTable = new Table(newQualifiedName);
        Optional<Expression> condition = Optional.empty();
        SqlBaseParser.PartitionSpecContext partitionSpecContext = ctx.partitionSpec();
        Expression expression;
        if (partitionSpecContext != null) {
            expression = getPartitionExpression(partitionSpecContext);
            condition = Optional.of(expression);
        }

        Select select = new Select(false, ImmutableList.of(new AllColumns()));
        QuerySpecification querySpecification = new QuerySpecification(
                select,
                Optional.of(newTable),
                condition,
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        Query query = new Query(
                Optional.empty(),
                querySpecification,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        return query;
    }

    @Override
    public Node visitSetOperation(SqlBaseParser.SetOperationContext ctx)
    {
        QueryBody left = getQueryBody(visit(ctx.left));
        QueryBody right = getQueryBody(visit(ctx.right));

        boolean distinct = ctx.setQuantifier() == null || ctx.setQuantifier().DISTINCT() != null;

        switch (ctx.operator.getType()) {
            case SqlBaseLexer.UNION:
                return new Union(getLocation(ctx.UNION()), ImmutableList.of(left, right), distinct);
            case SqlBaseLexer.INTERSECT:
                return new Intersect(getLocation(ctx.INTERSECT()), ImmutableList.of(left, right), distinct);
            case SqlBaseLexer.EXCEPT:
                return new Except(getLocation(ctx.EXCEPT()), left, right, distinct);
        }

        throw parseError("Unsupported set operation: " + ctx.operator.getText(), ctx);
    }

    @Override
    public Node visitCreateDatabase(SqlBaseParser.CreateDatabaseContext ctx)
    {
        List<Property> properties = new ArrayList<>();
        SqlBaseParser.TablePropertyListContext tablePropertyListContext = ctx.tablePropertyList();
        if (tablePropertyListContext != null) {
            List<SqlBaseParser.TablePropertyContext> tablePropertyContexts =
                    tablePropertyListContext.tableProperty();
            for (SqlBaseParser.TablePropertyContext tablePropertyContext : tablePropertyContexts) {
                Property property = (Property) visitTableProperty(tablePropertyContext);
                properties.add(property);
            }
        }
        return new CreateSchema(
            getLocation(ctx),
            getQualifiedName(ctx.identifier().getText()),
            ctx.EXISTS() != null,
            properties,
            Optional.empty());
    }

    @Override
    public Node visitDelete(SqlBaseParser.DeleteContext ctx)
    {
        return new Delete(
            getLocation(ctx),
                new Table(getLocation(ctx), getQualifiedName(ctx.qualifiedName())),
            visitIfPresent(ctx.booleanExpression(), Expression.class));
    }

    @Override
    public Node visitAddTableColumns(SqlBaseParser.AddTableColumnsContext ctx)
    {
        if (ctx.colTypeList().colType().size() != 1) {
            throw parseError("not support to add more than or less than 1 columns at one time", ctx);
        }

        SqlBaseParser.ColTypeContext colTypeContext = ctx.colTypeList().colType(0);
        String comment = "";
        if (colTypeContext.COMMENT() != null) {
            comment = colTypeContext.STRING().getText()
                .substring(1, colTypeContext.STRING().getText().length() - 1);
        }
        DataType dataType = (DataType) visit(colTypeContext.dataType());
        ColumnDefinition columnDefinition = new ColumnDefinition(
                new Identifier(getLocation(colTypeContext),
                tryUnquote(colTypeContext.identifier().getText()), true),
                dataType,
                true,
                new ArrayList<>(),
                Optional.of(comment));

        return new AddColumn(getLocation(ctx),
            getQualifiedName(ctx.tableIdentifier()),
            columnDefinition,
            true,
            true);
    }

    @Override
    public Node visitAddTablePartition(SqlBaseParser.AddTablePartitionContext ctx)
    {
        List<Map<Object, Object>> partitions = new ArrayList<>();

        List<SqlBaseParser.PartitionSpecLocationContext> partitionSpecLocationContexts = ctx.partitionSpecLocation();
        partitionSpecLocationContexts.forEach(partitionSpecLocationContext -> {
            Map<Object, Object> partition = new TreeMap<>();
            List<SqlBaseParser.PartitionValContext> partitionValContexts = partitionSpecLocationContext.partitionSpec().partitionVal();
            partitionValContexts.forEach(partitionValContext -> {
                String partitionColumnName = partitionValContext.identifier().getText();
                String partitionValue = partitionValContext.constant().getText().replace("'", "");
                partition.put(partitionColumnName, partitionValue);
            });
            partitions.add(partition);
        });
        // Do not support location temporarily
        return new AddPartition(getLocation(ctx), getQualifiedName(ctx.tableIdentifier()), null, partitions);
    }

    @Override
    public Node visitRenameTable(SqlBaseParser.RenameTableContext ctx)
    {
        return new RenameTable(
            getLocation(ctx),
            getQualifiedName(ctx.from),
            getQualifiedName(ctx.to),
            true);
    }

    @Override
    public Node visitLoadData(SqlBaseParser.LoadDataContext ctx)
    {
        if (ctx.LOCAL() != null) {
            throw parseError("hive on presto not support load data from local!", ctx);
        }
        List<SingleColumn> partitions = null;
        if (ctx.partitionSpec() != null) {
            partitions = visit(ctx.partitionSpec().partitionVal(), SingleColumn.class);
            for (int i = 0; i < partitions.size(); ++i) {
                SingleColumn singleColumn = partitions.get(i);
                if (!singleColumn.getAlias().isPresent()) {
                    throw parseError("load data to dynamic partition not support " + singleColumn.getAlias().get(), ctx);
                }
            }
        }
        return new LoadData(
                Optional.of(getLocation(ctx)),
                getQualifiedName(ctx.tableIdentifier()),
                ctx.path.getText(),
                ctx.OVERWRITE() != null,
                partitions == null ? ImmutableList.of() : partitions);
    }

    @Override
    public Node visitTableProperty(SqlBaseParser.TablePropertyContext ctx)
    {
        Expression value = null;
        Object type = ctx.value.STRING();
        if (type != null) {
            value = new StringLiteral(tryUnquote(ctx.value.getText()));
        }
        type = ctx.value.booleanValue();
        if (type != null) {
            value = new BooleanLiteral(ctx.value.getText());
        }
        type = ctx.value.DECIMAL_VALUE();
        if (type != null) {
            value = new DecimalLiteral(ctx.value.getText());
        }
        type = ctx.value.INTEGER_VALUE();
        if (type != null) {
            value = new LongLiteral(ctx.value.getText());
        }
        return new Property(getLocation(ctx),
                new Identifier(tryUnquote(ctx.key.getText()), false), value);
    }

    @Override
    public Node visitDropDatabase(SqlBaseParser.DropDatabaseContext ctx)
    {
        return new DropSchema(
            getLocation(ctx),
            getQualifiedName(ctx.identifier().getText()),
            ctx.EXISTS() != null,
            false);
    }

    @Override
    public Node visitSetDatabaseProperties(SqlBaseParser.SetDatabasePropertiesContext ctx)
    {
        throw parseError("not support alter properties in presto hive sql", ctx);
    }

    @Override
    public Node visitMultiInsertQueryBody(SqlBaseParser.MultiInsertQueryBodyContext ctx)
    {
        throw parseError("Don't support MultiInsertQuery", ctx);
    }

    @Override
    public Node visitAliasedQuery(SqlBaseParser.AliasedQueryContext ctx)
    {
        TableSubquery child = new TableSubquery(getLocation(ctx), (Query) visit(ctx.queryNoWith()));

        Identifier tableAlias = (Identifier) visit(ctx.tableAlias());
        Relation result;
        if (tableAlias == null) {
            result = child;
        }
        else {
            result = new AliasedRelation(getLocation(ctx), child, tableAlias, null);
        }
        if (ctx.sampledRelation() == null) {
            return result;
        }
        else {
            return getSampleRelation(ctx.sampledRelation(), result);
        }
    }

    @Override
    public Node visitAliasedRelation(SqlBaseParser.AliasedRelationContext ctx)
    {
        if (ctx.relation() != null) {
            Relation relation = (Relation) visit(ctx.relation());

            Identifier tableAlias = (Identifier) visit(ctx.tableAlias());
            Relation result;
            if (tableAlias == null) {
                result = relation;
            }
            else {
                result = new AliasedRelation(getLocation(ctx), relation, tableAlias, null);
            }
            if (ctx.sampledRelation() == null) {
                return result;
            }
            else {
                return getSampleRelation(ctx.sampledRelation(), result);
            }
        }

        return super.visitAliasedRelation(ctx);
    }

    @Override
    public Node visitConstantList(SqlBaseParser.ConstantListContext ctx)
    {
        return super.visitConstantList(ctx);
    }

    @Override
    public Node visitManageResource(SqlBaseParser.ManageResourceContext ctx)
    {
        if (ctx.LIST() != null) {
            throw parseError("Don't support 'list resource' command", ctx);
        }
        else {
            return new AddManageResource(Optional.of(getLocation(ctx)));
        }
    }

    private String getSpecialChar(String input)
    {
        ImmutableMap map = ImmutableMap.builder()
                .put('b', "\b")
                .put('f', "\f")
                .put('n', "\n")
                .put('r', "\r")
                .put('t', "\t")
                .put('\\', "\\")
                .put('\'', "\'")
                .put('\"', "\"")
                .build();
        if (input == null || input.length() != 2 || input.charAt(0) != '\\') {
            return input;
        }
        if (input.length() == 2 && input.charAt(0) == '\\') {
            return (String) map.get(input.charAt(1));
        }
        return input;
    }

    private String getFileFormat(String sqlFormat)
    {
        if (sqlFormat == null || sqlFormat.isEmpty()) {
            return "ORC";
        }
        ImmutableMap map = ImmutableMap.builder()
                .put("TEXTFILE", "TEXTFILE")
                .put("SEQUENCEFILE", "SEQUENCEFILE")
                .put("ORC", "ORC")
                .put("ORCFILE", "ORC")
                .put("PARQUET", "PARQUET")
                .put("AVRO", "AVRO")
                .put("RCFILE", "RCBINARY")
                .put("JSONFILE", "JSON")
                .build();
        if (map.containsKey(sqlFormat)) {
            return (String) map.get(sqlFormat.toUpperCase(Locale.ENGLISH));
        }
        return "ORC";
    }

    @Override
    public Node visitCreateHiveTable(SqlBaseParser.CreateHiveTableContext ctx)
    {
        // get table comment
        Optional<String> comment = Optional.empty();
        if (ctx.comment != null && ctx.comment.getText() != null) {
            comment = Optional.of(unquote(ctx.comment.getText()));
        }

        List<Property> properties = new ArrayList<>();

        // get partition columns
        if (ctx.partitionColumns != null) {
            List<SqlBaseParser.ColTypeContext> colTypeListContexts =
                    ctx.partitionColumns.colType();
            List<Expression> partitionedCol = new ArrayList<>();
            for (SqlBaseParser.ColTypeContext colTypeContext : colTypeListContexts) {
                partitionedCol.add(new StringLiteral(tryUnquote(colTypeContext.identifier().getText())));
            }
            Property partitionedBy =
                    new Property(new Identifier("partitioned_by", false),
                            new ArrayConstructor(partitionedCol));
            properties.add(partitionedBy);
        }

        boolean formatGet = false;
        // get store format
        if (ctx.rowFormat() != null && ctx.rowFormat(0) != null) {
            SqlBaseParser.RowFormatContext rowFormatContext = ctx.rowFormat(0);
            if (rowFormatContext instanceof SqlBaseParser.RowFormatSerdeContext) {
                String format = ((SqlBaseParser.RowFormatSerdeContext) rowFormatContext).name.getText();
                if (format.contains("org.apache.hive.hcatalog.data.JsonSerDe")
                        || format.contains("org.openx.data.jsonserde.JsonSerDe")) {
                    Property property = new Property(new Identifier("format", false),
                            new StringLiteral("JSON"));
                    properties.add(property);
                    formatGet = true;
                }
                else if (format.contains("ParquetHiveSerDe")) {
                    Property property = new Property(new Identifier("format", false),
                            new StringLiteral("PARQUET"));
                    properties.add(property);
                    formatGet = true;
                }
                else if (format.contains("org.apache.hadoop.hive.serde2.avro.AvroSerDe")) {
                    Property property = new Property(new Identifier("format", false),
                            new StringLiteral("AVRO"));
                    properties.add(property);
                    formatGet = true;
                }
                else {
                    Property property = new Property(new Identifier("format", false),
                            new StringLiteral("ORC"));
                    properties.add(property);
                    formatGet = true;
                }
            }
            else if (rowFormatContext instanceof SqlBaseParser.RowFormatDelimitedContext) {
                SqlBaseParser.RowFormatDelimitedContext rowFormatDelimitedContext =
                        ((SqlBaseParser.RowFormatDelimitedContext) rowFormatContext);
                if (rowFormatDelimitedContext.DELIMITED() != null) {
                    Property formatProperty = new Property(new Identifier("format", false),
                            new StringLiteral("TEXTFILE"));
                    properties.add(formatProperty);
                    formatGet = true;
                    if (rowFormatDelimitedContext.FIELDS() != null) {
                        Property property = new Property(new Identifier("textfile_field_separator", false),
                                new StringLiteral(getSpecialChar(unquote(rowFormatDelimitedContext.fieldsTerminatedBy.getText()))));
                        properties.add(property);
                    }
                    if (rowFormatDelimitedContext.ESCAPED() != null) {
                        Property property = new Property(new Identifier("textfile_field_separator_escape", false),
                                new StringLiteral(getSpecialChar(unquote(rowFormatDelimitedContext.escapedBy.getText()))));
                        properties.add(property);
                    }
                }
            }
        }

        if (!formatGet && ctx.createFileFormat() != null && ctx.createFileFormat(0) != null) {
            SqlBaseParser.CreateFileFormatContext createFileFormatContext = ctx.createFileFormat(0);

            if (createFileFormatContext.BY() != null) {
                throw parseError("not support stored by which is syntax for hive storage handler", createFileFormatContext);
            }
            String format = createFileFormatContext.fileFormat().getText();

            if (createFileFormatContext.fileFormat() instanceof SqlBaseParser.GenericFileFormatContext) {
                SqlBaseParser.GenericFileFormatContext genericFileFormatContext = (SqlBaseParser.GenericFileFormatContext) createFileFormatContext.fileFormat();
                String hiveFormat = getFileFormat(genericFileFormatContext.identifier().getText().toUpperCase(Locale.ENGLISH));
                Property property = new Property(new Identifier("format", false),
                        new StringLiteral(hiveFormat));
                properties.add(property);
                formatGet = true;
            }
            else {
                throw parseError("create table format " + format + " not supported!",
                    createFileFormatContext);
            }
        }

        // if external table
        if (ctx.createTableHeader().EXTERNAL() != null) {
            if (ctx.locationSpec() == null || ctx.locationSpec().size() == 0) {
                throw parseError("external table need a location", ctx);
            }
            String loc = tryUnquote(ctx.locationSpec(0).STRING().getText());
            Property location = new Property(new Identifier("external_location", true),
                    new StringLiteral(getLocation(ctx.locationSpec(0)), loc));
            properties.add(location);
        }
        else {
            if (ctx.locationSpec() != null && ctx.locationSpec().size() > 0) {
                String loc = tryUnquote(ctx.locationSpec(0).STRING().getText());
                Property location = new Property(new Identifier("external_location", true),
                        new StringLiteral(getLocation(ctx.locationSpec(0)), loc));
                properties.add(location);
            }
        }

        // get normal columns
        List<TableElement> elements = new ArrayList<>();
        if (ctx.columns != null) {
            List<SqlBaseParser.ColTypeContext> colTypeListContexts = ctx.columns.colType();
            if (ctx.partitionColumns != null) {
                colTypeListContexts.addAll(ctx.partitionColumns.colType());
            }
            if (colTypeListContexts.size() > 0) {
                for (SqlBaseParser.ColTypeContext colTypeContext : colTypeListContexts) {
                    Optional colComment = Optional.empty();
                    if (colTypeContext.COMMENT() != null) {
                        colComment = Optional.of(unquote(colTypeContext.COMMENT().getText()));
                    }
                    DataType dataType = (DataType) visit(colTypeContext.dataType());
                    TableElement tableElement = new ColumnDefinition(
                            new Identifier(getLocation(colTypeContext),
                            tryUnquote(colTypeContext.identifier().getText()), true),
                            dataType,
                            true,
                            new ArrayList<>(),
                            colComment);
                    elements.add(tableElement);
                }
            }
        }

        // process create table as select
        if (ctx.query() != null) {
            if (ctx.AS() == null) {
                throw parseError("'create table t as query' lack word of 'as' before 'query'", ctx.query());
            }
            SqlBaseParser.SingleInsertQueryContext singleInsertQueryContext =
                    (SqlBaseParser.SingleInsertQueryContext) ctx.query().queryNoWith();
            Object queryObject = visit(singleInsertQueryContext.queryTerm());
            QueryBody term = null;
            if (queryObject instanceof QueryBody) {
                term = (QueryBody) visit(singleInsertQueryContext.queryTerm());
            }
            else if (queryObject instanceof Query) {
                term = new TableSubquery(
                    getLocation(ctx.query()),
                    (Query) queryObject);
            }

            Query query = (Query) withQueryOrganization(term, singleInsertQueryContext.queryOrganization());
            QualifiedName target = getQualifiedName(ctx.createTableHeader().tableIdentifier());
            return new CreateTableAsSelect(
                target,
                query,
                ctx.createTableHeader().EXISTS() != null,
                properties,
                true,
                Optional.empty(),
                comment);
        }

        // only create table
        return new CreateTable(
            getLocation(ctx),
            getQualifiedName(ctx.createTableHeader().tableIdentifier()),
            elements,
            ctx.createTableHeader().EXISTS() != null,
            properties,
            comment);
    }

    @Override
    public Node visitArithmeticOperator(SqlBaseParser.ArithmeticOperatorContext ctx)
    {
        return super.visitArithmeticOperator(ctx);
    }

    @Override
    public Node visitCreateTableLike(SqlBaseParser.CreateTableLikeContext ctx)
    {
        return new CreateTableLike(
            Optional.of(getLocation(ctx)),
            getQualifiedName(ctx.target),
            getQualifiedName(ctx.source),
            ctx.EXISTS() != null && ctx.NOT() != null);
    }

    @Override
    public Node visitBooleanValue(SqlBaseParser.BooleanValueContext ctx)
    {
        return super.visitBooleanValue(ctx);
    }

    @Override
    public Node visitCtes(SqlBaseParser.CtesContext ctx)
    {
        With with;
        List<SqlBaseParser.NamedQueryContext> namedQueryContexts = ctx.namedQuery();
        List<WithQuery> queries = new ArrayList<>();
        for (SqlBaseParser.NamedQueryContext namedQueryContext : namedQueryContexts) {
            WithQuery withQuery = new WithQuery(
                    new Identifier(getLocation(namedQueryContext), namedQueryContext.name.getText(), false),
                    (Query) visitQuery(namedQueryContext.query()),
                    Optional.empty());
            queries.add(withQuery);
        }
        with = new With(getLocation(ctx), false, queries);
        return with;
    }

    @Override
    public Node visitCreateFunction(SqlBaseParser.CreateFunctionContext ctx)
    {
        return new CreateFunction(Optional.of(getLocation(ctx)), ctx.qualifiedName().getText(), ctx.className.getText());
    }

    @Override
    public Node visitExplain(SqlBaseParser.ExplainContext ctx)
    {
        return new Explain(getLocation(ctx),
                (Statement) visit(ctx.statement()),
                new ArrayList<>());
    }

    @Override
    public Node visitChangeColumn(SqlBaseParser.ChangeColumnContext ctx)
    {
        throw parseError("change column type not support yet!", ctx);
    }

    @Override
    public Node visitDropTablePartitions(SqlBaseParser.DropTablePartitionsContext ctx)
    {
        List<SqlBaseParser.PartitionSpecContext> partitionSpecContexts = ctx.partitionSpec();
        Expression expressionRoot;

        expressionRoot = getPartitionExpression(partitionSpecContexts.get(0));
        for (int i = 1; i < partitionSpecContexts.size(); ++i) {
            expressionRoot = new LogicalExpression(LogicalExpression.Operator.OR,
                ImmutableList.of(expressionRoot, getPartitionExpression(partitionSpecContexts.get(i))));
        }

        return new Delete(
                getLocation(ctx),
                new Table(getLocation(ctx),
                getQualifiedName(ctx.tableIdentifier())),
            Optional.of(expressionRoot));
    }

    private Expression getPartitionExpression(SqlBaseParser.PartitionSpecContext partitionSpecContext)
    {
        ImmutableList.Builder builder = ImmutableList.builder();
        for (SqlBaseParser.PartitionValContext partitionValContext : partitionSpecContext.partitionVal()) {
            Expression expression = new ComparisonExpression(getLocation(partitionValContext),
                    ComparisonExpression.Operator.EQUAL,
                    new Identifier(partitionValContext.identifier().getText()),
                    new StringLiteral(getLocation(partitionValContext),
                            unquote(partitionValContext.constant().getText())));
            builder.add(expression);
        }
        Expression expression;
        if (partitionSpecContext.partitionVal().size() == 1) {
            expression = (Expression) builder.build().get(0);
        }
        else {
            expression = new LogicalExpression(LogicalExpression.Operator.AND, builder.build());
        }
        return expression;
    }

    @Override
    public Node visitDropTable(SqlBaseParser.DropTableContext ctx)
    {
        QualifiedName qualifiedName;
        if (ctx.tableIdentifier().db != null) {
            qualifiedName = QualifiedName.of(ctx.tableIdentifier().db.getText(),
                ctx.tableIdentifier().table.getText());
        }
        else {
            qualifiedName = QualifiedName.of(ctx.tableIdentifier().table.getText());
        }
        if (ctx.VIEW() != null) {
            return new DropView(
                getLocation(ctx),
                qualifiedName,
                ctx.EXISTS() != null);
        }
        else if (ctx.TABLE() != null) {
            return new DropTable(
                getLocation(ctx),
                qualifiedName,
                ctx.EXISTS() != null);
        }
        else {
            throw parseError("not a proper drop command", ctx);
        }
    }

    @Override
    public Node visitCreateView(SqlBaseParser.CreateViewContext ctx)
    {
        if (ctx.AS() == null) {
            throw parseError("need AS before query when creating view", ctx.query());
        }
        QualifiedName name = getQualifiedName(ctx.tableIdentifier());
        Query query = (Query) visit(ctx.query());
        return new CreateView(name,
            query,
            ctx.REPLACE() != null,
            Optional.empty(),
            Optional.empty());
    }

    @Override
    public Node visitFunctionIdentifier(SqlBaseParser.FunctionIdentifierContext ctx)
    {
        return super.visitFunctionIdentifier(ctx);
    }

    @Override
    public Node visitInsertIntoTable(SqlBaseParser.InsertIntoTableContext ctx)
    {
        return super.visitInsertIntoTable(ctx);
    }

    @Override
    public Node visitInsertOverwriteDir(SqlBaseParser.InsertOverwriteDirContext ctx)
    {
        throw parseError("Don't support InsertOverwriteDir", ctx);
    }

    @Override
    public Node visitInsertOverwriteTable(SqlBaseParser.InsertOverwriteTableContext ctx)
    {
        return super.visitInsertOverwriteTable(ctx);
    }

    @Override
    public Node visitShowDatabases(SqlBaseParser.ShowDatabasesContext ctx)
    {
        return new ShowSchemas(
            getLocation(ctx), Optional.empty(), getTextIfPresent(ctx.pattern)
            .map(HiveAstBuilder::unquote), Optional.empty());
    }

    @Override
    public Node visitShowTables(SqlBaseParser.ShowTablesContext ctx)
    {
        Optional<QualifiedName> database;
        if (ctx.db != null) {
            database = Optional.of(getQualifiedName(ctx.db.getText()));
        }
        else {
            database = Optional.empty();
        }
        return new ShowTables(
                getLocation(ctx),
                database,
                getTextIfPresent(ctx.pattern)
            .map(HiveAstBuilder::unquote), Optional.empty());
    }

    @Override
    public Node visitRelation(SqlBaseParser.RelationContext ctx)
    {
        Relation left = (Relation) visit(ctx.relationPrimary());

        left = withLateralView(left, ctx);

        for (SqlBaseParser.JoinRelationContext joinRelationContext : ctx.joinRelation()) {
            left = withJoinRelation(left, joinRelationContext);
        }

        return left;
    }

    private Relation withLateralView(Relation left, SqlBaseParser.RelationContext ctx)
    {
        RuleContext parent = ctx.parent;
        if (parent instanceof SqlBaseParser.FromClauseContext) {
            SqlBaseParser.FromClauseContext fromClauseContext = (SqlBaseParser.FromClauseContext) parent;

            List<AliasedRelation> unnests = visit(fromClauseContext.lateralView(), AliasedRelation.class);
            if (unnests.size() >= 1) {
                AliasedRelation unnest = unnests.get(0);
                left = new Join(getLocation(ctx), Join.Type.CROSS, left, unnest, Optional.empty());

                for (int i = 1; i < unnests.size(); ++i) {
                    left = new Join(getLocation(ctx), Join.Type.CROSS, left, unnests.get(i), Optional.empty());
                }
            }
        }

        return left;
    }

    @Override
    public Node visitTableValuedFunction(SqlBaseParser.TableValuedFunctionContext ctx)
    {
        Identifier identifier = (Identifier) visit(ctx.functionTable().identifier());

        throw parseError("Don't support " + identifier.getValue(), ctx);
    }

    @Override
    public Node visitInlineTable(SqlBaseParser.InlineTableContext ctx)
    {
        List<SqlBaseParser.ExpressionContext> expressionContexts = ctx.expression();
        List<Expression> expressions = visit(expressionContexts, Expression.class);
        Values values = new Values(expressions);
        return values;
    }

    private Relation withJoinRelation(Relation left, SqlBaseParser.JoinRelationContext ctx)
    {
        if (ctx.joinType().ANTI() != null) {
            throw parseError("Don't support joinType: " + ctx.joinType().getText(), ctx);
        }
        else if (ctx.joinType().SEMI() != null) {
            Node node = visit(ctx.joinCriteria().booleanExpression());
            if (!(node instanceof ComparisonExpression)) {
                throw parseError("Don't support joinType: " + ctx.joinType().getText(), ctx);
            }
            ComparisonExpression comparisonExpression = (ComparisonExpression) node;
            Expression expression = comparisonExpression.getLeft();
            SingleColumn singleColumn = new SingleColumn(comparisonExpression.getRight());
            Select select = new Select(false, ImmutableList.of(singleColumn));
            Relation relation = (Relation) visit(ctx.right);
            QuerySpecification querySpecification =
                    new QuerySpecification(
                    select,
                    Optional.of(relation),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
            Query query = new Query(Optional.empty(), querySpecification, Optional.empty(), Optional.empty(), Optional.empty());
            SubqueryExpression subqueryExpression = new SubqueryExpression(query);
            InPredicate inPredicate = new InPredicate(expression, subqueryExpression);
            return new LeftSemiJoin(Optional.empty(), left, inPredicate);
        }

        Relation right = (Relation) visit(ctx.right);

        if (ctx.joinType().CROSS() != null) {
            return new Join(getLocation(ctx), Join.Type.CROSS, left, right, Optional.empty());
        }

        JoinCriteria criteria;
        if (ctx.NATURAL() != null) {
            criteria = new NaturalJoin();
        }
        else {
            if (ctx.joinCriteria() == null) {
                return new Join(getLocation(ctx), Join.Type.CROSS, left, right, Optional.empty());
            }

            if (ctx.joinCriteria().ON() != null) {
                criteria = new JoinOn((Expression) visit(ctx.joinCriteria().booleanExpression()));
            }
            else if (ctx.joinCriteria().USING() != null) {
                criteria = new JoinUsing(visit(ctx.joinCriteria().identifier(), Identifier.class));
            }
            else {
                throw parseError("Don't support joinCriteria: " + ctx.joinCriteria().getText(), ctx);
            }
        }

        Join.Type joinType;
        if (ctx.joinType().LEFT() != null) {
            joinType = Join.Type.LEFT;
        }
        else if (ctx.joinType().RIGHT() != null) {
            joinType = Join.Type.RIGHT;
        }
        else if (ctx.joinType().FULL() != null) {
            joinType = Join.Type.FULL;
        }
        else {
            joinType = Join.Type.INNER;
        }

        return new Join(getLocation(ctx), joinType, left, right, Optional.of(criteria));
    }

    @Override
    public Node visitIdentifierList(SqlBaseParser.IdentifierListContext ctx)
    {
        return super.visitIdentifierList(ctx);
    }

    @Override
    public Node visitComparisonOperator(SqlBaseParser.ComparisonOperatorContext ctx)
    {
        return super.visitComparisonOperator(ctx);
    }

    @Override
    public Node visitValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx)
    {
        return super.visitValueExpressionDefault(ctx);
    }

    @Override
    public Node visitNumericLiteral(SqlBaseParser.NumericLiteralContext ctx)
    {
        try {
            if (ctx.getText().contains("E") || ctx.getText().contains("e")) {
                return new DoubleLiteral(getLocation(ctx), ctx.getText());
            }
            Number num = NumberFormat.getInstance().parse(ctx.getText());
            // need to make sure it keeps it's original value, e.g. 1.0 is double not integer.
            if (num instanceof Double || !num.toString().equals(ctx.getText())) {
                switch (parsingOptions.getDecimalLiteralTreatment()) {
                    case AS_DOUBLE:
                        return new DoubleLiteral(getLocation(ctx), ctx.getText());
                    case AS_DECIMAL:
                        return new DecimalLiteral(getLocation(ctx), ctx.getText());
                    case REJECT:
                        throw parseError("Unexpected decimal literal: " + ctx.getText(), ctx);
                }
            }
            else if (num instanceof Integer || num instanceof Long) {
                return new LongLiteral(getLocation(ctx), ctx.getText());
            }
            else {
                throw parseError("Can't parser number: " + ctx.getText(), ctx);
            }
        }
        catch (ParseException e) {
            throw parseError("Can't parser number: " + ctx.getText(), ctx);
        }

        throw parseError("Can't parser number: " + ctx.getText(), ctx);
    }

    @Override
    public Node visitExpression(SqlBaseParser.ExpressionContext ctx)
    {
        return super.visitExpression(ctx);
    }

    @Override
    public Node visitMultiInsertQuery(SqlBaseParser.MultiInsertQueryContext ctx)
    {
        return super.visitMultiInsertQuery(ctx);
    }

    @Override
    public Node visitIdentifierSeq(SqlBaseParser.IdentifierSeqContext ctx)
    {
        return super.visitIdentifierSeq(ctx);
    }

    @Override
    public Node visitFromClause(SqlBaseParser.FromClauseContext ctx)
    {
        List<Relation> relations = visit(ctx.relation(), Relation.class);

        if (relations.size() == 0) {
            throw parseError("no relation for from", ctx);
        }
        else if (relations.size() == 1) {
            return relations.get(0);
        }
        else {
            Join result = new Join(getLocation(ctx), Join.Type.IMPLICIT,
                    relations.get(0),
                    relations.get(1),
                    Optional.empty());
            for (int i = 2; i < relations.size(); ++i) {
                result = new Join(getLocation(ctx), Join.Type.IMPLICIT,
                    result,
                    relations.get(i),
                    Optional.empty());
            }
            return result;
        }
    }

    @Override
    public Node visitColumnReference(SqlBaseParser.ColumnReferenceContext ctx)
    {
        return super.visitColumnReference(ctx);
    }

    @Override
    public Node visitIdentifier(SqlBaseParser.IdentifierContext ctx)
    {
        return super.visitIdentifier(ctx);
    }

    @Override
    public Node visitLateralView(SqlBaseParser.LateralViewContext ctx)
    {
        if (ctx.OUTER() != null) {
            throw parseError("Don't support Outer Lateral Views", ctx);
        }

        Identifier qualifiedName = (Identifier) visit(ctx.qualifiedName());
        String udtfName = qualifiedName.getValue().toLowerCase(Locale.ENGLISH);

        boolean withOrdinality;
        if (udtfName.equals("explode")) {
            withOrdinality = false;
        }
        else if (udtfName.equals("posexplode")) {
            withOrdinality = true;
        }
        else {
            throw parseError("Don't support UDTF: " + udtfName, ctx);
        }

        Unnest unnest = new Unnest(getLocation(ctx), visit(ctx.expression(), Expression.class), withOrdinality);

        List<Identifier> columnNames = visit(ctx.colName, Identifier.class);
        if (columnNames.size() > 0) {
            return new AliasedRelation(getLocation(ctx), unnest, (Identifier) visit(ctx.tblName), columnNames);
        }
        else {
            return new AliasedRelation(getLocation(ctx), unnest, (Identifier) visit(ctx.tblName), null);
        }
    }

    @Override
    public Node visitCreateTable(SqlBaseParser.CreateTableContext ctx)
    {
        return super.visitCreateTable(ctx);
    }

    @Override
    public Node visitConstantDefault(SqlBaseParser.ConstantDefaultContext ctx)
    {
        return super.visitConstantDefault(ctx);
    }

    private Node withQueryOrganization(QueryBody term, SqlBaseParser.QueryOrganizationContext ctx)
    {
        if (ctx.clusterBy != null && !ctx.clusterBy.isEmpty()) {
            throw parseError("Don't support cluster by", ctx);
        }
        if (ctx.distributeBy != null && !ctx.distributeBy.isEmpty()) {
            throw parseError("Don't support distribute by", ctx);
        }
        if (ctx.sort != null && !ctx.sort.isEmpty()) {
            throw parseError("Don't support sort by", ctx);
        }

        Optional<OrderBy> orderBy = Optional.empty();
        if (ctx.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(getLocation(ctx.ORDER()), visit(ctx.sortItem(), SortItem.class)));
        }

        Optional<Node> limit = Optional.empty();
        if (ctx.LIMIT() != null) {
            limit = Optional.of(
                new Limit(
                getLocation(ctx.LIMIT()),
                        new LongLiteral(ctx.limit.getText())));
        }

        if (term instanceof QuerySpecification) {
            QuerySpecification querySpecification = (QuerySpecification) term;

            return new Query(
                getLocation(ctx),
                Optional.empty(),
                    new QuerySpecification(
                    getLocation(ctx),
                    querySpecification.getSelect(),
                    querySpecification.getFrom(),
                    querySpecification.getWhere(),
                    querySpecification.getGroupBy(),
                    querySpecification.getHaving(),
                    querySpecification.getWindows(),
                    orderBy,
                    Optional.empty(),
                    limit),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        }
        else {
            return new Query(
                getLocation(ctx),
                Optional.empty(),
                term,
                orderBy,
                Optional.empty(),
                limit);
        }
    }

    @Override
    public Node visitPartitionVal(SqlBaseParser.PartitionValContext ctx)
    {
        if (ctx.constant() != null) {
            String exp = tryUnquote(ctx.constant().getText());
            String constant = ctx.identifier().getText().replace("\"\"", "");
            return new SingleColumn(getLocation(ctx),
                    new StringLiteral(exp),
                Optional.of(new Identifier(constant, true)));
        }
        else {
            return new SingleColumn(getLocation(ctx),
                    new Identifier(ctx.identifier().getText().replace("\"\"", ""), true), Optional.empty());
        }
    }

    @Override
    public Node visitSingleInsertQuery(SqlBaseParser.SingleInsertQueryContext ctx)
    {
        QueryBody term;
        SqlBaseParser.InsertIntoContext insertIntoContext = ctx.insertInto();
        Object o = visit(ctx.queryTerm());
        if (o instanceof Query) {
            if (insertIntoContext == null) {
                return withQueryOrganization(new TableSubquery(getLocation(ctx), (Query) o), ctx.queryOrganization());
            }
            else {
                term = new TableSubquery(getLocation(ctx), (Query) o);
            }
        }
        else {
            term = (QueryBody) o;
        }

        Query query = (Query) withQueryOrganization(term, ctx.queryOrganization());
        if (ctx.insertInto() == null) {
            return query;
        }
        QualifiedName target;
        SqlBaseParser.PartitionSpecContext partitionSpecContext = null;
        if (ctx.insertInto() instanceof SqlBaseParser.InsertIntoTableContext) {
            SqlBaseParser.InsertIntoTableContext insertIntoTableContext =
                    (SqlBaseParser.InsertIntoTableContext) ctx.insertInto();
            partitionSpecContext = insertIntoTableContext.partitionSpec();
        }
        else if (ctx.insertInto() instanceof SqlBaseParser.InsertOverwriteTableContext) {
            SqlBaseParser.InsertOverwriteTableContext insertOverwriteTableContext =
                    (SqlBaseParser.InsertOverwriteTableContext) ctx.insertInto();
            partitionSpecContext = insertOverwriteTableContext.partitionSpec();
        }

        if (partitionSpecContext != null) {
            List<SingleColumn> singleColumns = visit(partitionSpecContext.partitionVal(), SingleColumn.class);
            int dynamicPartitionCount = singleColumns.size();
            if (singleColumns.size() > 0) {
                SingleColumn parent = singleColumns.get(0);
                if (parent.getAlias().isPresent()) {
                    --dynamicPartitionCount;
                }
                for (int i = 1; i < singleColumns.size(); ++i) {
                    SingleColumn singleColumn = singleColumns.get(i);
                    if (singleColumn.getAlias().isPresent()) {
                        --dynamicPartitionCount;
                        if (!parent.getAlias().isPresent()) {
                            throw parseError("Dynamic partition cannot be the parent of a static partition " + singleColumn.getAlias().get(), ctx);
                        }
                    }
                    parent = singleColumn;
                }
                List<QuerySpecification> querySpecifications = new ArrayList<>();
                if (query.getQueryBody() instanceof Union) {
                    Union union = (Union) query.getQueryBody();
                    getRelationFromUnion(union, querySpecifications);
                }
                else if (query.getQueryBody() instanceof QuerySpecification) {
                    QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
                    List<SelectItem> selectItems = querySpecification.getSelect().getSelectItems();
                    ImmutableList.Builder builder = ImmutableList.builder();
                    List<SelectItem> tmp = new ArrayList<>();
                    tmp.addAll(selectItems);
                    for (int i = 0; i < singleColumns.size(); ++i) {
                        if (singleColumns.get(i).getAlias().isPresent()) {
                            tmp.add(selectItems.size() - dynamicPartitionCount + i, singleColumns.get(i));
                        }
                    }
                    tmp.forEach(selectItem -> builder.add(selectItem));
                    Select select = querySpecification.getSelect().ofSelectItems(builder.build());
                    QuerySpecification querySpecification1 = querySpecification.ofSelect(select);
                    query = query.ofQueryBody(querySpecification1);
                }
            }
        }

        if (ctx.insertInto() instanceof SqlBaseParser.InsertIntoTableContext) {
            target = getQualifiedName(((SqlBaseParser.InsertIntoTableContext) ctx.insertInto()).tableIdentifier());
            return new Insert(target, Optional.empty(), query, false);
        }
        else if (ctx.insertInto() instanceof SqlBaseParser.InsertOverwriteTableContext) {
            target = getQualifiedName(((SqlBaseParser.InsertOverwriteTableContext) ctx.insertInto()).tableIdentifier());
            return new Insert(target, Optional.empty(), query, true);
        }
        else if (ctx.insertInto() instanceof SqlBaseParser.InsertOverwriteDirContext) {
            throw parseError("Don't support insert overwrite dir at the moment, stay tuned ;)", ctx);
        }
        else if (ctx.insertInto() instanceof SqlBaseParser.InsertOverwriteHiveDirContext) {
            throw parseError("Don't support insert overwrite hive dir at the moment, stay tuned ;)", ctx);
        }
        else {
            throw parseError("Don't support insert syntax", ctx);
        }
    }

    private void getRelationFromUnion(Union union, List<QuerySpecification> querySpecifications)
    {
        for (Relation relation : union.getRelations()) {
            if (relation instanceof QuerySpecification) {
                querySpecifications.add((QuerySpecification) relation);
            }
            else if (relation instanceof Union) {
                getRelationFromUnion((Union) relation, querySpecifications);
            }
        }
    }

    @Override
    public Node visitSingleDataType(SqlBaseParser.SingleDataTypeContext ctx)
    {
        return super.visitSingleDataType(ctx);
    }

    @Override
    public Node visitSingleStatement(SqlBaseParser.SingleStatementContext ctx)
    {
        return visit(ctx.statement());
    }

    @Override
    public Node visitQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx)
    {
        return super.visitQueryTermDefault(ctx);
    }

    @Override
    public Node visitSingleExpression(SqlBaseParser.SingleExpressionContext ctx)
    {
        return visit(ctx.namedExpression());
    }

    @Override
    public Node visitStar(SqlBaseParser.StarContext ctx)
    {
        List<Identifier> identifiers = new ArrayList<>();
        if (ctx.qualifiedName() != null) {
            ctx.qualifiedName().identifier().stream().forEach(new Consumer<SqlBaseParser.IdentifierContext>() {
                @Override
                public void accept(SqlBaseParser.IdentifierContext identifierContext)
                {
                    new Identifier(tryUnquote(identifierContext.getText()));
                }
            });
            identifiers = visit(ctx.qualifiedName().identifier(), Identifier.class);
        }
        return new StarExpression(getLocation(ctx), identifiers);
    }

    @Override
    public Node visitNamedExpression(SqlBaseParser.NamedExpressionContext ctx)
    {
        NodeLocation nodeLocation = getLocation(ctx);
        Expression expression = (Expression) visit(ctx.expression());
        Optional<Identifier> identifier = visitIfPresent(ctx.identifier(), Identifier.class);

        if (expression == null) {
            throw parseError("Don't support this syntax", ctx);
        }

        if (expression instanceof StarExpression) {
            StarExpression starExpression = (StarExpression) expression;
            // select all
            if (identifier.isPresent()) {
                throw parseError("todo", ctx);
            }

            if (starExpression.getIdentifiers().size() == 2) {
                DereferenceExpression dereferenceExpression =
                        new DereferenceExpression(starExpression.getIdentifiers().get(0),
                        starExpression.getIdentifiers().get(1));
                return new AllColumns(nodeLocation,
                    Optional.of(dereferenceExpression), ImmutableList.of());
            }
            else if (starExpression.getIdentifiers().size() == 1) {
                return new AllColumns(nodeLocation,
                    Optional.of(starExpression.getIdentifiers().get(0)), ImmutableList.of());
            }
            else {
                return new AllColumns(nodeLocation, Optional.empty(), ImmutableList.of());
            }
        }
        else {
            return new SingleColumn(nodeLocation, expression, identifier);
        }
    }

    @Override
    public Node visitUse(SqlBaseParser.UseContext ctx)
    {
        Use use;
        if (ctx.catalog != null) {
            use = new Use(
                getLocation(ctx),
                Optional.of(new Identifier(getLocation(ctx), ctx.catalog.getText(), false)),
                    new Identifier(getLocation(ctx), ctx.db.getText(), false));
            visit(ctx.catalog);
        }
        else {
            use = new Use(
                    getLocation(ctx),
                    Optional.ofNullable(null),
                    new Identifier(getLocation(ctx), ctx.db.getText(), false));
        }
        visit(ctx.db);
        return use;
    }

    @Override
    public Node visitTruncateTable(SqlBaseParser.TruncateTableContext ctx)
    {
        Optional<Expression> condition = Optional.empty();
        SqlBaseParser.PartitionSpecContext partitionSpecContext = ctx.partitionSpec();
        Expression expression;
        if (partitionSpecContext != null) {
            expression = getPartitionExpression(partitionSpecContext);
            condition = Optional.of(expression);
        }
        return new Delete(
                getLocation(ctx),
                new Table(getLocation(ctx), getQualifiedName(ctx.tableIdentifier())),
            condition);
    }

    @Override
    public Node visitSetSession(SqlBaseParser.SetSessionContext ctx)
    {
        SqlBaseParser.ExpressionContext expression = ctx.expression();
        SetSession setSession = new SetSession(getLocation(ctx),
                getQualifiedName(ctx.qualifiedName()), (Expression) visit(expression));
        return setSession;
    }

    @Override
    public Node visitOr(SqlBaseParser.OrContext context)
    {
        List<ParserRuleContext> terms = flatten(context, element -> {
            if (element instanceof SqlBaseParser.OrContext) {
                SqlBaseParser.OrContext or = (SqlBaseParser.OrContext) element;
                return Optional.of(or.booleanExpression());
            }

            return Optional.empty();
        });

        return new LogicalExpression(getLocation(context), LogicalExpression.Operator.OR, visit(terms, Expression.class));
    }

    @Override
    public Node visitAnd(SqlBaseParser.AndContext context)
    {
        List<ParserRuleContext> terms = flatten(context, element -> {
            if (element instanceof SqlBaseParser.AndContext) {
                SqlBaseParser.AndContext and = (SqlBaseParser.AndContext) element;
                return Optional.of(and.booleanExpression());
            }

            return Optional.empty();
        });

        return new LogicalExpression(getLocation(context), LogicalExpression.Operator.AND, visit(terms, Expression.class));
    }

    private static List<ParserRuleContext> flatten(ParserRuleContext root, Function<ParserRuleContext, Optional<List<? extends ParserRuleContext>>> extractChildren)
    {
        List<ParserRuleContext> result = new ArrayList<>();
        Deque<ParserRuleContext> pending = new ArrayDeque<>();
        pending.push(root);

        while (!pending.isEmpty()) {
            ParserRuleContext next = pending.pop();

            Optional<List<? extends ParserRuleContext>> children = extractChildren.apply(next);
            if (!children.isPresent()) {
                result.add(next);
            }
            else {
                for (int i = children.get().size() - 1; i >= 0; i--) {
                    pending.push(children.get().get(i));
                }
            }
        }

        return result;
    }

    @Override
    public Node visitTableIdentifier(SqlBaseParser.TableIdentifierContext ctx)
    {
        return new Table(getLocation(ctx), getQualifiedName(ctx));
    }

    @Override
    public Node visitTableName(SqlBaseParser.TableNameContext ctx)
    {
        Table table = (Table) visitTableIdentifier(ctx.tableIdentifier());
        Relation result;
        if (ctx.tableAlias() != null && ctx.tableAlias().strictIdentifier() != null) {
            Identifier identifier = (Identifier) visit(ctx.tableAlias().strictIdentifier());
            List<Identifier> aliases = null;
            if (ctx.tableAlias().identifierList() != null) {
                throw parseError("todo", ctx);
                //aliases = visit(ctx.tableAlias().identifierList(), Identifier.class);
            }
            result = new AliasedRelation(getLocation(ctx), table, identifier, aliases);
        }
        else {
            result = table;
        }
        if (ctx.sampledRelation() == null) {
            return result;
        }
        else {
            return getSampleRelation(ctx.sampledRelation(), result);
        }
    }

    private Node getSampleRelation(SqlBaseParser.SampledRelationContext ctx, Relation relation)
    {
        SampledRelation.Type type;
        if (ctx.sampleTypePresto().BERNOULLI() != null) {
            type = SampledRelation.Type.BERNOULLI;
        }
        else if (ctx.sampleTypePresto().SYSTEM() != null) {
            type = SampledRelation.Type.SYSTEM;
        }
        else {
            throw parseError("not support sample function type", ctx);
        }
        SampledRelation sampledRelation = new SampledRelation(getLocation(ctx),
                relation,
                type,
                new LongLiteral(ctx.percentage.getText()));
        return sampledRelation;
    }

    @Override
    public Node visitShowFunctions(SqlBaseParser.ShowFunctionsContext ctx)
    {
        return new ShowFunctions(Optional.empty(), Optional.empty());
    }

    @Override
    public Node visitShowCreateTable(SqlBaseParser.ShowCreateTableContext ctx)
    {
        return new ShowCreate(
            getLocation(ctx),
            ctx.TABLE() != null ? ShowCreate.Type.TABLE : ShowCreate.Type.VIEW,
            getQualifiedName(ctx.tableIdentifier()));
    }

    @Override
    public Node visitDescribeTable(SqlBaseParser.DescribeTableContext ctx)
    {
        if (ctx.tableIdentifier().db != null) {
            return new ShowColumns(
                getLocation(ctx),
                QualifiedName.of(ctx.tableIdentifier().db.getText(),
                ctx.tableIdentifier().table.getText()),
                Optional.empty(),
                Optional.empty());
        }
        else {
            return new ShowColumns(
                getLocation(ctx),
                QualifiedName.of(ctx.tableIdentifier().table.getText()),
                Optional.empty(),
                Optional.empty());
        }
    }

    @Override
    public Node visitShowColumns(SqlBaseParser.ShowColumnsContext ctx)
    {
        if (ctx.tableIdentifier().db != null) {
            return new ShowColumns(
                getLocation(ctx),
                QualifiedName.of(ctx.tableIdentifier().db.getText(),
                    ctx.tableIdentifier().table.getText()),
                Optional.empty(),
                Optional.empty());
        }
        else {
            return new ShowColumns(
                getLocation(ctx),
                QualifiedName.of(ctx.tableIdentifier().table.getText()),
                Optional.empty(),
                Optional.empty());
        }
    }

    @Override
    public Node visitAggregation(SqlBaseParser.AggregationContext ctx)
    {
        List<GroupingElement> groupingElements = new ArrayList<>();

        if (ctx.GROUPING() != null) {
            // GROUP BY .... GROUPING SETS (...)
            List<List<Expression>> expresstionLists = ctx.groupingSet().stream().map(groupingSet -> visit(groupingSet.expression(), Expression.class)).collect(toList());

            groupingElements.add(new GroupingSets(getLocation(ctx), expresstionLists));
        }
        else {
            // GROUP BY .... (WITH CUBE | WITH ROLLUP)?
            List<Expression> expressions = visit(ctx.groupingExpressions, Expression.class);
            if (expressions.size() == 1 && expressions.get(0) instanceof Row) {
                expressions = ((Row) expressions.get(0)).getItems();
            }
            if (ctx.CUBE() != null) {
                GroupingElement groupingElement = new Cube(getLocation(ctx), expressions);
                groupingElements.add(groupingElement);
            }
            else if (ctx.ROLLUP() != null) {
                GroupingElement groupingElement = new Rollup(getLocation(ctx), expressions);
                groupingElements.add(groupingElement);
            }
            else {
                // this is a little bit awkward just trying to match whatever presto going to generate.
                for (SqlBaseParser.ExpressionContext groupingExpression : ctx.groupingExpressions) {
                    GroupingElement groupingElement = new SimpleGroupBy(getLocation(ctx), visit(ImmutableList.of(groupingExpression), Expression.class));
                    groupingElements.add(groupingElement);
                }
            }
        }

        return new GroupBy(getLocation(ctx), false, groupingElements);
    }

    @Override
    public Node visitQuery(SqlBaseParser.QueryContext ctx)
    {
        if (ctx.ctes() != null) {
            Node node = visit(ctx.queryNoWith());
            if (node instanceof Query) {
                Query query = (Query) node;
                return new Query(
                    Optional.of((With) visitCtes(ctx.ctes())),
                    query.getQueryBody(),
                    query.getOrderBy(),
                    query.getOffset(),
                    query.getLimit());
            }
            else if (node instanceof Insert) {
                Insert insert = (Insert) node;
                Query query = new Query(
                        Optional.of((With) visitCtes(ctx.ctes())),
                        insert.getQuery().getQueryBody(),
                        insert.getQuery().getOrderBy(),
                        insert.getQuery().getOffset(),
                        insert.getQuery().getLimit());
                Insert newInsert = new Insert(
                        insert.getTarget(),
                        insert.getColumns(),
                        query,
                        insert.isOverwrite());
                return newInsert;
            }
        }
        return visitChildren(ctx);
    }

    @Override
    public Node visitQuerySpecification(SqlBaseParser.QuerySpecificationContext ctx)
    {
        if (ctx.kind == null) {
            throw parseError("Missing Select Statement", ctx);
        }

        if (ctx.kind.getType() == SqlBaseParser.SELECT) {
            SqlBaseParser.NamedExpressionSeqContext namedExpressionSeqContext = ctx.namedExpressionSeq();
            List<SqlBaseParser.NamedExpressionContext> namedExpressionContexts =
                    namedExpressionSeqContext.namedExpression();

            List<SelectItem> selectItems = new ArrayList<>();
            for (SqlBaseParser.NamedExpressionContext namedExpressionContext : namedExpressionContexts) {
                SelectItem selectItem = (SelectItem) visit(namedExpressionContext);
                selectItems.add(selectItem);
            }

            NodeLocation nodeLocation = getLocation(ctx);
            Select select = new Select(getLocation(ctx.SELECT()), isDistinct(ctx.setQuantifier()), selectItems);

            Optional<Relation> from = visitIfPresent(ctx.fromClause(), Relation.class);

            Optional<Expression> where = visitIfPresent(ctx.where, Expression.class);
            if (from.isPresent()) {
                Relation relation = from.get();
                if (relation instanceof LeftSemiJoin) {
                    LeftSemiJoin leftSemiJoin = (LeftSemiJoin) relation;
                    from = Optional.of(leftSemiJoin.getLeft());
                    if (!where.isPresent()) {
                        where = Optional.of(leftSemiJoin.getInPredicate());
                    }
                    else {
                        where = Optional.of(
                            new LogicalExpression(LogicalExpression.Operator.AND,
                                ImmutableList.of(leftSemiJoin.getInPredicate(), where.get())));
                    }
                }
            }
            return new QuerySpecification(
                nodeLocation,
                select,
                from,
                where,
                visitIfPresent(ctx.aggregation(), GroupBy.class),
                visitIfPresent(ctx.having, Expression.class),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        }
        else {
            throw parseError("Don't support kind: " + ctx.kind.getText(), ctx);
        }
    }

    private static boolean isDistinct(io.hivesql.sql.parser.SqlBaseParser.SetQuantifierContext setQuantifier)
    {
        return setQuantifier != null && setQuantifier.DISTINCT() != null;
    }

    private static QueryBody getQueryBody(Node node)
    {
        if (node instanceof Query) {
            Query query = (Query) node;

            return query.getQueryBody();
        }
        else {
            return (QueryBody) node;
        }
    }

    @Override
    public Node visitLogicalNot(SqlBaseParser.LogicalNotContext ctx)
    {
        return new NotExpression(getLocation(ctx), (Expression) visit(ctx.booleanExpression()));
    }

    @Override
    public Node visitExists(SqlBaseParser.ExistsContext ctx)
    {
        return new ExistsPredicate(getLocation(ctx), new SubqueryExpression(getLocation(ctx), (Query) visit(ctx.query())));
    }

    @Override
    public Node visitComparison(SqlBaseParser.ComparisonContext ctx)
    {
        return new ComparisonExpression(
            getLocation(ctx.comparisonOperator()),
            getComparisonOperator(((TerminalNode) ctx.comparisonOperator().getChild(0)).getSymbol()),
            (Expression) visit(ctx.left),
            (Expression) visit(ctx.right));
    }

    private Node withPredicate(Expression expression, SqlBaseParser.PredicateContext ctx)
    {
        switch (ctx.kind.getType()) {
            case SqlBaseParser.NULL:
                if (ctx.NOT() != null) {
                    return new IsNotNullPredicate(getLocation(ctx), expression);
                }
                else {
                    return new IsNullPredicate(getLocation(ctx), expression);
                }
            case SqlBaseParser.EXISTS:
                return new ExistsPredicate(getLocation(ctx), new SubqueryExpression(getLocation(ctx), (Query) visit(ctx.query())));
            case SqlBaseParser.BETWEEN:
                Expression betweenPredicate = new BetweenPredicate(
                        getLocation(ctx),
                        expression,
                        (Expression) visit(ctx.lower),
                        (Expression) visit(ctx.upper));

                if (ctx.NOT() != null) {
                    return new NotExpression(getLocation(ctx), betweenPredicate);
                }
                else {
                    return betweenPredicate;
                }
            case SqlBaseParser.IN:
                if (ctx.query() != null) {
                    // In subquery
                    Expression inPredicate = new InPredicate(
                            getLocation(ctx),
                            expression,
                            new SubqueryExpression(getLocation(ctx), (Query) visit(ctx.query())));

                    if (ctx.NOT() != null) {
                        return new NotExpression(getLocation(ctx), inPredicate);
                    }
                    else {
                        return inPredicate;
                    }
                }
                else {
                    // In value list
                    InListExpression inListExpression = new InListExpression(getLocation(ctx), visit(ctx.expression(), Expression.class));
                    Expression inPredicate = new InPredicate(
                            getLocation(ctx),
                            expression,
                            inListExpression);

                    if (ctx.NOT() != null) {
                        return new NotExpression(getLocation(ctx), inPredicate);
                    }
                    else {
                        return inPredicate;
                    }
                }
            case SqlBaseParser.LIKE:
                Expression likePredicate = new LikePredicate(
                        getLocation(ctx),
                        expression,
                        (Expression) visit(ctx.pattern),
                        Optional.empty());

                if (ctx.NOT() != null) {
                    return new NotExpression(getLocation(ctx), likePredicate);
                }
                else {
                    return likePredicate;
                }
            case SqlBaseParser.DISTINCT:
                Expression comparisonExpression = new ComparisonExpression(
                        getLocation(ctx),
                        ComparisonExpression.Operator.IS_DISTINCT_FROM,
                        expression,
                        (Expression) visit(ctx.right));

                if (ctx.NOT() != null) {
                    return new NotExpression(getLocation(ctx), comparisonExpression);
                }
                else {
                    return comparisonExpression;
                }
            case SqlBaseParser.RLIKE:
                Expression pattern = (Expression) visit(ctx.pattern);
                if (pattern instanceof StringLiteral) {
                    pattern = new StringLiteral(decreaseSlash(((StringLiteral) pattern).getValue()));
                }
                Expression rLikePredicate = new FunctionCall(
                        getLocation(ctx),
                        QualifiedName.of("regexp_like"),
                        ImmutableList.of(
                            new Cast(
                                getLocation(ctx), expression,
                                    new GenericDataType(Optional.empty(), new Identifier("string"), ImmutableList.of())),
                            new Cast(
                                getLocation(ctx), pattern,
                                    new GenericDataType(Optional.empty(), new Identifier("string"), ImmutableList.of()))));
                if (ctx.NOT() != null) {
                    return new NotExpression(getLocation(ctx), rLikePredicate);
                }
                else {
                    return rLikePredicate;
                }
            default:
                throw parseError("Not supported type: " + ctx.kind.getText(), ctx);
        }
    }

    @Override
    public Node visitPredicated(SqlBaseParser.PredicatedContext ctx)
    {
        Expression expression = (Expression) visit(ctx.valueExpression());

        if (ctx.predicate() != null) {
            return withPredicate(expression, ctx.predicate());
        }

        return expression;
    }

    @Override
    public Node visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext ctx)
    {
        if (ctx.operator.getText().equals("||")) {
            List<Expression> expressions = new ArrayList<>();
            expressions.add((Expression) visit(ctx.left));
            expressions.add((Expression) visit(ctx.right));
            return new FunctionCall(
                getLocation(ctx),
                QualifiedName.of("concat"),
                expressions);
        }

        return new ArithmeticBinaryExpression(
            getLocation(ctx.operator),
            getArithmeticBinaryOperator(ctx.operator),
            (Expression) visit(ctx.left),
            (Expression) visit(ctx.right));
    }

    @Override
    public Node visitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext ctx)
    {
        Expression child = (Expression) visit(ctx.valueExpression());

        switch (ctx.operator.getType()) {
            case SqlBaseLexer.MINUS:
                return ArithmeticUnaryExpression.negative(getLocation(ctx), child);
            case SqlBaseLexer.PLUS:
                return ArithmeticUnaryExpression.positive(getLocation(ctx), child);
            default:
                throw new UnsupportedOperationException("Unsupported sign: " + ctx.operator.getText());
        }
    }

    @Override
    public Node visitCast(SqlBaseParser.CastContext ctx)
    {
        DataType dataType = (DataType) visit(ctx.dataType());
        return new Cast(getLocation(ctx), (Expression) visit(ctx.expression()), dataType, false);
    }

    // need to be implemented
    @Override
    public Node visitStruct(SqlBaseParser.StructContext ctx)
    {
        return super.visitStruct(ctx);
    }

    // need to be implemented
    @Override
    public Node visitFirst(SqlBaseParser.FirstContext ctx)
    {
        return super.visitFirst(ctx);
    }

    // need to be implemented
    @Override
    public Node visitLast(SqlBaseParser.LastContext ctx)
    {
        return super.visitLast(ctx);
    }

    @Override
    public Node visitPosition(SqlBaseParser.PositionContext ctx)
    {
        List<Expression> arguments = Lists.reverse(visit(ctx.valueExpression(), Expression.class));
        return new FunctionCall(getLocation(ctx), QualifiedName.of("strpos"), arguments);
    }

    @Override
    public Node visitExtract(SqlBaseParser.ExtractContext ctx)
    {
        String fieldString = ctx.identifier().getText();
        Extract.Field field;
        try {
            field = Extract.Field.valueOf(fieldString.toUpperCase(Locale.ENGLISH));
        }
        catch (IllegalArgumentException e) {
            throw parseError("Invalid EXTRACT field: " + fieldString, ctx);
        }
        return new Extract(getLocation(ctx), (Expression) visit(ctx.valueExpression()), field);
    }

    private final List<String> udtfNames = ImmutableList.of("explode", "posexplode", "inline", "stack", "json_tuple", "parse_url_tuple");

    private void checkUDTF(SqlBaseParser.FunctionCallContext ctx)
    {
        String functionName = ctx.qualifiedName().getText().toLowerCase(Locale.ENGLISH);

        if (udtfNames.contains(functionName)) {
            throw parseError("Don't Support call UDTF: " + functionName + " directly, please try lateral view syntax instead.", ctx);
        }
    }

    @Override
    public Node visitFunctionCall(SqlBaseParser.FunctionCallContext ctx)
    {
        checkUDTF(ctx);

        QualifiedName name = getQualifiedName(ctx.qualifiedName());
        boolean distinct = isDistinct(ctx.setQuantifier());

        Optional<Window> window = visitIfPresent(ctx.windowSpec(), Window.class);

        if (name.toString().equalsIgnoreCase("if")) {
            check(ctx.expression().size() == 2 || ctx.expression().size() == 3, "Invalid number of arguments for 'if' function", ctx);
            check(!window.isPresent(), "OVER clause not valid for 'if' function", ctx);
            check(!distinct, "DISTINCT not valid for 'if' function", ctx);

            Expression elseExpression = null;
            if (ctx.expression().size() == 3) {
                elseExpression = (Expression) visit(ctx.expression(2));
            }

            return new IfExpression(
                getLocation(ctx),
                (Expression) visit(ctx.expression(0)),
                (Expression) visit(ctx.expression(1)),
                elseExpression);
        }

        if (name.toString().equalsIgnoreCase("nullif")) {
            check(ctx.expression().size() == 2, "Invalid number of arguments for 'nullif' function", ctx);
            check(!window.isPresent(), "OVER clause not valid for 'nullif' function", ctx);
            check(!distinct, "DISTINCT not valid for 'nullif' function", ctx);

            return new NullIfExpression(
                getLocation(ctx),
                (Expression) visit(ctx.expression(0)),
                (Expression) visit(ctx.expression(1)));
        }

        if (name.toString().equalsIgnoreCase("coalesce")) {
            check(ctx.expression().size() >= 2, "The 'coalesce' function must have at least two arguments", ctx);
            check(!window.isPresent(), "OVER clause not valid for 'coalesce' function", ctx);
            check(!distinct, "DISTINCT not valid for 'coalesce' function", ctx);

            return new CoalesceExpression(getLocation(ctx), visit(ctx.expression(), Expression.class));
        }

        List<Expression> expressions = visit(ctx.expression(), Expression.class);
        ImmutableList.Builder expressionsBuileder = ImmutableList.builder();
        expressions.stream().forEach(expression -> {
            if (expression instanceof StringLiteral) {
                expressionsBuileder.add(new StringLiteral(
                        decreaseSlash(((StringLiteral) expression).getValue())));
            }
            else {
                expressionsBuileder.add(expression);
            }
        });
        if (expressions.size() == 1 && expressions.get(0) instanceof StarExpression) {
            return new FunctionCall(
                Optional.of(getLocation(ctx)),
                name,
                window,
                Optional.empty(),
                Optional.empty(),
                distinct,
                Optional.empty(),
                Optional.empty(),
                    new ArrayList<>());
        }

        if (ctx.qualifiedName().getText().equalsIgnoreCase("from_json")) {
            StringLiteral stringLiteral = (StringLiteral) expressions.get(1);
            String typeString = stringLiteral.getValue();
            SqlParser sqlParser = new SqlParser();
            ParsingOptions parsingOptions = new ParsingOptions();
            parsingOptions.setIfUseHiveParser(true);
            DataType dataType = sqlParser.createType(typeString, parsingOptions);
            QualifiedName expectName = QualifiedName.of("json_parse");
            FunctionCall functionCall = new FunctionCall(
                    Optional.of(getLocation(ctx)),
                    expectName,
                    window,
                    Optional.empty(),
                    Optional.empty(),
                    distinct,
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableList.of(expressions.get(0)));
            Cast cast = new Cast(functionCall, dataType);
            return cast;
        }

        if (expressions.size() != 1 && ctx.qualifiedName().getText().equalsIgnoreCase("count")) {
            if (expressions == null || expressions.size() == 0) {
                throw parseError("count() need a parameter, count(1) for example", ctx);
            }
            Row row = new Row(getLocation(ctx), expressions);
            return new FunctionCall(
                Optional.of(getLocation(ctx)),
                name,
                window,
                Optional.empty(),
                Optional.empty(),
                distinct,
                Optional.empty(),
                Optional.empty(),
                Arrays.asList(row));
        }
        return new FunctionCall(
            Optional.of(getLocation(ctx)),
            name,
            window,
            Optional.empty(),
            Optional.empty(),
            distinct,
            Optional.empty(),
            Optional.empty(),
            expressionsBuileder.build());
    }

    @Override
    public Node visitWindowRef(SqlBaseParser.WindowRefContext ctx)
    {
        throw parseError("Don't support Window Clause", ctx);
    }

    @Override
    public Node visitWindowDef(SqlBaseParser.WindowDefContext ctx)
    {
        if (ctx.SORT() != null) {
            throw parseError("Don't support SORT", ctx);
        }
        if (ctx.CLUSTER() != null) {
            throw parseError("Don't support CLUSTER", ctx);
        }
        if (ctx.DISTRIBUTE() != null) {
            throw parseError("Don't support DISTRIBUTE", ctx);
        }

        Optional<OrderBy> orderBy = Optional.empty();
        if (ctx.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(getLocation(ctx.ORDER()), visit(ctx.sortItem(), SortItem.class)));
        }

        return new WindowSpecification(
            getLocation(ctx),
            Optional.empty(),
            visit(ctx.partition, Expression.class),
            orderBy,
            visitIfPresent(ctx.windowFrame(), WindowFrame.class));
    }

    private static WindowFrame.Type getFrameType(ParserRuleContext ctx, Token type)
    {
        switch (type.getType()) {
            case SqlBaseLexer.RANGE:
                return WindowFrame.Type.RANGE;
            case SqlBaseLexer.ROWS:
                return WindowFrame.Type.ROWS;
        }

        throw parseError("Don't support frame type: " + type.getText(), ctx);
    }

    private static FrameBound.Type getBoundedFrameBoundType(ParserRuleContext ctx, Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.PRECEDING:
                return FrameBound.Type.PRECEDING;
            case SqlBaseLexer.FOLLOWING:
                return FrameBound.Type.FOLLOWING;
            case SqlBaseLexer.CURRENT:
                return FrameBound.Type.CURRENT_ROW;
        }

        throw parseError("Don't support bound type: " + token.getText(), ctx);
    }

    private static FrameBound.Type getUnboundedFrameBoundType(ParserRuleContext ctx, Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.PRECEDING:
                return FrameBound.Type.UNBOUNDED_PRECEDING;
            case SqlBaseLexer.FOLLOWING:
                return FrameBound.Type.UNBOUNDED_FOLLOWING;
        }

        throw parseError("Don't support bound type: " + token.getText(), ctx);
    }

    @Override
    public Node visitWindowFrame(SqlBaseParser.WindowFrameContext ctx)
    {
        return new WindowFrame(
            getLocation(ctx),
            getFrameType(ctx, ctx.frameType),
            (FrameBound) visit(ctx.start),
            visitIfPresent(ctx.end, FrameBound.class),
            ImmutableList.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            ImmutableList.of(),
            ImmutableList.of());
    }

    @Override
    public Node visitFrameBound(SqlBaseParser.FrameBoundContext ctx)
    {
        Expression expression = null;
        if (ctx.expression() != null) {
            expression = (Expression) visit(ctx.expression());
        }

        FrameBound.Type frameBoundType = null;
        if (ctx.UNBOUNDED() == null) {
            frameBoundType = getBoundedFrameBoundType(ctx, ctx.boundType);
        }
        else {
            frameBoundType = getUnboundedFrameBoundType(ctx, ctx.boundType);
        }

        return new FrameBound(getLocation(ctx), frameBoundType, expression);
    }

    @Override
    public Node visitRowConstructor(SqlBaseParser.RowConstructorContext ctx)
    {
        List<SqlBaseParser.NamedExpressionContext> namedExpressionContexts = ctx.namedExpression();
        List<Expression> expressions = new ArrayList<>();
        namedExpressionContexts.stream().forEach(new Consumer<SqlBaseParser.NamedExpressionContext>() {
            @Override
            public void accept(SqlBaseParser.NamedExpressionContext namedExpressionContext)
            {
                expressions.add((Expression) visit(namedExpressionContext.expression()));
            }
        });
        return new Row(getLocation(ctx), expressions);
    }

    @Override
    public Node visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext ctx)
    {
        return new SubqueryExpression(getLocation(ctx), (Query) visit(ctx.query()));
    }

    @Override
    public Node visitSimpleCase(SqlBaseParser.SimpleCaseContext ctx)
    {
        return new SimpleCaseExpression(
            getLocation(ctx),
            (Expression) visit(ctx.value),
            visit(ctx.whenClause(), WhenClause.class),
            visitIfPresent(ctx.elseExpression, Expression.class));
    }

    @Override
    public Node visitComplexDataType(SqlBaseParser.ComplexDataTypeContext ctx)
    {
        List<DataType> dataTypes = visit(ctx.dataType(), DataType.class);
        List<DataTypeParameter> typeParameters = new ArrayList<>();
        dataTypes.stream().forEach(dataType -> typeParameters.add(new TypeParameter(dataType)));
        DataType dataType = null;
        if (ctx.ARRAY() != null) {
            dataType = new GenericDataType(getLocation(ctx), new Identifier(getLocation(ctx), ctx.ARRAY().getText(), true),
                typeParameters);
        }
        else if (ctx.MAP() != null) {
            dataType = new GenericDataType(getLocation(ctx), new Identifier(getLocation(ctx), ctx.MAP().getText(), true),
                typeParameters);
        }
        else if (ctx.STRUCT() != null) {
            List<RowDataType.Field> fields = visit(ctx.complexColTypeList().complexColType(), RowDataType.Field.class);
            dataType = new RowDataType(getLocation(ctx), fields);
        }
        else if (ctx.NEQ() != null) {
            throw parseError("where is the type?", ctx);
        }
        return dataType;
    }

    @Override
    public Node visitPrimitiveDataType(SqlBaseParser.PrimitiveDataTypeContext ctx)
    {
        GenericDataType genericDataType = new GenericDataType(getLocation(ctx), new Identifier(getLocation(ctx), ctx.identifier().getText(), true), ImmutableList.of());
        return genericDataType;
    }

    @Override
    public Node visitComplexColType(SqlBaseParser.ComplexColTypeContext ctx)
    {
        RowDataType.Field field = new RowDataType.Field(getLocation(ctx), Optional.of(new Identifier(getLocation(ctx.identifier()), tryUnquote(ctx.identifier().getText()), true)), (DataType) visit(ctx.dataType()));
        return field;
    }

    @Override
    public Node visitWhenClause(SqlBaseParser.WhenClauseContext ctx)
    {
        return new WhenClause(getLocation(ctx), (Expression) visit(ctx.condition), (Expression) visit(ctx.result));
    }

    @Override
    public Node visitSearchedCase(SqlBaseParser.SearchedCaseContext ctx)
    {
        return new SearchedCaseExpression(
            getLocation(ctx),
            visit(ctx.whenClause(), WhenClause.class),
            visitIfPresent(ctx.elseExpression, Expression.class));
    }

    @Override
    public Node visitDereference(SqlBaseParser.DereferenceContext ctx)
    {
        return new DereferenceExpression(
            getLocation(ctx),
            (Expression) visit(ctx.base),
            (Identifier) visit(ctx.fieldName));
    }

    @Override
    public Node visitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext ctx)
    {
        return new Identifier(getLocation(ctx), unquote(ctx.getText()), true);
    }

    @Override
    public Node visitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext ctx)
    {
        return new Identifier(getLocation(ctx), tryUnquote(ctx.getText()), true);
    }

    @Override
    public Node visitSubscript(SqlBaseParser.SubscriptContext ctx)
    {
        return new SubscriptExpression(getLocation(ctx),
            (Expression) visit(ctx.value), (Expression) visit(ctx.index));
    }

    @Override
    public Node visitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext ctx)
    {
        return visit(ctx.expression());
    }

    @Override
    public Node visitSortItem(SqlBaseParser.SortItemContext ctx)
    {
        return new SortItem(
            getLocation(ctx),
            (Expression) visit(ctx.expression()),
            Optional.ofNullable(ctx.ordering)
                .map(HiveAstBuilder::getOrderingType)
                .orElse(SortItem.Ordering.ASCENDING),
            Optional.ofNullable(ctx.nullOrder)
                .map(HiveAstBuilder::getNullOrderingType)
                .orElse(SortItem.NullOrdering.UNDEFINED));
    }

    @Override
    public Node visitTypeConstructor(SqlBaseParser.TypeConstructorContext ctx)
    {
        Object o = visit(ctx.STRING());
        if (o == null) {
            throw parseError("cannot recognize input near " +
                ctx.start.getText() + " or " + ctx.stop.getText() +
                "most reason may be missing column before like/rlike/between or missing =/!=/>/</>=/<=/<> between column and condition expression", ctx);
        }
        String value = ((StringLiteral) o).getValue();

        String type = ctx.identifier().getText();
        if (type.equalsIgnoreCase("time")) {
            return new TimeLiteral(getLocation(ctx), value);
        }
        if (type.equalsIgnoreCase("timestamp")) {
            return new TimestampLiteral(getLocation(ctx), value);
        }
        if (type.equalsIgnoreCase("decimal")) {
            return new DecimalLiteral(getLocation(ctx), value);
        }
        if (type.equalsIgnoreCase("char")) {
            return new CharLiteral(getLocation(ctx), value);
        }

        return new GenericLiteral(getLocation(ctx), type, value);
    }

    @Override
    public Node visitNullLiteral(SqlBaseParser.NullLiteralContext ctx)
    {
        return new NullLiteral(getLocation(ctx));
    }

    @Override
    public Node visitBooleanLiteral(SqlBaseParser.BooleanLiteralContext ctx)
    {
        return new BooleanLiteral(getLocation(ctx), ctx.getText());
    }

    @Override
    public Node visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx)
    {
        return new LongLiteral(getLocation(ctx), ctx.getText());
    }

    @Override
    public Node visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext ctx)
    {
        return new DoubleLiteral(getLocation(ctx), ctx.getText());
    }

    @Override
    public Node visitDoubleLiteral(SqlBaseParser.DoubleLiteralContext ctx)
    {
        return new DoubleLiteral(getLocation(ctx), ctx.getText());
    }

    @Override
    public Node visitStringLiteral(SqlBaseParser.StringLiteralContext ctx)
    {
        return new StringLiteral(getLocation(ctx), unquote(ctx.getText()));
    }

    /////////////////////
    // Utility methods //
    /////////////////////
    private static SortItem.Ordering getOrderingType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.ASC:
                return SortItem.Ordering.ASCENDING;
            case SqlBaseLexer.DESC:
                return SortItem.Ordering.DESCENDING;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static SortItem.NullOrdering getNullOrderingType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.FIRST:
                return SortItem.NullOrdering.FIRST;
            case SqlBaseLexer.LAST:
                return SortItem.NullOrdering.LAST;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static ParsingException parseError(String message, ParserRuleContext context)
    {
        return new ParsingException(message, null, context.getStart().getLine(), context.getStart().getCharPositionInLine() + 1);
    }

    private String getType(SqlBaseParser.DataTypeContext type)
    {
        if (type instanceof SqlBaseParser.ComplexDataTypeContext) {
            SqlBaseParser.ComplexDataTypeContext complexType = (SqlBaseParser.ComplexDataTypeContext) type;
            if (complexType.ARRAY() != null) {
                return "ARRAY(" + getType((complexType.dataType(0))) + ")";
            }

            if (complexType.MAP() != null) {
                return "MAP(" + getType((complexType.dataType(0))) + "," + getType((complexType.dataType(1))) + ")";
            }

            if (complexType.STRUCT() != null) {
                StringBuilder builder = new StringBuilder("(");

                for (int i = 0; i < complexType.complexColTypeList().complexColType().size(); i++) {
                    SqlBaseParser.ComplexColTypeContext complexColTypeContext = complexType.complexColTypeList().complexColType().get(i);

                    if (i != 0) {
                        builder.append(",");
                    }

                    Node node = visit(complexColTypeContext.identifier());
                    builder.append(tryUnquote(node.toString()));
                    builder.append(" ");
                    builder.append(getType(complexColTypeContext.dataType()));
                }
                builder.append(")");
                return "ROW" + builder.toString();
            }
        }
        else {
            return type.getText();
        }

        throw parseError("Don't support type specification: " + type.getText(), type);
    }

    private static ArithmeticBinaryExpression.Operator getArithmeticBinaryOperator(Token operator)
    {
        switch (operator.getType()) {
            case io.hivesql.sql.parser.SqlBaseLexer.PLUS:
                return ArithmeticBinaryExpression.Operator.ADD;
            case io.hivesql.sql.parser.SqlBaseLexer.MINUS:
                return ArithmeticBinaryExpression.Operator.SUBTRACT;
            case io.hivesql.sql.parser.SqlBaseLexer.ASTERISK:
                return ArithmeticBinaryExpression.Operator.MULTIPLY;
            case io.hivesql.sql.parser.SqlBaseLexer.SLASH:
                return ArithmeticBinaryExpression.Operator.DIVIDE;
            case io.hivesql.sql.parser.SqlBaseLexer.PERCENT:
                return ArithmeticBinaryExpression.Operator.MODULUS;
            case io.hivesql.sql.parser.SqlBaseLexer.DIV:
                return ArithmeticBinaryExpression.Operator.DIV;
            case io.hivesql.sql.parser.SqlBaseLexer.PIPE:
                return ArithmeticBinaryExpression.Operator.PIPE;
            case io.hivesql.sql.parser.SqlBaseLexer.TILDE:
                return ArithmeticBinaryExpression.Operator.TILDE;
            case io.hivesql.sql.parser.SqlBaseLexer.AMPERSAND:
                return ArithmeticBinaryExpression.Operator.AMPERSAND;
            case io.hivesql.sql.parser.SqlBaseLexer.CONCAT_PIPE:
                return ArithmeticBinaryExpression.Operator.CONCAT_PIPE;
            case io.hivesql.sql.parser.SqlBaseLexer.HAT:
                return ArithmeticBinaryExpression.Operator.HAT;
        }

        throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
    }

    private static ComparisonExpression.Operator getComparisonOperator(Token symbol)
    {
        switch (symbol.getType()) {
            case io.hivesql.sql.parser.SqlBaseLexer.EQ:
                return ComparisonExpression.Operator.EQUAL;
            case io.hivesql.sql.parser.SqlBaseLexer.NSEQ:
                return ComparisonExpression.Operator.EQNSF;
            case io.hivesql.sql.parser.SqlBaseLexer.NEQ:
                return ComparisonExpression.Operator.NOT_EQUAL;
            case io.hivesql.sql.parser.SqlBaseLexer.NEQJ:
                return ComparisonExpression.Operator.NOT_EQUAL;
            case io.hivesql.sql.parser.SqlBaseLexer.LT:
                return ComparisonExpression.Operator.LESS_THAN;
            case io.hivesql.sql.parser.SqlBaseLexer.LTE:
                return ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
            case io.hivesql.sql.parser.SqlBaseLexer.GT:
                return ComparisonExpression.Operator.GREATER_THAN;
            case io.hivesql.sql.parser.SqlBaseLexer.GTE:
                return ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
        }

        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }

    private static LogicalExpression.Operator getLogicalBinaryOperator(Token token)
    {
        switch (token.getType()) {
            case io.hivesql.sql.parser.SqlBaseLexer.AND:
                return LogicalExpression.Operator.AND;
            case io.hivesql.sql.parser.SqlBaseLexer.OR:
                return LogicalExpression.Operator.OR;
        }

        throw new IllegalArgumentException("Unsupported operator: " + token.getText());
    }

    private QualifiedName getQualifiedName(SqlBaseParser.TableIdentifierContext context)
    {
        List<Identifier> identifiers = new ArrayList<>();
        for (SqlBaseParser.IdentifierContext identifierContext : context.identifier()) {
            String qualifiedName = identifierContext.getText();
            String[] tmp = tryUnquote(qualifiedName).split("\\.");

            for (String id : tmp) {
                Identifier identifier = new Identifier(getLocation(identifierContext),
                        id, true);
                identifiers.add(identifier);
            }
        }
        return QualifiedName.of(identifiers);
    }

    private QualifiedName getQualifiedName(SqlBaseParser.QualifiedNameContext context)
    {
        List<Identifier> identifiers = new ArrayList<>();
        for (SqlBaseParser.IdentifierContext identifierContext : context.identifier()) {
            Identifier identifier = new Identifier(getLocation(identifierContext), identifierContext.getText(), true);
            identifiers.add(identifier);
            visit(identifierContext);
        }
        return QualifiedName.of(identifiers);
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz)
    {
        return contexts.stream()
            .map(this::visit)
            .map(clazz::cast)
            .collect(toList());
    }

    public Node visitUnquotedIdentifier(io.trino.sql.parser.SqlBaseParser.UnquotedIdentifierContext context)
    {
        return new Identifier(getLocation(context), context.getText(), false);
    }

    private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz)
    {
        return Optional.ofNullable(context)
            .map(this::visit)
            .map(clazz::cast);
    }

    private static Optional<String> getTextIfPresent(Token token)
    {
        return Optional.ofNullable(token)
            .map(Token::getText);
    }

    public static NodeLocation getLocation(ParserRuleContext parserRuleContext)
    {
        requireNonNull(parserRuleContext, "parserRuleContext is null");
        return getLocation(parserRuleContext.getStart());
    }

    public static NodeLocation getLocation(TerminalNode terminalNode)
    {
        requireNonNull(terminalNode, "terminalNode is null");
        return getLocation(terminalNode.getSymbol());
    }

    public static NodeLocation getLocation(Token token)
    {
        requireNonNull(token, "token is null");
        return new NodeLocation(token.getLine(), token.getCharPositionInLine() + 1);
    }

    private static void check(boolean condition, String message, ParserRuleContext context)
    {
        if (!condition) {
            throw parseError(message, context);
        }
    }

    private static QualifiedName getQualifiedName(String qualifiedName)
    {
        if (qualifiedName == null) {
            return null;
        }
        String[] tmp = qualifiedName.replace("`", "")
            .replace("\"", "").split("\\.");
        if (tmp.length == 0) {
            return null;
        }
        return QualifiedName.of(tmp[0], Arrays.copyOfRange(tmp, 1, tmp.length));
    }

    /**
     * this will remove single quotation, double quotation and backtick.
     */
    public static String unquote(String value)
    {
        return value.substring(1, value.length() - 1)
            .replace("''", "'");
    }

    /**
     * replace \\ with \
     * @param value
     * @return replaced value
     */
    public static String decreaseSlash(String value)
    {
        return value.replace("\\\\", "\\");
    }

    public static String tryUnquote(String value)
    {
        if (value == null) {
            return null;
        }
        return value.replaceAll("\'", "")
            .replaceAll("\"", "")
            .replaceAll("`", "");
    }
}
