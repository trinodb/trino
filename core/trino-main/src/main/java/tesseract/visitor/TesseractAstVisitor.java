/**
 * Licensed to the Airtel International LLP (AILLP) under one
 * or more contributor license agreements.
 * The AILLP licenses this file to you under the AA License, Version 1.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author Akash Roy
 * @department Big Data Analytics Airtel Africa
 * @since Thu, 03-02-2022
 */
package tesseract.visitor;

import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.spi.tesseract.TesseractTableInfo;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.WindowDefinition;
import tesseract.pojos.TesseractAstContext;
import tesseract.pojos.TesseractAstTable;

import java.util.Optional;

import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;

/**
 * Responsible to traverse the AST of the original query and collect tesseract optimized tables and their required info.
 */
public class TesseractAstVisitor
        extends DefaultTraversalVisitor<TesseractAstContext>
{

    private final Metadata metadata;
    private final Session session;

    public TesseractAstVisitor(Metadata metadata, Session session)
    {
        this.metadata = metadata;
        this.session = session;
    }

    @Override
    protected Void visitTable(Table node, TesseractAstContext context)
    {

        NodeLocation currentLocationInfo;
        /*
         if alias is present then it will override the table location
         */
        if (context.getAliasLocationInfo().isPresent()) {
            currentLocationInfo = context.getAliasLocationInfo().get();
        }
        else {
            int offset = 0;
            Identifier tableIdentifier = node.getName().getOriginalParts().get(node.getName().getOriginalParts().size() - 1);
            //enclosed by quotes
            if (tableIdentifier.isDelimited()) {
                offset += 2;
            }
            offset += tableIdentifier.getValue().length();
            currentLocationInfo = getRightBoundedLocationInfo(tableIdentifier.getLocation().get(), offset);
        }

        QualifiedObjectName tableName = createQualifiedObjectName(session, node, node.getName());
        Optional<TableHandle> optionalTable = metadata.getTableHandle(session, tableName);
        if (optionalTable.isPresent()) {

            Optional<TesseractTableInfo> tesseractTableInfo = optionalTable.get().getConnectorHandle().getTesseractTableInfo();

            if (tesseractTableInfo.isPresent() && tesseractTableInfo.get().isTesseractOptimizedTable()) {
                context.getQuerySpecificationLocalAstTables().add(
                        new TesseractAstTable(currentLocationInfo, node.getName().getSuffix(), node.getLocation().get()));
            }
        }
        /*
         we only require alias info local to a table , a sub query's alias should not be visible to outer table
         */
        context.setAliasLocationInfo(null);
        return super.visitTable(node, context);
    }

    /**
     * If there is a alias present for a table then capture its location as it will be required
     * to insert the optimal predicate later on
     */
    @Override
    protected Void visitAliasedRelation(AliasedRelation node, TesseractAstContext context)
    {
        int offset = 0;
        //enclosed by quotes
        if (node.getAlias().isDelimited()) {
            offset += 2;
        }

        offset += node.getAlias().getValue().length();
        context.setAliasLocationInfo(getRightBoundedLocationInfo(node.getAlias().getLocation().get(), offset));
        return super.visitAliasedRelation(node, context);
    }

    @Override
    protected Void visitQuerySpecification(QuerySpecification node, TesseractAstContext context)
    {
        process(node.getSelect(), context);
        context.setAliasLocationInfo(null);
        if (node.getFrom().isPresent()) {
            process(node.getFrom().get(), context);
        }
        if (node.getWhere().isPresent()) {
            context.getQuerySpecificationLocalAstTables().forEach(table -> table.setWherePresent(true));
            process(node.getWhere().get(), context);
        }
        if (node.getGroupBy().isPresent()) {
            process(node.getGroupBy().get(), context);
        }
        if (node.getHaving().isPresent()) {
            process(node.getHaving().get(), context);
        }
        for (WindowDefinition windowDefinition : node.getWindows()) {
            process(windowDefinition, context);
        }
        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }
        if (node.getOffset().isPresent()) {
            process(node.getOffset().get(), context);
        }
        if (node.getLimit().isPresent()) {
            process(node.getLimit().get(), context);
        }
        // all tesseract tables captured in the current query section should be added to the complete query's AST table list
        context.getQuerySpecificationLocalAstTables().forEach(
                localQuerySectionTable -> context.getNodeLocationToTesseractAstTables().put(localQuerySectionTable.getOriginalNodeLocation(), localQuerySectionTable)
        );

        // this section's tesseract tables are not required anymore
        context.getQuerySpecificationLocalAstTables().clear();
        return null;
    }

    private static NodeLocation getRightBoundedLocationInfo(NodeLocation nodeLocation, int offset)
    {
        return new NodeLocation(nodeLocation.getLineNumber(), nodeLocation.getColumnNumber() + offset);
    }
}
