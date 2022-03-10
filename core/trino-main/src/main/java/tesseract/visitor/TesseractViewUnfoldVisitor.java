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
 * @since Thu, 03-03-2022
 */
package tesseract.visitor;

import io.trino.Session;
import io.trino.execution.QueryPreparer;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.tesseract.TesseractTableInfo;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Table;
import tesseract.pojos.TesseractViewContext;
import tesseract.pojos.TesseractViewRewriteInfo;

import java.util.Optional;

import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;

/**
 * Visitor used to visit all the views in the AST of the original query
 */
public class TesseractViewUnfoldVisitor extends DefaultTraversalVisitor<TesseractViewContext>
{

    private final Metadata metadata;
    private final Session session;
    private final QueryPreparer queryPreparer;

    public TesseractViewUnfoldVisitor(Metadata metadata, Session session, QueryPreparer queryPreparer)
    {
        this.metadata = metadata;
        this.session = session;
        this.queryPreparer = queryPreparer;
    }

    @Override
    protected Void visitTable(Table node, TesseractViewContext context)
    {

        Identifier endOfTableIdentifier = node.getName().getOriginalParts().get(node.getName().getOriginalParts().size() - 1);
        int totalLengthOfTableEnd = endOfTableIdentifier.isDelimited() ? endOfTableIdentifier.getValue().length() + 2 : endOfTableIdentifier.getValue().length();

        Identifier startOfTableIdentifier = node.getName().getOriginalParts().get(0);

        QualifiedObjectName name = createQualifiedObjectName(session, node, node.getName());
        Optional<ConnectorViewDefinition> optionalView = metadata.getView(session, name);

        if (optionalView.isPresent()) {
            /*
            Start a new traversal on the view definition and find if its referring a tesseract table
             */
            TesseractViewContext viewAstContext = new TesseractViewContext();
            queryPreparer.prepareQuery(session,optionalView.get().getOriginalSql()).getStatement().accept(this, viewAstContext);

            if(viewAstContext.isReferringTesseractTable()){
                context.getViewLocationToRewriteInfo().add(new TesseractViewRewriteInfo(
                        startOfTableIdentifier.getLocation().get(),
                        endOfTableIdentifier.getLocation().get(),
                        totalLengthOfTableEnd,
                        optionalView.get().getOriginalSql())
                );
            }
        }
        else {
            // this leg is only relevant for the visitor visting the view definition of a view and encounters the table in the view definition
            QualifiedObjectName tableName = createQualifiedObjectName(session, node, node.getName());
            Optional<TableHandle> optionalTable = metadata.getTableHandle(session, tableName);
            if (optionalTable.isPresent()) {
                Optional<TesseractTableInfo> tesseractTableInfo = optionalTable.get().getConnectorHandle().getTesseractTableInfo();
                if (tesseractTableInfo.isPresent() && tesseractTableInfo.get().isTesseractOptimizedTable()) {
                    context.setReferringTesseractTable(true);
                }
            }
        }
        return super.visitTable(node, context);
    }



}
