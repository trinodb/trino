import io.prestosql.sql.ExpressionFormatter;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Identifier;

public class AliasAdditionExpressionFormatter extends ExpressionFormatter.Formatter
{
    private final String tableAlias;

    public AliasAdditionExpressionFormatter(String tableAlias)
    {
        this.tableAlias = tableAlias;
    }

    @Override
    protected String visitIdentifier(Identifier node, Void context)
    {
        if ( !node.isDelimited() )
        {
            return tableAlias + "." + node.getValue();
        }
        else
        {
            return '"' + (tableAlias + "." + node.getValue()).replace( "\"", "\"\"" ) + '"';
        }
    }

    @Override protected String visitDereferenceExpression(DereferenceExpression node, Void context)
    {
        throw new IllegalStateException( "dereference expressions not allowed, eg: use column_name instead of r.column_name" );
    }
}
