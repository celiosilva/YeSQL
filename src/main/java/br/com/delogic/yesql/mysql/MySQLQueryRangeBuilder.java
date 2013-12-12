package br.com.delogic.yesql.mysql;

import br.com.delogic.yesql.QueryParameters;
import br.com.delogic.yesql.QueryRangeBuilder;

/**
 * Will add the start row and end end row filters to the query for MySQL
 * database.
 *
 * @author celio@delogic.com.br
 *
 */
public class MySQLQueryRangeBuilder implements QueryRangeBuilder {

    public String buildRangeQuery(String query, QueryParameters configuration) {
        long startRow = configuration.getStartRow() != null ? configuration.getStartRow() : 0;
        long endRow = configuration.getEndRow() != null ? configuration.getEndRow() : Long.MAX_VALUE;

        query += (" LIMIT " + startRow + ", " + endRow);
        return query;
    }

}
