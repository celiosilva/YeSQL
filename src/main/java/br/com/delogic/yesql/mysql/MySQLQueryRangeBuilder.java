package br.com.delogic.yesql.mysql;

import br.com.delogic.yesql.QueryParameters;
import br.com.delogic.yesql.QueryRangeBuilder;

public class MySQLQueryRangeBuilder implements QueryRangeBuilder {

    public String buildRangeQuery(String query, QueryParameters configuration) {
        long startRow = configuration.getStartRow() != null ? configuration.getStartRow() : 0;
        long endRow = configuration.getEndRow() != null ? configuration.getEndRow() : Long.MAX_VALUE;

        query += (" Limit " + startRow + ", " + endRow);
        return query;
    }

}
