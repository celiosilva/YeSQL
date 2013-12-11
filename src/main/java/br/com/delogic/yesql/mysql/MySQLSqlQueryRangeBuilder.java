package br.com.delogic.yesql.mysql;

import br.com.delogic.yesql.SqlQueryParameters;
import br.com.delogic.yesql.SqlQueryRangeBuilder;

public class MySQLSqlQueryRangeBuilder implements SqlQueryRangeBuilder {

    public String buildRangeQuery(String query, SqlQueryParameters configuration) {
        long startRow = configuration.getStartRow() != null ? configuration.getStartRow() : 0;
        long endRow = configuration.getEndRow() != null ? configuration.getEndRow() : Long.MAX_VALUE;

        query += (" Limit " + startRow + ", " + endRow);
        return query;
    }

}
