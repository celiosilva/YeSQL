package br.com.delogic.yesql;

public interface SqlQueryRangeBuilder {

    String buildRangeQuery(String query, SqlQueryParameters configuration);

}
