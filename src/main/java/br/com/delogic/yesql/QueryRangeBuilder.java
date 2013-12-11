package br.com.delogic.yesql;

public interface QueryRangeBuilder {

    String buildRangeQuery(String query, QueryParameters configuration);

}
