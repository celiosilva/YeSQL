package br.com.delogic.yesql;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class TestQuery extends Assert {

    @Test
    public void testComposeQuery() {

        Query<String> query = new Query<String>();
        query.setSelect("col1, col2, col3");
        query.setFrom("tab1 tb1 inner join tab2 tb2 on tb1.id = tb2.id");

        assertEquals("select col1, col2, col3 from tab1 tb1 inner join tab2 tb2 on tb1.id = tb2.id",
            query.composeQuery(null));

        query.setWhere("tb1.active = 1 and tb2.name like '%test%'");
        assertEquals(
            "select col1, col2, col3 from tab1 tb1 inner join tab2 tb2 on tb1.id = tb2.id " +
                "where tb1.active = 1 and tb2.name like '%test%'",
            query.composeQuery(null));

        query.setGroupBy("tb1.name");
        assertEquals(
            "select col1, col2, col3 from tab1 tb1 inner join tab2 tb2 on tb1.id = tb2.id " +
                "where tb1.active = 1 and tb2.name like '%test%' " +
                "group by tb1.name",
            query.composeQuery(null));

        query.setOrderBy("tb1.id");
        assertEquals(
            "select col1, col2, col3 from tab1 tb1 inner join tab2 tb2 on tb1.id = tb2.id " +
                "where tb1.active = 1 and tb2.name like '%test%' " +
                "group by tb1.name " +
                "order by tb1.id",
            query.composeQuery(null));

        Map<String, String> and = new HashMap<String, String>();
        and.put("name:text", "tb1.name like :name");
        and.put("id:number", "tb1.id = :id");

        assertEquals(
            "select col1, col2, col3 from tab1 tb1 inner join tab2 tb2 on tb1.id = tb2.id " +
                "where tb1.active = 1 and tb2.name like '%test%' " +
                "group by tb1.name " +
                "order by tb1.id",
            query.composeQuery(null));

    }

}