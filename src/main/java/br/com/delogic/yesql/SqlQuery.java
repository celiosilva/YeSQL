package br.com.delogic.yesql;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import br.com.delogic.yesql.rowmapper.IntegerRowMapper;
import br.com.delogic.yesql.rowmapper.LongRowMapper;
import br.com.delogic.yesql.rowmapper.StringRowMapper;

public class SqlQuery<E> implements InitializingBean {

    private String                              select;
    private String                              from;
    private String                              where;
    private String                              groupBy;
    private String                              orderBy;
    private Map<String, String>                 orders;
    private Map<String, String>                 and;

    private Map<String, PermittedParameterType> registeredParameters   = new HashMap<String, SqlQuery.PermittedParameterType>();
    private Class<E>                            returnType;
    private Map<String, PermittedParameterType> mandatoryParameters;

    private static final String                 ORDER_STATEMENT        = " order by ";
    private static final String                 SELECT_STATEMENT       = "select ";
    private static final String                 SELECT_COUNT_STATEMENT = "select count(*) ";
    private static final String                 FROM_STATEMENT         = " from ";
    private static final String                 WHERE_STATEMENT        = " where ";
    private static final String                 GROUP_STATEMENT        = " group by ";
    private static final String                 AND_OPERATOR           = " and ";

    private NamedParameterJdbcTemplate          template;
    private RowMapper<E>                        rowMapper;

    @Inject
    private SqlQueryRangeBuilder                rangeBuilder;

    @Inject
    private DataSource                          dataSource;

    private Logger                              logger                 = Logger.getLogger(SqlQuery.class);

    public void afterPropertiesSet() throws Exception {
        template = new NamedParameterJdbcTemplate(dataSource);

        mandatoryParameters = new HashMap<String, SqlQuery.PermittedParameterType>();
        Pattern pattern = Pattern.compile(":\\w+:\\w+");
        select = removeMandatoryParamType(select, pattern, mandatoryParameters);
        where = removeMandatoryParamType(where, pattern, mandatoryParameters);
        orderBy = removeMandatoryParamType(orderBy, pattern, mandatoryParameters);
        groupBy = removeMandatoryParamType(groupBy, pattern, mandatoryParameters);
        if (f.notEmpty(orders)) {
            for (Entry<String, String> entry : orders.entrySet()) {
                entry.setValue(removeMandatoryParamType(entry.getValue(), pattern, mandatoryParameters));
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Mandatory parameters:" + mandatoryParameters);
        }

        if (returnType.equals(String.class)) {
            rowMapper = new StringRowMapper<E>();

        } else if (returnType.equals(Integer.class)) {
            rowMapper = new IntegerRowMapper<E>();

        } else if (returnType.equals(Long.class)) {
            rowMapper = new LongRowMapper<E>();

        } else {
            rowMapper = BeanPropertyRowMapper.newInstance(returnType);
        }
    }

    public String removeMandatoryParamType(String statement, Pattern pattern, Map<String, PermittedParameterType> params) {
        if (!f.hasValue(statement) || !statement.contains(":")) {
            return statement;
        }
        Matcher matcher = pattern.matcher(statement);
        while (matcher.find()) {
            String param = matcher.group();
            String[] paramSplitted = param.split(":");
            params.put(paramSplitted[1], PermittedParameterType.fromName(paramSplitted[2]));
            statement = statement.replace(":" + paramSplitted[2], "");
        }
        return statement;
    }

    public List<E> getList() {
        return getList(null);
    }

    public List<E> getList(SqlQueryParameters config) {
        if (returnType == null) {
            throw new IllegalStateException("returnType cannot be null when getting a list with type mapping");
        }
        Map<String, Object> params = config != null && config.getParameters() != null ? config.getParameters()
                                                                                     : new HashMap<String, Object>();
        String composedQuery = composeQuery(config);

        if (config != null && (config.getStartRow() != null || config.getEndRow() != null)) {
            composedQuery = rangeBuilder.buildRangeQuery(composedQuery, config);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Executando consulta:" + composedQuery.replaceAll("\\s+", " "));
            logger.debug("Com parametros:" + (config != null && config.getParameters() != null ? config.getParameters() : ""));
        }

        List<E> data = null;

        data = template.query(composedQuery, params, rowMapper);

        if (logger.isDebugEnabled()) {
            logger.debug("Total de itens encontrados:" + data.size());
        }

        return data;

    }

    public E getFirst(SqlQueryParameters config) {
        if (returnType == null) {
            throw new IllegalStateException("returnType cannot be null when getting a result with type mapping");
        }
        Map<String, Object> params = config != null && config.getParameters() != null ? config.getParameters()

                                                                                     : new HashMap<String, Object>();
        // getting only the first result
        config.setStartRow(0L);
        config.setEndRow(1L);

        String composedQuery = composeQuery(config);

        if (config != null && (config.getStartRow() != null || config.getEndRow() != null)) {
            composedQuery = rangeBuilder.buildRangeQuery(composedQuery, config);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Executando consulta:" + composedQuery.replaceAll("\\s+", " "));
            logger.debug("Com parametros:" + (config != null && config.getParameters() != null ? config.getParameters() : ""));
        }

        List<E> data = null;

        data = template.query(composedQuery, params, rowMapper);

        if (logger.isDebugEnabled()) {
            logger.debug("Total de itens encontrados:" + data.size());
        }

        if (data.isEmpty()) {
            return null;
        }

        return data.get(0);

    }

    public long getCount(SqlQueryParameters config) {

        Map<String, Object> params = config != null && config.getParameters() != null ? config.getParameters()
                                                                                     : new HashMap<String, Object>();
        String composedQuery = composeCount(config);

        if (logger.isDebugEnabled()) {
            logger.debug("Executando consulta:" + composedQuery.replaceAll("\\s+", " "));
            logger.debug("Com parametros:" + (config != null && config.getParameters() != null ? config.getParameters() : ""));
        }

        long count = template.queryForLong(composedQuery, params);

        if (logger.isDebugEnabled()) {
            logger.debug("Total de itens encontrados:" + count);
        }

        return count;
    }

    public long getCount() {
        return getCount(null);
    }

    String composeQuery(SqlQueryParameters configuration) {
        StringBuilder sbQuery = new StringBuilder(SELECT_STATEMENT + select + FROM_STATEMENT + from);
        if (f.notEmpty(where)) {
            sbQuery.append(WHERE_STATEMENT);
            sbQuery.append(where);
        }

        Map<String, Object> params = configuration != null && f.notEmpty(configuration.getParameters()) ? configuration.getParameters()
                                                                                                       : null;

        if (f.notEmpty(and) && f.notEmpty(params)) {

            sbQuery.append(f.isEmpty(where) ? WHERE_STATEMENT : AND_OPERATOR);

            for (Iterator<String> it =
                params.keySet().iterator(); it.hasNext();) {
                String key = it.next();
                if (!and.containsKey(key)) continue;

                sbQuery.append(and.get(key));

                if (it.hasNext()) sbQuery.append(AND_OPERATOR);
            }
            // if the query ends with where or and statements we need to remove
            // them.
            String q = sbQuery.toString();
            if (q.endsWith(WHERE_STATEMENT)) {
                q = q + "tbr";
                q = q.replace(WHERE_STATEMENT + "tbr", "");
            } else if (q.endsWith(AND_OPERATOR)) {
                q = q + "tbr";
                q = q.replace(AND_OPERATOR + "tbr", "");
            }
            sbQuery = new StringBuilder(q);
        }
        if (f.notEmpty(groupBy)) {
            sbQuery.append(GROUP_STATEMENT).append(groupBy);
        }

        if (configuration == null || (f.isEmpty(configuration.getOrderByKey()) && f.isEmpty(orders))) {
            if (f.notEmpty(orderBy)) {
                sbQuery.append(ORDER_STATEMENT).append(orderBy);
            }
        } else if (configuration != null && f.notEmpty(configuration.getOrderByKey()) && f.notEmpty(orders)) {
            String orderStatement = "";
            for (String orderByKey : configuration.getOrderByKey()) {
                orderStatement += orders.containsKey(orderByKey) ? (f.isEmpty(orderStatement) ? "" : ",")
                    + orders.get(orderByKey) : "";
            }
            if (f.notEmpty(orderStatement)) {
                sbQuery.append(ORDER_STATEMENT + orderStatement);
            }
        }
        return sbQuery.toString();
    }

    String composeCount(SqlQueryParameters configuration) {
        StringBuilder sbQuery = new StringBuilder(SELECT_COUNT_STATEMENT + FROM_STATEMENT + "(" + SELECT_STATEMENT + select
            + FROM_STATEMENT + from);
        if (f.notEmpty(where)) {
            sbQuery.append(WHERE_STATEMENT);
            sbQuery.append(where);
        }

        Map<String, Object> params = configuration != null && f.notEmpty(configuration.getParameters()) ? configuration.getParameters()
                                                                                                       : null;

        if (f.notEmpty(and) && f.notEmpty(params)) {

            sbQuery.append(f.isEmpty(where) ? WHERE_STATEMENT : AND_OPERATOR);

            for (Iterator<String> it =
                params.keySet().iterator(); it.hasNext();) {
                String key = it.next();
                if (!and.containsKey(key)) continue;

                sbQuery.append(and.get(key));

                if (it.hasNext()) sbQuery.append(AND_OPERATOR);
            }
            // if the query ends with where or and statements we need to remove
            // them.
            String q = sbQuery.toString();
            if (q.endsWith(WHERE_STATEMENT)) {
                q = q + "tbr";
                q = q.replace(WHERE_STATEMENT + "tbr", "");
            } else if (q.endsWith(AND_OPERATOR)) {
                q = q + "tbr";
                q = q.replace(AND_OPERATOR + "tbr", "");
            }
            sbQuery = new StringBuilder(q);
        }
        if (f.notEmpty(groupBy)) {
            sbQuery.append(GROUP_STATEMENT).append(groupBy);
        }

        return sbQuery.toString() + ") sql_query_count_table";
    }

    String parseOrderBy(String[] orderBy) {
        String[] orders = orderBy;
        String newOrderBy = " ";
        boolean addComma = false;
        for (String s : orders) {
            int columnIndex = Integer.parseInt(s);
            if (addComma) newOrderBy += " , ";
            if (columnIndex < 0) {
                columnIndex = Math.abs(columnIndex);
                newOrderBy += (columnIndex + " desc");
            } else {
                newOrderBy += columnIndex;
            }
            addComma = true;
        }
        return newOrderBy;
    }

    public String getSelect() {
        return select;
    }

    public void setSelect(String select) {
        this.select = select;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    public String getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(String groupBy) {
        this.groupBy = groupBy;
    }

    public String getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(String orderBy) {
        this.orderBy = orderBy;
    }

    public Map<String, String> getAnd() {
        return and;
    }

    public void setAnd(Map<String, String> andParam) {
        if (and == null) {
            and = new HashMap<String, String>();
        }

        for (Entry<String, String> entry : andParam.entrySet()) {
            String[] keyType = entry.getKey().split(":");
            if (keyType.length <= 1) {
                throw new IllegalArgumentException(
                    "You should provide the key parameter name with a colon and the data type, " +
                        "like myparam:number. Permitted types are:"
                        + PermittedParameterType.stringValues());
            }
            PermittedParameterType type = PermittedParameterType.valueOf(keyType[1]);
            if (type == null) {
                throw new IllegalArgumentException(
                    "You should provide the key parameter name with a colon and the data type, " +
                        "like myparam:number. Permitted types are:"
                        + PermittedParameterType.stringValues());
            }

            and.put(keyType[0], entry.getValue());
            registeredParameters.put(keyType[0], type);
        }
    }

    public Class<E> getReturnType() {
        return returnType;
    }

    public void setReturnType(Class<E> returnType) {
        this.returnType = returnType;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Map<String, String> getOrders() {
        return orders;
    }

    public void setOrders(Map<String, String> multiOrderBy) {
        this.orders = multiOrderBy;
    }

    public SqlQueryRangeBuilder getRangeBuilder() {
        return rangeBuilder;
    }

    public void setRangeBuilder(SqlQueryRangeBuilder rangeBuilder) {
        this.rangeBuilder = rangeBuilder;
    }

    public Map<String, PermittedParameterType> getRegisteredParameters() {
        return registeredParameters;
    }

    public Map<String, PermittedParameterType> getMandatoryParameters() {
        return mandatoryParameters;
    }

    public void setMandatoryParameters(Map<String, PermittedParameterType> mandatoryParameters) {
        this.mandatoryParameters = mandatoryParameters;
    }

    public enum PermittedParameterType {
        number(1), date(new Date()), text("text"), bool(Boolean.TRUE), numberslist(Arrays.asList(1, 2, 3)), textslist(Arrays.asList(
            "text1", "text2"));

        private final Object example;

        private PermittedParameterType(Object ex) {
            this.example = ex;
        }

        public static String stringValues() {
            return Arrays.toString(values());
        };

        public static PermittedParameterType fromName(String type) {
            return PermittedParameterType.valueOf(type);
        }

        public Object getExample() {
            return example;
        }

    }

}

class f {

    /**
     * If both objects are null it'll return false
     *
     * @param obj1
     * @param obj2
     * @return
     */
    public static final <E> boolean equals(E obj1, E obj2) {
        if (obj1 == null && obj2 == null) {
            return false;
        } else if ((obj1 != null && obj2 == null)
            || (obj1 == null && obj2 != null)) {
            return false;
        } else {
            return obj1.equals(obj2);
        }
    }

    public static final <E> boolean notNull(E... es) {
        for (E e : es) {
            if (e == null) {
                return false;
            }
        }
        return true;
    }

    public static final <E> boolean notEmpty(E... es) {
        if (es == null) {
            return false;
        }
        for (E e : es) {
            if (e == null || String.valueOf(e).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    public static final <E> boolean isEmpty(E... es) {
        if (es == null) {
            return true;
        }
        for (E e : es) {
            if (e == null || e.toString().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public static final boolean hasValue(String value) {
        return value != null && (!value.isEmpty() && !value.trim().isEmpty());
    }

    public static final boolean isEmpty(String value) {
        return value == null || value.isEmpty() || value.trim().isEmpty();
    }

    public static final <E> boolean notEmpty(Collection<E> col) {
        return col != null && !col.isEmpty();
    }

    public static final <E> boolean notEmpty(Map<?, ?> col) {
        return col != null && !col.isEmpty();
    }

    public static final <E> boolean isEmpty(Collection<E> col) {
        return col == null || col.isEmpty();
    }

    public static final <E> boolean isEmpty(Map<?, ?> col) {
        return col == null || col.isEmpty();
    }

}
