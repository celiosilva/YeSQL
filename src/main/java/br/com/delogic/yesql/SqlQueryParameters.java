package br.com.delogic.yesql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SqlQueryParameters {

    private String[]            orderByKey;
    private Long                startRow;
    private Long                endRow;
    private Map<String, Object> parameters;

    private Logger              logger = Logger.getLogger(SqlQueryParameters.class.getName());

    /**
     * Will add the parameter with its key to the query only if the value is not
     * null
     *
     * @param key
     * @param value
     */
    public SqlQueryParameters(String key, Object value) {
        addParameter(key, value);
    }

    /**
     * Will add the parameter with its key to the query only if the value is not
     * null
     *
     * @param key
     * @param values
     */
    public SqlQueryParameters(String key, Object... values) {
        addParameter(key, values);
    }

    public SqlQueryParameters() {}

    public String[] getOrderByKey() {
        return orderByKey;
    }

    public void setOrderByKey(String... orderByKey) {
        this.orderByKey = orderByKey;
    }

    public Long getStartRow() {
        return startRow;
    }

    public Long getEndRow() {
        return endRow;
    }

    public void setStartRow(Long startRow) {
        this.startRow = startRow;
    }

    public void setEndRow(Long endRow) {
        this.endRow = endRow;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    /**
     * Will add the parameter with its key to the query only if the value is not
     * null.
     *
     * @param key
     * @param value
     * @return
     */
    public SqlQueryParameters addParameter(String key, Object value) {
        if (value == null) {
            logger.log(Level.WARNING, "Could not add empty value for key:" + key);
            return this;
        }
        if (parameters == null) {
            parameters = new HashMap<String, Object>();
        }
        parameters.put(key, value);
        return this;
    }

    /**
     * Will add the parameter with its key to the query only if the value is not
     * null.
     *
     * @param key
     * @param value
     * @return
     */
    public SqlQueryParameters addParameter(String key, Enum<?> value) {
        if (value == null) {
            logger.log(Level.WARNING, "Could not add null value for key:" + key);
            return this;
        }
        return addParameter(key, value.name());
    }

    /**
     * Will add the parameter with its key to the query only if the value is not
     * null
     *
     * @param key
     * @param values
     * @return
     */
    public SqlQueryParameters addParameter(String key, Object... values) {
        if (values == null || values.length == 0) {
            logger.log(Level.WARNING, "Could not add empty values for key:" + key);
            return this;
        }
        return addParameter(key, Arrays.asList(values));
    }

    public SqlQueryParameters activateParameter(String key) {
        return addParameter(key, "");
    }

}
