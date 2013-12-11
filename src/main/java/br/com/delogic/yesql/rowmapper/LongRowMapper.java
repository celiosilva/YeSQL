package br.com.delogic.yesql.rowmapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class LongRowMapper<E> implements RowMapper<E> {

    @SuppressWarnings("unchecked")
    public E mapRow(ResultSet rs, int rowNum) throws SQLException {
        E e = (E) Long.valueOf(rs.getLong(1));
        if (e instanceof Integer) {
            return e;
        } else {
            throw new IllegalStateException("LongRowMapper is designed to return Longs from a one column only query");
        }
    }

}
