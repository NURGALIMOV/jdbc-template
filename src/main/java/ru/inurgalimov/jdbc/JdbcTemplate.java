package ru.inurgalimov.jdbc;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;

public interface JdbcTemplate {

    static <T> List<T> query(DataSource ds, String sql, RowMapper<T> rowMapper, Object... args) {
        return (List<T>) execute(ds, sql, args, stmt -> {
            try (final var rs = stmt.executeQuery()) {
                if (Objects.nonNull(rs)) {
                    final var result = new ArrayList<>();
                    while (rs.next()) {
                        result.add(rowMapper.mapRow(rs));
                    }
                    return result;
                } else {
                    return Collections.<T>emptyList();
                }
            }
        });
    }

    static <T> Optional<T> querySingleForOptional(DataSource ds, String sql, RowMapper<T> rowMapper,
                                                  Object... args) {
        return Optional.ofNullable(querySingle(ds, sql, rowMapper, args));
    }

    static <T> T querySingle(DataSource ds, String sql, RowMapper<T> rowMapper, Object... args) {
        return execute(ds, sql, args, stmt -> {
            stmt.execute();
            try (final var rs = stmt.getResultSet()) {
                if (Objects.nonNull(rs)) {
                    return rs.next() ? rowMapper.mapRow(rs) : null;
                }
                return null;
            }
        });
    }

    private static <T> T execute(DataSource ds, String sql, Object[] args, Executor<T> executor) {
        try (final var conn = ds.getConnection();
             final var stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                final var arg = args[i];
                stmt.setObject(i + 1, arg);
            }
            return executor.execute(stmt);
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

}
