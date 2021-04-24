package ru.inurgalimov.jdbc;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public interface JdbcTemplate {

    static <T> List<T> query(DataSource ds, String sql, RowMapper<T> rowMapper, Object... args) {
        return execute(ds, sql, args, stmt -> {
            try (final var rs = stmt.executeQuery()) {
                List<T> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(rowMapper.mapRow(rs));
                }
                return result;
            }
        });
    }

    static <T> Optional<T> querySingle(DataSource ds, String sql, RowMapper<T> rowMapper, Object... args) {
        return Optional.ofNullable(execute(ds, sql, args, stmt -> {
            stmt.execute();
            ResultSet rs = stmt.getResultSet();
            rs.next();
            return rowMapper.mapRow(rs);
        }));
    }

    static void queryWithoutReturning(DataSource ds, String sql, Object... args) {
        execute(ds, sql, args, stmt -> {
            stmt.execute();
            return null;
        });
    }

    static <T> T queryWithReturning(DataSource ds, String sql, RowMapper<T> rowMapper, Object... args) {
        return execute(ds, sql, args, stmt -> {
            stmt.execute();
            ResultSet rs = stmt.getResultSet();
            rs.next();
            return rowMapper.mapRow(rs);
        });
    }

    private static <T> T execute(DataSource ds, String sql, Object[] args, Executor<T> executor) {
        try (final var conn = ds.getConnection();
             final var stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                Object arg = args[i];
                stmt.setObject(i + 1, arg);
            }
            return executor.execute(stmt);
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

}
