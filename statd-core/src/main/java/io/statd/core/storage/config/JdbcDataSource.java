package io.statd.core.storage.config;

import io.statd.core.exception.StatdException;
import lombok.Data;

import java.util.Objects;

@Data
public class JdbcDataSource implements Datasource {
    private String url;
    private String user;
    private String password;
    private String driverClass;
    private int maximumPoolSize;
    private String connectionTestQuery;



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JdbcDataSource that = (JdbcDataSource) o;
        return Objects.equals(url, that.url) && Objects.equals(user, that.user) && Objects.equals(password, that.password) && Objects.equals(driverClass, that.driverClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, user, password, driverClass);
    }

    @Override
    public void validate() throws StatdException {
        Objects.requireNonNull(url, "jdbc url not provided");
    }


}