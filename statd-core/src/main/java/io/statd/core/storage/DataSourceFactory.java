package io.statd.core.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.springframework.data.mongodb.MongoDatabaseFactory;

import javax.sql.DataSource;

public interface DataSourceFactory {

    @JsonIgnore
    DataSource getDataSource(String dataSource);

    MongoDatabaseFactory getMongoDatabaseFactory(String dataSource);

}
