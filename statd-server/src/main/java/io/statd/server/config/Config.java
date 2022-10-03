package io.statd.server.config;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.statd.core.storage.DataSourceFactory;
import io.statd.core.storage.config.Datasource;
import io.statd.core.storage.config.JdbcDataSource;
import io.statd.core.storage.config.MongoDataSource;
import io.statd.server.model.ModuleMetric;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class Config implements DataSourceFactory {

    private LoadingCache<String, DataSource> nameToJdbcDatasource = Caffeine.newBuilder()
            .expireAfterWrite(1, TimeUnit.MINUTES)
            .build(this::loadJdbcDatasource);

    private LoadingCache<String, MongoDatabaseFactory> nameToMongoTemplate = Caffeine.newBuilder()
            .expireAfterWrite(1, TimeUnit.MINUTES).build(this::loadMongoDatabaseFactory);


    //module-> storage config
    private LoadingCache<ModuleMetric, StorageConfig> moduleToStorageConfig = Caffeine.newBuilder()
            .expireAfterWrite(1, TimeUnit.MINUTES)
            .build(this::loadStorage);

    //datasource-> storage config
    private LoadingCache<String, Datasource> nameToDataSource = Caffeine.newBuilder()
            .expireAfterWrite(1, TimeUnit.MINUTES)
            .build(this::loadDatasource);

    @Value("${config.path}")
    private String confPath;

    @Value("${config.storage.cached:false}")
    private boolean storageCached;

    @Value("${config.load.in.resource:false}")
    private boolean loadInResource;

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    private StorageConfig loadStorage(ModuleMetric moduleMetric) {
        String metricFile = String.format("/%s/%s/%s.yaml", confPath, moduleMetric.getModule(), moduleMetric.getMetric());
        try (InputStream configInput = loadResourceFile(metricFile)) {
            return mapper.readValue(configInput, StorageConfig.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }


    private Datasource loadDatasource(String dataSource) {
        String datasourceFile = String.format("/%s/datasource.yaml", confPath);
        try (InputStream configInput = loadResourceFile(datasourceFile)) {
            DatasourceConfig datasourceConfig = mapper.readValue(configInput, DatasourceConfig.class);
            datasourceConfig.validate();
            return datasourceConfig.getDatasource(dataSource);
        } catch (IOException e) {
            throw new UncheckedIOException(String.format("fail to load datasource '%s' with file '%s'", dataSource, datasourceFile), e);
        }
    }

    private InputStream loadResourceFile(String datasourceFile) {
        InputStream configInput;
        if (loadInResource) {
            configInput = this.getClass().getResourceAsStream(datasourceFile);
        } else {
            try {
                configInput = new FileInputStream(datasourceFile);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return configInput;
    }

    public StorageConfig getStorage(ModuleMetric moduleMetric) {
        if (storageCached) {
            return this.moduleToStorageConfig.get(moduleMetric);
        } else {
            return this.loadStorage(moduleMetric);
        }

    }

    @Override
    public MongoDatabaseFactory getMongoDatabaseFactory(String dataSource) {
        return this.nameToMongoTemplate.get(dataSource);
    }

    public DataSource getDataSource(String dataSource) {
        return this.nameToJdbcDatasource.get(dataSource);
    }


    private DataSource loadJdbcDatasource(String datasourceName) {



        Datasource datasource = this.getDatasource(datasourceName);
        if (datasource instanceof JdbcDataSource) {
            JdbcDataSource jdbcDataSource = (JdbcDataSource) datasource;

            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setDriverClassName(jdbcDataSource.getDriverClass());
            hikariConfig.setJdbcUrl(jdbcDataSource.getUrl());
            if (jdbcDataSource.getUser() != null) {
                hikariConfig.setUsername(jdbcDataSource.getUser());
            }
            if (jdbcDataSource.getPassword() != null) {

                hikariConfig.setPassword(jdbcDataSource.getPassword());
            }
            if (jdbcDataSource.getMaximumPoolSize()<=0) {
                hikariConfig.setMaximumPoolSize(10);
            } else {
                hikariConfig.setMaximumPoolSize(jdbcDataSource.getMaximumPoolSize());
            }
            if (jdbcDataSource.getConnectionTestQuery() != null) {
                hikariConfig.setConnectionTestQuery(jdbcDataSource.getConnectionTestQuery());
            }
            hikariConfig.setPoolName(datasourceName);
            return new HikariDataSource(hikariConfig);
        } else {
            throw new IllegalArgumentException(String.format("jdbc datasource %s not defined", datasourceName));
        }
    }

    private MongoDatabaseFactory loadMongoDatabaseFactory(String datasourceName) {
        Datasource datasource = getDatasource(datasourceName);
        if (!(datasource instanceof MongoDataSource)) {
            throw new IllegalArgumentException(String.format("mongo datasource %s not defined", datasourceName));
        }
        MongoDataSource mongoDataSource = (MongoDataSource) datasource;
        return new SimpleMongoClientDatabaseFactory(mongoDataSource.getUrl());
    }

    private Datasource getDatasource(String datasourceName) {
        return this.nameToDataSource.get(datasourceName);
    }


}
