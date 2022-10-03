package io.statd.core.storage.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.statd.core.storage.config.compact.CompactConfig;
import io.statd.core.storage.jdbc.CompactJdbcReader;
import io.statd.core.storage.jdbc.CompactMysqlDialect;
import io.statd.core.storage.jdbc.DefaultJdbcReader;
import io.statd.core.storage.jdbc.DruidDialect;
import io.statd.core.storage.jdbc.MysqlDialect;
import io.statd.core.storage.jdbc.SqlDialect;
import io.statd.core.storage.jdbc.StarrockDialect;
import io.statd.core.storage.jdbc.TrinoDialect;
import lombok.Data;

import java.util.Objects;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JdbcStorage implements SourceStorage {
    private String datasource;

    //eventTime时间字段
    protected String eventTimeField;

    //eventTime的字段类型
    private String timeType;

    private JdbcStorageType storageType;

    //底层存储的实际时间使用什么进行存储的，如果是用中国时区存储，这个值必须为8
    private int zoneOffsetHour;
    //查询的sql语句
    protected String sql;

    //数据库底层存储时间的时区
    private String timezone;

    private CompactConfig compact;

    //最大的查询天数
    private int maxQueryDays = 7;

    @JsonIgnore
    @Override
    public Class getReaderClass() {
        if (isCompact()) {
            return CompactJdbcReader.class;
        } else {
            return DefaultJdbcReader.class;
        }
    }

    @Override
    public void validate() {
        Objects.requireNonNull(datasource, "datasource not provided");
        Objects.requireNonNull(sql, "sql not provided");
        Objects.requireNonNull(storageType, "storageType not provided");

    }

    public String getTimezone() {
        //druid默认用utc
        if (JdbcStorageType.Druid == storageType && timezone == null) {
            return "UTC";
        }
        return timezone;
    }

    public SqlDialect dialect() {
        if (storageType == JdbcStorageType.MySql) {
            if (isCompact()) {
                return new CompactMysqlDialect(this);
            } else {
                return new MysqlDialect(this);
            }
        } else if (storageType == JdbcStorageType.Trino) {
            return new TrinoDialect(this);
        } else if (storageType == JdbcStorageType.Starrocks) {
            return new StarrockDialect(this);
        } else {
            return new DruidDialect(this);
        }
    }

    public boolean isCompact() {
        return compact != null;
    }

    public boolean disablePreparedStatement() {
        return false;
    }
}