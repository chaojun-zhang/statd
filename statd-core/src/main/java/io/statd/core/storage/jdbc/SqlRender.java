package io.statd.core.storage.jdbc;

import io.statd.core.query.Query;
import io.statd.core.storage.config.JdbcStorage;
import io.statd.core.storage.template.TemplateUtil;
import lombok.Getter;

import java.util.Map;

public final class SqlRender {

    private final SqlDialect sqlDialect;
    private final Query query;
    private final JdbcStorage storage;
    @Getter
    private final String viewPrefix;

    public SqlRender(Query query, JdbcStorage storage) {
        this.sqlDialect = storage.dialect();
        this.query = query;
        this.storage = storage;
        this.viewPrefix = String.format("v_%s_", query.getQueryId());
    }


    public String sql() {
        Map<String, Object> params = query.templateParams();
        params.put("view", viewPrefix);
        params.put("timeRange", sqlDialect.timeRange(query.getFrom(), query.getTo()));
        sqlDialect.timePeriod(query.getGranularity()).ifPresent(p -> params.put("period", p));
        return TemplateUtil.render(storage.getSql(), params);
    }

    public static SqlRender create(Query query, JdbcStorage storage) {
        return new SqlRender(query, storage);
    }

}
