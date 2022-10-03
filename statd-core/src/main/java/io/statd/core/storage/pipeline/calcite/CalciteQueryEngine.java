package io.statd.core.storage.pipeline.calcite;

import com.nhl.dflib.DataFrame;
import io.statd.core.dataframe.Catalog;
import io.statd.core.dataframe.Table;
import io.statd.core.query.Query;
import io.statd.core.storage.config.PipelineStorage;
import io.statd.core.storage.jdbc.JdbcUtil;
import io.statd.core.storage.template.TemplateUtil;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

public class CalciteQueryEngine implements Closeable {

    private final Query query;
    private final CalciteConnection calciteConnection;
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final PipelineStorage pipelineStorage;
    private final Catalog catalog;


    public CalciteQueryEngine(PipelineStorage pipelineStorage, Catalog catalog, Query query) {
        this.pipelineStorage = pipelineStorage;
        this.catalog = catalog;
        this.query = query;
        final Properties properties = new Properties();
        properties.setProperty("caseSensitive", "false");
        properties.setProperty("unquotedCasing", "UNCHANGED");
        try {
            Connection connection = DriverManager.getConnection("jdbc:calcite:parserFactory=org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl#FACTORY", properties);
            this.calciteConnection = connection.unwrap(CalciteConnection.class);
            SingleConnectionDataSource dataSource = new SingleConnectionDataSource(connection, false);
            jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
            catalog.getNameToTable().forEach(this::registerTable);
            catalog.getFunctions().forEach((name, clazz) -> {
                try {
                    calciteConnection.getRootSchema().add(name, ScalarFunctionImpl.create(Class.forName(clazz), name));

                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(" function register failed, '" + name + "'", e);
                }
            });
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void registerTable(String name, Table table) {
        calciteConnection.getRootSchema().add(name, new DataFrameScannalbeTable(table.getDataFrame()));
    }

    public void execute(String transform) {
        String[] statements = transform.split(";");
        for (String statement : statements) {
            if (StringUtils.isNotBlank(statement)) {
                String sql = TemplateUtil.render(statement, query.templateParams());
                jdbcTemplate.getJdbcTemplate().execute(sql);
            }
        }

        //把需要输出的output查出来，注册到catalog，方便最后输出
        for (PipelineStorage.Output output : pipelineStorage.getOutput()) {
            final SqlRowSet sqlRowSet = jdbcTemplate.queryForRowSet("select * from " + output.getDataFrameName(), new HashMap<>());
            DataFrame dataFrame = JdbcUtil.toDataFrame(sqlRowSet, null);
            catalog.registerTable(output.getDataFrameName(), Table.create(dataFrame));
        }
    }

    @Override
    public void close() throws IOException {
        try {
            calciteConnection.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
