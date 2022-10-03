package io.statd.server.controller;


import io.statd.core.exception.QueryRequestError;
import io.statd.core.exception.StatdException;
import io.statd.server.metric.MetricQueryService;
import io.statd.server.model.MetricQuery;
import io.statd.server.model.ModuleMetric;
import io.statd.server.model.QueryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.function.Function;

@RestController
@Slf4j
public class MetricQueryController {

    private final MetricQueryService metricService;

    @Autowired
    public MetricQueryController(MetricQueryService metricService) {
        this.metricService = metricService;
    }


    @PostMapping(value = "/v1/stat/{module}/{metric}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResultBody postStat(@PathVariable(name = "module") String module,
                               @PathVariable(name = "metric") String metric,
                               @RequestHeader Map<String, String> headers,
                               @RequestBody Map<String, Object> params) {

        Object queryResult = this.loadMetric(module, metric, headers, params, metricService::load);
        return ResultBody.ok(queryResult);
    }

    @GetMapping(value = "/v1/stat/{module}/{metric}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResultBody getStat(@PathVariable(name = "module") String module,
                              @PathVariable(name = "metric") String metric,
                              @RequestParam Map<String, Object> params,
                              @RequestHeader Map<String, String> headers) {
        return this.postStat(module, metric, headers, params);
    }


    @GetMapping(value = "/v1/export/{module}/{metric}")
    public void export(@PathVariable(name = "module") String module,
                       @PathVariable(name = "metric") String metric,
                       @RequestHeader Map<String, String> headers,
                       @RequestParam Map<String, Object> params, HttpServletResponse response) throws IOException {
        long contentLength = this.loadMetric(module, metric, headers, params, query -> {
            String exportType = (String) params.getOrDefault("export", "csv");
            try {
                return metricService.export(query, response.getOutputStream(), exportType);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        response.setContentLengthLong(contentLength);
        response.setContentType("application/octet-stream");
        response.setStatus(200);
        response.addHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment;fileName=" + module + "_" + metric);
    }

    private <R> R loadMetric(String module,
                             String metric,
                             Map<String, String> headers,
                             Map<String, Object> params,
                             Function<MetricQuery, R> fn) {

        if (module == null) {
            throw new StatdException(QueryRequestError.InvalidQueryModule, "module is missing in url path");
        }
        if (metric == null) {
            throw new StatdException(QueryRequestError.InvalidQueryMetric, "metric is missing in url path");
        }
        MetricQuery metricQuery = new MetricQuery(QueryBuilder.build(params),
                ModuleMetric.of(module, metric));
        log.info("query: {}", metricQuery);
        return fn.apply(metricQuery);
    }

}
