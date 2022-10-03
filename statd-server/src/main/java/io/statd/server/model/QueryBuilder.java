package io.statd.server.model;

import io.statd.core.exception.QueryRequestError;
import io.statd.core.exception.StatdException;
import io.statd.core.query.Granularity;
import io.statd.core.query.Query;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.statd.core.exception.QueryRequestError.*;

@Slf4j
public class QueryBuilder {

    private final static DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final static DateTimeFormatter DATE_TIME_FORMAT_MINUTE = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    private final Map<String, Object> requestParams;

    private final Map<String,String> headers;

    QueryBuilder(Map<String, Object> requestParams,
                 Map<String, String> headers) {
        this.requestParams = requestParams;
        this.headers = headers;

    }

    public static Query build(Map<String, Object> requestParams,Map<String,String> headers) {
        return new QueryBuilder(requestParams, headers).parse();
    }

    public static Query build(Map<String, Object> requestParams) {
        return new QueryBuilder(requestParams, new HashMap<>()).parse();
    }

    public Query parse() {
        Query query = new Query();
        for (Map.Entry<String, Object> param : requestParams.entrySet()) {
            String paramKey = param.getKey();
            Object paramValue = param.getValue();
            if (paramValue == null) {
                throw new StatdException(InvalidQueryParams, "param '" + paramKey + "' should not be null");
            }
            switch (paramKey) {
                case "from":
                    query.setFrom(parseDateTime(paramValue));
                    break;
                case "to":
                    query.setTo(parseDateTime(paramValue));
                    break;
                case "g":
                    query.setGranularity(parseGranularity(paramValue));
                    break;
                case "dimension":
                    query.setDimensions(parseDimension(paramValue));
                    break;
                case "metric":
                    query.setMetrics(parseMetric(paramValue));
                    break;
                default:
                    query.getQueryParams().put(paramKey, parseQueryParam(paramKey, paramValue));
                    break;
            }
        }

        if (query.getFrom() == null || query.getTo() == null) {
            throw new StatdException(InvalidInterval, "'from' and 'to' are both required");
        }

        return query;
    }

    private Object parseQueryParam(String paramKey, Object paramValue) {
        if (paramValue instanceof String
                || Number.class.isAssignableFrom(paramValue.getClass())
                || paramValue instanceof Collection) {
            if (paramValue instanceof Collection) {
                Collection collectionParam = (Collection) paramValue;
                if (!collectionParam.isEmpty()) {
                    boolean isString = collectionParam.stream().allMatch(it -> it != null && it.getClass() == String.class);
                    boolean isNumber = collectionParam.stream().allMatch(it -> it != null && Number.class.isAssignableFrom(it.getClass()));
                    if (!isString && !isNumber) {
                        throw new StatdException(InvalidQueryParams, "array value for '" + paramKey + "' must have same type");
                    }
                } else {
                    throw new StatdException(InvalidQueryParams, "value for '" + paramKey + "' should not be an empty array");
                }
            }
            return paramValue;
        }
        throw new StatdException(InvalidFilter, "value for'" + paramKey + "' can only be one of [string, number, string array, number array]");
    }

    private List<String> parseMetric(Object paramValue) {
        if (paramValue instanceof ArrayList || paramValue instanceof String[]) {
            return (List<String>) paramValue;
        } else if (paramValue instanceof String) {
            String[] metrics = paramValue.toString().split("\\s*,\\s*");
            return Arrays.asList(metrics).stream().map(String::trim).collect(Collectors.toList());
        } else {
            throw new StatdException(InvalidQueryParams, "select should be an array of string or a string separated by ','");
        }
    }

    private Granularity parseGranularity(Object paramValue) {
        try {
            return Granularity.from(paramValue.toString());
        } catch (Exception e) {
            throw new StatdException(QueryRequestError.InvalidGranule, "g should be one of value in [5min,hour,day,week,month,quarter,year,all]", e);
        }
    }

    private List<String> parseDimension(Object paramValue) {
        if (paramValue instanceof ArrayList) {
            return (List<String>) paramValue;
        } else if (paramValue instanceof String) {
            String[] dimensions = ((String) paramValue).split("\\s*,\\s*");
            return Arrays.stream(dimensions).map(String::trim).collect(Collectors.toList());
        } else {
            throw new StatdException(InvalidQueryParams, "dimension must be either a string array or a string separated by ','");
        }
    }

    private LocalDateTime parseDateTime(Object value) {
        LocalDateTime result = null;
        if (value instanceof String) {
            List<DateTimeFormatter> formatters = Arrays.asList(DATE_TIME_FORMAT_MINUTE, DATE_TIME_FORMAT);
            for (DateTimeFormatter formatter : formatters) {
                if (result == null) {
                    try {
                        result = LocalDateTime.parse(((String) value).trim(), formatter);
                    } catch (Exception e) {
                        //try to use  next formatter to parse
                    }
                } else {
                    break;
                }
            }
        }
        if (result == null) {
            throw new StatdException(InvalidTimeFormat, "from/to must be a dateTime like '2000-01-01 01:01'");
        }
        return result;
    }




}
