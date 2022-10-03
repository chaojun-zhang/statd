package io.statd.core.query;

import io.statd.core.exception.QueryRequestError;
import io.statd.core.exception.StatdException;
import io.statd.core.query.filter.And;
import io.statd.core.query.filter.Between;
import io.statd.core.query.filter.Filter;
import io.statd.core.query.filter.NumberIn;
import io.statd.core.query.filter.Or;
import io.statd.core.query.filter.StringIn;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FilterBuilder {

    private final Map<String,Object> params;

    public FilterBuilder(Map<String, Object> params) {
        this.params = Objects.requireNonNull(params);
    }

    private Filter parseFilter(String fieldName, Object fieldValue) {
        if (fieldValue == null) {
            throw new StatdException(QueryRequestError.InvalidQueryParams, "value is required for field '" + fieldName + "'");
        }
        if (fieldValue instanceof Collection) {
            return parseFilter(fieldName, fieldValue, "in");
        } else if (fieldValue instanceof String || fieldValue instanceof Number) { //字符串或者是数字过滤
            return parseFilter(fieldName, fieldValue, "eq");
        } else if (fieldValue instanceof Map) { //复杂类型的过滤
            Map valueMap = (Map) fieldValue;
            if ("and".equals(fieldName) || "or".equals(fieldName)) {
                return parseFilter(fieldName, fieldValue, fieldName);
            } else {
                return parseFilter(fieldName, valueMap.get("value"), (String) valueMap.get("type"));
            }
        } else {
            throw new StatdException(QueryRequestError.InvalidQueryParams, "invalid filter for field '" + fieldName + "', value must be one of json type in [string, number, collection, map]");
        }
    }

    public Filter build() {
        List<Filter> filters = new ArrayList<>();
        for (Map.Entry<String, Object> nameToValue : params.entrySet()) {
            String fieldName = nameToValue.getKey();
            Object fieldValue = nameToValue.getValue();
            filters.add(parseFilter(fieldName, fieldValue));
        }
        return filters.stream().reduce(Filter::and).orElse(null);
    }


    private Filter parseFilter(String fieldName, Object fieldValue, String filterType) {
        if (fieldValue == null) {
            throw new StatdException(QueryRequestError.InvalidQueryParams, "value is required for field '" + fieldName + "'");
        }
        if (StringUtils.isEmpty(filterType)) {
            throw new StatdException(QueryRequestError.InvalidQueryParams, "type is required for field '" + fieldName + "'");
        }
        if (fieldValue instanceof String && StringUtils.isEmpty((String) fieldValue)) {
            throw new StatdException(QueryRequestError.InvalidQueryParams, "value cannot be empty for field '" + fieldName + "'");
        }

        Objects.requireNonNull(filterType);
        switch (filterType) {
            case "and":
            case "or":
                if (fieldValue instanceof Map) {
                    Map<String, Object> andFilter = (Map<String, Object>) fieldValue;
                    if (andFilter.size() == 2) {
                        String[] fields = andFilter.keySet().toArray(new String[0]);
                        Object[] values = andFilter.values().toArray();
                        Filter left = parseFilter(fields[0], values[0]);
                        Filter right = parseFilter(fields[1], values[1]);
                        if ("and".equals(filterType)) {
                            return new And(left, right);
                        } else {
                            return new Or(left, right);
                        }
                    }
                }
                throw new StatdException(QueryRequestError.InvalidQueryParams, "$and filter must be a json object");
            case "eq":
            case "=":
                if (fieldValue instanceof String || fieldValue instanceof Number) {
                    return Filter.eq(fieldName, fieldValue);
                } else {
                    throw new StatdException(QueryRequestError.InvalidQueryParams, "value for '" + fieldName + "' must be a string or a number");
                }
            case "ne":
            case "<>":
            case "!=":
                if (fieldValue instanceof String || fieldValue instanceof Number) {
                    return Filter.ne(fieldName, fieldValue);
                } else {
                    throw new StatdException(QueryRequestError.InvalidQueryParams, "value for '" + fieldName + "' must be a string or a number");
                }
            case "gt":
            case ">":
                if (fieldValue instanceof String || fieldValue instanceof Number) {
                    return Filter.gt(fieldName, fieldValue);
                } else {
                    throw new StatdException(QueryRequestError.InvalidQueryParams, "value for '" + fieldName + "' must be a string or a number");
                }
            case "gte":
            case ">=":
                if (fieldValue instanceof String || fieldValue instanceof Number) {
                    return Filter.gte(fieldName, fieldValue);
                } else {
                    throw new StatdException(QueryRequestError.InvalidQueryParams, "value for '" + fieldName + "' must be a string or a number");
                }
            case "lt":
            case "<":
                if (fieldValue instanceof String || fieldValue instanceof Number) {
                    return Filter.lt(fieldName, fieldValue);
                } else {
                    throw new StatdException(QueryRequestError.InvalidQueryParams, "value for '" + fieldName + "' must be a string or a number");
                }
            case "lte":
            case "<=":
                if (fieldValue instanceof String || fieldValue instanceof Number) {
                    return Filter.lte(fieldName, fieldValue);
                } else {
                    throw new StatdException(QueryRequestError.InvalidQueryParams, "value for '" + fieldName + "' must be a string or a number");
                }
            case "startsWith":
                if (fieldValue instanceof String) {
                    return Filter.startWith(fieldName, (String) fieldValue);
                } else {
                    throw new StatdException(QueryRequestError.InvalidQueryParams, "value for '" + fieldName + "' must be a string");
                }
            case "endsWith":
                if (fieldValue instanceof String) {
                    return Filter.endWith(fieldName, (String) fieldValue);
                } else {
                    throw new StatdException(QueryRequestError.InvalidQueryParams, "value for '" + fieldName + "' must be a string");
                }
            case "contains":
                if (fieldValue instanceof String) {
                    return Filter.contains(fieldName, (String) fieldValue);
                } else {
                    throw new StatdException(QueryRequestError.InvalidQueryParams, "value for '" + fieldName + "' must be a string");
                }
            case "match":
            case "reg":
                if (fieldValue instanceof String) {
                    return Filter.match(fieldName, (String) fieldValue);
                } else {
                    throw new StatdException(QueryRequestError.InvalidQueryParams, "value for '" + fieldName + "' must be a regex string");
                }
            case "between":
                if (fieldValue instanceof List) {
                    List collectionValue = (List) fieldValue;
                    if (collectionValue.size() == 2) {
                        if (collectionValue.get(0) instanceof String && collectionValue.get(1) instanceof String) {
                            return new Between(fieldName, collectionValue.get(0), collectionValue.get(1));
                        } else if (collectionValue.get(0) instanceof Number && collectionValue.get(1) instanceof Number) {
                            return new Between(fieldName, collectionValue.get(0), collectionValue.get(1));
                        }
                    }
                }
                throw new StatdException(QueryRequestError.InvalidQueryParams, "value for '" + fieldName + "' should either be an array of string or an array of number");
            case "in":
                if (fieldValue instanceof Collection) {
                    Collection listValue = (Collection) fieldValue;
                    if (!listValue.isEmpty()) {
                        boolean isString = listValue.stream().allMatch(it -> it != null && it.getClass() == String.class);
                        boolean isNumber = listValue.stream().allMatch(it -> it != null && Number.class.isAssignableFrom(it.getClass()));
                        if (isNumber) {
                            return Filter.numberIn(fieldName, (List<Number>) fieldValue);
                        } else if (isString) {
                            return Filter.stringIn(fieldName, (List<String>) fieldValue);
                        }
                    }
                }
                throw new StatdException(QueryRequestError.InvalidQueryParams, "value for '" + fieldName + "' must either be an array of string or an array of number");

            case "notIn":
                if (fieldValue instanceof Collection) {
                    Collection listValue = (Collection) fieldValue;
                    if (!listValue.isEmpty()) {
                        boolean isNumber = listValue.stream().allMatch(it -> it != null && Number.class.isAssignableFrom(it.getClass()));
                        boolean isString = listValue.stream().allMatch(it -> it != null && it.getClass() == String.class);
                        if (isNumber) {
                            NumberIn numberIn = Filter.numberIn(fieldName, (List<Number>) fieldValue);
                            numberIn.setNegate(true);
                            return numberIn;
                        } else if (isString) {
                            StringIn stringIn = Filter.stringIn(fieldName, (List<String>) fieldValue);
                            stringIn.setNegate(true);
                            return stringIn;
                        }
                    }
                }
                throw new StatdException(QueryRequestError.InvalidQueryParams, "value for '" + fieldName + "' must either be an array of string or an array of number");
            default:
                throw new StatdException(QueryRequestError.InvalidFilter, "filter type '" + filterType + "' is not supported");
        }

    }
}
