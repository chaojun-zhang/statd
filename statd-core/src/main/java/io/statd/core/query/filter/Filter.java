package io.statd.core.query.filter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.statd.core.query.Query;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = EqualTo.class, name = "eq"),
        @JsonSubTypes.Type(value = NotEqualTo.class, name = "ne"),
        @JsonSubTypes.Type(value = LessThan.class, name = "lt"),
        @JsonSubTypes.Type(value = Between.class, name = "between"),
        @JsonSubTypes.Type(value = LessAndEqualTo.class, name = "lte"),
        @JsonSubTypes.Type(value = GreaterThan.class, name = "gt"),
        @JsonSubTypes.Type(value = GreaterAndEualTo.class, name = "gte"),
        @JsonSubTypes.Type(value = StartWith.class, name = "startWith"),
        @JsonSubTypes.Type(value = EndWith.class, name = "endWith"),
        @JsonSubTypes.Type(value = Matcher.class, name = "match"),
        @JsonSubTypes.Type(value = Contain.class, name = "contain"),
        @JsonSubTypes.Type(value = And.class, name = "and"),
        @JsonSubTypes.Type(value = Or.class, name = "or"),
        @JsonSubTypes.Type(value = Not.class, name = "not"),
        @JsonSubTypes.Type(value = StringIn.class, name = "in"),
        @JsonSubTypes.Type(value = NumberIn.class, name = "numberIn")}
)
public interface Filter {

    default Filter and(Filter filter) {
        return new And(this, filter);
    }

    default Filter or(Filter filter) {
        return new Or(this, filter);
    }

    default Filter not() {
        return new Not(this);
    }

    void validate(Query search);


    static Filter startWith(String field, String value) {
        return new StartWith(field, value);
    }

    static Filter endWith(String field, String value) {
        return new EndWith(field, value);
    }

    static Filter contains(String field, String value) {
        return new Contain(field, value);
    }

    static Filter eq(String field, Object value) {
        return new EqualTo(field, value);
    }

    static Filter ne(String field, Object value) {
        return new NotEqualTo(field, value);
    }

    static Filter $and(Filter left, Filter right) {
        return new And(left, right);
    }

    static Filter $or(Filter left, Filter right) {
        return new Or(left, right);
    }
    static Filter $not(Filter filter) {
        return new Not(filter);
    }

    static Filter gt(String field, Object value) {
        return new GreaterThan(field, value);
    }

    static Filter lt(String field, Object value) {
        return new LessThan(field, value);
    }

    static Filter gte(String field, Object value) {
        return new GreaterAndEualTo(field, value);
    }

    static Filter lte(String field, Object value) {
        return new LessAndEqualTo(field, value);
    }

    static Filter match(String field, String value) {
        return new Matcher(field, value);
    }

    static StringIn stringIn(String field, List<String> values) {
        return new StringIn(field, values, false);
    }

    static StringIn notStringIn(String field, List<String> values) {
        return new StringIn(field, values, true);
    }

    static NumberIn numberIn(String field, List<Number> values) {
        return new NumberIn(field, values, false);
    }

    static NumberIn notNumberIn(String field, List<Number> values) {
        return new NumberIn(field, values, true);
    }

    <T> T accept(FilterVisitor<T> visitor);
}
