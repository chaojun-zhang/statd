package io.statd.core.query.filter;

import io.statd.core.exception.QueryRequestError;
import io.statd.core.exception.StatdException;
import io.statd.core.query.Query;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

@Data
@NoArgsConstructor
public final class Matcher implements NameFilter {

    private String field;

    private String pattern;

    public Matcher(String dimension, String pattern) {
        this.field = Objects.requireNonNull(dimension);
        this.pattern = Objects.requireNonNull(pattern);
    }


    @Override
    public void validate(Query search) {
        if (field == null) {
            throw new StatdException(QueryRequestError.InvalidFilter, "field is required for Matcher filter");
        }
        if (pattern == null) {
            throw new StatdException(QueryRequestError.InvalidFilter, "pattern is required for Matcher filter, field:'" + field + "'");
        }
    }

    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
       return visitor.visit(this);
    }


}
