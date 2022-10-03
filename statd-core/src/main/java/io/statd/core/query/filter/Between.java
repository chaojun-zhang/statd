package io.statd.core.query.filter;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.statd.core.exception.QueryRequestError;
import io.statd.core.exception.StatdException;
import io.statd.core.query.Query;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
@NoArgsConstructor
public final class Between implements NameFilter {
    private String field;
    private Object startInclusive;
    private Object endExclusive;

    public Between(String field, Object startInclusive, Object endExclusive) {
        this.field = Objects.requireNonNull(field);
        this.startInclusive = Objects.requireNonNull(startInclusive);
        this.endExclusive = Objects.requireNonNull(endExclusive);

    }


    @Override
    public void validate(Query search) {
        if (field == null) {
            throw new StatdException(QueryRequestError.InvalidFilter, "field is required for between filter");
        }
        if (startInclusive == null) {
            throw new StatdException(QueryRequestError.InvalidFilter, "startInclusive is required between filter, field '" + field + "'");
        }
        if (endExclusive == null) {
            throw new StatdException(QueryRequestError.InvalidFilter, "endExclusive is required between filter, field '" + field + "'");
        }
    }

    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
        return visitor.visit(this);
    }


}
