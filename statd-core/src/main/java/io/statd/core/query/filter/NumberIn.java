package io.statd.core.query.filter;

import io.statd.core.exception.QueryRequestError;
import io.statd.core.exception.StatdException;
import io.statd.core.query.Query;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Objects;

@Data
@NoArgsConstructor
public final class NumberIn implements NameFilter {

    private String field;

    private List<Number> values;

    private boolean negate;

    public NumberIn(String field, List<Number> values) {
        this.field = Objects.requireNonNull(field);
        this.values = Objects.requireNonNull(values);
    }

    public NumberIn(String field, List<Number> values, boolean negative) {
        this.field = Objects.requireNonNull(field);
        this.values = Objects.requireNonNull(values);
        this.negate = negative;
    }

    @Override
    public void validate(Query search) {
        if (field == null) {
            throw new StatdException(QueryRequestError.InvalidFilter, "field is required for NumberIn filter");
        }
        if (values == null || values.isEmpty()) {
            throw new StatdException(QueryRequestError.InvalidFilter, "values is required for NumberIn filter, field:'" + field + "'");
        }
    }

    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
