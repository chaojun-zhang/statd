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
public final class StringIn implements NameFilter {

    private String field;

    private List<String> values;

    private boolean negate;

    public StringIn(String field, List<String> values) {
        this.field = Objects.requireNonNull(field);
        this.values = Objects.requireNonNull(values);
    }


    public StringIn(String field, List<String> values, boolean negate) {
        this.field = Objects.requireNonNull(field);
        this.values = Objects.requireNonNull(values);
        this.negate = negate;
    }


    @Override
    public void validate(Query search) {
        if (field == null) {
            throw new StatdException(QueryRequestError.InvalidFilter, "field is required for StringIn filter");
        }
        if (values == null || values.isEmpty()) {
            throw new StatdException(QueryRequestError.InvalidFilter, "values is required for StringIn filter, field:'" + field + "'");
        }
    }

    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
        return visitor.visit(this);
    }

}
