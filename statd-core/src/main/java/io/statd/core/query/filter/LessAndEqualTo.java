package io.statd.core.query.filter;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.statd.core.exception.StatdException;
import io.statd.core.query.Query;
import io.statd.core.exception.QueryRequestError;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
@NoArgsConstructor
public final class LessAndEqualTo implements FieldFilter {
    private String field;
    private Object value;

    public LessAndEqualTo(String field, Object value) {
        this.field = Objects.requireNonNull(field);
        this.value = Objects.requireNonNull(value);

    }

    @Override
    public void validate(Query search) {
        if (field == null) {
            throw new StatdException(QueryRequestError.InvalidFilter, "field is required for LessAndEqualTo filter");
        }
        if (value == null) {
            throw new StatdException(QueryRequestError.InvalidFilter, "value is required for LessAndEqualTo filter, field:'" + field + "'");
        }
    }

    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
