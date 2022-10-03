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
public final class EndWith implements LikeFilter {
    private String field;
    private String value;

    public EndWith(String field, String value) {
        this.field = Objects.requireNonNull(field);
        this.value = Objects.requireNonNull(value);

    }

    @Override
    public void validate(Query search) {
        if (field == null) {
            throw new StatdException(QueryRequestError.InvalidFilter, "field is required for EndWith filter");
        }
        if (value == null) {
            throw new StatdException(QueryRequestError.InvalidFilter, "value is required for EndWith filter, field:'" + field + "'");
        }
    }

    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
