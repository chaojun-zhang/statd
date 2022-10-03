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
public final class StartWith implements LikeFilter {
    private String field;
    private String value;

    public StartWith(String field, String value) {
        this.field = Objects.requireNonNull(field);
        this.value = Objects.requireNonNull(value);

    }

    @Override
    public void validate(Query search) {
        if (field == null) {
            throw new StatdException(QueryRequestError.InvalidFilter, "field is required for StartWith filter");
        }
        if (value == null || value.length() == 0) {
            throw new StatdException(QueryRequestError.InvalidFilter, "value is required for StartWith filter, field:'" + field + "'");
        }
    }


    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
