package io.statd.core.query.filter;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.statd.core.exception.QueryRequestError;
import io.statd.core.exception.StatdException;
import io.statd.core.query.Query;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
@NoArgsConstructor
public final class And implements LogicalFilter {

    private Filter left;
    private Filter right;

    public And(Filter left,Filter right) {
        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
    }

    @Override
    public void validate(Query search) {
        if (left == null) {
            throw new StatdException(QueryRequestError.InvalidFilter, "left is required for And filter");
        }
        if (right == null ) {
            throw new StatdException(QueryRequestError.InvalidFilter, "right is required for And filter");
        }
    }

    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
        return visitor.visit(this);
    }


}
