package io.statd.core.query.filter;

import io.statd.core.exception.QueryRequestError;
import io.statd.core.exception.StatdException;
import io.statd.core.query.Query;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
public final class Or implements LogicalFilter {

    private Filter left;
    private Filter right;

    public Or(Filter left,Filter right) {
        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
    }

    @Override
    public void validate(Query search) {
        if (left == null) {
            throw new StatdException(QueryRequestError.InvalidFilter, "left is required for Or filter");
        }
        if (right == null ) {
            throw new StatdException(QueryRequestError.InvalidFilter, "right is required for Or filter");
        }
    }

    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
       return visitor.visit(this);
    }
}
