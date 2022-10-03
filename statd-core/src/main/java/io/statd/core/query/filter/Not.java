package io.statd.core.query.filter;

import io.statd.core.exception.QueryRequestError;
import io.statd.core.exception.StatdException;
import io.statd.core.query.Query;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
@NoArgsConstructor
public final class Not implements Filter {

    private Filter child;

    public Not(Filter child) {
        this.child = Objects.requireNonNull(child);
    }


    @Override
    public void validate(Query search) {
        if (child == null) {
            throw new StatdException(QueryRequestError.InvalidFilter, "child is required for Not filter");
        }

    }

    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
