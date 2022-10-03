package io.statd.core.query.filter;

public interface LogicalFilter extends Filter {

    Filter getLeft();

    Filter getRight();
}
