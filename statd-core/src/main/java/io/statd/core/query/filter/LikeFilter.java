package io.statd.core.query.filter;

public interface LikeFilter extends NameFilter {

    String getField();
    String getValue();
}
