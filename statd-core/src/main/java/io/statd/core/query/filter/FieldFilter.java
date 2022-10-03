package io.statd.core.query.filter;


public interface FieldFilter extends NameFilter {

    Object getValue();

    String getField();

}
