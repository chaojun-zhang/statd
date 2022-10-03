package io.statd.core.query.filter;

public interface FilterVisitor<T> {

    T visit(And filter);

    T visit(Or filter);

    T visit(Between filter);

    T visit(Contain filter);

    T visit(EndWith filter);

    T visit(EqualTo filter);

    T visit(GreaterAndEualTo filter);

    T visit(GreaterThan filter);

    T visit(LessAndEqualTo filter);

    T visit(LessThan filter);

    T visit(Matcher filter);

    T visit(Not filter);

    T visit(NotEqualTo filter);

    T visit(NumberIn filter);

    T visit(StartWith filter);

    T visit(StringIn filter);

}
