package io.statd.core.storage.mongo;


import io.statd.core.query.filter.And;
import io.statd.core.query.filter.Between;
import io.statd.core.query.filter.Contain;
import io.statd.core.query.filter.EndWith;
import io.statd.core.query.filter.EqualTo;
import io.statd.core.query.filter.FilterVisitor;
import io.statd.core.query.filter.GreaterAndEualTo;
import io.statd.core.query.filter.GreaterThan;
import io.statd.core.query.filter.LessAndEqualTo;
import io.statd.core.query.filter.LessThan;
import io.statd.core.query.filter.Matcher;
import io.statd.core.query.filter.Not;
import io.statd.core.query.filter.NotEqualTo;
import io.statd.core.query.filter.NumberIn;
import io.statd.core.query.filter.Or;
import io.statd.core.query.filter.StartWith;
import io.statd.core.query.filter.StringIn;
import org.springframework.data.mongodb.core.query.Criteria;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class MongoCriteriaVisitor implements FilterVisitor<Criteria> {

    @Override
    public Criteria visit(And filter) {
        Criteria left = filter.getLeft().accept(this);
        Criteria right = filter.getRight().accept(this);
        return new Criteria().andOperator(left, right);
    }

    @Override
    public Criteria visit(Or filter) {
        Criteria left = filter.getLeft().accept(this);
        Criteria right = filter.getRight().accept(this);
        return new Criteria().orOperator(left, right);

    }

    @Override
    public Criteria visit(Between filter) {
        return where(filter.getField()).gte(filter.getStartInclusive()).lt(filter.getEndExclusive());
    }

    @Override
    public Criteria visit(Contain filter) {
        return where(filter.getField()).regex(filter.getValue(), "i");
    }

    @Override
    public Criteria visit(EndWith filter) {
        return where(filter.getField()).regex(filter.getValue() + "$", "i");
    }

    @Override
    public Criteria visit(EqualTo filter) {
        return where(filter.getField()).is(filter.getValue());

    }

    @Override
    public Criteria visit(GreaterAndEualTo filter) {
        return where(filter.getField()).gte(filter.getValue());
    }

    @Override
    public Criteria visit(GreaterThan filter) {
        return where(filter.getField()).gt(filter.getValue());
    }

    @Override
    public Criteria visit(LessAndEqualTo filter) {
        return where(filter.getField()).lte(filter.getValue());
    }

    @Override
    public Criteria visit(LessThan filter) {
        return where(filter.getField()).lt(filter.getValue());
    }

    @Override
    public Criteria visit(Matcher filter) {
        return where(filter.getField()).regex(filter.getPattern());
    }

    @Override
    public Criteria visit(Not filter) {
        Criteria left = filter.getChild().accept(this);
        return left.not();
    }

    @Override
    public Criteria visit(NotEqualTo filter) {
        return where(filter.getField()).ne(filter.getValue());
    }

    @Override
    public Criteria visit(NumberIn filter) {
        if (filter.isNegate()) {
            return where(filter.getField()).nin(filter.getValues());
        } else {
            return where(filter.getField()).in(filter.getValues());
        }
    }

    @Override
    public Criteria visit(StartWith filter) {
        return where(filter.getField()).regex("^" + filter.getValue(), "i");
    }

    @Override
    public Criteria visit(StringIn filter) {
        if (filter.isNegate()) {
            return where(filter.getField()).nin(filter.getValues());
        } else {
            return where(filter.getField()).in(filter.getValues());
        }

    }
}
