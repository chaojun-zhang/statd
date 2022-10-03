package io.statd.core.parser.filter;

import io.statd.core.parser.CompileException;
import io.statd.query.ExpBaseVisitor;
import io.statd.query.ExpParser;
import io.statd.core.query.filter.Filter;
import io.statd.core.query.filter.Matcher;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

public class FilterExpVisitor extends ExpBaseVisitor<Object> {


    @Override
    public Filter visitPredicateAndOrExpr(ExpParser.PredicateAndOrExprContext ctx) {
        Filter leftCond = (Filter) this.visit(ctx.predicate(0));
        Filter rightCond = (Filter) this.visit(ctx.predicate(1));
        if (ctx.AND()!=null) {
            return leftCond.and(rightCond);
        } else {
            return leftCond.or(rightCond);
        }
    }

    @Override
    public Filter visitNestedPredicateExpr(ExpParser.NestedPredicateExprContext ctx) {
        Filter condition = (Filter) this.visit(ctx.predicate());

        if (ctx.NEGATE() != null) {
            return condition.not();
        }
        return condition;
    }

    @Override
    public Filter visitNestedFieldPredicate(ExpParser.NestedFieldPredicateContext ctx) {
        Filter condition = (Filter) this.visit(ctx.fieldPredicate());
        if (ctx.NEGATE() != null) {
            return condition.not();
        }
        return condition;
    }

    @Override
    public Long visitLongLiteral(ExpParser.LongLiteralContext ctx) {
        return Long.valueOf(ctx.getText());
    }

    @Override
    public Double visitDoubleLiteral(ExpParser.DoubleLiteralContext ctx) {
        return Double.valueOf(ctx.getText());
    }

    @Override
    public String visitStringLiteral(ExpParser.StringLiteralContext ctx) {
        //字符串需要去除前后缀
        String stringElement = ctx.stringElement().getText();
        return StringUtils.substring(stringElement, 1, stringElement.length() - 1);
    }

    @Override
    public String visitColumn(ExpParser.ColumnContext ctx) {
        return ctx.IDENTIFIER().getText();
    }

    @Override
    public Filter visitRegFieldPredicate(ExpParser.RegFieldPredicateContext ctx) {
        Object columnExpr = this.visit(ctx.expr());
        if (columnExpr instanceof String) {
            String dimension = columnExpr.toString();
            String regex = ctx.REGEX().getText();
            if (!regex.startsWith("/") || !regex.endsWith("/")) {
                throw new CompileException(String.format("wrong pattern format: %s", regex));
            }
            String pattern = regex.substring(1, regex.length() - 1);
            return new Matcher(dimension, pattern);
        } else {
            throw new CompileException("unsupported filter, ctx: " + ctx.getText());
        }
    }

    @Override
    public Filter visitStrInFieldPredicate(ExpParser.StrInFieldPredicateContext ctx) {
        Object columnExpr = this.visit(ctx.expr());
        if (columnExpr instanceof String) {
            String colName = columnExpr.toString();
            List<String> stringArray = ctx.stringArray().stringElement().stream()
                    .map(ParseTree::getText)
                    .map(it ->{
                        if (it.startsWith("'") || it.startsWith("\"")) {
                            return it.substring(1, it.length() - 1);
                        } else {
                            return it;
                        }
                    } )
                    .collect(Collectors.toList());
            if (ctx.NOT() != null) {
                return Filter.notStringIn(colName, stringArray);
            } else {
                return Filter.stringIn(colName, stringArray);
            }
        } else {
            throw new CompileException("unsupported filter, ctx: " + ctx.getText());
        }
    }

    @Override
    public Object visitLongInFieldPredicate(ExpParser.LongInFieldPredicateContext ctx) {
        Object columnExpr = this.visit(ctx.expr());
        if (columnExpr instanceof String) {
            String colName = columnExpr.toString();
            List<Number> longArray = ctx.longArray().longElement().stream()
                    .map(ParseTree::getText)
                    .map(Long::parseLong)
                    .collect(Collectors.toList());
            if (ctx.NOT() != null) {
                return Filter.notNumberIn(colName, longArray);
            } else {
                return Filter.numberIn(colName, longArray);
            }
        } else {
            throw new CompileException("unsupported filter, ctx: " + ctx.getText());
        }
    }

    @Override
    public Filter visitOpFieldPredicate(ExpParser.OpFieldPredicateContext ctx) {
        Object columnExpr = this.visit(ctx.expr(0));
        Object value = this.visit(ctx.expr(1));
        if (columnExpr instanceof String) {
            String colName = columnExpr.toString();
            if (ctx.GT() != null) {
                return Filter.gt(colName, value);
            } else if (ctx.GEQ() != null) {
                return Filter.gte(colName, value);
            } else if (ctx.LT() != null) {
                return Filter.lt(colName, value);
            } else if (ctx.LEQ() != null) {
                return Filter.lte(colName, value);
            } else if (ctx.EQ() != null) {
                return Filter.eq(colName, value);
            } else if (ctx.NEQ() != null) {
                return Filter.ne(colName, value);
            } else if (ctx.LIKE() != null && value instanceof String) {
                boolean startWith = value.toString().startsWith("%");
                boolean endWith = value.toString().endsWith("%");
                if (startWith && endWith) {
                    return Filter.contains(colName, value.toString().substring(1, value.toString().length() - 1));
                } else if (startWith) {
                    return Filter.endWith(colName, value.toString().substring(1));
                } else if (endWith) {
                    return Filter.startWith(colName, value.toString().substring(0, value.toString().length() - 1));
                }
            }
        }
        throw new CompileException("unsupported filter, ctx: " + ctx.getText());
    }

}