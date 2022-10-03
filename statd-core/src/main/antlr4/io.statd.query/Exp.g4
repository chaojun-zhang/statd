grammar Exp;

compilationUnit : namedExpr | expr | predicateExpr;

namedExpr: expr AS identifier ;

predicateExpr: predicate;

predicate: fieldPredicate                                                  # fieldPredicateExpr
         | predicate (AND |OR ) predicate                                  # predicateAndOrExpr
         | '(' predicate (AND |OR) predicate ')'                           # predicateAndOrExpr
         | NEGATE?  '(' predicate ')'                                      # nestedPredicateExpr
         ;

fieldPredicate:  NEGATE? '(' fieldPredicate ')'                            # nestedFieldPredicate
           | expr NOT? IN stringArray                                      # strInFieldPredicate
           | expr NOT? IN longArray                                        # longInFieldPredicate
           | expr NOT? IN doubleArray                                      # doubleInFieldPredicate
           | expr MATCH REGEX                                              # regFieldPredicate
           | expr (LT|LEQ|GT|GEQ|EQ|NEQ | LIKE) expr                       # opFieldPredicate
           ;

expr : expr (MUL|DIV|MODULO) expr                                          # mulDivModuloExpr
     | expr (PLUS|MINUS) expr                                              # addSubExpr
     | '(' expr ')'                                                        # nestedExpr
     | identifier '(' fnArgs? ')'                                          # functionExpr
     | column                                                              # columnExpr
     | doubleElement                                                       # doubleLiteral
     | longElement                                                         # longLiteral
     | stringElement                                                       # stringLiteral
     | CASE whenExpr+ ELSE alternative=expr END                            # caseWhenExpr
     | IF '(' predicate ',' expr ','  expr ')'                             # ifExpr
     ;

fnArgs : expr (',' expr)*;
whenExpr: WHEN predicate THEN expr;

stringArray:   '(' stringElement (',' stringElement)* ')' ;
longArray:     '(' longElement (',' longElement)* ')' ;
doubleArray:   '(' numericElement (',' numericElement)* ')' ;
stringElement: STRING;
longElement: LONG;
doubleElement: DOUBLE;
numericElement : (doubleElement| longElement);

identifier:IDENTIFIER;
column: IDENTIFIER;

AS: ('AS'|'as');
CASE: ('CASE'|'case');
WHEN:('WHEN'|'when');
THEN:('THEN'|'then');
END: ('END'|'end');
AND: ('AND'|'and'|'&&') ;
OR:  ('OR'|'or' |'||') ;
LIKE: ('Like'|'like');
NOT: ('NOT'|'not');
IN: ('IN'|'in');
IF: ('IF'|'if');
ELSE: ('ELSE'|'else');

REGEX : '/' (~('/') | '\\/')+ '/';
MATCH : ('~=' | '~' );
LT : '<' ;
LEQ : '<=' ;
GT : '>' ;
GEQ : '>=' ;
EQ : '=' ;
NEGATE: '!';
NEQ : '!=' ;
PLUS : '+' ;
MINUS : '-' ;
MUL : '*' ;
DIV : '/' ;
MODULO : '%' ;

LONG : [0-9]+;
EXP:   [eE] [-]? LONG;
DOUBLE :(LONG '.' LONG?) | (LONG EXP) | (LONG '.' LONG? EXP);
STRING : '\'' (~('\'')|'\\\'') * '\''|'"' (~('"')|'\\"')* '"';
IDENTIFIER : [_a-zA-Z][_a-zA-Z0-9]*;

WS: [ \r\n\t]+ -> channel(HIDDEN);
