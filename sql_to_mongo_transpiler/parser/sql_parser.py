import ply.yacc as yacc
from sql_to_mongo_transpiler.lexer.sql_lexer import SqlLexer
from sql_to_mongo_transpiler.ast.nodes import SelectQuery, LogicalCondition, Comparison,OrderByItem,Aggregate

class SqlParser:
    def __init__(self):
        self.lexer = SqlLexer()
        self.lexer.build()
        self.tokens = self.lexer.tokens
        self.parser = yacc.yacc(module=self)

    # Precedence rules
    precedence = (
            ('left', 'OR'), 
        ('left', 'AND'),
    )

    def p_query(self, p):
        '''query : SELECT select_list FROM table_list where_clause_opt group_by_clause_opt having_clause_opt order_by_clause_opt limit_clause_opt SEMICOLON'''
        p[0] = SelectQuery(columns=p[2], table=p[4][0] if len(p[4])==1 else p[4], where=p[5],group_by=p[6],having=p[7],order_by=p[8],limit=p[9])

    def p_select_list_star(self, p):
        '''select_list : STAR'''
        p[0] = ['*']
    def p_table_list_single(self, p):
        '''table_list : IDENTIFIER'''
        p[0] = [p[1]]
    def p_table_list_multi(self, p):
        '''table_list : table_list COMMA IDENTIFIER'''
        p[0] = p[1] + [p[3]]
    def p_select_list_columns(self, p):
        '''select_list : column_list'''
        p[0] = p[1]

    def p_column_list_single(self, p):
        '''column_list : column'''
        p[0] = [p[1]]

    def p_column_list_multi(self, p):
        '''column_list : column_list COMMA column'''
        p[0] = p[1] + [p[3]]

    def p_column_identifier(self,p):
        '''column : identifier'''
        p[0]=p[1]

    def p_column_aggregate(self, p):
        '''column : COUNT LPAREN STAR RPAREN
                  | COUNT LPAREN IDENTIFIER RPAREN
                  | MIN LPAREN IDENTIFIER RPAREN
                  | MAX LPAREN IDENTIFIER RPAREN
                  | AVG LPAREN IDENTIFIER RPAREN
                  | SUM LPAREN IDENTIFIER RPAREN'''
        if p[1].upper() == "COUNT" and p[3] == "*":
            p[0] = Aggregate("COUNT", "*")
        else:
            p[0] = Aggregate(p[1].upper(), p[3])

    def p_where_clause_opt(self, p):
        '''where_clause_opt : WHERE condition
                            | empty'''
        if len(p) == 3:
            p[0] = p[2]
        else:
            p[0] = None

    def p_group_by_clause_opt(self, p):
        '''group_by_clause_opt : GROUP BY group_list
                               | empty'''
        if len(p) == 4:
            p[0] = p[3]
        else:
            p[0] = []
    def p_group_list_single(self, p):
        '''group_list : IDENTIFIER'''
        p[0] = [p[1]]

    def p_group_list_multiple(self, p):
        '''group_list : group_list COMMA IDENTIFIER'''
        p[0] = p[1] + [p[3]]

    def p_having_clause_opt(self, p):
        '''having_clause_opt : HAVING condition
                             | empty'''
        if len(p) == 3:
            p[0] = p[2]
        else:
            p[0] = None

    def p_condition_visual(self, p):
        '''condition : condition AND term
                     | condition OR term'''
        p[0] = LogicalCondition(left=p[1], operator=p[2], right=p[3])
    def p_condition_term(self, p):
        '''condition : term'''
        p[0] = p[1]
    def p_term(self, p):
        '''term : comparison'''
        p[0] = p[1]
    def p_aggregate_expr(self, p):
        '''aggregate_expr : COUNT LPAREN STAR RPAREN
                          | COUNT LPAREN IDENTIFIER RPAREN
                          | MIN LPAREN IDENTIFIER RPAREN
                          | MAX LPAREN IDENTIFIER RPAREN
                          | AVG LPAREN IDENTIFIER RPAREN
                          | SUM LPAREN IDENTIFIER RPAREN'''
        if p[1].upper() == "COUNT" and p[3] == "*":
            p[0] = Aggregate("COUNT", "*")
        else:
            p[0] = Aggregate(p[1].upper(), p[3])

    def p_comparison(self, p):
        '''comparison : identifier operator identifier
                  | identifier operator literal
                  | aggregate_expr operator literal'''
        if isinstance(p[1],Aggregate):
            p[0] = Comparison(identifier=p[1], operator=p[2], value=p[3])
        else:
            p[0] = Comparison(identifier=p[1], operator=p[2], value=p[3])

    def p_comparison_between(self, p):
        '''comparison : IDENTIFIER BETWEEN literal AND literal'''
        p[0] = Comparison(
                identifier=p[1],
                operator="BETWEEN",
                value=(p[3], p[5])
                )
    def p_literal_list_single(self, p):
        '''literal_list : literal'''
        p[0] = [p[1]]

    def p_literal_list_multi(self, p):
        '''literal_list : literal_list COMMA literal'''
        p[0] = p[1] + [p[3]]

    def p_comparison_in(self, p):
        '''comparison : IDENTIFIER IN LPAREN literal_list RPAREN'''
        p[0] = Comparison(
                identifier=p[1],
                operator="IN",
                value=p[4]
                )

    def p_operator(self, p):
        '''operator : EQ
                    | NE
                    | GT
                    | LT
                    | GE
                    | LE'''
        p[0] = p[1]

    def p_literal_number(self, p):
        '''literal : NUMBER'''
        p[0] = p[1]

    def p_literal_string(self, p):
        '''literal : STRING'''
        p[0] = p[1]

    def p_order_by_clause_opt(self, p):
        '''order_by_clause_opt : ORDER BY order_list
                               | empty'''
        if len(p) == 4:
            p[0] = p[3]
        else:
            p[0] = []
    def p_order_list_single(self, p):
        '''order_list : order_item'''
        p[0] = [p[1]]

    def p_order_list_multiple(self, p):
        '''order_list : order_list COMMA order_item'''
        p[0] = p[1] + [p[3]]

    def p_order_item_default(self, p):
        '''order_item : IDENTIFIER'''
        p[0] = OrderByItem(column=p[1],direction="ASC")

    def p_order_item_direction(self, p):
        '''order_item : IDENTIFIER ASC
                      | IDENTIFIER DESC'''
        p[0] = OrderByItem(column=p[1],direction=p[2])

    def p_limit_clause_opt(self, p):
        '''limit_clause_opt : LIMIT NUMBER
                            | empty'''
        if len(p) == 3:
            p[0] = p[2]
        else:
            p[0] = None

    def p_empty(self, p):
        '''empty :'''
        pass

    def p_identifier(self, p):
        '''identifier : IDENTIFIER
                      | IDENTIFIER DOT IDENTIFIER'''
        if len(p) == 2:
            p[0] = {
                "table": None,
                "column": p[1]
            }
        else:
            p[0] = {
                "table": p[1],
                "column": p[3]
            }

    def p_error(self, p):
        if p:
            raise SyntaxError(f"Syntax error at '{p.value}', line {p.lineno}")
        else:
            raise SyntaxError("Syntax error at EOF")

    def parse(self, data):
        return self.parser.parse(data, lexer=self.lexer.lexer)

# Helper function
def get_parser():
    return SqlParser()
