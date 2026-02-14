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
        '''query : SELECT select_list FROM IDENTIFIER where_clause_opt order_by_clause_opt limit_clause_opt SEMICOLON'''
        p[0] = SelectQuery(columns=p[2], table=p[4], where=p[5],order_by=p[6],limit=p[7])

    def p_select_list_star(self, p):
        '''select_list : STAR'''
        p[0] = ['*']

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
        '''column : IDENTIFIER'''
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

    def p_condition_visual(self, p):
        '''condition : condition AND condition
                     | condition OR condition'''
        p[0] = LogicalCondition(left=p[1], operator=p[2], right=p[3])

    def p_condition_comparison(self, p):
        '''condition : comparison'''
        p[0] = p[1]

    def p_comparison(self, p):
        '''comparison : IDENTIFIER operator literal'''
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
