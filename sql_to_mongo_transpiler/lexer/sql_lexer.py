import ply.lex as lex

class LexerError(Exception):
    def __init__(self, message, line, column):
        self.message = message
        self.line = line
        self.column = column
        super().__init__(f"{message} at line {line}, column {column}")

class SqlLexer:
    # List of token names
    tokens = (
        'SELECT',
        'FROM',
        'WHERE',
        'AND',
        'OR',
        'IDENTIFIER',
        'NUMBER',
        'STRING',
        'EQ',
        'NE',
        'GT',
        'LT',
        'GE',
        'LE',
        'COMMA',
        'SEMICOLON',
        'STAR',
        'LIMIT',
        'ORDER',
        'BY',
        'ASC',
        'DESC',
        'BETWEEN',
        'IN',
        'LPAREN',
        'RPAREN',
        'COUNT',
        'MIN',
        'MAX',
        'AVG',
        'SUM'
    )

    # Regular expression rules for simple tokens
    t_EQ = r'='
    t_NE = r'!='
    t_GE = r'>='
    t_LE = r'<='
    t_GT = r'>'
    t_LT = r'<'
    t_COMMA = r','
    t_SEMICOLON = r';'
    t_STAR = r'\*'
    t_LPAREN=r'\('
    t_RPAREN=r'\)'
    # Ignored characters (whitespace)
    t_ignore = ' \t'

    # Reserved keywords map
    reserved = {
        'SELECT': 'SELECT',
        'FROM': 'FROM',
        'WHERE': 'WHERE',
        'AND': 'AND',
        'OR': 'OR',
        'LIMIT':'LIMIT',
        'ORDER':'ORDER',
        'BY':'BY',
        'ASC':'ASC',
        'DESC':'DESC',
        'IN':'IN',
        'BETWEEN':'BETWEEN',
        'COUNT':'COUNT',
        'MIN':'MIN',
        'MAX':'MAX',
        'AVG':'AVG',
        'SUM':'SUM'
    }

    def t_IDENTIFIER(self, t):
        r'[a-zA-Z_][a-zA-Z0-9_]*'
        # Check for reserved words (case-insensitive)
        t.type = self.reserved.get(t.value.upper(), 'IDENTIFIER')
        return t

    def t_NUMBER(self, t):
        r'\d+'
        t.value = int(t.value)
        return t

    def t_STRING(self, t):
        r"'[^']*'"
        t.value = t.value[1:-1] # Remove quotes
        return t

    def t_newline(self, t):
        r'\n+'
        t.lexer.lineno += len(t.value)

    def find_column(self, input_data, token):
        line_start = input_data.rfind('\n', 0, token.lexpos) + 1
        return (token.lexpos - line_start) + 1

    def t_error(self, t):
        column = self.find_column(t.lexer.lexdata, t)
        raise LexerError(f"Illegal character '{t.value[0]}'", t.lexer.lineno, column)
        # t.lexer.skip(1)

    def build(self, **kwargs):
        self.lexer = lex.lex(module=self, **kwargs)
        return self.lexer

    def input(self, data):
        self.lexer.input(data)

    def token(self):
        return self.lexer.token()

    def tokenize(self, data):
        self.lexer.input(data)
        tokens = []
        while True:
            tok = self.lexer.token()
            if not tok:
                break
            tokens.append(tok)
        return tokens

# Helper function to expose the lexer easily
def get_lexer():
    l = SqlLexer()
    l.build()
    return l
