from typing import List, Union, Optional

class ASTNode:
    """Base class for all AST nodes."""
    def __repr__(self):
        return self.__class__.__name__

class Comparison(ASTNode):
    """Represents a comparison: identifier operator literal"""
    def __init__(self, identifier: str, operator: str, value: Union[int, str]):
        self.identifier = identifier
        self.operator = operator
        self.value = value

    def __repr__(self):
        return (f"Comparison(\n"
                f"    identifier='{self.identifier}',\n"
                f"    operator='{self.operator}',\n"
                f"    value={repr(self.value)}\n"
                f")")

class LogicalCondition(ASTNode):
    """Represents a logical condition: condition AND/OR condition"""
    def __init__(self, left: ASTNode, operator: str, right: ASTNode):
        self.left = left
        self.operator = operator
        self.right = right

    def __repr__(self):
        left_repr = repr(self.left).replace('\n', '\n    ')
        right_repr = repr(self.right).replace('\n', '\n    ')
        return (f"LogicalCondition(\n"
                f"    left={left_repr},\n"
                f"    operator='{self.operator}',\n"
                f"    right={right_repr}\n"
                f")")

class OrderByItem(ASTNode):
    """Represents a single ORDER BY item"""
    def __init__(self, column: str, direction: str = "ASC"):
        self.column = column
        self.direction = direction.upper()

    def __repr__(self):
        return (f"OrderByItem(\n"
                f"    column='{self.column}',\n"
                f"    direction='{self.direction}'\n"
                f")")

class Aggregate(ASTNode):
    """Represents an aggregate function like COUNT, MIN, MAX, AVG, SUM"""
    def __init__(self, func: str, column: str):
        self.func = func.upper()
        self.column = column  # '*' or column name

    def __repr__(self):
        return (f"Aggregate(\n"
                f"    func='{self.func}',\n"
                f"    column='{self.column}'\n"
                f")")


class SelectQuery(ASTNode):
    """Represents a SELECT query"""
    def __init__(self, columns: List[Union[str,Aggregate]], table: str, where: Optional[ASTNode] = None,order_by:Optional[List[OrderByItem]] = None,limit:Optional[int] = None,offset:Optional[int] = None):
        self.columns = columns
        self.table = table
        self.where = where
        self.order_by = order_by or []
        self.limit = limit
        self.offset = offset

    def __repr__(self):
        if self.where:
            where_repr = repr(self.where).replace('\n', '\n    ')
        else:
            where_repr = 'None'
        if self.order_by:
            order_repr = "[\n        " + ",\n        ".join(
                repr(o).replace('\n', '\n        ') for o in self.order_by
            ) + "\n    ]"
        else:
            order_repr = "[]"
            
        return (f"SelectQuery(\n"
                f"    columns={self.columns},\n"
                f"    table='{self.table}',\n"
                f"    where={where_repr},\n"
                f"    order_by={order_repr},\n"
                f"    limit={self.limit},\n"
                f"    offset={self.offset}\n"
                f")")
