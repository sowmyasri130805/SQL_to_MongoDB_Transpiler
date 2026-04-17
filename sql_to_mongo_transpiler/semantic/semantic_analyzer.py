from sql_to_mongo_transpiler.ast.nodes import SelectQuery, LogicalCondition, Comparison,Aggregate

class SemanticError(Exception):
    pass

class SemanticAnalyzer:
    def __init__(self, schema):
        self.schema = schema

    def validate_query(self, ast):
        if isinstance(ast, SelectQuery):
            self.validate_select(ast)
        else:
            raise SemanticError(f"Unsupported query type: {type(ast)}")

    def validate_select(self, node: SelectQuery):
        # 1. Validate Table Exists
        tables = node.table if isinstance(node.table, list) else [node.table]
        print("tables:",tables)
        if len(tables) > 2:
            raise SemanticError("Only 2-table JOIN supported currently")
        for t in tables:
            if t not in self.schema:
                raise SemanticError(f"Table '{t}' does not exist")
        # pick primary table (for Mongo base collection)
        table_name = tables[0]
        if table_name not in self.schema:
            raise SemanticError(f"Table '{table_name}' does not exist")

        # 2. Validate Columns
        self.validate_columns(node.columns, table_name,node)

        # 3. Validate WHERE Clause
        tables = node.table if isinstance(node.table, list) else [node.table]
        if isinstance(node.table, list) and len(node.table) == 2:
            if not node.where:
                raise SemanticError("JOIN condition required for multiple tables")
        if node.where:
            self.validate_condition(node.where, tables)
        node.join = None
        node.filter_condition=None
        if isinstance(node.table, list):
            if len(node.table)==2:
                join_cond, filter_cond = self.split_join_and_filter(node.where)
                if not join_cond:
                    raise SemanticError("JOIN condition not found in WHERE clause")
                node.join = self.extract_join_condition(join_cond)
                node.filter_condition=filter_cond
            elif len(node.table) > 2:
                raise SemanticError("Only 2-table JOIN supported currently")
        # HAVING requires GROUP BY
        if node.having and not node.group_by:
            raise SemanticError("HAVING clause requires GROUP BY")
        # Validate HAVING condition
        if node.having:
            self.validate_condition(node.having, tables)
        # 4. Validate GROUP BY
        if node.group_by:
            table_schema = self.schema[table_name]
            # validate group_by columns exist
            for col in node.group_by:
                if col not in table_schema:
                    raise SemanticError(f"Column '{col}' does not exist in table '{table_name}'")
            # validate SELECT columns follow SQL rules
            for col in node.columns:
                if isinstance(col, str):
                    if col not in node.group_by:
                        raise SemanticError(
                                f"Column '{col}' must appear in GROUP BY or be aggregated")

    def validate_columns(self, columns, table_name,node):
        table_schema = self.schema[table_name]
        
        # Handle 'SELECT *'
        if columns == ['*']:
            return


        # Check for duplicates
        seen = set()
        for col in columns:
            # --- NORMALIZE ---
            if isinstance(col, dict):
                column_name = col.get("column")
                table = col.get("table")
                # FIX: resolve missing table
                if table is None or "." not in col:
                    matches = []
                    tables = node.table if isinstance(node.table, list) else [node.table]
                    for t in tables:
                        if column_name in self.schema[t]:
                            matches.append(t)
                    if len(matches) == 0:
                        raise SemanticError(f"Column '{column_name}' not found in any table")
                    if len(matches) > 1:
                        raise SemanticError(f"Ambiguous column '{column_name}', specify table")
                    table = matches[0]
            elif isinstance(col, str):
                if "." in col:
                    table, column_name = col.split(".")
                else:
                    column_name = col
                    # search across all tables
                    matches = []
                    tables = node.table if isinstance(node.table, list) else [node.table]
                    for t in tables:
                        if column_name in self.schema[t]:
                            matches.append(t)
                    if len(matches) == 0:
                        raise SemanticError(f"Column '{column_name}' not found in any table")
                    if len(matches) > 1:
                        raise SemanticError(f"Ambiguous column '{column_name}', specify table")

                    table = matches[0]
            elif isinstance(col, Aggregate):
                if col.func == "COUNT" and col.column == "*":
                    continue
                if "." in col.column:
                    table, column_name = col.column.split(".")
                else:
                    table = table_name
                    column_name = col.column
            else:
                raise SemanticError(f"Invalid column type: {col}")
            # --- DUPLICATE CHECK ---
            key = f"{table}.{column_name}"
            if key in seen:
                raise SemanticError(f"Duplicate column '{column_name}' in SELECT list")
            seen.add(key)
            # ✅ STORE RESOLVED COLUMN (ADD THIS BLOCK)
            if not hasattr(node, "resolved_columns"):
                node.resolved_columns = []
            node.resolved_columns.append({
                "table": table,
                "column": column_name
                })
            # --- VALIDATE TABLE ---
            if table not in self.schema:
                raise SemanticError(f"Table '{table}' does not exist")
            table_schema = self.schema[table]

            # --- VALIDATE COLUMN ---
            if column_name not in table_schema:
                raise SemanticError(
                        f"Column '{column_name}' does not exist in table '{table}'"
                )
    def extract_join_condition(self, condition):
        if isinstance(condition, Comparison):
            left = condition.identifier
            right = condition.value
            # --- normalize left ---
            if isinstance(left, dict):
                left_table = left.get("table")
                left_col = left.get("column")
            elif isinstance(left, str) and "." in left:
                left_table, left_col = left.split(".")
            else:
                return None
            # --- normalize right ---
            if isinstance(right, dict):
                right_table = right.get("table")
                right_col = right.get("column")
            elif isinstance(right, str) and "." in right:
                right_table, right_col = right.split(".")
            else:
                return None
            return {
                    "left_table": left_table,
                    "left_col": left_col,
                    "right_table": right_table,
                    "right_col": right_col
                }
        elif isinstance(condition, LogicalCondition):
            # search left side
            left_result = self.extract_join_condition(condition.left)
            if left_result:
                return left_result
            # search right side
            return self.extract_join_condition(condition.right)
        return None
    def split_join_and_filter(self, condition):
        from sql_to_mongo_transpiler.ast.nodes import LogicalCondition, Comparison
        if isinstance(condition, LogicalCondition):
            left_join, left_filter = self.split_join_and_filter(condition.left)
            right_join, right_filter = self.split_join_and_filter(condition.right)
            # pick join condition
            join_cond = left_join if left_join else right_join
            # collect filters
            filters = []
            if left_filter:
                filters.append(left_filter)
            if right_filter:
                filters.append(right_filter)
            # rebuild filter condition
            if len(filters) == 2:
                return join_cond, LogicalCondition(filters[0], "AND", filters[1])
            elif len(filters) == 1:
                return join_cond, filters[0]
            else:
                return join_cond, None
        elif isinstance(condition, Comparison):
            if self.extract_join_condition(condition):
                return condition, None
            else:
                return None, condition
        return None, None
    def validate_condition(self, node, tables):
        if isinstance(node, LogicalCondition):
            self.validate_condition(node.left, tables)
            self.validate_condition(node.right, tables)
        elif isinstance(node, Comparison):
            self.validate_comparison(node, tables)

    def validate_comparison(self, node: Comparison, tables):
        tables = tables if isinstance(tables, list) else [tables]
        if ((isinstance(node.identifier, dict) or (isinstance(node.identifier, str) and "." in node.identifier))and(isinstance(node.value, dict) or (isinstance(node.value, str) and "." in node.value))):
            left = node.identifier
            right = node.value
            # normalize left
            if isinstance(left, dict):
                left_table = left.get("table")
                left_col = left.get("column")
            elif isinstance(left, str):
                if "." in left:
                    left_table, left_col = left.split(".")
                else:
                    # infer from available tables
                    matches = [t for t in tables if left in self.schema[t]]
                    if len(matches) == 1:
                        left_table = matches[0]
                        left_col = left
                    else:
                        raise SemanticError(f"Ambiguous or unknown column '{left}'")
            else:
                raise SemanticError(f"Invalid JOIN condition identifier: {left}")
            # normalize right
            if isinstance(right, dict):
                right_table = right.get("table")
                right_col = right.get("column")
            elif isinstance(right, str):
                if "." in right:
                    right_table, right_col = right.split(".")
                else:
                    matches = [t for t in tables if right in self.schema[t]]
                    if len(matches) == 1:
                        right_table = matches[0]
                        right_col = right
                    else:
                        raise SemanticError(f"Ambiguous or unknown column '{right}'")
            else:
                raise SemanticError(f"Invalid JOIN condition value: {right}")
            # validate tables
            if left_table not in self.schema:
                raise SemanticError(f"Table '{left_table}' does not exist")
            if right_table not in self.schema:
                raise SemanticError(f"Table '{right_table}' does not exist")
            # validate columns
            if left_col not in self.schema[left_table]:
                raise SemanticError(f"Column '{left_col}' does not exist in table '{left_table}'")
            if right_col not in self.schema[right_table]:
                raise SemanticError(f"Column '{right_col}' does not exist in table '{right_table}'")
            # type match
            if self.schema[left_table][left_col] != self.schema[right_table][right_col]:
                raise SemanticError("Type mismatch in JOIN")
            if left_col != right_col:
                if not (
                        left_col.endswith("_id") and right_col == "id"
                        or right_col.endswith("_id") and left_col == "id"
                        ):
                    raise SemanticError(f"Invalid JOIN condition: '{left_table}.{left_col}' and '{right_table}.{right_col}' are not related"
                                        )
            return
        identifier = node.identifier
        if isinstance(identifier, dict):
            col_name = identifier.get("column")
            col_table = identifier.get("table")
        else:
            col_name = identifier
            col_table = None
        # resolve correct table
        if col_table:
            if col_table not in self.schema:
                raise SemanticError(f"Table '{col_table}' does not exist")
            if col_name not in self.schema[col_table]:
                raise SemanticError(f"Column '{col_name}' not in table '{col_table}'")
            expected_type = self.schema[col_table][col_name]
        else:
            # search across all tables
            found = False
            matches = []
            for t in tables:
                if col_name in self.schema[t]:
                    matches.append(t)
            if len(matches) == 0:
                raise SemanticError(f"Column '{col_name}' not found")
            if len(matches) > 1:
                raise SemanticError(f"Ambiguous column '{col_name}'")
            expected_type = self.schema[matches[0]][col_name]
        
        #if identifier is aggregate
        if isinstance(col_name, Aggregate):
            # Just validate literal type
            if not isinstance(node.value, (int, str)):
                raise SemanticError("Invalid HAVING condition value")
            return
        actual_value = node.value

        # ----- BETWEEN -----
        if node.operator == "BETWEEN":
            if not isinstance(actual_value, tuple) or len(actual_value) != 2:
                raise SemanticError("Invalid BETWEEN syntax")
            lower, upper = actual_value
            if expected_type == 'int':
                if not isinstance(lower, int) or not isinstance(upper, int):
                    raise SemanticError(f"Type mismatch for column '{col_name}'. Expected int.")
            elif expected_type == 'string':
                if not isinstance(lower, str) or not isinstance(upper, str):
                    raise SemanticError(
                            f"Type mismatch for column '{col_name}'. Expected string.")
            return
        # ----- IN -----
        if node.operator == "IN":
            if not isinstance(actual_value, list):
                raise SemanticError("Invalid IN syntax")
            for val in actual_value:
                if expected_type == 'int' and not isinstance(val, int):
                    raise SemanticError(f"Type mismatch for column '{col_name}'. Expected int.")
                if expected_type == 'string' and not isinstance(val, str):
                    raise SemanticError(f"Type mismatch for column '{col_name}'. Expected string.")
            return

        # Determine actual type
        if isinstance(actual_value, int):
            actual_type = 'int'
        elif isinstance(actual_value, str):
            actual_type = 'string'
        else:
            actual_type = 'unknown'

        if expected_type != actual_type:
            raise SemanticError(
                f"Type mismatch for column '{col_name}'. Expected {expected_type} but got {actual_type}."
            )
