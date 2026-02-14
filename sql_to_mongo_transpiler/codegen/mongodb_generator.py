from sql_to_mongo_transpiler.ast.nodes import SelectQuery, LogicalCondition, Comparison,OrderByItem,Aggregate
import json

class MongoDBGenerator:
    def _has_aggregate(self, node):
        for col in node.columns:
            if isinstance(col, Aggregate):
                return True
        return False

    def generate(self, ast):
        if self._has_aggregate(ast):
            return self._generate_aggregate(ast)
        elif isinstance(ast, SelectQuery):
            return self._generate_find(ast)
        else:
            raise ValueError(f"Unsupported AST node: {type(ast)}")
    def _generate_aggregate(self, node):
        pipeline = []
        # WHERE → $match
        if node.where:
            match_stage = {"$match": self._generate_filter(node.where)}
            pipeline.append(match_stage)
        group_stage = { "_id": None }
        for col in node.columns:
            if isinstance(col, Aggregate):
                func = col.func
                column = col.column
                if func == "COUNT" and column == "*":
                    group_stage["count"] = { "$sum": 1 }
                else:
                    mongo_operator = {
                            "MIN": "$min",
                            "MAX": "$max",
                            "AVG": "$avg",
                            "SUM": "$sum"
                            }.get(func)
                    group_stage[f"{func.lower()}_{column}"] = {mongo_operator: f"${column}"}
        pipeline.append({ "$group": group_stage })
        return f"db.{node.table}.aggregate({pipeline})"

    def _generate_find(self, node: SelectQuery):
        collection = node.table
        filter_doc = self._generate_filter(node.where) if node.where else {}
        projection = self._generate_projection(node.columns)

        
        # Format as MongoDB shell command (custom format, not JSON)
        filter_str = self._format_mongo_shell(filter_doc)
        
        if projection:
            proj_str = self._format_mongo_shell(projection)
            query = f"db.{collection}.find({filter_str}, {proj_str})"
        else:
            query = f"db.{collection}.find({filter_str})"

        if node.order_by:
            sort_doc = self._generate_sort(node.order_by)
            sort_str = self._format_mongo_shell(sort_doc)
            #print("DEBUG order_by:", node.order_by)
            #print("DEBUG sort_doc:", sort_doc)
            query += f".sort({sort_str})"

        if node.limit is not None:
            query += f".limit({node.limit})"
        return query
    def _format_mongo_shell(self, obj):
        """Recursively formats Python objects to MongoDB shell syntax (keys unquoted)."""
        if isinstance(obj, dict):
            # { key: value, key2: value2 }
            items = []
            for k, v in obj.items():
                formatted_value = self._format_mongo_shell(v)
                items.append(f"{k}: {formatted_value}")
            return "{ " + ", ".join(items) + " }"
        
        elif isinstance(obj, list):
            # [ item1, item2 ]
            items = [self._format_mongo_shell(i) for i in obj]
            return "[ " + ", ".join(items) + " ]"
        
        elif isinstance(obj, str):
            # Strings must be quoted
            return f'"{obj}"'
        
        else:
            # Numbers, booleans, etc.
            return str(obj)

    def _generate_projection(self, columns):
        if columns == ['*']:
            return None
        
        projection = {}
        for col in columns:
            projection[col] = 1
        return projection

    def _generate_filter(self, node):
        if isinstance(node, LogicalCondition):
            return self._handle_logical(node)
        elif isinstance(node, Comparison):
            return self._handle_comparison(node)
        else:
            raise ValueError(f"Unknown filter node: {type(node)}")

    def _handle_logical(self, node: LogicalCondition):
        left = self._generate_filter(node.left)
        right = self._generate_filter(node.right)
        
        op_map = {
            'AND': '$and',
            'OR': '$or'
        }
        
        mongo_op = op_map.get(node.operator.upper())
        if not mongo_op:
             raise ValueError(f"Unknown logical operator: {node.operator}")
             
        # Combine if valid
        return {mongo_op: [left, right]}

    def _handle_comparison(self, node: Comparison):
        field = node.identifier
        value = node.value
        operator = node.operator
        
        # Direct equality check
        if operator == '=':
            return {field: value}
        
        # Inequality and ranges
        if operator == '!=':
            return {field: {'$ne': value}}

        # BETWEEN
        if operator == "BETWEEN":
            lower, upper = value
            return {field: {'$gte': lower, '$lte': upper}}
        # IN
        if operator == "IN":
            return {field: {'$in': value}}

        op_map = {
            '>': '$gt',
            '<': '$lt',
            '>=': '$gte',
            '<=': '$lte'
        }
        
        mongo_op = op_map.get(operator)
        if not mongo_op:
            raise ValueError(f"Unknown comparison operator: {operator}")
            
        return {field: {mongo_op: value}}
    def _generate_sort(self, order_by_list):
        sort_doc = {}
        for item in order_by_list:
            if not isinstance(item, OrderByItem):
                continue
            direction = 1 if item.direction.upper() == "ASC" else -1
            sort_doc[item.column] = direction
        return sort_doc

