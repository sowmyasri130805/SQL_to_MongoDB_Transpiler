from sql_to_mongo_transpiler.ast.nodes import SelectQuery, LogicalCondition, Comparison,OrderByItem,Aggregate
import json

class MongoDBGenerator:
    def _has_aggregate(self, node):
        for col in node.columns:
            if isinstance(col, Aggregate):
                return True
        return False
    def _get_schema_columns(self, node, table):
        # quick helper if schema not available here
        # ideally pass schema to generator, but for now:
        return []  # TEMP (see better fix below)
    def _generate_join(self, node):
        tables = node.table
        left_table = tables[0]
        right_table = tables[1]
        join_cond, filter_list = self._split_conditions(node.where)
        if not join_cond:
            raise ValueError("JOIN condition not found")
        left = join_cond.identifier
        right = join_cond.value
        # Determine mapping
        if left["table"] == left_table:
            localField = left["column"]
            foreignField = right["column"]
        else:
            localField = right["column"]
            foreignField = left["column"]
        pipeline = []
        # $lookup
        pipeline.append({
            "$lookup": {
                "from": right_table,
                "localField": localField,
                "foreignField": foreignField,
                "as": right_table
            }
        })
        pipeline.append({
            "$unwind": f"${right_table}"
            })
        # filter condition
        if filter_list:
            if len(filter_list) == 1:
                match = self._generate_filter(filter_list[0])
            else:
                match = {
                        "$and": [self._generate_filter(f) for f in filter_list]
                        }
            pipeline.append({"$match": match})
        # $project
        projection = {}
        for col in node.columns:
            # --- normalize column ---
            if isinstance(col, dict):
                table = col.get("table")
                field = col.get("column")
            elif isinstance(col, str):
                if "." in col:
                    table, field = col.split(".")
                else:
                    matches = [t for t in node.table if col in self.schema[t]]
                    if len(matches) == 1:
                        table = matches[0]
                        field = col
                    else:
                        raise ValueError(f"Ambiguous column '{col}' in JOIN")
            else:
                continue
            # --- projection mapping ---
            if table == right_table:
                projection[f"{right_table}.{field}"] = 1
            else:
                projection[field] = 1
        pipeline.append({"$project": projection})
        return {
            "string": f"db.{left_table}.aggregate({pipeline})",
            "collection": left_table,
            "pipeline": pipeline
            }
    def _contains_in_subquery(self, node):
        from sql_to_mongo_transpiler.ast.nodes import LogicalCondition, Comparison

        if isinstance(node, Comparison):
            return node.operator == "IN_SUBQUERY"

        if isinstance(node, LogicalCondition):
            return (
                self._contains_in_subquery(node.left) or
                self._contains_in_subquery(node.right)
            )

        return False
    def generate(self, ast):
        if isinstance(ast,SelectQuery):
            self.current_base_table = ast.table
            if ast.where and self._contains_in_subquery(ast.where):
                return self._generate_in_subquery(ast)
            if hasattr(ast, "joins") and ast.joins:
                return self._generate_explicit_join(ast)
            if isinstance(ast.table, list) and len(ast.table) > 1:
                return self._generate_join(ast)
            # Aggregation
            if self._has_aggregate(ast) or ast.group_by:
                return self._generate_aggregate(ast)
            # Normal SELECT
            return self._generate_find(ast)
        else:
            raise ValueError(f"Unsupported AST node: {type(ast)}")
    def _generate_explicit_join(self, node):
        base_table = node.table
        join = node.joins[0]   # minimal support: single JOIN
        join_table = join["table"]
        condition = join["condition"]
        left = condition.identifier
        right = condition.value
        # Determine mapping
        if left["table"] == base_table:
            localField = left["column"]
            foreignField = right["column"]
        else:
            localField = right["column"]
            foreignField = left["column"]
        pipeline = []
        # $lookup
        pipeline.append({
            "$lookup": {
                "from": join_table,
                "localField": localField,
                "foreignField": foreignField,
                "as": join_table
            }
        })
        # $unwind
        pipeline.append({
            "$unwind": f"${join_table}"
        })
        # WHERE support (important)
        if node.where:
            match = self._generate_filter(node.where)
            pipeline.append({"$match": match})
        #  projection (reuse your logic)
        projection = {}
        for col in node.columns:
            if isinstance(col, dict):
                table = col.get("table")
                field = col.get("column")
            elif isinstance(col, str):
                if "." in col:
                    table, field = col.split(".")
                else:
                    table = base_table
                    field = col
            else:
                continue
            if table == join_table:
                projection[f"{join_table}.{field}"] = 1
            else:
                projection[field] = 1
        pipeline.append({"$project": projection})
        return {
            "string": f"db.{base_table}.aggregate({pipeline})",
            "collection": base_table,
            "pipeline": pipeline
            }
    def _split_conditions(self, node):
        if isinstance(node, Comparison):
            if isinstance(node.value, dict):
                return node, []
            else:
                return None, [node]
        elif isinstance(node, LogicalCondition):
            left_join, left_filters = self._split_conditions(node.left)
            right_join, right_filters = self._split_conditions(node.right)
            join = left_join or right_join
            filters = left_filters + right_filters
            return join, filters
        return None, []
    def _generate_lookup(self, node):
        join = node.join
        #print("DEBUG JOIN:", node.join, type(node.join))
        base_table = join.get("left_table")
        foreign_table = join.get("right_table")
        pipeline = []
        # $lookup
        pipeline.append({
            "$lookup": {
                "from": foreign_table,
                "localField": join.get("left_col"),
                "foreignField": join.get("right_col"),
                "as": foreign_table
            }
        })
        pipeline.append({
            "$unwind": f"${foreign_table}"
            })
        if hasattr(node, "filter_condition") and node.filter_condition:
            pipeline.append({
               "$match": self._generate_filter(node.filter_condition)
                })
        # $project
        projection = {}
        for col in node.columns:
            # --- normalize ---
            if isinstance(col, dict):
                table = col.get("table")
                field = col.get("column")
            elif isinstance(col, str):
                if "." in col:
                    table, field = col.split(".")
                else:
                    table = None
                    field = col
            else:
                continue
            # --- assign projection ---
            if table == foreign_table:
                projection[f"{table}.{field}"] = 1
            else:
                projection[field] = 1
        pipeline.append({"$project": projection})
        return {
                "string": f"db.{base_table}.aggregate({json.dumps(pipeline, indent=2)})",
                "collection": base_table,
                "pipeline": pipeline
                }
    def _generate_aggregate(self, node):
        pipeline = []
        # WHERE → $match
        if node.where:
            match_stage = {"$match": self._generate_filter(node.where)}
            pipeline.append(match_stage)
        group_stage = {}
        if node.group_by:
            if len(node.group_by) == 1:
                group_stage["_id"] = f"${node.group_by[0]}"
            else:
                group_stage["_id"] = {
                        col: f"${col}" for col in node.group_by
                        }
        else:
            group_stage["_id"] = None
        for col in node.columns:
            if isinstance(col, Aggregate):
                func = col.func
                column = col.column
                if func == "COUNT":
                    if column == "*":
                        group_stage["count"] = { "$sum": 1 }
                    else:
                        group_stage[f"count_{column}"] = {
                                "$sum": {
                                    "$cond": [
                                        {"$ne": [f"${column}", None]},
                                        1,
                                        0
                                    ]
                            }
                    }
                elif func in ["MIN","MAX","AVG","SUM"]:
                    mongo_operator = {
                            "MIN": "$min",
                            "MAX": "$max",
                            "AVG": "$avg",
                            "SUM": "$sum"
                            }[func]
                    group_stage[f"{func.lower()}_{column}"] = {mongo_operator: f"${column}"}
        pipeline.append({ "$group": group_stage })
        # HAVING → $match AFTER $group
        if node.having:
            pipeline.append({
                "$match": self._generate_filter(node.having)
                })
        # ORDER BY after GROUP
        if node.order_by:
            sort_doc = {}
            for item in node.order_by:
                direction = 1 if item.direction.upper() == "ASC" else -1
                # If sorting by grouped column → map to _id
                if node.group_by and item.column in node.group_by:
                    if len(node.group_by) == 1:
                        sort_doc["_id"] = direction
                    else:
                        sort_doc[f"_id.{item.column}"] = direction
                else:
                    # Sorting by aggregate field
                    sort_doc[item.column] = direction
            pipeline.append({"$sort": sort_doc})

        # LIMIT after GROUP
        if node.limit is not None:
            pipeline.append({"$limit": node.limit})

        #return f"db.{node.table}.aggregate({pipeline})"
        return {
                "string": f"db.{node.table}.aggregate({pipeline})",
                "collection": node.table,
                "pipeline": pipeline
                }

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
        #return query
        result={
                "string": query,
                "collection": collection,
                "filter": filter_doc,
                "projection": projection
                }
        if node.order_by:
            result["sort"] = sort_doc
        if node.limit is not None:
            result["limit"] = node.limit
        return result
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
            if isinstance(col, dict):
                column_name = col["column"]
            else:
                column_name = col
        
            projection[column_name] = 1

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
        identifier=node.identifier
        if isinstance(identifier, Aggregate):
            func = identifier.func
            column = identifier.column
            if func == "COUNT":
                if column == "*":
                    field = "count"
                else:
                    field = f"count_{column}"
            else:
                field = f"{func.lower()}_{column}"
        else:
            if isinstance(identifier, dict):
                if identifier["table"]:
                    if hasattr(self, "current_base_table") and identifier["table"] == self.current_base_table:
                        field = identifier["column"]
                    else:
                            field = f"{identifier['table']}.{identifier['column']}"
                else:
                    field = identifier["column"]
            else:
                field = identifier
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
    def _generate_in_subquery(self, node):
        condition = node.where

        # assume simple condition: id IN (subquery)
        identifier = condition.identifier
        subquery = condition.value

        # base table
        base_table = node.table

        # extract base field
        if isinstance(identifier, dict):
            base_field = identifier["column"]
        else:
            base_field = identifier

        # extract subquery info
        sub_table = subquery.table
        sub_column = subquery.columns[0]["column"]

        pipeline = []

        #  $lookup
        pipeline.append({
            "$lookup": {
                "from": sub_table,
                "localField": base_field,
                "foreignField": sub_column,
                "as": sub_table
            }
        })

        #  match non-empty (IN logic)
        pipeline.append({
            "$match": {
                sub_table: {"$ne": []}
            }
        })

        # optional projection (reuse your logic)
        projection = {}
        for col in node.columns:
            if isinstance(col, dict):
                projection[col["column"]] = 1
            elif isinstance(col, str):
                projection[col] = 1

        if projection:
            pipeline.append({"$project": projection})

        return {
            "string": f"db.{base_table}.aggregate({pipeline})",
            "collection": base_table,
            "pipeline": pipeline
        }
