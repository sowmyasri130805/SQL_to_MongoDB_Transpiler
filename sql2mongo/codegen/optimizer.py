import re
import ast


class MongoOptimizer:
    def _format_mongo_shell(self, obj):
        if isinstance(obj, dict):
            items = [f"{k}: {self._format_mongo_shell(v)}" for k, v in obj.items()]
            return "{ " + ", ".join(items) + " }"
        elif isinstance(obj, list):
            items = [self._format_mongo_shell(i) for i in obj]
            return "[ " + ", ".join(items) + " ]"
        elif isinstance(obj, str):
            return f'"{obj}"'
        else:
            return str(obj)

    def _rebuild_find_query(self, mongo_data):
        collection = mongo_data["collection"]
        filter_dict = mongo_data.get("filter", {})
        projection = mongo_data.get("projection")

        filter_str = self._format_mongo_shell(filter_dict) if filter_dict else "{}"
        
        if projection:
            proj_str = self._format_mongo_shell(projection)
            query = f"db.{collection}.find({filter_str}, {proj_str})"
        else:
            query = f"db.{collection}.find({filter_str})"
            
        if "sort" in mongo_data:
            sort_str = self._format_mongo_shell(mongo_data["sort"])
            query += f".sort({sort_str})"
            
        if "limit" in mongo_data:
            query += f".limit({mongo_data['limit']})"
            
        return query
    def _sort_in_operator(self, doc):
        if isinstance(doc, dict):
            for k, v in doc.items():
                if isinstance(v, dict) and "$in" in v:
                    v["$in"] = sorted(set(v["$in"]))
                else:
                    self._sort_in_operator(v)
        elif isinstance(doc, list):
            for item in doc:
                self._sort_in_operator(item)
        return doc
    def optimize(self, mongo_data):
        if "filter" in mongo_data:
            optimized_filter = self._optimize_filter(mongo_data["filter"])
            #  fix order
            optimized_filter = self._sort_in_operator(optimized_filter)
            mongo_data["filter"] = optimized_filter
            # regenerate string (optional but good)
            mongo_data["string"] = self._rebuild_find_query(mongo_data)
            return mongo_data
        elif "pipeline" in mongo_data:
            optimized_pipeline = self._optimize_pipeline(mongo_data["pipeline"])
            mongo_data["pipeline"] = optimized_pipeline
            # regenerate string
            mongo_data["string"] = self._rebuild_aggregate_query(mongo_data)

            return mongo_data
        return mongo_data
    # ---------------- FIND ----------------
    def _optimize_find(self, query):

        collection = re.search(r"db\.(\w+)\.find", query).group(1)

        match = re.search(r"find\((.*)\)", query, re.DOTALL)
        if not match:
            return query

        filter_str = match.group(1).strip()

        try:
            filter_doc = self._safe_eval(filter_str)
        except Exception as e:
            # print("⚠️ Optimizer parse failed:", e)
            return query

        optimized = self._optimize_filter(filter_doc)

        return f"db.{collection}.find({optimized})"


    # ---------------- CORE LOGIC ----------------
    def _optimize_filter(self, doc):

        #  OR OPTIMIZATION
        if "$or" in doc:

            flat = self._flatten_or(doc["$or"])

            # --- CASE 1: OR → IN (equality)
            field = None
            values = []

            all_equal = True

            for cond in flat:
                if not (isinstance(cond, dict) and len(cond) == 1):
                    all_equal = False
                    break

                k, v = list(cond.items())[0]

                if isinstance(v, dict):
                    all_equal = False
                    break

                if field is None:
                    field = k
                elif field != k:
                    all_equal = False
                    break

                values.append(v)

            if all_equal:
                # remove duplicates
                values = list(set(values))
                return {field: {"$in": values}}

            # --- CASE 2: RANGE OPTIMIZATION (>, <)
            field = None
            gt_values = []
            lt_values = []

            valid_range = True

            for cond in flat:
                if not (isinstance(cond, dict) and len(cond) == 1):
                    valid_range = False
                    break

                k, expr = list(cond.items())[0]

                if field is None:
                    field = k
                elif field != k:
                    valid_range = False
                    break

                if not isinstance(expr, dict):
                    valid_range = False
                    break

                if "$gt" in expr:
                    gt_values.append(expr["$gt"])
                elif "$lt" in expr:
                    lt_values.append(expr["$lt"])
                else:
                    valid_range = False
                    break

            if valid_range:
                if gt_values:
                    return {field: {"$gt": min(gt_values)}}
                if lt_values:
                    return {field: {"$lt": max(lt_values)}}

            # fallback → flattened OR
            return {"$or": flat}

        #  AND MERGE
        if "$and" in doc:

            merged = {}

            for cond in doc["$and"]:
                for k, v in cond.items():
                    if k not in merged:
                        merged[k] = v
                    else:
                        if isinstance(v, dict) and isinstance(merged[k], dict):
                            merged[k].update(v)

            return merged

        return doc


    # ---------------- FLATTEN OR ----------------
    def _flatten_or(self, conditions):

        result = []

        for cond in conditions:
            if isinstance(cond, dict) and "$or" in cond:
                result.extend(self._flatten_or(cond["$or"]))
            else:
                result.append(cond)

        return result


    # ---------------- AGGREGATE ----------------
    def _optimize_aggregate(self, query):

        collection = re.search(r"db\.(\w+)\.aggregate", query).group(1)

        pipeline_str = re.search(r"aggregate\((.*)\)", query, re.DOTALL).group(1)

        try:
            pipeline = ast.literal_eval(pipeline_str)
        except:
            return query

        new_pipeline = self._optimize_pipeline(pipeline)

        return f"db.{collection}.aggregate({new_pipeline})"

    def _optimize_pipeline(self, pipeline):
        match_stages = []
        rest = []
        for stage in pipeline:
            if "$match" in stage:
                match_stages.append(stage)
            else:
                rest.append(stage)
        return match_stages + rest

    def _rebuild_aggregate_query(self, mongo_data):
        return f"db.{mongo_data['collection']}.aggregate({mongo_data['pipeline']})"


    # ---------------- SAFE PARSER ----------------
    def _safe_eval(self, text):

        text = text.strip()

        if not text:
            return {}

        #  Quote Mongo operators
        text = re.sub(r'(\$\w+)', r'"\1"', text)

        #  Quote field names
        text = re.sub(r'([a-zA-Z_]\w*)\s*:', r'"\1":', text)

        #  Fix quotes
        text = text.replace("'", '"')

        return ast.literal_eval(text)
