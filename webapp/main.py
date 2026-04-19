import json
from fastapi import FastAPI, Form, File, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request
import traceback
import os

from sql2mongo.parser.sql_parser import get_parser
from sql2mongo.semantic.semantic_analyzer import SemanticAnalyzer
from sql2mongo.codegen.mongodb_generator import MongoDBGenerator
from sql2mongo.codegen.optimizer import MongoOptimizer

app = FastAPI(title="SQL to MongoDB Web Transpiler")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")

@app.post("/transpile")
async def transpile_endpoint(schema: UploadFile = File(...), query: str = Form(...)):
    try:
        from sql2mongo.cli import transpile

        # Load constraints
        content = await schema.read()
        schema_dict = json.loads(content)

        # Build pipeline sequence evaluating string arrays securely gracefully bypassing breaks
        results = transpile(schema_dict, query)

        return {"queries": results}

    except Exception as e:
        # Pass Exception natively back to user to help format their SQL cleanly
        trace = traceback.format_exc()
        return JSONResponse(status_code=400, content={"error": str(e), "trace": trace})
