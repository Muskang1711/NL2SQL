import pandas as pd
import uuid
from pathlib import Path
from typing import Dict
from utils.logger import setup_logger
from services.data_analyzer import DataAnalyzer

logger = setup_logger(__name__)


class ExcelService:
    _sessions: Dict[str, dict] = {}
    
    @classmethod
    def load_excel(cls, file_path: str) -> str:
        session_id = str(uuid.uuid4())
        logger.info(f"Loading Excel file: {file_path}, session_id: {session_id}")
        
        excel_file = pd.ExcelFile(file_path)
        sheets = excel_file.sheet_names
        logger.info(f"Found {len(sheets)} sheets: {sheets}")
        
        dataframes = {}
        for sheet in sheets:
            logger.info(f"Reading sheet: {sheet}")
            df = pd.read_excel(excel_file, sheet_name=sheet, header=None)
            dataframes[sheet] = df
        
        cls._sessions[session_id] = {
            "file_path": file_path,
            "original_sheets": sheets,
            "dataframes": dataframes,
            "cleaned_dataframes": {},
            "analysis": {},
            "schema": {}
        }
        
        logger.info(f"Excel file loaded successfully, session: {session_id}")
        return session_id
    
    @classmethod
    def get_session(cls, session_id: str) -> dict:
        if session_id not in cls._sessions:
            logger.error(f"Session not found: {session_id}")
            raise ValueError(f"Session {session_id} not found")
        return cls._sessions[session_id]
    
    @classmethod
    def analyze_sheet(cls, session_id: str, sheet_name: str) -> dict:
        logger.info(f"Analyzing sheet: {sheet_name}")
        session = cls.get_session(session_id)
        df = session["dataframes"][sheet_name]
        
        skip_rows = DataAnalyzer.detect_header_row(df)
        
        if skip_rows > 0:
            df_clean = pd.read_excel(
                session["file_path"], 
                sheet_name=sheet_name, 
                skiprows=skip_rows
            )
        else:
            df_clean = pd.read_excel(session["file_path"], sheet_name=sheet_name)
        
        summary = DataAnalyzer.get_dataframe_summary(df_clean)
        samples = DataAnalyzer.get_sample_data(df_clean)
        issues = DataAnalyzer.detect_data_issues(df_clean)
        pk_candidates, fk_candidates = DataAnalyzer.detect_potential_keys(df_clean)
        
        analysis = {
            "sheet_name": sheet_name,
            "skip_rows": skip_rows,
            "summary": summary,
            "samples": samples,
            "issues": issues,
            "pk_candidates": pk_candidates,
            "fk_candidates": fk_candidates,
            "dataframe": df_clean
        }
        
        session["analysis"][sheet_name] = analysis
        logger.info(f"Analysis completed for sheet: {sheet_name}")
        return analysis
    
    @classmethod
    def apply_cleaning(cls, session_id: str, sheet_name: str, config: dict) -> pd.DataFrame:
        logger.info(f"Applying cleaning to sheet: {sheet_name}")
        session = cls.get_session(session_id)
        
        df = pd.read_excel(
            session["file_path"],
            sheet_name=sheet_name,
            skiprows=config.get("skip_rows", 0)
        )
        
        if config.get("columns_to_drop"):
            df = df.drop(columns=config["columns_to_drop"], errors="ignore")
            logger.info(f"Dropped columns: {config['columns_to_drop']}")
        
        for col in config.get("date_columns", []):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                logger.info(f"Converted {col} to datetime")
        
        df.columns = [str(c).strip().replace(" ", "_").lower() for c in df.columns]
        logger.info("Standardized column names")
        
        new_name = config.get("new_name", sheet_name)
        session["cleaned_dataframes"][new_name] = df
        
        logger.info(f"Cleaning applied, new name: {new_name}")
        return df
    
    @classmethod
    def get_all_analysis_for_agent(cls, session_id: str) -> str:
        logger.info(f"Preparing analysis data for agent, session: {session_id}")
        session = cls.get_session(session_id)
        
        agent_data = {"sheets": {}}
        
        for sheet_name, analysis in session["analysis"].items():
            agent_data["sheets"][sheet_name] = {
                "summary": analysis["summary"],
                "head_sample": analysis["samples"]["head_sample"][:10],
                "issues": analysis["issues"],
                "pk_candidates": analysis["pk_candidates"],
                "fk_candidates": analysis["fk_candidates"]
            }
        
        import json
        return json.dumps(agent_data, indent=2, default=str)