import pandas as pd
import json
from typing import Tuple
from config import get_settings
from utils.logger import setup_logger

logger = setup_logger(__name__)


class DataAnalyzer:
    
    @staticmethod
    def get_dataframe_summary(df: pd.DataFrame) -> dict:
        logger.info(f"Generating summary for dataframe with shape {df.shape}")
        settings = get_settings()
        
        summary = {
            "shape": {"rows": df.shape[0], "columns": df.shape[1]},
            "columns": {},
            "null_counts": df.isnull().sum().to_dict(),
            "memory_usage": df.memory_usage(deep=True).sum()
        }
        
        for col in df.columns:
            col_info = {
                "dtype": str(df[col].dtype),
                "null_count": int(df[col].isnull().sum()),
                "unique_count": int(df[col].nunique())
            }
            
            if df[col].nunique() <= settings.max_unique_values:
                col_info["unique_values"] = df[col].dropna().unique().tolist()[:settings.max_unique_values]
            
            if pd.api.types.is_numeric_dtype(df[col]):
                col_info["stats"] = {
                    "min": float(df[col].min()) if not pd.isna(df[col].min()) else None,
                    "max": float(df[col].max()) if not pd.isna(df[col].max()) else None,
                    "mean": float(df[col].mean()) if not pd.isna(df[col].mean()) else None
                }
            
            summary["columns"][str(col)] = col_info
        
        logger.info("Summary generation completed")
        return summary
    
    @staticmethod
    def get_sample_data(df: pd.DataFrame) -> dict:
        settings = get_settings()
        logger.info(f"Getting sample data, sample_size={settings.sample_size}")
        
        head_sample = df.head(min(100, len(df))).to_dict(orient="records")
        
        random_sample = []
        if len(df) > 100:
            random_sample = df.sample(min(settings.sample_size, len(df))).to_dict(orient="records")
        
        return {
            "head_sample": head_sample,
            "random_sample": random_sample
        }
    
    @staticmethod
    def detect_header_row(df: pd.DataFrame) -> int:
        logger.info("Detecting actual header row")
        
        for idx in range(min(10, len(df))):
            row = df.iloc[idx]
            if row.notna().sum() >= len(df.columns) * 0.7:
                non_numeric = sum(1 for v in row if isinstance(v, str) and not v.replace(".", "").isdigit())
                if non_numeric >= len(df.columns) * 0.5:
                    logger.info(f"Header row detected at index {idx}")
                    return idx
        
        logger.info("No header row shift needed")
        return 0
    
    @staticmethod
    def detect_data_issues(df: pd.DataFrame) -> list[str]:
        logger.info("Detecting data issues")
        issues = []
        
        null_percentage = (df.isnull().sum() / len(df) * 100)
        high_null_cols = null_percentage[null_percentage > 50].index.tolist()
        if high_null_cols:
            issues.append(f"High null percentage in columns: {high_null_cols}")
        
        for col in df.columns:
            if df[col].dtype == "object":
                non_null = df[col].dropna()
                if len(non_null) > 0:
                    types = set(type(v).__name__ for v in non_null.head(100))
                    if len(types) > 1:
                        issues.append(f"Mixed types in column '{col}': {types}")
        
        for col in df.columns:
            if df[col].dtype == "object":
                sample = df[col].dropna().head(50)
                date_patterns = 0
                for val in sample:
                    if isinstance(val, str):
                        if any(c in val for c in ["-", "/", "."]) and any(c.isdigit() for c in val):
                            date_patterns += 1
                if date_patterns > len(sample) * 0.5:
                    issues.append(f"Potential date column needing standardization: '{col}'")
        
        logger.info(f"Found {len(issues)} issues")
        return issues
    
    @staticmethod
    def infer_sql_type(series: pd.Series) -> str:
        dtype = series.dtype
        
        if pd.api.types.is_integer_dtype(dtype):
            if series.max() > 2147483647 or series.min() < -2147483648:
                return "BIGINT"
            return "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            return "DOUBLE PRECISION"
        elif pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "TIMESTAMP"
        else:
            max_len = series.astype(str).str.len().max()
            if max_len and max_len <= 255:
                return f"VARCHAR({int(max_len) + 50})"
            return "TEXT"
    
    @staticmethod
    def detect_potential_keys(df: pd.DataFrame) -> Tuple[list, list]:
        logger.info("Detecting potential primary and foreign keys")
        primary_candidates = []
        foreign_candidates = []
        
        for col in df.columns:
            col_lower = str(col).lower()
            unique_ratio = df[col].nunique() / len(df)
            
            if unique_ratio > 0.95 and df[col].notna().all():
                if "id" in col_lower or col_lower.endswith("_id"):
                    primary_candidates.append(str(col))
            
            if col_lower.endswith("_id") and col_lower != "id":
                if 0.1 < unique_ratio < 0.95:
                    foreign_candidates.append(str(col))
        
        logger.info(f"Found {len(primary_candidates)} PK candidates, {len(foreign_candidates)} FK candidates")
        return primary_candidates, foreign_candidates