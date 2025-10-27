# =============================================
# ICB (Ideal Candidate Blueprint) - Streamlit Web App
#  - Interactive database connection
#  - CSV file upload option
#  - Editable attribute configuration
#  - Job selection and ICP generation
#  - Download and copy functionality
#  - Simplified progress display during execution
# - FIX: Correctly saves 'Y'/'N' to database from boolean checkboxes
# - NEW: Added attribute_order for consistent JSON output
# - NEW: Added "Generate for All" functionality
# - NEW: Saves results to database after generation (Results tab unchanged)
# - NEW: MODIFIED: Improved nested JSON format for skills and certifications
# - NEW: Added compensation as an attribute with range value
# - FIX: Deduplicated skills/certifications and fixed incorrect matching for "Soft Skills"
# - NEW: Added CSV upload option as alternative to database connection
# - FIX: Added error handling for XGBoost model fitting
# - FIX: Fixed feature importances array size mismatch
# - FIX: Fixed NaN values in quantile calculation
# - FIX: Handle feature names and importances length mismatch
# - FIX: Sort nested JSON values by percentage
# - FIX: Fix database insertion for results
# - NEW: Added explicit button for inserting results into database
# - NEW: Added feature to save JSON outputs to server path
# - FIX: Addressed feature name/importance mismatch and phantom features
# - NEW: Added JSON file configuration for CSV data source
# - FIX: Always save each job as a separate JSON file
# - FIX: Updated database insertion to save each job separately
# - FIX: Updated View Insights tab to handle individual job profiles
# - FIX: Lowered minimum record threshold and added visibility for skipped jobs
# - FIX: Simplified save messages and removed overwhelming dropdown
# - FIX: Process all jobs regardless of record count
# - FIX: Added dropdown for generated jobs only and included model name in JSON
# - FIX: Fixed f-string syntax error with backslash
# =============================================

import streamlit as st
import pandas as pd
import numpy as np
import sys
from pathlib import Path
from typing import List, Dict, Any
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.linear_model import LinearRegression
from sqlalchemy import create_engine, text
import oracledb   # modern Oracle driver, works in thin mode
import json
import openpyxl
from xgboost import XGBRegressor
from catboost import CatBoostRegressor
import io
import base64
from datetime import datetime
import time
import warnings
import os
import re

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)

# Page configuration
st.set_page_config(
    page_title="ICP Generator",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    /* --- CSS FOR LOGO --- */
    .logo-container {
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .logo-container img {
        max-height: 80px;
        width: auto;
    }
    /* --- END CSS FOR LOGO --- */

    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .section-header {
        font-size: 1.5rem;
        color: #2ca02c;
        margin-top: 2rem;
        margin-bottom: 1rem;
    }
    .success-box {
        background-color: #d4edda;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #c3e6cb;
    }
    .warning-box {
        background-color: #fff3cd;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #ffeaa7;
    }
    .error-box {
        background-color: #f8d7da;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #f5c6cb;
    }
    .progress-container {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    .model-metrics {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 0.5rem;
        margin: 0.5rem 0;
        background-color: #e9ecef;
        border-radius: 0.25rem;
    }
    .model-name {
        font-weight: bold;
        color: #495057;
    }
    .model-stats {
        display: flex;
        gap: 1rem;
    }
    .stat-item {
        display: flex;
        flex-direction: column;
        align-items: center;
    }
    .stat-label {
        font-size: 0.8rem;
        color: #6c757d;
    }
    .stat-value {
        font-weight: bold;
        color: #495057;
    }
    .best-model {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
    }
    .best-model .model-name {
        color: #155724;
    }
    .progress-step {
        padding: 0.5rem;
        margin: 0.25rem 0;
        border-left: 3px solid #007bff;
        padding-left: 1rem;
    }
    .progress-step.completed {
        border-left-color: #28a745;
    }
    .progress-step.active {
        border-left-color: #ffc107;
        background-color: #fff3cd;
    }
    .job-stats {
        display: flex;
        justify-content: space-between;
        margin-top: 1rem;
        padding: 0.5rem;
        background-color: #e9ecef;
        border-radius: 0.25rem;
    }
    .job-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
        gap: 1rem;
        margin-top: 1rem;
    }
    .job-card {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 0.5rem;
        padding: 1rem;
    }
    .json-display {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 0.5rem;
        padding: 1rem;
        font-family: monospace;
        white-space: pre-wrap;
        max-height: 500px;
        overflow-y: auto;
    }
</style>
""", unsafe_allow_html=True)

# ---------- Helper Functions ----------
def get_oracle_engine(user, password, host, port, sid):
    """Creates and returns a SQLAlchemy engine for Oracle."""
    try:
        url = (
            "oracle+oracledb://"
            f"{user}:{password}@{host}:{port}/"
            f"?service_name={service_name_or_sid}"
        )
        engine = create_engine(url) //oracle+oracledb
        return engine
    except Exception as e:
        st.error(f"Failed to create database connection: {e}")
        return None

def load_csv_data(uploaded_file):
    """Load data from uploaded CSV file."""
    try:
        df = pd.read_csv(uploaded_file)
        return df
    except Exception as e:
        st.error(f"Error reading CSV file: {e}")
        return None

def pick_col(df: pd.DataFrame, candidates: List[str]) -> str:
    """Finds the first matching column name from a list of candidates."""
    if not candidates:
        return None
    cols = {c.lower(): c for c in df.columns}
    for name in candidates:
        if name.lower() in cols:
            return cols[name.lower()]
    return None

def extract_original_feature(feature_name: str, original_features: List[str]) -> str:
    """Extract the original feature name from a one-hot encoded feature name."""
    for orig_feat in original_features:
        if feature_name.startswith(orig_feat):
            return orig_feat
    
    parts = feature_name.split('_')
    if len(parts) > 1:
        for orig_feat in original_features:
            if parts[0] == orig_feat:
                return orig_feat
    
    return parts[0] if parts else feature_name

def calculate_ideal_range(series: pd.Series) -> str:
    # Handle NaN values
    clean_series = series.dropna()
    if len(clean_series) == 0:
        return "N/A"
    
    q1 = clean_series.quantile(0.25)
    q3 = clean_series.quantile(0.75)
    
    # Handle NaN in quantiles
    if pd.isna(q1) or pd.isna(q3):
        return "N/A"
    
    return f"{round(q1)}-{round(q3)}"

def calculate_compensation_range(series: pd.Series) -> str:
    """Calculate ideal compensation range in k format."""
    # Handle NaN values
    clean_series = series.dropna()
    if len(clean_series) == 0:
        return "N/A"
    
    q1 = clean_series.quantile(0.25)
    q3 = clean_series.quantile(0.75)
    
    # Handle NaN in quantiles
    if pd.isna(q1) or pd.isna(q3):
        return "N/A"
    
    return f"{round(q1/1000)}k-{round(q3/1000)}k"

# --- CORRECTED FUNCTION ---
# This function now deduplicates skills/certifications and keeps the one with the highest proficiency.
def process_skills_certifications(group_df: pd.DataFrame, prefix: str, max_count: int = 10) -> List[Dict[str, str]]:
    """
    Process skills or certifications into a deduplicated list of dictionaries.
    Reads the most frequent value from the column to get the name.
    If a skill appears in multiple columns, it keeps the entry with the highest proficiency.
    e.g., [{"name": "SQL", "proficiency": "80%"}, {"name": "PL/SQL", "proficiency": "70%"}]
    """
    temp_list = []
    
    # Find all columns with the given prefix (e.g., 'skill_', 'certification_')
    cols = [col for col in group_df.columns if col.lower().startswith(prefix.lower())]
    
    # Limit to max_count columns to avoid overly long lists
    cols = cols[:max_count]
    
    for col in cols:
        # Get the most common value (mode) from the column data itself
        mode_val = group_df[col].mode()
        
        # Proceed only if a valid mode (name) is found
        if not mode_val.empty and pd.notna(mode_val.iloc[0]):
            name = str(mode_val.iloc[0])
            
            # Calculate the percentage of employees in this group who have this skill/certification
            count = group_df[col].notna().sum()
            percentage = round(100 * count / len(group_df))
            
            # Append to a temporary list
            temp_list.append({"name": name, "proficiency": percentage})

    # --- Deduplication Step ---
    # Use a dictionary to store the highest proficiency for each unique skill name.
    aggregated_results = {}
    for item in temp_list:
        name = item['name']
        proficiency = item['proficiency']
        # If the skill is new, or if this entry has a higher proficiency, store it.
        if name not in aggregated_results or proficiency > aggregated_results[name]:
            aggregated_results[name] = proficiency

    # Convert the dictionary back to the required list of dictionaries format
    final_list = [{"name": name, "proficiency": f"{prof}%"} for name, prof in aggregated_results.items()]
    
    # Sort by proficiency (highest first) instead of alphabetically
    final_list.sort(key=lambda x: int(x['proficiency'].replace('%', '')), reverse=True)
    
    return final_list

# --- FIXED: Improved function to handle feature importances correctly ---
def topk_importances(pipe: Pipeline, feature_names: List[str], original_features: List[str], k: int = 10) -> pd.DataFrame:
    """Return top-k features with importances, grouping one-hot encoded features."""
    model = pipe.named_steps["model"]
    importances = None
    
    try:
        if hasattr(model, 'feature_importances_'):
            importances = model.feature_importances_
        elif hasattr(model, 'coef_'):
            importances = np.abs(model.coef_)
    except Exception as e:
        st.error(f"Error extracting importances: {e}")
        return pd.DataFrame(columns=["Feature", "Influence (%)"])
    
    if importances is None:
        return pd.DataFrame(columns=["Feature", "Influence (%)"])

    # --- FIX: Handle mismatch between feature_names and importances length properly ---
    if len(feature_names) != len(importances):
        # Instead of just truncating, let's try to match features properly
        min_length = min(len(feature_names), len(importances))
        
        # Create a mapping of feature names to importances
        feature_importance_map = {}
        for i in range(min_length):
            feature_name = feature_names[i]
            importance = importances[i]
            
            # Extract the original feature name
            original_feature = extract_original_feature(feature_name, original_features)
            
            # Aggregate importances for the same original feature
            if original_feature in feature_importance_map:
                feature_importance_map[original_feature] += importance
            else:
                feature_importance_map[original_feature] = importance
        
        # Convert to DataFrame
        grouped_df = pd.DataFrame({
            "Feature": list(feature_importance_map.keys()),
            "Importance": list(feature_importance_map.values())
        })
    else:
        # If lengths match, proceed with normal grouping
        grouped_importance = {}
        for i, feature in enumerate(feature_names):
            original_feature = extract_original_feature(feature, original_features)
            
            if original_feature not in grouped_importance:
                grouped_importance[original_feature] = 0
            
            grouped_importance[original_feature] += importances[i]
        
        grouped_df = pd.DataFrame({
            "Feature": list(grouped_importance.keys()),
            "Importance": list(grouped_importance.values())
        })
    
    # Calculate influence percentages
    if grouped_df["Importance"].sum() > 0:
        grouped_df["Influence (%)"] = 100 * grouped_df["Importance"] / grouped_df["Importance"].sum()
    else:
        grouped_df["Influence (%)"] = 0
    
    # Sort by importance and take top k
    result = grouped_df.sort_values("Importance", ascending=False).head(k)
    return result[["Feature", "Influence (%)"]].reset_index(drop=True)

# --- NEW: Function to format ICP results into a readable text string ---
def format_icp_as_text(icp_results: List[Dict]) -> str:
    """Converts the ICP JSON list into a single, human-readable string."""
    if not icp_results:
        return "No ICP data available."
    
    output_lines = []
    for profile in icp_results:
        output_lines.append(f"========================================")
        output_lines.append(f"Job: {profile['job']} ({profile['count']} employees)")
        output_lines.append(f"========================================")
        
        output_lines.append("\n--- Ideal Features ---")
        for feature in profile['features']:
            if isinstance(feature['ideal'], dict):
                output_lines.append(f"- {feature['feature']}:")
                for key, value in feature['ideal'].items():
                    output_lines.append(f"  - {key}: {value}")
            elif isinstance(feature['ideal'], list):
                output_lines.append(f"- {feature['feature']}:")
                for item in feature['ideal']:
                    if isinstance(item, dict):
                        details = ", ".join([f"{k}: {v}" for k, v in item.items()])
                        output_lines.append(f"  - {details}")
                    else:
                        output_lines.append(f"  - {item}")
            else:
                output_lines.append(f"- {feature['feature']}: {feature['ideal']}")
            
        output_lines.append("\n--- Key Influence Factors ---")
        for influence in profile['influence']:
            output_lines.append(f"- {influence['feature']}: {influence['influence']:.2f}%")
        
        output_lines.append("\n") # Add a blank line between profiles

    return "\n".join(output_lines)

# --- UPDATED: Function to save ICP results to the database ---
def save_icp_results_to_db(engine, jobs_processed: List[str], icp_results: List[Dict]):
    """Saves the generated ICP results to the wfa_icp_results table."""
    if not engine or not icp_results:
        return False
    
    success_count = 0
    
    try:
        with engine.connect() as conn:
            for profile in icp_results:
                job_name = profile['job']
                json_output = json.dumps([profile], indent=4)  # Save each profile as a list with one item
                text_output = format_icp_as_text([profile])  # Format just this profile
                generated_time = datetime.now()
                
                insert_stmt = text("""
                    INSERT INTO wfa_icp_results (job_name, generated_time, json_output, text_output)
                    VALUES (:job_name, :generated_time, :json_output, :text_output)
                """)
                conn.execute(insert_stmt, {
                    "job_name": job_name,
                    "generated_time": generated_time,
                    "json_output": json_output,
                    "text_output": text_output
                })
                success_count += 1
            
            conn.commit()
        return success_count == len(icp_results)
    except Exception as e:
        st.error(f"Failed to save ICP results to database: {e}")
        return False

# --- NEW: Function to save JSON to server path ---
def save_json_to_server_path(json_data, filename):
    """Save JSON data to the specified server path."""
    # Define the server path
    server_path = "/home/oracle/pvtest/icp_outputs/json_outputs"
    
    # Create the directory if it doesn't exist
    os.makedirs(server_path, exist_ok=True)
    
    # Define the full file path
    file_path = os.path.join(server_path, filename)
    
    try:
        # Save the JSON data to the file
        with open(file_path, 'w') as f:
            json.dump(json_data, f, indent=4)
        return True
    except Exception as e:
        st.error(f"Failed to save JSON to server path: {e}")
        return False

# --- NEW: Function to sanitize job names for filenames ---
def sanitize_filename(job_name):
    """Sanitize job name to create a valid filename."""
    # Replace spaces with underscores
    sanitized = job_name.replace(' ', '_')
    # Remove special characters except underscores and hyphens
    sanitized = re.sub(r'[^\w\-]', '', sanitized)
    return sanitized

# --- NEW: Functions for JSON configuration handling ---
def load_config_from_json(json_file_path):
    """Load attribute configuration from a JSON file."""
    try:
        if os.path.exists(json_file_path):
            with open(json_file_path, 'r') as f:
                config_data = json.load(f)
            return pd.DataFrame(config_data)
        else:
            # Create a default configuration if the file doesn't exist
            default_config = [
                {
                    "attribute_order": 1,
                    "attribute_name": "Experience",
                    "use_in_input": "Y",
                    "show_in_output": "Y",
                    "columns_in_view": "experience,exp,years_experience"
                },
                {
                    "attribute_order": 2,
                    "attribute_name": "Education",
                    "use_in_input": "Y",
                    "show_in_output": "Y",
                    "columns_in_view": "education,edu,degree"
                },
                {
                    "attribute_order": 3,
                    "attribute_name": "Skills",
                    "use_in_input": "Y",
                    "show_in_output": "Y",
                    "columns_in_view": "skills,skill"
                },
                {
                    "attribute_order": 4,
                    "attribute_name": "Certification",
                    "use_in_input": "Y",
                    "show_in_output": "Y",
                    "columns_in_view": "certification,cert"
                },
                {
                    "attribute_order": 5,
                    "attribute_name": "Compensation",
                    "use_in_input": "Y",
                    "show_in_output": "Y",
                    "columns_in_view": "compensation,salary"
                }
            ]
            save_config_to_json(default_config, json_file_path)
            return pd.DataFrame(default_config)
    except Exception as e:
        st.error(f"Error loading configuration from JSON: {e}")
        return None

def save_config_to_json(config_df, json_file_path):
    """Save attribute configuration to a JSON file."""
    try:
        # Convert DataFrame to list of dictionaries
        config_data = config_df.to_dict('records')
        
        # Save to JSON file
        with open(json_file_path, 'w') as f:
            json.dump(config_data, f, indent=4)
        return True
    except Exception as e:
        st.error(f"Error saving configuration to JSON: {e}")
        return False

def generate_icp(df, config_df, selected_jobs, TARGET, JOB_COL):
    """Generate ICP for selected jobs."""
    progress_container = st.container()
    
    with progress_container:
        st.markdown('<div class="progress-container">', unsafe_allow_html=True)
        st.markdown("### 🚀 Model Training Progress")
        
        st.markdown('<div class="progress-step completed">📋 Preparing data and configuration...</div>', unsafe_allow_html=True)
        
        config_df['columns_in_view'] = config_df['columns_in_view'].astype(str)
        config_df['candidate_list'] = config_df['columns_in_view'].apply(
            lambda x: [s.strip().strip("'\"").lower() for s in x.split(',')] if x and x != 'nan' else [])
        
        input_features_config = config_df[config_df['use_in_input'] == 'Y']
        output_features_config = config_df[config_df['show_in_output'] == 'Y']
        
        output_features_config = output_features_config.sort_values(by='attribute_order', ascending=True)
        
        feature_cols = []
        for _, row in input_features_config.iterrows():
            found_col = pick_col(df, row['candidate_list'])
            if found_col and found_col not in [TARGET, JOB_COL]:
                feature_cols.append(found_col)
        
        feature_cols = list(set(feature_cols))
        
        if not feature_cols:
            st.error("No matching features found from config.")
            return None
        
        X_all = df[feature_cols]
        y_all = df[TARGET]
        
        # Check for NaN values in target variable
        if y_all.isna().any():
            st.warning(f"Found {y_all.isna().sum()} NaN values in target variable. Removing these rows.")
            valid_indices = ~y_all.isna()
            X_all = X_all[valid_indices]
            y_all = y_all[valid_indices]
        
        num_cols = X_all.select_dtypes(include=[np.number]).columns.tolist()
        cat_cols = X_all.select_dtypes(exclude=[np.number]).columns.tolist()
        
        numeric_transformer = SimpleImputer(strategy="median")
        categorical_transformer = Pipeline(steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("ohe", OneHotEncoder(handle_unknown="ignore", sparse_output=False))
        ])
        
        prep = ColumnTransformer(
            transformers=[
                ("num", numeric_transformer, num_cols),
                ("cat", categorical_transformer, cat_cols)
            ],
            remainder="drop"
        )
        
        st.markdown('<div class="progress-step active">🤖 Training models...</div>', unsafe_allow_html=True)
        
        # Use simpler models to avoid potential issues
        models_to_evaluate = {
            "LinearRegression": LinearRegression(),
            "CatBoost": CatBoostRegressor(verbose=0, random_state=42)
        }
        
        # Try to add XGBoost with error handling
        try:
            models_to_evaluate["XGBoost"] = XGBRegressor(
                n_estimators=400,  
                max_depth=6,       
                learning_rate=0.06, 
                subsample=0.85,
                colsample_bytree=0.85,
                random_state=42,
                tree_method="hist"
            )
        except Exception as e:
            st.warning(f"Could not initialize XGBoost model: {e}")
        
        best_model = {"name": "", "pipeline": None, "rmse": float('inf')}
        
        # Check if we have enough data for train-test split
        if len(X_all) < 10:
            st.error("Not enough data for model training. Need at least 10 records.")
            return None
            
        X_train, X_test, y_train, y_test = train_test_split(X_all, y_all, test_size=0.25, random_state=42)
        
        model_results = []
        
        for name, model in models_to_evaluate.items():
            try:
                st.markdown(f'<div class="progress-step active">  -> Training {name}...</div>', unsafe_allow_html=True)
                pipe = Pipeline(steps=[("prep", prep), ("model", model)])
                
                # Fit the model with error handling
                pipe.fit(X_train, y_train)
                y_pred = pipe.predict(X_test)
                
                rmse = mean_squared_error(y_test, y_pred) ** 0.5
                mae = mean_absolute_error(y_test, y_pred)
                r2 = r2_score(y_test, y_pred)
                
                model_results.append({
                    "name": name, "rmse": rmse, "mae": mae, "r2": r2, "pipeline": pipe
                })
                
                if rmse < best_model["rmse"]:
                    best_model = {"name": name, "pipeline": pipe, "rmse": rmse}
                    
            except Exception as e:
                st.error(f"Error training {name} model: {e}")
                continue
        
        if not model_results:
            st.error("No models could be trained successfully.")
            return None
        
        st.markdown("### 📊 Model Performance Comparison")
        for result in model_results:
            is_best = result["name"] == best_model["name"]
            css_class = "best-model" if is_best else ""
            st.markdown(f'''
            <div class="model-metrics {css_class}">
                <span class="model-name">{result["name"]} {'🏆' if is_best else ''}</span>
                <div class="model-stats">
                    <div class="stat-item"><span class="stat-label">RMSE</span><span class="stat-value">{result["rmse"]:.3f}</span></div>
                    <div class="stat-item"><span class="stat-label">MAE</span><span class="stat-value">{result["mae"]:.3f}</span></div>
                    <div class="stat-item"><span class="stat-label">R²</span><span class="stat-value">{result["r2"]:.3f}</span></div>
                </div>
            </div>
            ''', unsafe_allow_html=True)
        
        st.markdown(f'<div class="progress-step completed">📝 Generating ICP JSON profiles using best model: {best_model["name"]}...</div>', unsafe_allow_html=True)
        
        try:
            fitted_prep = best_model['pipeline'].named_steps["prep"]
            all_feature_names = num_cols.copy()
            if cat_cols:
                ohe = fitted_prep.named_transformers_["cat"].named_steps["ohe"]
                ohe_feature_names = ohe.get_feature_names_out(cat_cols)
                all_feature_names.extend(ohe_feature_names)
            
            # --- FIX: Use the improved topk_importances function ---
            top_imp_df = topk_importances(best_model['pipeline'], all_feature_names, feature_cols, k=5)
        except Exception as e:
            st.error(f"Error getting feature importances: {e}")
            # Create a dummy dataframe with default influence values
            top_imp_df = pd.DataFrame({
                "Feature": feature_cols[:5] if len(feature_cols) >= 5 else feature_cols,
                "Influence (%)": [20.0] * (5 if len(feature_cols) >= 5 else len(feature_cols))
            })

        # --- FIX: Ensure only valid features from the input data are included ---
        # Filter out any features that aren't in the original feature_cols
        valid_features = [f for f in top_imp_df["Feature"] if f in feature_cols]
        top_imp_df = top_imp_df[top_imp_df["Feature"].isin(valid_features)]

        influence_with_priority = top_imp_df.merge(
            input_features_config[['attribute_name', 'attribute_order']],
            left_on='Feature',
            right_on='attribute_name',
            how='left'
        )
        influence_with_priority = influence_with_priority.sort_values(by=['attribute_order', 'Influence (%)'], ascending=[True, False])

        icb_profiles = []
        low_record_jobs = []
        
        for job in selected_jobs:
            group_df = df[df[JOB_COL] == job]
            
            # Add warning for jobs with very few records but still process them
            if len(group_df) < 5:
                low_record_jobs.append({"job": job, "count": len(group_df)})
            
            profile = {"job": job, "count": len(group_df), "features": [], "influence": [], "model": best_model["name"]}
            
            # For jobs with very few records, use simplified influence calculation
            if len(group_df) < 5:
                # Create a simple influence based on feature variance instead of model training
                simple_influence = []
                for col in feature_cols:
                    if col in group_df.columns:
                        if pd.api.types.is_numeric_dtype(group_df[col]):
                            # For numeric columns, use standard deviation as influence
                            std_val = group_df[col].std()
                            if not pd.isna(std_val) and std_val > 0:
                                simple_influence.append({"feature": col, "influence": min(100, std_val * 10)})
                        else:
                            # For categorical columns, use number of unique values as influence
                            unique_count = group_df[col].nunique()
                            if unique_count > 1:
                                simple_influence.append({"feature": col, "influence": min(100, unique_count * 20)})
                
                # Normalize influence to sum to 100%
                if simple_influence:
                    total_influence = sum(item["influence"] for item in simple_influence)
                    if total_influence > 0:
                        for item in simple_influence:
                            item["influence"] = (item["influence"] / total_influence) * 100
                    
                    profile["influence"] = simple_influence[:5]  # Take top 5
                else:
                    # Fallback influence if calculation fails
                    profile["influence"] = [
                        {"feature": "Experience", "influence": 25.0},
                        {"feature": "Education", "influence": 25.0},
                        {"feature": "Skills", "influence": 25.0},
                        {"feature": "Compensation", "influence": 25.0}
                    ]
            else:
                # Use the model-based influence for jobs with sufficient records
                for _, row in influence_with_priority.iterrows():
                    profile["influence"].append({"feature": row["Feature"], "influence": row["Influence (%)"]})
            
            for _, row in output_features_config.iterrows():
                attribute_name = row['attribute_name']
                actual_col = pick_col(group_df, row['candidate_list'])
                if not actual_col:
                    continue
                
                ideal_value = "N/A"
                
                # --- CORRECTED: Use exact matching for attributes ---
                # Special handling for skills
                if attribute_name.lower() == "skills":
                    ideal_value = process_skills_certifications(group_df, "skill", 10)
                # Special handling for certifications
                elif attribute_name.lower() == "certification":
                    ideal_value = process_skills_certifications(group_df, "certification", 10)
                # Special handling for compensation
                elif attribute_name.lower() == "compensation" and pd.api.types.is_numeric_dtype(group_df[actual_col]):
                    ideal_value = calculate_compensation_range(group_df[actual_col])
                # Regular handling for other attributes (including "Soft Skills")
                elif pd.api.types.is_numeric_dtype(group_df[actual_col]):
                    ideal_value = calculate_ideal_range(group_df[actual_col])
                else:
                    mode_val = group_df[actual_col].mode()
                    ideal_value = mode_val.iloc[0] if not mode_val.empty else "N/A"
                
                profile["features"].append({"feature": attribute_name, "ideal": ideal_value})
            
            icb_profiles.append(profile)
        
        # Show summary of processed jobs
        st.markdown(f'<div class="progress-step completed">✅ Successfully generated ICP profiles!</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Display job statistics
        st.markdown('<div class="job-stats">', unsafe_allow_html=True)
        st.markdown(f"**Processed:** {len(icb_profiles)} jobs | **Low record count:** {len(low_record_jobs)} jobs (fewer than 5 records)", unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Show low record jobs if any
        if low_record_jobs:
            with st.expander(f"View jobs with low record count ({len(low_record_jobs)})"):
                low_record_df = pd.DataFrame(low_record_jobs)
                st.dataframe(low_record_df)
                st.warning("⚠️ Jobs with very few records use simplified influence calculations and may be less accurate.")
    
    return icb_profiles

def main():
    # --- LOGO SECTION ---
    logo_server_path = r'/home/oracle/pvtest/SplashBI11.png'
    try:
        if Path(logo_server_path).is_file():
            with open(logo_server_path, "rb") as image_file:
                encoded_logo = base64.b64encode(image_file.read()).decode()
            st.markdown(f'<div class="logo-container"><img src="data:image/png;base64,{encoded_logo}" alt="Company Logo"></div>', unsafe_allow_html=True)
        else:
            st.error(f"Logo file not found at {logo_server_path}")
    except Exception as e:
        st.error(f"Could not load logo. Error: {e}")

    st.markdown('<h1 class="main-header">🏗️ Ideal Candidate Profile (ICP) Generator</h1>', unsafe_allow_html=True)
    
    # Initialize session state variables
    if 'connected' not in st.session_state: st.session_state.connected = False
    if 'engine' not in st.session_state: st.session_state.engine = None
    if 'df' not in st.session_state: st.session_state.df = None
    if 'config_df' not in st.session_state: st.session_state.config_df = None
    if 'data_source' not in st.session_state: st.session_state.data_source = None
    if 'config_json_path' not in st.session_state: st.session_state.config_json_path = "attribute_config.json"
    
    # Data source selection in sidebar
    st.sidebar.markdown('<h2 class="section-header">📊 Data Source</h2>', unsafe_allow_html=True)
    data_source = st.sidebar.radio("Select data source:", ["Database", "CSV File"])
    st.session_state.data_source = data_source
    
    if data_source == "Database":
        st.sidebar.markdown('<h2 class="section-header">🔌 Database Connection</h2>', unsafe_allow_html=True)
        with st.sidebar.form("db_connection_form"):
            st.write("Enter your Oracle database connection details:")
            user = st.text_input("Username", value="csv_one_hd100")
            password = st.text_input("Password", value="csv_one_hd100", type="password")
            host = st.text_input("Host", value="192.168.4.23")
            port = st.text_input("Port", value="1521")
            sid = st.text_input("SID", value="19cdev")
            connect_button = st.form_submit_button("Connect to Database")
        
        if connect_button:
            with st.spinner("Connecting to database..."):
                engine = get_oracle_engine(user, password, host, port, sid)
                if engine:
                    try:
                        df = pd.read_sql("SELECT * FROM wfa_employee_history_v", con=engine)
                        config_df = pd.read_sql("SELECT * FROM wfa_attribute_config WHERE enabled_flag = 'Y' ORDER BY attribute_order ASC", con=engine)
                        
                        st.session_state.engine = engine
                        st.session_state.df = df
                        st.session_state.config_df = config_df
                        st.session_state.connected = True
                        st.markdown('<div class="success-box">✅ Successfully connected to database!</div>', unsafe_allow_html=True)
                    except Exception as e:
                        st.markdown(f'<div class="error-box">❌ Failed to connect: {e}</div>', unsafe_allow_html=True)
    else:  # CSV File option
        st.sidebar.markdown('<h2 class="section-header">📁 CSV Upload</h2>', unsafe_allow_html=True)
        uploaded_file = st.sidebar.file_uploader("Upload a CSV file", type=['csv'])
        
        if uploaded_file is not None:
            with st.spinner("Reading CSV file..."):
                df = load_csv_data(uploaded_file)
                if df is not None:
                    # Load configuration from JSON file
                    config_df = load_config_from_json(st.session_state.config_json_path)
                    
                    if config_df is not None:
                        st.session_state.df = df
                        st.session_state.config_df = config_df
                        st.session_state.connected = True
                        st.session_state.engine = None  # No database engine for CSV
                        st.markdown('<div class="success-box">✅ Successfully loaded CSV file with JSON configuration!</div>', unsafe_allow_html=True)
                        
                        # Display data preview
                        st.sidebar.markdown("### Data Preview")
                        st.sidebar.dataframe(df.head())
                    else:
                        st.markdown('<div class="error-box">❌ Failed to load configuration from JSON file.</div>', unsafe_allow_html=True)
                        st.session_state.df = df
                        st.session_state.connected = True
    
    if st.session_state.connected:
        tab1, tab2, tab3 = st.tabs(["📋 Setup", "🎯 Generate ICP", "📊 View Insights"])
        
        with tab1:
            st.markdown('<h2 class="section-header">Attribute Configuration</h2>', unsafe_allow_html=True)
            
            # Only show configuration editor if we have the config_df
            if 'config_df' in st.session_state and st.session_state.config_df is not None:
                original_config_df = st.session_state.config_df.copy()
                
                display_config_df = original_config_df.copy()
                display_config_df['use_in_input'] = display_config_df['use_in_input'] == 'Y'
                display_config_df['show_in_output'] = display_config_df['show_in_output'] == 'Y'
                
                edited_config = st.data_editor(
                    display_config_df[['attribute_order', 'attribute_name', 'use_in_input', 'show_in_output', 'columns_in_view']],
                    column_config={
                        "attribute_order": st.column_config.NumberColumn("Order", min_value=1, step=1, width="small"),
                        "attribute_name": st.column_config.TextColumn("Attribute Name"),
                        "use_in_input": st.column_config.CheckboxColumn("Use in Input", help="Include this attribute in model training"),
                        "show_in_output": st.column_config.CheckboxColumn("Show in Output", help="Include this attribute in final ICP output"),
                        "columns_in_view": st.column_config.TextColumn("Columns in View", help="Comma-separated list of column names to look for")
                    },
                    key="config_editor"
                )
                
                if st.button("Save Configuration", type="primary"):
                    with st.spinner("Saving configuration..."):
                        try:
                            # For CSV, save to JSON file
                            if st.session_state.data_source == "CSV File":
                                # Convert boolean values back to 'Y'/'N'
                                edited_config['use_in_input'] = edited_config['use_in_input'].apply(lambda x: 'Y' if x else 'N')
                                edited_config['show_in_output'] = edited_config['show_in_output'].apply(lambda x: 'Y' if x else 'N')
                                
                                # Save to JSON file
                                save_success = save_config_to_json(edited_config, st.session_state.config_json_path)
                                
                                if save_success:
                                    st.session_state.config_df = edited_config
                                    st.markdown('<div class="success-box">✅ Configuration successfully saved to JSON file!</div>', unsafe_allow_html=True)
                                    st.rerun()
                                else:
                                    st.markdown('<div class="error-box">❌ Failed to save configuration to JSON file.</div>', unsafe_allow_html=True)
                            # For Database, update the database
                            else:
                                engine = st.session_state.engine
                                if not engine:
                                    st.markdown('<div class="error-box">❌ Database connection lost.</div>', unsafe_allow_html=True)
                                else:
                                    with engine.connect() as conn:
                                        for _, row in edited_config.iterrows():
                                            use_in_input_val = 'Y' if row['use_in_input'] else 'N'
                                            show_in_output_val = 'Y' if row['show_in_output'] else 'N'
                                            update_stmt = text("""
                                                UPDATE wfa_attribute_config
                                                SET use_in_input = :use_in_input,
                                                    show_in_output = :show_in_output,
                                                    columns_in_view = :columns_in_view,
                                                    attribute_order = :attribute_order
                                                WHERE attribute_name = :attribute_name
                                            """)
                                            conn.execute(update_stmt, {
                                                "use_in_input": use_in_input_val,
                                                "show_in_output": show_in_output_val,
                                                "columns_in_view": row['columns_in_view'],
                                                "attribute_order": row['attribute_order'],
                                                "attribute_name": row['attribute_name']
                                            })
                                        conn.commit()
                                    
                                    st.session_state.config_df = edited_config.copy()
                                    st.session_state.config_df['use_in_input'] = st.session_state.config_df['use_in_input'].apply(lambda x: 'Y' if x else 'N')
                                    st.session_state.config_df['show_in_output'] = st.session_state.config_df['show_in_output'].apply(lambda x: 'Y' if x else 'N')
                                    st.markdown('<div class="success-box">✅ Configuration successfully saved!</div>', unsafe_allow_html=True)
                                    st.rerun()
                        except Exception as e:
                            st.markdown(f'<div class="error-box">❌ Failed to save configuration: {e}</div>', unsafe_allow_html=True)
            else:
                st.warning("Configuration data is not available. Please ensure you have a database connection for attribute configuration.")
        
        with tab2:
            st.markdown('<h2 class="section-header">Job Selection & ICP Generation</h2>', unsafe_allow_html=True)
            df = st.session_state.df
            TARGET = pick_col(df, ["TENURE_YEARS", "tenure", "years_at_company"])
            JOB_COL = pick_col(df, ["JOB_NAME", "job_group", "job_title", "position"])
            
            if not TARGET or not JOB_COL:
                st.error("Could not identify target and job columns. Please ensure your data contains columns like 'TENURE_YEARS' and 'JOB_NAME'.")
            else:
                st.write(f"**Target Variable:ICP(** {TARGET})")
                st.write(f"**Job Column:** {JOB_COL}")
                unique_jobs = df[JOB_COL].value_counts()
                job_counts = pd.DataFrame({'Job': unique_jobs.index, 'Count': unique_jobs.values})
                
                st.write("### Available Jobs")
                selected_jobs = st.multiselect("Select jobs to generate ICP for:", options=job_counts['Job'].tolist(), 
                                             format_func=lambda x: f"{x} ({job_counts[job_counts['Job'] == x]['Count'].iloc[0]} records)")
                
                st.info("💡 Leave the selection empty to generate ICPs for all available jobs.")
                
                all_jobs = job_counts['Job'].tolist()
                jobs_to_process = selected_jobs if selected_jobs else all_jobs
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    generate_button = st.button("🚀 Generate ICP Profiles", type="primary", help="Generate ICPs for the selected jobs.")
                    
                with col2:
                    st.button("📅 Schedule", type="primary",help="Schedule the ICP generation for a later time.")
                    
                with col3:
                    st.button("💾 Save Profile", type="primary",help="Save the generated profile in database.")
                
                if generate_button:
                    if not jobs_to_process:
                        st.warning("No jobs available to process.")
                    else:
                        if len(jobs_to_process) == len(all_jobs):
                            st.write(f"**Generating ICPs for all {len(jobs_to_process)} jobs...**")
                        else:
                            st.write(f"**Generating ICPs for {len(jobs_to_process)} selected job(s): {', '.join(jobs_to_process)}**")
                        
                        config_df = st.session_state.config_df
                        icp_results = generate_icp(df, config_df, jobs_to_process, TARGET, JOB_COL)
                        
                        if icp_results:
                            st.session_state.icp_results = icp_results
                            st.session_state.jobs_processed = jobs_to_process
                            
                            # Save JSON to server path - always save each job as a separate JSON file
                            # But only show one summary message at the end
                            successful_saves = 0
                            for profile in icp_results:
                                job_name = profile['job']
                                sanitized_name = sanitize_filename(job_name)
                                filename = f"{sanitized_name}.json"
                                job_success = save_json_to_server_path([profile], filename)
                                if job_success:
                                    successful_saves += 1

                            # Show just one summary message
                            if successful_saves > 0:
                                st.success(f"✅ Successfully saved job profiles as individual JSON files to server path.")
                            else:
                                st.error("❌ Failed to save any job profiles to server path.")
                            
                            st.markdown(
                            '<div class="success-box">✅ View Insights in next tab. '
                            'You can also view the live cheat sheet here --> '
                            '<a href="http://192.168.0.131:8516/" target="_blank">Recruiter Cheat Sheet</a>'
                            '</div>', unsafe_allow_html=True
                            )
        
        # --- UPDATED: Results tab with dropdown for generated jobs only ---
        with tab3:
            st.markdown('<h2 class="section-header">ICP Results</h2>', unsafe_allow_html=True)
            
            if 'icp_results' in st.session_state and st.session_state.icp_results:
                icp_results = st.session_state.icp_results
                
                # Create a dropdown for selecting from generated jobs only
                job_names = [profile['job'] for profile in icp_results]
                selected_job = st.selectbox("Select a job to view:", job_names)
                
                # Find the selected profile
                selected_profile = next((p for p in icp_results if p['job'] == selected_job), None)
                
                if selected_profile:
                #     # Display job details
                #     st.markdown(f"### {selected_profile['job']} ({selected_profile['count']} employees)")
                    
                #     # Display model used
                #     st.markdown(f"**Model Used:** {selected_profile['model']}")
                    
                #     # Display features and influence in columns
                #     col1, col2 = st.columns(2)
                    
                #     with col1:
                #         st.write("**Features:**")
                #         for feature in selected_profile['features']:
                #             if isinstance(feature['ideal'], dict):
                #                 st.write(f"- {feature['feature']}:")
                #                 for key, value in feature['ideal'].items():
                #                     st.write(f"  - {key}: {value}")
                #             elif isinstance(feature['ideal'], list):
                #                 st.write(f"- {feature['feature']}:")
                #                 for item in feature['ideal']:
                #                     if isinstance(item, dict):
                #                         details = ", ".join([f"{k}: {v}" for k, v in item.items()])
                #                         st.write(f"  - {details}")
                #                     else:
                #                         st.write(f"  - {item}")
                #             else:
                #                 st.write(f"- {feature['feature']}: {feature['ideal']}")
                    
                #     with col2:
                #         st.write("**Influence Factors:**")
                #         for influence in selected_profile['influence']:
                #             st.write(f"- {influence['feature']}: {influence['influence']:.2f}%")
                    
                    # JSON display section
                    st.markdown("### JSON Output")
                    
                    # Create JSON string with model included
                    json_str = json.dumps([selected_profile], indent=4)
                    
                    # Display JSON in a formatted box
                    st.markdown('<div class="json-display">' + json_str + '</div>', unsafe_allow_html=True)
                    
                    # Download and copy buttons
                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(
                            label="📥 Download JSON",
                            data=json_str,
                            file_name=f"icp_profile_{sanitize_filename(selected_profile['job'])}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json"
                        )
                    
                    with col2:
                        # Copy to clipboard button - FIXED SYNTAX ERROR
                        # Prepare the escaped JSON string outside the f-string
                        escaped_json = json_str.replace('`', '\\`')
                        # if st.button("📋 Copy to Clipboard"):
                        #     # Use JavaScript to copy to clipboard
                        #     st.markdown(f"""
                        #     <script>
                        #     navigator.clipboard.writeText(`{escaped_json}`);
                        #     </script>
                        #     """, unsafe_allow_html=True)
                        #     st.success("Copied to clipboard!")
                
                # Add button to insert results into database
                st.markdown("### Database Operations")
                
                # Only show the insert button if connected to database
                if st.session_state.data_source == "Database":
                    insert_button = st.button("💾 Insert Results into Database", type="primary", help="Save the current ICP results to the database.")
                    
                    if insert_button:
                        with st.spinner("Inserting results into database..."):
                            save_success = save_icp_results_to_db(
                                st.session_state.engine, 
                                st.session_state.jobs_processed, 
                                st.session_state.icp_results
                            )
                            if save_success:
                                st.markdown('<div class="success-box">✅ Results successfully inserted into database!</div>', unsafe_allow_html=True)
                else:
                    st.info("Database insertion is only available when using Database as the data source.")
            else:
                st.info("No ICP results available. Please generate ICP first in the 'Generate ICP' tab.")
    
    else:
        if st.session_state.data_source == "Database":
            st.markdown('<div class="warning-box">⚠️ Please connect to the database in the sidebar to continue.</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="warning-box">⚠️ Please upload a CSV file in the sidebar to continue.</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main()



