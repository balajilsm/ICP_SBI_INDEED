# =============================================
# ICB (Ideal Candidate Blueprint) - Streamlit Web App
#  - Interactive database connection
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
from sqlalchemy import create_engine
from sqlalchemy import text
import cx_Oracle
import json
import openpyxl
from xgboost import XGBRegressor
from catboost import CatBoostRegressor
import io
import base64
from datetime import datetime
import time

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
</style>
""", unsafe_allow_html=True)

# ---------- Helper Functions ----------
def get_oracle_engine(user, password, host, port, sid):
    """Creates and returns a SQLAlchemy engine for Oracle."""
    try:
        dsn = cx_Oracle.makedsn(host, port, sid=sid)
        engine = create_engine(f"oracle+cx_oracle://{user}:{password}@{dsn}")
        return engine
    except Exception as e:
        st.error(f"Failed to create database connection: {e}")
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
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    return f"{round(q1)}-{round(q3)}"

def calculate_compensation_range(series: pd.Series) -> str:
    """Calculate ideal compensation range in k format."""
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
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
    
    # Sort the list alphabetically by name for consistent output
    final_list.sort(key=lambda x: x['name'])
    
    return final_list

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
    grouped_df["Influence (%)"] = 100 * grouped_df["Importance"] / grouped_df["Importance"].sum()
    
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

# --- NEW: Function to save ICP results to the database ---
def save_icp_results_to_db(engine, jobs_processed: List[str], icp_results: List[Dict]):
    """Saves the generated ICP results to the wfa_icp_results table."""
    if not engine or not icp_results:
        return

    json_output = json.dumps(icp_results, indent=4)
    text_output = format_icp_as_text(icp_results)
    generated_time = datetime.now()
    
    # Determine the job name for the database entry
    if len(jobs_processed) == 1:
        job_name_db = jobs_processed[0]
    else:
        job_name_db = f"All Jobs ({len(jobs_processed)} profiles)"
        
    try:
        with engine.connect() as conn:
            insert_stmt = text("""
                INSERT INTO wfa_icp_results (job_name, generated_time, json_output, text_output)
                VALUES (:job_name, :generated_time, :json_output, :text_output)
            """)
            conn.execute(insert_stmt, {
                "job_name": job_name_db,
                "generated_time": generated_time,
                "json_output": json_output,
                "text_output": text_output
            })
            conn.commit()
    except Exception as e:
        st.error(f"Failed to save ICP results to database: {e}")

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
        
        models_to_evaluate = {
            "XGBoost": XGBRegressor(n_estimators=400, max_depth=6, learning_rate=0.06, subsample=0.85, colsample_bytree=0.85, random_state=42, tree_method="hist"),
            "CatBoost": CatBoostRegressor(verbose=0, random_state=42),
            "LinearRegression": LinearRegression()
        }
        
        best_model = {"name": "", "pipeline": None, "rmse": float('inf')}
        X_train, X_test, y_train, y_test = train_test_split(X_all, y_all, test_size=0.25, random_state=42)
        
        model_results = []
        
        for name, model in models_to_evaluate.items():
            st.markdown(f'<div class="progress-step active">  -> Training {name}...</div>', unsafe_allow_html=True)
            pipe = Pipeline(steps=[("prep", prep), ("model", model)])
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
            
            top_imp_df = topk_importances(best_model['pipeline'], all_feature_names, feature_cols, k=5)
        except Exception as e:
            st.error(f"Error getting feature importances: {e}")
            top_imp_df = pd.DataFrame(columns=["Feature", "Influence (%)"])

        influence_with_priority = top_imp_df.merge(
            input_features_config[['attribute_name', 'attribute_order']],
            left_on='Feature',
            right_on='attribute_name',
            how='left'
        )
        influence_with_priority = influence_with_priority.sort_values(by=['attribute_order', 'Influence (%)'], ascending=[True, False])

        icb_profiles = []
        for job in selected_jobs:
            group_df = df[df[JOB_COL] == job]
            if len(group_df) < 30:
                continue
            
            profile = {"job": job, "count": len(group_df), "features": [], "influence": []}
            
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
        
        st.markdown(f'<div class="progress-step completed">✅ Successfully generated {len(icb_profiles)} ICP profiles!</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    return icb_profiles

def main():
    # --- LOGO SECTION ---
    logo_server_path = r'/icp_sbi_indeed/assets/ICPLOGO.png'
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
    
    if 'connected' not in st.session_state: st.session_state.connected = False
    if 'engine' not in st.session_state: st.session_state.engine = None
    if 'df' not in st.session_state: st.session_state.df = None
    if 'config_df' not in st.session_state: st.session_state.config_df = None
    
    st.sidebar.markdown('<h2 class="section-header">🔌 Database Connection</h2>', unsafe_allow_html=True)
    with st.sidebar.form("db_connection_form"):
        st.write("Enter your Oracle database connection details:")
        user = st.text_input("Username", value="csv_one_hd100")
        password = st.text_input("Password", value="csv_one_hd100", type="password")
        host = st.text_input("Host", value="192.168.4.23")
        port = st.text_input("Port", value="1521")
        sid = st.text_input("SID", value="dev")
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
    
    if st.session_state.connected:
        tab1, tab2, tab3 = st.tabs(["📋 Configure Attributes", "🎯 Select Jobs & Generate", "📊 Results"])
        
        with tab1:
            st.markdown('<h2 class="section-header">Attribute Configuration</h2>', unsafe_allow_html=True)
            original_config_df = st.session_state.config_df.copy()
            
            display_config_df = original_config_df.copy()
            display_config_df['use_in_input'] = display_config_df['use_in_input'] == 'Y'
            display_config_df['show_in_output'] = display_config_df['show_in_output'] == 'Y'
            
            edited_config = st.data_editor(
                display_config_df[['attribute_order', 'attribute_name', 'use_in_input', 'show_in_output', 'columns_in_view']],
                column_config={
                    "attribute_order": st.column_config.NumberColumn("Order", min_value=1, step=1, width="small"),
                    "attribute_name": st.column_config.TextColumn("Attribute Name", disabled=True),
                    "use_in_input": st.column_config.CheckboxColumn("Use in Input", help="Include this attribute in model training"),
                    "show_in_output": st.column_config.CheckboxColumn("Show in Output", help="Include this attribute in final ICP output"),
                    "columns_in_view": st.column_config.TextColumn("Columns in View", help="Comma-separated list of column names to look for")
                },
                key="config_editor"
            )
            
            if st.button("Save Configuration", type="primary"):
                with st.spinner("Saving configuration to database..."):
                    try:
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
        
        with tab2:
            st.markdown('<h2 class="section-header">Job Selection & ICP Generation</h2>', unsafe_allow_html=True)
            df = st.session_state.df
            TARGET = pick_col(df, ["TENURE_YEARS", "tenure", "years_at_company"])
            JOB_COL = pick_col(df, ["JOB_NAME", "job_group", "job_title", "position"])
            
            if not TARGET or not JOB_COL:
                st.error("Could not identify target data.")
            else:
                st.write(f"**Target Variable:** {TARGET}")
                st.write(f"**Job Column:** {JOB_COL}")
                unique_jobs = df[JOB_COL].value_counts()
                job_counts = pd.DataFrame({'Job': unique_jobs.index, 'Count': unique_jobs.values})
                
                st.write("### Available Jobs")
                selected_jobs = st.multiselect("Select jobs to generate ICP for:", options=job_counts['Job'].tolist(), 
                                             format_func=lambda x: f"{x} ({job_counts[job_counts['Job'] == x]['Count'].iloc[0]} records)")
                
                st.info("💡 Leave the selection empty to generate ICPs for all available jobs.")
                
                all_jobs = job_counts['Job'].tolist()
                jobs_to_process = selected_jobs if selected_jobs else all_jobs
                
                if st.button("Generate ICP Profiles", type="primary"):
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
                            # --- NEW: Save the results to the database ---
                            save_icp_results_to_db(st.session_state.engine, jobs_to_process, icp_results)
                            
                            st.session_state.icp_results = icp_results
                            st.markdown('<div class="success-box">✅ Check the Results tab for your ICP profiles.</div>', unsafe_allow_html=True)
        
        # --- REVERTED: Results tab is back to its original form ---
        with tab3:
            st.markdown('<h2 class="section-header">ICP Results</h2>', unsafe_allow_html=True)
            
            if 'icp_results' in st.session_state and st.session_state.icp_results:
                icp_results = st.session_state.icp_results
                
                # Display results
                for profile in icp_results:
                    with st.expander(f"📋 {profile['job']} ({profile['count']} employees)"):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write("**Features:**")
                            for feature in profile['features']:
                                if isinstance(feature['ideal'], dict):
                                    st.write(f"- {feature['feature']}:")
                                    for key, value in feature['ideal'].items():
                                        st.write(f"  - {key}: {value}")
                                elif isinstance(feature['ideal'], list):
                                    st.write(f"- {feature['feature']}:")
                                    for item in feature['ideal']:
                                        if isinstance(item, dict):
                                            details = ", ".join([f"{k}: {v}" for k, v in item.items()])
                                            st.write(f"  - {details}")
                                        else:
                                            st.write(f"  - {item}")
                                else:
                                    st.write(f"- {feature['feature']}: {feature['ideal']}")
                        
                        with col2:
                            st.write("**Influence Factors:**")
                            for influence in profile['influence']:
                                st.write(f"- {influence['feature']}: {influence['influence']:.2f}%")
                
                # Download
                st.markdown("### Export Options")
                
                # JSON download
                json_str = json.dumps(icp_results, indent=4)
                st.download_button(
                    label="📥 Download JSON",
                    data=json_str,
                    file_name=f"icp_profiles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
                
                # Copy to clipboard
                st.text_area("JSON Output (click to copy):", value=json_str, height=300)
                
                # Copy button
                if st.button("📋 Copy to Clipboard"):
                    st.write("JSON copied to clipboard! (You may need to manually copy from the text area above)")
            else:
                st.info("No ICP results available. Please generate ICP first in the 'Select Jobs & Generate' tab.")
    
    else:
        st.markdown('<div class="warning-box">⚠️ Please connect to the database in the sidebar to continue.</div>', unsafe_allow_html=True)

if __name__ == "__main__":

    main()
