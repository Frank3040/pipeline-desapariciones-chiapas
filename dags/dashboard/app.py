import os
import psycopg2
from flask import Flask, render_template, jsonify
import pandas as pd

app = Flask(__name__)

def get_db_connection():
    conn = psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'postgres'),
        database=os.environ.get('POSTGRES_DB', 'desapariciones'),
        user=os.environ.get('ANALYST_USER', 'data_analyst'),
        password=os.environ.get('ANALYST_PASSWORD', 'analyst123')
    )
    return conn

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/kpis')
def get_kpis():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # KPI 1: Total Cases
    cursor.execute("SELECT SUM(total_casos) FROM trusted.v_desapariciones_por_anio")
    total_cases = cursor.fetchone()[0] or 0
    
    # KPI 2: Localization Rate (Localizada / Total)
    cursor.execute("SELECT total_casos FROM trusted.v_estatus_personas WHERE estatus = 'Localizada'")
    res = cursor.fetchone()
    localized = res[0] if res else 0
    localization_rate = round((localized / total_cases * 100), 2) if total_cases > 0 else 0
    
    # KPI 3: Most Dangerous Municipality
    cursor.execute("SELECT municipio, total_casos FROM trusted.v_desapariciones_por_municipio ORDER BY total_casos DESC LIMIT 1")
    res = cursor.fetchone()
    top_municipio = f"{res[0]} ({res[1]})" if res else "N/A"

    conn.close()
    
    return jsonify({
        'total_cases': total_cases,
        'localization_rate': f"{localization_rate}%",
        'top_municipio': top_municipio
    })

@app.route('/api/charts')
def get_charts():
    conn = get_db_connection()
    
    df_year = pd.read_sql("SELECT anio, total_casos FROM trusted.v_desapariciones_por_anio ORDER BY anio", conn)

    df_sex = pd.read_sql("SELECT sexo, total_casos FROM trusted.v_desapariciones_por_sexo", conn)
    
    df_age = pd.read_sql("SELECT grupo_etario, total_casos FROM trusted.v_desapariciones_por_grupo_etario", conn)
    
    df_status = pd.read_sql("SELECT estatus, total_casos FROM trusted.v_estatus_personas", conn)
    
    conn.close()
    
    return jsonify({
        'year_trend': {
            'labels': df_year['anio'].tolist(),
            'data': df_year['total_casos'].tolist()
        },
        'sex_dist': {
            'labels': df_sex['sexo'].tolist(),
            'data': df_sex['total_casos'].tolist()
        },
        'age_dist': {
            'labels': df_age['grupo_etario'].tolist(),
            'data': df_age['total_casos'].tolist()
        },
        'status_dist': {
            'labels': df_status['estatus'].tolist(),
            'data': df_status['total_casos'].tolist()
        }
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
