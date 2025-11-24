-- create_raw_desapariciones_table.sql
-- Raw schema table for kids disappearance dataset
-- Idempotent: creates table only if not exists

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.desapariciones_niños_raw (
    sexo TEXT,
    edad TEXT,
    grupo_etario TEXT,
    municipio TEXT,
    region TEXT,
    colonia_localidad TEXT,
    migrante TEXT,
    fecha_desaparicion TEXT,
    dia_semana TEXT,
    horario TEXT,
    estatus TEXT,
    fecha_localizacion TEXT,
    dias_sin_localizar TEXT,
    rango_desaparicion TEXT,
    reincidencia TEXT,
    numero_reincidencia TEXT,
    desaparicion_multiple TEXT,
    persona_con_quien_desaparecio TEXT,
    hipotesis TEXT,
    fuente TEXT,
    sistematizo TEXT
);
