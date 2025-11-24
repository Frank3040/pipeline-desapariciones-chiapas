CREATE SCHEMA IF NOT EXISTS processed;

CREATE TABLE IF NOT EXISTS processed.desapariciones_niños_processed (
    id SERIAL PRIMARY KEY,

    sexo VARCHAR(10),
    edad INTEGER,
    grupo_etario TEXT,

    municipio TEXT,
    estado TEXT,
    region TEXT,
    colonia_localidad TEXT,

    migrante TEXT, -- Normalizado: 'Yes' / 'No'

    fecha_desaparicion DATE,
    dia_semana TEXT,
    horario TEXT,

    estatus TEXT,
    fecha_localizacion DATE,

    dias_sin_localizar INTEGER,
    rango_desaparicion TEXT,

    reincidencia TEXT, -- 'Yes' / 'No'
    numero_reincidencia TEXT,

    desaparicion_multiple TEXT, -- 'Yes' / 'No'
    persona_con_quien_desaparecio TEXT,

    hipotesis TEXT,
    fuente TEXT,
    sistematizo TEXT,

    processed_at TIMESTAMP DEFAULT NOW()
);
