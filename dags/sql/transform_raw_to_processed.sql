INSERT INTO processed.desapariciones_niños_processed (
    sexo,
    edad,
    grupo_etario,
    municipio,
    estado,
    region,
    colonia_localidad,
    migrante,
    fecha_desaparicion,
    dia_semana,
    horario,
    estatus,
    fecha_localizacion,
    dias_sin_localizar,
    rango_desaparicion,
    reincidencia,
    numero_reincidencia,
    desaparicion_multiple,
    persona_con_quien_desaparecio,
    hipotesis,
    fuente,
    sistematizo
)
SELECT
    NULLIF(TRIM(sexo), ''),
    -- Cast edad to INTEGER, handling non-numeric values
    CASE 
        WHEN TRIM(edad) ~ '^[0-9]+$' THEN TRIM(edad)::INTEGER 
        ELSE NULL 
    END,
    NULLIF(TRIM(grupo_etario), ''),
    -- Split municipio and estado
    TRIM(SPLIT_PART(municipio, ',', 1)),
    TRIM(SPLIT_PART(municipio, ',', 2)),
    NULLIF(TRIM(region), ''),
    NULLIF(TRIM(colonia_localidad), ''),
    NULLIF(TRIM(migrante), ''),
    -- Parse dates (Handle DD/MM/YYYY and MM/DD/YYYY)
    CASE 
        WHEN fecha_desaparicion ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
            CASE 
                WHEN SPLIT_PART(fecha_desaparicion, '/', 1)::INTEGER > 12 THEN TO_DATE(fecha_desaparicion, 'DD/MM/YYYY')
                ELSE TO_DATE(fecha_desaparicion, 'MM/DD/YYYY')
            END
        ELSE NULL 
    END,
    NULLIF(TRIM(dia_semana), ''),
    NULLIF(TRIM(horario), ''),
    NULLIF(TRIM(estatus), ''),
    CASE 
        WHEN fecha_localizacion ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
            CASE 
                WHEN SPLIT_PART(fecha_localizacion, '/', 1)::INTEGER > 12 THEN TO_DATE(fecha_localizacion, 'DD/MM/YYYY')
                ELSE TO_DATE(fecha_localizacion, 'MM/DD/YYYY')
            END
        ELSE NULL 
    END,
    -- Cast dias_sin_localizar to INTEGER
    CASE 
        WHEN TRIM(dias_sin_localizar) ~ '^[0-9]+$' THEN TRIM(dias_sin_localizar)::INTEGER 
        ELSE NULL 
    END,
    NULLIF(TRIM(rango_desaparicion), ''),
    NULLIF(TRIM(reincidencia), ''),
    NULLIF(TRIM(numero_reincidencia), ''),
    NULLIF(TRIM(desaparicion_multiple), ''),
    NULLIF(TRIM(persona_con_quien_desaparecio), ''),
    NULLIF(TRIM(hipotesis), ''),
    NULLIF(TRIM(fuente), ''),
    NULLIF(TRIM(sistematizo), '')
FROM raw.desapariciones_niños_raw;
