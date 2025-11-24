CREATE SCHEMA IF NOT EXISTS trusted;

-- =========================================================
-- 1. Conteo anual de desapariciones
-- =========================================================
CREATE OR REPLACE VIEW trusted.v_desapariciones_por_anio AS
SELECT
    DATE_PART('year', fecha_desaparicion) AS anio,
    COUNT(*) AS total_casos
FROM processed.desapariciones_niños_processed
WHERE fecha_desaparicion IS NOT NULL
GROUP BY 1
ORDER BY 1;


-- =========================================================
-- 2. Desapariciones por sexo
-- =========================================================
CREATE OR REPLACE VIEW trusted.v_desapariciones_por_sexo AS
SELECT
    sexo,
    COUNT(*) AS total_casos
FROM processed.desapariciones_niños_processed
GROUP BY sexo
ORDER BY total_casos DESC;


-- =========================================================
-- 3. Desapariciones por grupo etario
-- =========================================================
CREATE OR REPLACE VIEW trusted.v_desapariciones_por_grupo_etario AS
SELECT
    grupo_etario,
    COUNT(*) AS total_casos
FROM processed.desapariciones_niños_processed
GROUP BY grupo_etario
ORDER BY total_casos DESC;


-- =========================================================
-- 4. Casos por región
-- =========================================================
CREATE OR REPLACE VIEW trusted.v_desapariciones_por_region AS
SELECT
    region,
    COUNT(*) AS total_casos
FROM processed.desapariciones_niños_processed
GROUP BY region
ORDER BY total_casos DESC;


-- =========================================================
-- 5. Casos por municipio
-- =========================================================
CREATE OR REPLACE VIEW trusted.v_desapariciones_por_municipio AS
SELECT
    municipio,
    COUNT(*) AS total_casos
FROM processed.desapariciones_niños_processed
GROUP BY municipio
ORDER BY total_casos DESC;


-- =========================================================
-- 6. Estado actual de la persona (Localizada vs No localizada)
-- =========================================================
CREATE OR REPLACE VIEW trusted.v_estatus_personas AS
SELECT
    estatus,
    COUNT(*) AS total_casos
FROM processed.desapariciones_niños_processed
GROUP BY estatus
ORDER BY total_casos DESC;


-- =========================================================
-- 7. Promedio de días sin localizar por grupo etario
-- =========================================================
CREATE OR REPLACE VIEW trusted.v_promedio_dias_sin_localizar_por_grupo AS
SELECT
    grupo_etario,
    AVG(dias_sin_localizar) AS promedio_dias
FROM processed.desapariciones_niños_processed
WHERE dias_sin_localizar IS NOT NULL
GROUP BY grupo_etario
ORDER BY promedio_dias DESC;


-- =========================================================
-- 8. Casos por día de la semana
-- =========================================================
CREATE OR REPLACE VIEW trusted.v_desapariciones_por_dia_semana AS
SELECT
    dia_semana,
    COUNT(*) AS total_casos
FROM processed.desapariciones_niños_processed
GROUP BY dia_semana
ORDER BY total_casos DESC;


-- =========================================================
-- 9. Casos por rango de desaparición (mañana/tarde/noche)
-- =========================================================
CREATE OR REPLACE VIEW trusted.v_desapariciones_por_rango AS
SELECT
    rango_desaparicion,
    COUNT(*) AS total_casos
FROM processed.desapariciones_niños_processed
GROUP BY rango_desaparicion
ORDER BY total_casos DESC;


-- =========================================================
-- 10. Casos de reincidencia (sí/no)
-- =========================================================
CREATE OR REPLACE VIEW trusted.v_reincidencia AS
SELECT
    reincidencia,
    COUNT(*) AS total_casos
FROM processed.desapariciones_niños_processed
GROUP BY reincidencia
ORDER BY total_casos DESC;


-- =========================================================
-- 11. Desapariciones múltiples (sí/no)
-- =========================================================
CREATE OR REPLACE VIEW trusted.v_desapariciones_multiples AS
SELECT
    desaparicion_multiple,
    COUNT(*) AS total_casos
FROM processed.desapariciones_niños_processed
GROUP BY desaparicion_multiple
ORDER BY total_casos DESC;


-- =========================================================
-- 12. Casos por año y sexo (útil para gráficos apilados)
-- =========================================================
CREATE OR REPLACE VIEW trusted.v_desapariciones_por_anio_y_sexo AS
SELECT
    DATE_PART('year', fecha_desaparicion) AS anio,
    sexo,
    COUNT(*) AS total_casos
FROM processed.desapariciones_niños_processed
WHERE fecha_desaparicion IS NOT NULL
GROUP BY anio, sexo
ORDER BY anio, sexo;


-- =========================================================
-- 13. Casos por región y sexo
-- =========================================================
CREATE OR REPLACE VIEW trusted.v_desapariciones_por_region_y_sexo AS
SELECT
    region,
    sexo,
    COUNT(*) AS total_casos
FROM processed.desapariciones_niños_processed
GROUP BY region, sexo
ORDER BY region, sexo;


-- =========================================================
-- 14. Casos por municipio y sexo
-- =========================================================
CREATE OR REPLACE VIEW trusted.v_desapariciones_por_municipio_y_sexo AS
SELECT
    municipio,
    sexo,
    COUNT(*) AS total_casos
FROM processed.desapariciones_niños_processed
GROUP BY municipio, sexo
ORDER BY municipio, sexo;
