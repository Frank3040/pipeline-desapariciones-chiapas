# Pipeline de Datos: Desapariciones de Niños, Niñas y Adolescentes en Chiapas

Este proyecto implementa un pipeline ELT (Extract, Load, Transform) completo y un dashboard interactivo para analizar datos sobre desapariciones de niños, niñas y adolescentes en Chiapas, México. El sistema está contenerizado utilizando Docker y orquestado mediante Apache Airflow, con PostgreSQL como base de datos y Flask para la visualización.

## Contexto del Dataset

El dataset proviene de la plataforma [datamx](https://datamx.io/about). Especificamente, el dataset usado es [DESAPARICIÓN DE NIÑAS, NIÑOS Y ADOLESCENTES EN CHIAPAS 2019-2025](https://datamx.io/dataset/desaparicion-de-ninas-ninos-y-adolescentes-en-chiapas-2019-2025).

### Relevancia Social
Este dataset es de crítica importancia social y humanitaria. La desaparición de menores es una crisis que afecta profundamente el tejido social, la seguridad y los derechos humanos fundamentales. Contar con datos estructurados y accesibles es el primer paso para visibilizar la magnitud del problema y movilizar recursos de manera efectiva.

### Problema a Resolver
El análisis de estos datos permite abordar la falta de información centralizada y procesable. Al identificar patrones, como los municipios con mayor incidencia, los grupos etarios más vulnerables o las tendencias temporales, se pueden diseñar estrategias de prevención más efectivas y optimizar los esfuerzos de búsqueda y localización.

### Beneficiarios
Analizar estos datos beneficia a comunidades y familias al proporcionarles información transparente sobre su entorno; a los organismos gubernamentales les brinda insumos para decisiones basadas en evidencia y una mejor asignación de recursos de seguridad; a ONGs y colectivos de búsqueda les ofrece herramientas que fundamentan sus demandas y focalizan sus acciones; y a la sociedad civil le genera mayor conciencia sobre la problemática.



## Arquitectura del Proyecto

![Arquitectura del proyecto](/screenshots/pipeline_diagram.jpg)

El proyecto utiliza una arquitectura moderna basada en contenedores:

1.  **Orquestación (Apache Airflow):** Gestiona el flujo de trabajo ELT.
    *   **Sensor:** Detecta la llegada de nuevos archivos de datos.
    *   **Carga:** Ingesta datos crudos CSV a PostgreSQL (`schema: raw`).
    *   **Transformación:** Limpieza y normalización de datos mediante SQL (`schema: processed`).
    *   **Modelado:** Creación de vistas analíticas (`schema: trusted`).
    *   **Permisos:** Gestión automática de roles de base de datos para seguridad.
2.  **Almacenamiento (PostgreSQL):** Base de datos relacional con esquema en capas (Raw -> Processed -> Trusted).
3.  **Visualización (Flask + Chart.js):** Dashboard web que consume las vistas confiables para mostrar KPIs y gráficos en tiempo real.
4.  **Infraestructura (Docker):** Todo el entorno se despliega mediante `docker-compose`.



## Instrucciones de Reproducción

Sigue estos pasos para levantar el proyecto desde cero en tu máquina local.

### Prerrequisitos
*   Docker y Docker Compose instalados.
*   Git.

### Pasos

1.  **Clonar el Repositorio**
    ```bash
    git clone <url-del-repositorio>
    cd pipeline_missing_kids
    ```

2.  **Preparar los Datos**
    Asegúrate de que el archivo de datos fuente esté en la ubicación correcta. El pipeline espera encontrar el archivo `base-desapariciones-dataton-2025.csv` en:
    ```
    dags/data/raw/base-desapariciones-dataton-2025.csv
    ```
    *(Si no tienes el archivo, colócalo en esa ruta antes de continuar).*

3.  **Iniciar los Servicios**
    Construye y levanta los contenedores:
    ```bash
    docker-compose up -d --build
    ```

4.  **Ejecutar el Pipeline**
    *   Abre tu navegador y ve a Airflow: [http://localhost:8080](http://localhost:8080).
    *   Usuario/Contraseña por defecto: `airflow` / `airflow`.
    *   Busca el DAG llamado `desapariciones_pipeline`.
    *   Actívalo (toggle ON) y ejecútalo (botón Play).

5.  **Ver el Dashboard**
    Una vez que el DAG haya completado sus tareas (especialmente `create_trusted_views` y `setup_db_permissions`), accede al dashboard de visualización:
    *   URL: [http://localhost:5000](http://localhost:5000)


## Estructura del Proyecto

```
pipeline_missing_kids/
├── dags/
│   ├── dashboard/          # Código de la aplicación Flask
│   │   ├── templates/      # HTML del dashboard
│   │   └── app.py          # Backend del dashboard
│   ├── data/raw/           # Directorio de entrada de datos
│   ├── scripts/            # Scripts Python para tareas ELT
│   ├── sql/                # Scripts SQL (DDL y Transformaciones)
│   └── dag.py              # Definición del DAG de Airflow
├── docker-compose.yaml     # Definición de servicios Docker
├── Dockerfile              # Imagen personalizada de Airflow
└── init-scripts/           # Scripts de inicialización de BD
```
