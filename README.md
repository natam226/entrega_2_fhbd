
# Proyecto 2: Arquitectura Lakehouse y Pipeline de Ingesta de Datos

Este proyecto implementa una arquitectura Lakehouse completa utilizando Docker para orquestar todos los servicios. La arquitectura sigue el modelo Medallion (Bronze, Silver, Gold) para procesar y analizar datos del dataset de [StackOverflow](https://clickhouse.com/docs/getting-started/example-datasets/stackoverflow).

### Integrantes del Proyecto
*[Liseth Esmeralda Erazo Varela](https://github.com/memerazo)*

*[Natalia Moreno Montoya](https://github.com/natam226)*

*[Natalia Lopez Gallego](https://github.com/ntlg72)*

*[Valentina Bueno Collazos](https://github.com/valentinabc19)*

---

## Arquitectura General

El proyecto utiliza los siguientes componentes:
- **MinIO**: Almacenamiento de objetos compatible con S3.
- **Nessie**: Catálogo de metadatos para tablas Iceberg.
- **Apache Iceberg**: Formato de tabla para el data lake.
- **Apache Spark**: Motor de procesamiento distribuido para las transformaciones.
- **Apache Airflow**: Orquestador de flujos de trabajo (pipelines).
- **DLT (Data Load Tool)**: Herramienta para la ingesta de datos en la capa Bronze.
- **Jupyter Notebook**: Para ejecución manual y pruebas.
- **Dremio**: Motor de consultas SQL para la capa Gold.
- **Trino**: Motor de consultas SQL para la capa Silver.

## 1. Despliegue de los Contenedores

Para desplegar la infraestructura completa, se utiliza `docker-compose`.

### Prerrequisitos
- Docker y Docker Compose instalados en tu máquina.

### Pasos para el Despliegue

1.  **Clona el repositorio:**
    ```bash
    git clone https://github.com/natam226/entrega_2_fhbd
    cd entrega_2_fhbd
    ```

2.  **Configura las variables de entorno de Airflow:**
    Use el archivo de ejemplo [airflow.env.example](./airflow.env.example) y agregue los valores de cada variable para su caso

3.  **Levanta los servicios con Docker Compose:**
    Ejecuta el siguiente comando en la raíz del proyecto para construir y levantar todos los contenedores en modo detached (-d):
    ```bash
    docker-compose up -d
    ```

4.  **Verifica que todos los contenedores estén en ejecución:**
    Puedes listar los contenedores activos con el siguiente comando:
    ```bash
    docker-compose ps
    ```
    Deberías ver todos los servicios (MinIO, Nessie, Spark, Airflow, Postgres, Jupyter, Dremio, Trino) en estado "Up" o "Running".

## 3. Acceso a los Servicios

Una vez que los contenedores están en ejecución, puedes acceder a las interfaces de usuario (UI) de los diferentes servicios a través de tu navegador web.

### MinIO (Object Storage)

-   **URL:** [http://localhost:9001](http://localhost:9001)
-   **Usuario:** `admin`
-   **Contraseña:** `password`
-   **Uso:** Aquí podrás ver y crear los buckets y los archivos Parquet e Iceberg generados en las capas Bronze, Silver y Gold.

### Apache Airflow (Orquestación)

-   **URL:** [http://localhost:8080](http://localhost:8080)
-   **Usuario:** `airflow`
-   **Contraseña:** `airflow`
-   **Uso:** Desde aquí puedes activar, monitorizar y depurar el DAG `stack_overflow_pipeline` que orquesta todo el proceso.
  > **Antes de la ejecución**: No olvides cargar la tabla de posts a MinIO de manera manual ...

### Jupyter Notebook (Ejecución Manual)

-   **URL:** [http://localhost:8888](http://localhost:8888)
-   **Uso:** Permite ejecutar los notebooks (`bronze_ingest.ipynb`, `silver_transform.ipynb`, `gold_agg.ipynb`) de forma manual para pruebas o en caso de fallo del DAG de Airflow.

### Dremio (Consulta SQL - Gold)

-   **URL:** [http://localhost:9047](http://localhost:9047)
-   **Uso:** Dremio se conecta a la capa Gold. La primera vez que accedas, te pedirá crear un usuario administrador. Luego, deberás configurar la conexión a MinIO y al catálogo de Nessie para poder consultar las tablas de la capa Gold.

### Trino (Consulta SQL - Silver)

-   **URL:** [http://localhost:8090](http://localhost:8090) (La interfaz de Trino es básica, se usa principalmente a través de un cliente SQL)
-   **Usuario:** `admin` 
-   **Uso:** Conéctate a Trino usando un cliente SQL como DBeaver o el CLI de Trino para ejecutar consultas sobre las tablas de la capa Silver.

---

 > **⚠️ Al terminar**: No olvides apagar tus contenedores de Docker usando el comando `docker compose down`.
