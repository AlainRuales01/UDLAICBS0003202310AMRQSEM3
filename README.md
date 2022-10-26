# Repositorio Análisis de datos - Alain Ruales

<h2>UDLAICBS0003202310AMRQSEM3</h2>

<h2>Descripción de repositorio</h2>
En este repositorio, se tiene un proyecto que implementa concepto de bodega de datos, en donde se tienen métodos de extracción, transformación y carga de datos desde CSV.
El proyecto fue realizado con python 3.10.8 y con base de datos MySQL 8.

<h2>Creación de bases de datos</h2>
Antes de la ejecución del proyecto es necesario crear las diferentes bases de datos dentro de MySQL. Para esto, se deben ejecutar los siguientes scripts:

- DataBaseCreation.sql: Script utilizado para crear las bases de datos amrqdbsor y amrqdbstg 
- TablesCreation.sql: Script utilizado para crear las tablas en la base de datos amrqdbsor
- TablesCreationStg.sql: Script utilizado para la creación de tablas en la base de datos amrqdbstg
- TablesCreationTra.sql: Script utilizado para la creación de tablas de transform en la base de datos amrqdbstg
- TableCreationETLSync.sql: Script utilizado para la creación de una tabla que tendrá la información de los procesos ETL ejecutados

<h2>IMPORTANTE</h2>
Las tablas de transform tienen agregadas un campo llamado ETL_SYNC_ID, el cual representa el proceso ETL en el cual un registro fue agregado. Este valor es utilizado posteriormente en el proceso de load para tomar solo los datos de las tablas transform que fueron sincronizados en el proceso actual. Esto se hace con el objetivo de comparar cambios con datos que recientemente fueron agregados.

<h2>Paquetes necesarios</h2>

- Jproperties
- Pandas
- SqlAlchemy
- PyMySQL

<h2>Configuración de bases de datos</h2>

Para configurar la base de datos que se va a utilizar, se debe cambiar los valores en el archivo .properties
<img src="https://user-images.githubusercontent.com/87552871/196557112-7b41abec-95b2-4a76-aa79-01c86b6933e1.png">

En este caso, se debe modificar la etiqueta DB_TARGET_STG para poder ejecutar la etapa da stage.
Estas configuraciones son leidas por el archivo configurationReader.py, el cual se encuentra en el directorio de util

<h2>Pruebas unitarias</h2>
Para ejecutar pruebas unitarias se debe ejecutar el archivo py_startup.py. Este es un archivo python que simplemente ejecuta los diferentes archivos que contienen la lógica necesaria para ejecutar el proceso ETL.

<h2>Evidencias</h2>
Las evidencias de los diagramas y el conteo de filas de la base de datos Stage, se encuentra dentro de la carpeta EvidencesSem3
Las evidencias del proceso de extracción y de carga se encuentran en la carpeta EvidencesSem4
