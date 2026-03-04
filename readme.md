# Steam Data Engineering Pipeline

Small data engineering project that extracts data from the Steam API and stores it in a Delta Lake data lake.

## Features

- Extract data from Steam API
- Convert API responses to pandas DataFrames
- Store data in Delta Lake format
- Incremental ingestion for player counts
- Full extraction for game metadata
- Modular ETL architecture

## Project Structure

config/      -> configuration files  
extract/     -> API extraction logic  
transform/   -> data transformation  
load/        -> data storage in Delta Lake  

main.py      -> pipeline orchestration

## Technologies

Python  
Pandas  
Delta Lake  
Requests  

---

# Pipeline de Ingeniería de Datos con Steam

Pequeño proyecto de ingeniería de datos que extrae información de la API de Steam y la almacena en un data lake utilizando Delta Lake.

## Características

- Extracción de datos desde la API de Steam
- Conversión de respuestas de API a DataFrames de pandas
- Almacenamiento en formato Delta Lake
- Ingestión incremental para conteo de jugadores
- Extracción completa de metadata de juegos
- Arquitectura ETL modular

## Estructura del Proyecto

config/      -> archivos de configuración  
extract/     -> lógica de extracción de API  
transform/   -> transformación de datos  
load/        -> almacenamiento en Delta Lake  

main.py      -> orquestación del pipeline