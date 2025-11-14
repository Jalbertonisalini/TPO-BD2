# 📄 Trabajo Práctico: Sistema de Gestión de Aseguradoras (BD2)

Este proyecto implementa un sistema de backoffice para una compañía de seguros utilizando una arquitectura de persistencia políglota (las bases electas fueron **MongoDB** y **Redis**) en un entorno de desarrollo autocontenido de **GitHub Codespaces**.

## 🚀 Cómo Iniciar el Entorno

Este repositorio está 100% configurado para GitHub Codespaces. No se necesita ninguna instalación local.

1.  Hacer click en el botón verde **`<> Code`** en la página principal del repositorio.
2.  Ir a la pestaña **`Codespaces`**.
3.  Haz clic en **"Crear codespace en main"** (o en el codespace existente si ya fue creado).
4.  Esperar 1-2 minutos. Codespaces leerá la configuración `.devcontainer` y levantará automáticamente los tres servicios: la aplicación de Python, la base de datos MongoDB y la base de datos Redis.



---

## ⚙️ Ejecución del Proyecto

Todo el proyecto se ejecuta desde la terminal de Codespaces.

### Paso 1: Cargar los Datos (¡Hacer esto primero!)

Antes de poder consultar, poblar las bases de datos desde los archivos `.csv` provistos. Este comando limpia las bases de datos, lee los `.csv` y pobla MongoDB y Redis.

Abre una terminal en Codespaces (`Ctrl+Shift+Ñ`) y ejecuta:

```bash
python ./src/loader/load_data.py
```
Espera a que el script termine y muestre el mensaje "¡Carga de datos completada con éxito!".

## Ejecutar las consultas:
Todas las consultas y servicios se ejecutan usando `main.py` desde la terminal.

### Consultas de lectura (1-12)
Estas consultas solo reciben el numero de query como argumento 

```bash
python main.py 1
python main.py 2
python main.py 3
python main.py 4
python main.py 5
python main.py 6
python main.py 7
python main.py 8
python main.py 9
python main.py 10
python main.py 11
python main.py 12
```

### Servicios de escritura (13-15)
Estos servicios requieren argumentos adicionales.

- Nota Importante: Usar comillas `""` alrededor de cualquier argumento que contenga espacios (ej. `"Buenos Aires"` o `"Robo en cochera"`).

#### Servicio 13: ABM de Clientes

- Alta:

```bash
python main.py 13 alta <id_cliente> "<nombre>" "<apellido>" "<dni>" "<email>" "<telefono>" "<direccion>" "<ciudad>" "<provincia>"
```

- Modificación:

```bash
python main.py 13 modificar <id_cliente_a_modificar> <campo_a_modificar> "<nuevo_valor>"
```

- Baja:

```bash
python main.py 13 baja <id_cliente_a_dar_de_baja>
```
#### Servicio 14: Alta de Siniestro

- Alta:

```bash
python main.py 14 <id_siniestro> "<nro_poliza>" "<fecha_dd/mm/aaaa>" "<tipo>" <monto_estimado> "<descripcion>" "<estado>"
```

#### Servicio 15: Emisión de Póliza

- Emisión:

```bash
python main.py 15 "<nro_poliza>" <id_cliente> <id_agente> "<tipo>" "<fecha_inicio_dd/mm/aaaa>" "<fecha_fin_dd/mm/aaaa>" <prima_mensual> <cobertura_total> "<estado>"
```
