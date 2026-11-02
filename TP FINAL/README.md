Jose Lopez Gaffney
Variante 2: Ejecucion
Opcion A: Arquitectura medallion (Bronze, Silver, Gold) en Databricks


La idea de este proyecto es cargar los datos del relevamiento de precios mayoristas y minoristas, para que a medida que vaya pasando el tiempo y se vayan agregando mas datos, poder ir haciendo predicciones y analisis mas precisas. Con este proyecto se puede analizar la tendencia de los precios mayoristas (inflacion mayorias), la tendencia de los precios minoristas (inflacion minorista) y poder ver el efecto de la inflacion mayorista en la inflacion minorista futura. Por otro lado, se compara el precio de un mismo producto (identificado por EAN) entre comercios, se compara el precio de un mismo producto entre provincias, y se mide la calidad de lo que informa cada comercio.

Los datos se obtienen mediante la pagina del gobierno, que sube cada cierto tiempo:
https://www.datos.gob.ar/dataset/precios-claros-sepa-mayorista
Se utilizan los comprimidos zip del relevamiento, y un excel con informacion adicional llamado maestro-provincias.xlsx.
Tambien se utiliza una api del gobierno
https://apis.datos.gob.ar/georef/api/v2.0

Eleji utilizar este dataset ya que es uno publico, y su informacion es relevante para tomar desiciones como para aconsejar clientes

Formato de entrada: cada día llega un zip diario que contiene un zip por comercio, y adentro hay tres CSV: productos.csv, sucursales.csv y comercio.csv. 

Los notebook se corren en el orden que figuran: 00 Config, 01 Bronce, 01b Bronce, 02 Silver, 02b Silver y Gold

El bronce guarda los zips recibido sin corregir nada, agregando únicamente metadatos de trazabilidad. Es idempotente porque si se vuelve a ejecutar sin archivos nuevos, no se agrega nada. Por otro lado, si un zip viene roto o no se puede leer, no se corta la ingesta sino que queda registrado en la tabla de auditoría con su motivo y el proceso sigue con el resto.

En el siver arranca la limpieza. Por esto mmismo, las filas rechazadas conservan su texto original y la causa del rechazo (columnas_inesperadas, sin_fecha, sin_id_comercio_o_sucursal, sin_id_producto, precio_con_iva_invalido, precio_sin_iva_invalido, unidad_venta_invalida o sin_iva_mayor_que_con_iva). Una versión nueva reemplaza a la existente solo si su fecha de exportación es más reciente. Si la versión nueva viene con fallas de validación, se conserva la última versión válida. Tambien, procesa únicamente las fechas afectadas por datos nuevos. Otra caracteristica es que ejecutarlo con full_refresh = si reconstruye todo desde Bronze si hace falta cambiar el esquema. Esto sirve mas que nada a la hora de estar armando el codigo, por si se quiere agregar o cambiar algo.


