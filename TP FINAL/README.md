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





