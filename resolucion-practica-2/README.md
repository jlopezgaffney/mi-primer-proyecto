Nombre: José López Gaffney
Studentid: jose1

Captura del JOB adjunta en el Repo
https://dbc-2a3056e0-a728.cloud.databricks.com/jobs/572335449947243?o=7474648454980404

Primera ejecucion del JOB:
job_run_id	expected_batch_id	silver_rows	quarantine_rows	gold_rows	gold_total_amount	recorded_at	idempotence_compared
684404254907536	batch_002	50149	53	598	50286183.33	2026-10-03T13:14:32.377+00:00	false

Segunda ejecucion del JOB:
job_run_id	expected_batch_id	silver_rows	quarantine_rows	gold_rows	gold_total_amount	recorded_at	idempotence_compared
489911397137198	batch_002	50149	53	598	50286183.33	2026-10-03T13:23:33.742+00:00	true
684404254907536	batch_002	50149	53	598	50286183.33	2026-10-03T13:14:32.377+00:00	false

Se ve claramente en la segunda ejecucion que son los mismos datos, ya que la idemportencia es verdadera, y el tiempo de corrida es significativametne inferior

En la etapa silver del JOB se ven que hay 1 que tiene INVALID_AMOUNT y un UKNOWN_CUSTOMER en batch_002. Total: 2 rechazos en Batch_002

El copy into aparece en "la ingesta bronce", sineod que carga los archivos en el sistema sin procesar dos veces el mismo archivo. El merge into aparece en "la construccion de silver", sienod que actualiza o inserta registros cuando se actualizan

Las respuestas de la visualizacion, con sus graficos, están dentro del archivo "05_visualizacion"

1) El batch_002 recibió 204 filas, y el batch_003 otras 204.


SELECT source_batch_id, COUNT(*) AS count
FROM bronze_transactions_incremental
GROUP BY source_batch_id
ORDER BY source_batch_id;


2) En esta etapa se rechazaron 2 de batch_002 y 2 de batch_003. En ambos casos fue una por INVALID_AMOUNT y una por UNKNOWN_CUSTOMER. Por otro lado, se logra divisar que se rechazan 51 por INVALID_AMOUNT
Esto concuerda con lo que se muestra en el gold batch summary


3) Los rechazon fueron 51 del lote inicial por INVALID_AMOUNT, 2 en batch_002 y 2 en batch_003 por INVALID_AMOUNT y UNKNOWN_CUSTOMER.

Los rechazos coinciden con los casos que introduce el generador, porque cada lote nuevo trae dos filas inválidas a propósito: una con amount = 'N/A' y otra con un customer_id que no existe. En cambio, el duplicado exacto y la corrección de la transacción 42 no aparecen en cuarentena: el duplicado se elimina con dropDuplicates y la corrección es un registro válido. Los 51 INVALID_AMOUNT de initial vienen del generador de la práctica 1.

4)En Bronze hay 3 versiones de la transaccion 42: la inicial (initial) y una corrección en cada lote (batch_002 y batch_003). En Silver queda una sola: la de batch_003, con importe 1999,99 porque tiene el updated_at mas reciente. La versión inicial pierde porque su updated_at es el event_at original, que es anterior a las dos correcciones.

5) 
SELECT transaction_id, COUNT(*) AS veces
FROM workspace.bigdata_jose1.silver_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;


Debe devolver 0 filas, si no, la deduplicación habría fallado

6)
Lote  Rechazadas  Total  Tasa
initial  51  49.999  0,102%
batch_002  2  202  0,990%
batch_003  2  203  0,985%

No es correcto comparar solamente las cantidades rechazadas, porque no tiene en cuenta la foto total. No tiene en cuenta los tamanos de lote, y ses ve que a pesar que el lote inicial haya tenido 51 rechazos, al tener una base mayor esto representa un rechazo significativamente menor

7) El mayor monto y la mayor cantidad de transacciones ocurren el 1ro de Marzo ($7.310.840,77 y 7.236 transacciones).


SELECT sale_date, SUM(total_amount), SUM(transaction_count)
FROM workspace.bigdata_jose1.gold_daily_sales
GROUP BY sale_date
ORDER BY monto_total DESC;


8)
Canal  Transacciones  Fraudes  Tasa
transfer  16.723  2.327  13,91%
wallet  16.724  2.169  12,97%
card  16.902  2.119  12,54%


SELECT payment_channel,
       SUM(transaction_count) AS transacciones,
       SUM(fraud_transactions) AS fraudes,
       ROUND(100 * SUM(fraud_transactions) / SUM(transaction_count), 2) AS tasa_fraude_pct
FROM workspace.bigdata_jose1.gold_daily_sales
GROUP BY payment_channel
ORDER BY tasa_fraude_pct DESC;


No corresponde promediar fraud_rate porque cada fila de gold_daily_sales tiene en cuenta día, país, categoría y canal. Cada fila tiene un número distinto de transacciones, y un promedio simple le daría el mismo peso a un grupo de 3 transacciones que a uno de 300. 

9)La combinación con mayor monto es BR × home ($2.361.959,97). 


SELECT country, category, ROUND(SUM(total_amount), 2) AS monto
FROM workspace.bigdata_jose1.gold_daily_sales GROUP BY country, category ORDER BY monto DESC LIMIT 1;


La categoría líder dentro de cada país es: home en Brasil, home en Uruguar, books en Argentina, books en Mexico y home en Chile.


SELECT country, category, monto FROM (
  SELECT country, category, ROUND(SUM(total_amount), 2) AS monto,
         ROW_NUMBER() OVER (PARTITION BY country ORDER BY SUM(total_amount) DESC) AS rn
  FROM workspace.bigdata_jose1.gold_daily_sales GROUP BY country, category
) WHERE rn = 1 ORDER BY monto DESC;


10)
Entre las dos ejecuciones de batch_002 no cambian silver_rows (50149), quarantine_rows (53), gold_rows (598) ni gold_total_amount (50286183.33). Cambian solo job_run_id y recorded_at. La columna que demuestra que se hizo la comparación de idempotencia es idempotence_compared, siendo que da false en la primera corrida porque no había una anterior, y true en la segunda.

11)(01_ingest_bronze_incremental, celda de COPY INTO) COPY INTO resuelve la carga incremental de archivos: lee solo los archivos nuevos de la carpeta de entrada. Para no cargar dos veces el mismo archivo físico, Delta guarda en el log de la tabla destino el registro de los archivos ya procesados, así que volver a ejecutarlo no recarga transactions_batch_002.csv.

12)(01_ingest_bronze_incremental, celda de la vista bronze_transactions_all) Se usa UNION ALL porque Bronze debe conservar todas las versiones tal como llegaron, y además es más barato que UNION, que deduplica. Los duplicados de negocio (mismo transaction_id con versiones distintas) se resuelven en Silver, la capa donde se decide cuál versión es la vigente.

13)(01_ingest_bronze_incremental, vista unificada) Las transacciones iniciales no vienen de ningún lote, por lo que se etiquetan 'initial', y como no tienen updated_at propio se usa event_ts como equivalente. Eso hace que la versión original de la 42 tenga un updated_at viejo (primera semana de marzo) y que las correcciones de batch_002 y batch_003 la superen al ordenar y en el MERGE.

14)(quality_rules.py, add_transaction_types) Un cast convencional sobre 'N/A' lanza un error (en modo ANSI) y tumba todo el job. try_cast devuelve NULL, de modo que esa fila se manda a cuarentena con INVALID_AMOUNT y el pipeline sigue funcionando.

15)(quality_rules.py, add_quality_reason) La columna se arma con una cadena de when(...).when(...): gana la primera condición que se cumple y las demás se ignoran. Si un registro viola varias reglas, solo se registra una, así que el orden de las condiciones define la prioridad. Por ejemplo, una fila con importe N/A y cliente desconocido se rechaza como INVALID_AMOUNT, porque esa regla está antes que UNKNOWN_CUSTOMER.

16)(02_build_silver, celda de latest) _record_key es coalesce(transaction_id numérico como texto, sha2(todas las columnas crudas)), y se usa como PARTITION BY en row_number para quedarse con la versión más reciente de cada registro. Si el transaction_id no se puede convertir a número, queda NULL; sin el hash, todas esas filas caerían en la misma partición y row_number conservaría solo una, perdiendo registros distintos. Con el hash, cada fila inválida distinta tiene su propia clave.

17)(02_build_silver, MERGE INTO silver_transactions)

WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *: si el transaction_id ya existe y la versión nueva es más reciente, se actualiza. Así se aplica la corrección de la 42.
WHEN NOT MATCHED THEN INSERT *: si el id no existe, se inserta.

18)(03_build_gold y 02_build_silver) Gold se reconstruye completo con CREATE OR REPLACE: es simple y determinista (reejecutar siempre da el mismo resultado), pero recalcula todo cada vez, por lo que el costo crece con el volumen. Silver se actualiza con MERGE: procesa solo lo que cambió y conserva el historial de la tabla Delta, pero es más complejo y depende de tener una buena clave y una columna de versión (updated_at).

19)(01_ingest_bronze_incremental y 04_validate_pipeline) Quien descubre batch_003 es COPY INTO, en la tarea ingest_bronze, que revisa la carpeta de entrada y detecta el archivo nuevo. expected_batch_id lo usa solamente 04_validate_pipeline, para saber qué lote verificar. Si se deposita batch_003 pero se deja expected_batch_id = batch_002, el ETL procesa igual el archivo nuevo, pero validate puede fallar por evaluar las expectativas del lote equivocado.

20)Si build_silver falla, build_gold y validate quedan omitidas (skipped) porque dependen de ella en el DAG lineal ingest_bronze → build_silver → build_gold → validate. Así no se publican tablas Gold calculadas sobre un Silver incompleto ni se valida un resultado parcial.


