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

1) ¿Cuántas filas físicas recibió cada lote en bronze_transactions_incremental? Escribí una consulta que muestre el resultado por source_batch_id.

El batch_002 recibió 204 filas, y el batch_003 otras 204.


SELECT source_batch_id, COUNT(*) AS count
FROM bronze_transactions_incremental
GROUP BY source_batch_id
ORDER BY source_batch_id;


2) Para cada lote, ¿cuántas transacciones fueron aceptadas y cuántas quedaron en silver_transactions_quarantine? Reconciliá tus resultados con gold_batch_summary.

En esta etapa se rechazaron 2 de batch_002 y 2 de batch_003. En ambos casos fue una por INVALID_AMOUNT y una por UNKNOWN_CUSTOMER. Por otro lado, se logra divisar que se rechazan 51 por INVALID_AMOUNT

Esto concuerda con lo que se muestra en el gold batch summary


3)¿Qué motivos de rechazo aparecen en la cuarentena y cuántos registros tiene cada uno por lote? ¿Los rechazos observados coinciden con los casos introducidos por el generador?

Los rechazon fueron 51 del lote inicial por INVALID_AMOUNT, 2 en batch_002 y 2 en batch_003 por INVALID_AMOUNT y UNKNOWN_CUSTOMER.

Los rechazos coinciden con los casos que introduce el generador, porque cada lote nuevo trae dos filas inválidas a propósito: una con amount = 'N/A' y otra con un customer_id que no existe. En cambio, el duplicado exacto y la corrección de la transacción 42 no aparecen en cuarentena: el duplicado se elimina con dropDuplicates y la corrección es un registro válido. Los 51 INVALID_AMOUNT de initial vienen del generador de la práctica 1.

4)Seguí la transacción 42 desde bronze_transactions_all hasta silver_transactions. ¿Cuántas versiones existen en Bronze y cuál quedó vigente en Silver? Mostrá las columnas que justifican la elección.

En Bronze hay 3 versiones de la transaccion 42: la inicial (initial) y una corrección en cada lote (batch_002 y batch_003). En Silver queda una sola: la de batch_003, con importe 1999,99 porque tiene el updated_at mas reciente. La versión inicial pierde porque su updated_at es el event_at original, que es anterior a las dos correcciones.

5) Comprobá mediante una consulta que silver_transactions tiene una sola fila por transaction_id. ¿Qué resultado indicaría que la deduplicación falló?

SELECT transaction_id, COUNT(*) AS veces
FROM workspace.bigdata_jose1.silver_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;

Debe devolver 0 filas, si no, la deduplicación habría fallado

6)Calculá la tasa de rechazo de cada lote como rechazadas / (aceptadas + rechazadas) en gold_batch_summary. ¿Es correcto comparar solamente las cantidades absolutas si los lotes tienen tamaños diferentes?

Lote  Rechazadas  Total  Tasa
initial  51  49.999  0,102%
batch_002  2  202  0,990%
batch_003  2  203  0,985%

No es correcto comparar solamente las cantidades rechazadas, porque no tiene en cuenta la foto total. No tiene en cuenta los tamanos de lote, y ses ve que a pesar que el lote inicial haya tenido 51 rechazos, al tener una base mayor esto representa un rechazo significativamente menor

7) ¿Qué día presenta el mayor monto total y cuál presenta la mayor cantidad de transacciones? Consultá gold_daily_sales y explicá si ambos máximos coinciden.

El mayor monto y la mayor cantidad de transacciones ocurren el 1ro de Marzo ($7.310.840,77 y 7.236 transacciones).

SELECT sale_date, SUM(total_amount), SUM(transaction_count)
FROM workspace.bigdata_jose1.gold_daily_sales
GROUP BY sale_date
ORDER BY monto_total DESC;


8)¿Qué canal de pago tiene la mayor tasa global de fraude? Calculala como SUM(fraud_transactions) / SUM(transaction_count) y explicá por qué no corresponde promediar directamente fraud_rate.

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

9)¿Qué combinación de país y categoría concentra el mayor monto vendido? Mostrá también la combinación líder dentro de cada país.

La combinación con mayor monto es BR × home ($2.361.959,97). 


SELECT country, category, ROUND(SUM(total_amount), 2) AS monto
FROM workspace.bigdata_jose1.gold_daily_sales GROUP BY country, category ORDER BY monto DESC LIMIT 1;


La categoría líder dentro de cada país es: home en Brasil, home en Uruguar, books en Argentina, books en Mexico y home en Chile.


SELECT country, category, monto FROM (
  SELECT country, category, ROUND(SUM(total_amount), 2) AS monto,
         ROW_NUMBER() OVER (PARTITION BY country ORDER BY SUM(total_amount) DESC) AS rn
  FROM workspace.bigdata_jose1.gold_daily_sales GROUP BY country, category
) WHERE rn = 1 ORDER BY monto DESC;


10)Compará las dos primeras filas de pipeline_run_audit correspondientes a la reejecución de batch_002. ¿Qué métricas permanecen iguales y qué columna demuestra que se realizó la comparación de idempotencia?

Entre las dos ejecuciones de batch_002 no cambian silver_rows (50149), quarantine_rows (53), gold_rows (598) ni gold_total_amount (50286183.33). Cambian solo job_run_id y recorded_at. La columna que demuestra que se hizo la comparación de idempotencia es idempotence_compared, siendo que da false en la primera corrida porque no había una anterior, y true en la segunda.

11)En 01_ingest_bronze_incremental.ipynb, ¿qué problema resuelve COPY INTO y qué información utiliza para evitar cargar dos veces el mismo archivo físico?

El COPY INTO carga solo los archivos que todavia no cargo, porque Delta anota en el log de la tabla cuales ya proceso. De esta manera, al volver a ejecutarlo no recarga transactions_batch_002.csv.

12)¿Por qué la vista bronze_transactions_all usa UNION ALL en lugar de eliminar duplicados? ¿En qué capa se resuelven los duplicados de negocio y por qué?

La vista bronze_transactions_all junta la tabla inicial con la incremental. UNION compararia todas las filas para borrar repeitdos, y eso haria desaparecer ya en Bronze el duplicado exactyo que trae cada lote. Bronce tiene que ser un refistro fiel de lo que llego. Los duplicados de negocio se resuelven en Silver, esta es la capa donde se decide cuál versión vale.

13)¿Por qué las transacciones iniciales reciben source_batch_id='initial' y usan event_ts como updated_at? ¿Cómo afecta eso a la corrección de la transacción 42?

Las transacciones iniciales no vienen de ningún lote, por lo que se etiquetan 'initial', y como no tienen updated_at propio se usa event_ts. Eso hace que la versión original de la 42 tenga un updated_at viejo (primera semana de marzo) y que las correcciones de batch_002 y batch_003 la superen al ordenar y en el MERGE.

14)En quality_rules.py, ¿qué ventaja ofrece try_cast frente a un cast convencional cuando llega un importe como N/A?

Un cast convencional sobre 'N/A' lanzaría un error y haría que falle el job. En cambio, try_cast devuelve NULL, por lo que esa fila se manda a cuarentena con INVALID_AMOUNT y el pipeline sigue funcionando.

15)Las reglas de calidad asignan una única quality_reason. ¿Qué sucede si un registro viola más de una regla y por qué importa el orden de las condiciones?

El orden de las condiciones es: id, cliente, producto, fecha, importe, canal, flag de fraude, updtate_at, cliente desconociedo, prodcuto desonocido. Se va en ese orden, validando que cada campo sea válido, y cuando no cumple una se registra y no se continúa. Supongamos que falla importe y clietne desconocido, se registra como INVALID_AMOUNT porque esa regla está primero.

16)Explicá cómo se construye _record_key y cómo se usa junto con row_number. ¿Qué caso cubre el hash cuando transaction_id no puede convertirse a un número?

Con dropDuplicate se eliminan las filas iguales, y luego se calcula la clave. Se toma el transaction_id si es numerico, y si no, un sha2 de todas las columnas. Luego, ejecuta row_number() por esa clave, ordenando por updated_at descendente, y se queda con la fila 1 que sería la mas reciente. Si el transaction_id no se puede convertir a número, queda NULL. De esta manera, sin el hash, todas esas filas caerían en la misma partición y row_number conservaría solo una, perdiendo registros distintos.

17)Interpretá las dos cláusulas principales del MERGE de silver_transactions. ¿Cuándo se actualiza una fila existente y cuándo se inserta una nueva?

Cuando el id existe y la versión nueva es mas reciente se hace un UPDATE. Si el id no existe se hace un INSERT. Si el id existe es igual o mas vieja no se hace nada. 

18)¿Por qué las tablas Gold se reconstruyen completamente en esta práctica mientras Silver se actualiza con MERGE? Mencioná una ventaja y una limitación de cada estrategia.

Gold se reconstruye completo con CREATE OR REPLACE a partir de Silver. Esto es simple y siempre consistente con Silver, pero recalcula todo en cada corrida y su costo crece con el volumen. Por otro lado, Silver se actualiza con MERGE. Esto toca solo las filas nuevas o corregidas y conserva lo cargado, pero exige una clave confiable y una columna de version, siendo mas complejo.


19)¿Por qué expected_batch_id no participa en la detección del archivo nuevo? Indicá qué parte del pipeline descubre batch_003 y qué parte utiliza el parámetro.

Se descubre batch_003 en la tarea ingest_bronze, que se revisa la carpeta de entrada y detecta el archivo nuevo con el COPY INTO. El expected_batch_id lo usa solamente 04_validate_pipeline para saber qué lote verificar. Si se deposita batch_003 pero se deja expected_batch_id = batch_002, el ETL procesa igual el archivo nuevo, pero validate falla por evaluar las expectativas del lote equivocado.

20)Si la tarea build_silver falla, ¿qué ocurre con build_gold y validate en el Job? Explicá cómo las dependencias del DAG evitan publicar o validar resultados incompletos.

Una tarea corre solo si todas las que depende terminaron bien: Si falla build_silver falla, build_gold y validate no se ejecutan. De esta manera, no se publican tablas Gold calculadas sobre un Silver incompleto ni se valida un resultado parcial.


