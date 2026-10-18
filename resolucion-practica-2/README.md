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

4)En Bronze existen 3 versiones: la inicial (initial) y una corrección en cada lote (batch_002 y batch_003). En Silver queda una sola: la de batch_003, con importe 1999,99. Se elige por tener el mayor updated_at; la versión inicial usa su event_ts como updated_at, que es anterior a las dos correcciones.

AMPLIAR!!

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

7) El mayor monto y la mayor cantidad de transacciones ocurren el 1ro de Marzo ($7.310.840,77 y 7.236 transacciones). Coinciden porque el ticket promedio es parecido entre días (≈ $1.010 ese día contra ≈ $1.002 global).

8)

9)

10)


11)

12)

13)

14)

15)

16)

17)

18)

19)

20)

21)

