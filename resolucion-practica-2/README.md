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

En la etapa silver del JOB se ven que hay 1 que tiene INVALID_AMOUNT y un INVALID_AMOUNT en batch_002. Total: 2 rechazos en Batch_002

El copy into aparece en "la ingesta bronce", sineod que carga los archivos en el sistema sin procesar dos veces el mismo archivo. El merge into aparece en "la construccion de silver", sienod que actualiza o inserta registros cuando se actualizan

Las respuestas de la visualizacion, con sus graficos, están dentro del archivo "05_visualizacion"

1) El batch_002 recibió 204 filas, y el batch_003 otras 204
SELECT source_batch_id, COUNT(*) AS count
FROM bronze_transactions_incremental
GROUP BY source_batch_id
ORDER BY source_batch_id;

2) En esta etapa se rechazaron 2 de batch_002 y 2 de batch_003. En ambos casos fue una por INVALID_AMOUNT y una por UNKNOWN_CUSTOMER. Por otro lado, se logra divisar que se rechazan 51 por INVALID_AMOUNT
Esto concuerda con lo que se muestra en el gold batch summary

3)

4)

5)

6)

7)

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

