Jose Lopez Gaffney
student_id: jose1
Escala: small

1)En el ejemplo en Python, ¿cuántos mensajes quedaron en la cola y cuántos en el log después de leer tres? ¿Qué diferencia hay entre una cola tradicional y un log como Kafka?

Quedaron 2 en la cola (pago_4, pago_5) y 5 en el log (el consumidor sólo avanzó su offset a 3). 

En una cola tradicional leer un mensaje lo borra, mientras que por otro lado en un log como Kafka los mensajes se conservan.


2)Después de la llegada 2, ¿cuántos mensajes había leído el consumidor antifraude y cuántos reportes? ¿El atraso de reportes afectó a antifraude? ¿Por qué?

Después de la llegada 2, antifraude había leído 180 mensajes y reportes 0. El atraso de reportes no afecta a antifraude porque leer no borra nada del topic, ya que se sabe hasta donde se leyó con el offset; Avanza cada uno a su ritmo.


3)¿Qué es un offset y dónde lo guarda cada consumidor en esta práctica?

Un offset es la posición hasta donde se leyó en el log. En esta práctica lo guarda el checkpoint de cada consulta.


4)¿Cuántos mensajes leyó el replay? ¿Qué propone la arquitectura Kappa para reprocesar datos y en qué se diferencia de Lambda?

El replay leyó 280 mensajes, es decir todos. 

Kappa propone un único código de streaming y, para reprocesar, lanzar una consulta nueva que lee el log desde el principio. Lambda mantiene dos sistemas: batch preciso y  streaming rápido, que tiene la misma lógica escrita dos veces.


5)¿Cuántas filas nuevas procesó Bronze en cada llegada y cuántas al reejecutar sin archivos nuevos? ¿Qué componente evita leer dos veces el mismo archivo?

Bronze procesó 60, 120, 40, 40 y 20 filas nuevas en las llegadas 1 a 5. 

Proceso 0 al reejecutar sin archivos nuevos, siendo que esto lo evita el checkpoint de Auto Loader que recuerda los archivos que ya leyó.


6)¿Qué motivos de rechazo aparecieron en la cuarentena y en qué llegada? ¿Por qué Bronze guarda el texto original en lugar de descartar los registros con errores?

En la cuarentena aparecieron 20 INVALID_AMOUNT y 20 UNKNOWN_CUSTOMER, ambos en la llegada stream_002. 

Bronze guarda el texto original para no perder información: tener mas info sirve para saber como proceder a futuro.


7)¿Qué columnas agregó el join con silver_customers y silver_products? ¿Qué tipo de join es (stream-stream, stream-tabla o tabla-tabla) según la teoría?

El join agregó country desde silver_customers y category desde silver_products. 

Es un join stream-tabla porque cada evento del stream se enriquece con una tabla estática de referencia.


8)¿Qué diferencia hay entre event time y processing time? Usá late_ok como ejemplo.

Event time es cuándo ocurrió el evento y processing time es cuándo llega al sistema. 

late_ok ocurrió a las 12:03 (event time) pero llegó en la llegada 2, después de eventos de las 12:04 de la llegada 1. Pero como las ventanas se calculan por event time, cuenta en la ventana 12:00–12:05 aunque llegue tarde.


9)¿Cuánto valía el watermark al terminar cada llegada? ¿Cómo se calcula a partir del máximo event time visto?

El watermark al terminar cada llegada valía 11:54, 11:58, 12:16, 12:17 y 12:35. 

El watermak es el máximo event time visto (12:04, 12:08, 12:26, 12:27 y 12:45) disminuido en 10 minutos.


10) ¿En qué llegada aparecieron las primeras ventanas tumbling en modo append y por qué no antes?

Aparecieron en la llegada 3, con 4 ventanas emitidas. Antes no, porque para que se cierre la ventana de 12 a 12:05 el watermark tuvo que haber superado a 12:05, siendo que recien se supera en la llegada 3.


11)¿Qué pasó con late_bad? ¿Por qué se aceptó late_ok y se descartó late_bad?

late_bad llegó en la llegada 4, siendo que habia ocurrido a las 12:02. Como el watermark de esta lleagda era 12:16, se debe descartar. 

late_ok se aceptó porque cuando llego el watermark valía 11:54, y se habia generado a las 12:03hs.


12)Mirando el gráfico de ventanas, ¿en qué se diferencian tumbling, hopping y session? Da un ejemplo de negocio para cada una.

Tumbling son bloques fijos de tiempo que no se solapan, siendo un evento cae en una sola ventana. Un caso podria ser: cuantas compras ingresaron en los ultimos 5 minutos.

En hopping los bloques son mas largo sque el salto con que avanzan: 10 min cada 5. De esta manera se solapan y un evento puede caer en 2 bloques. Un caso podria ser un promedio móvil de compras de los últimos 10 minutos para detectar picos de actividad.

En session no hay horarios fijos. Una sesion empieza cuando el cliente hace algo y termina cuando parsa un rato sin actividad. Un caso puede ser una sesión de compra de un cliente en la web.


13)Para la ventana [12:00, 12:05) del canal card, ¿cuántas veces y con qué valores se emitió en modo update y en modo append? ¿Qué ventaja y qué desventaja tiene cada modo?

Para la ventana del canal card, en update se emitió 2 veces: llegada 1 con 40 compras / $6000 y llegada 2 con 60 compras / $8400 (corregida con late_ok). En append se emitió 1 vez, en la llegada 3, con 60 compras / $8400. 

Update da resultados rápidos, pero el consumidor debe tolerar correcciones. Por otro lado, append entrega un valor final único y simple, pero con más demora.


14)¿Qué se gana y qué se pierde si el watermark fuera de 1 minuto en lugar de 10?

Con 1 minuto de watermark las ventanas se cerrarían antes, teniendo menos latencia en append. En cambio se pierde completitud: los eventos que lleguen con más de 1 minuto de retraso se descartarían, siendo que con 10 minutos hay más margen porque aquellos que llegan no tan tarde se catalogarian como late_ok.


15)¿Cuántas filas quedaron en el destino sin deduplicar y cuántas deduplicando por event_id? ¿Por qué el productor puede enviar el mismo evento dos veces?

Sin deduplicar quedaron 220 filas y deduplicando por event_id 200. 

El productor puede reenviar el mismo evento porque ante la duda reintenta.


16)¿Qué pasó al reiniciar con el mismo checkpoint y qué pasó al perderlo? ¿Qué garantía de entrega se observa en cada caso?

Al reiniciar con el mismo checkpoint el destino siguió en 200: el checkpoint sabe qué procesó y no reescribe nada. 

Perdiendo el checkpoint la consulta empezó de cero y el destino pasó a 400: se observa at-least-once, sin pérdida pero con duplicados.


17)¿Por qué con MERGE el resultado no cambió aunque se perdiera el checkpoint? ¿Qué significa que una escritura sea idempotente?

Con MERGE por event_id el destino se mantuvo en 200 aun con otro checkpoint: al reprocesar, los eventos que ya estaban no se insertan de nuevo. 

Una escritura es idempotente cuando al aplicarla dos veces produce el mismo efecto que aplicarla una sola vez.


18)¿Cuántos eventos de cambio generó el log y de qué tipos? ¿Por qué un UPDATE genera dos eventos?

El log generó 13 eventos de cambio: 6 insert, 3 update_preimage, 3 update_postimage y 1 delete. 

Un UPDATE genera dos eventos porque registra la fila antes (preimage) y después (postimage) del cambio.


19)¿Se pudo reconstruir la tabla a partir del log? ¿Qué información tiene el log que la tabla actual no tiene?

Se pudo reconstruir la tabla desde el log.

El log tiene toda la historia que la tabla perdió.


20)Según la comparación de motores de la teoría, ¿cuándo elegirías Spark Structured Streaming, Flink o Kafka Streams? Mencioná una ventaja de cada uno.

Elegiría Spark Structured Streaming si quiero unificar batch y streaming con el mismo código y SQL/DataFrames. 

Elegiría Flink cuando necesito latencia muy baja y manejo de estado y tiempo de eventos muy fino, ya que procesa evento por evento. 

Elegiría Kafka Streams para procesar directamente sobre Kafka como una librería embebida en una aplicación, sin tener que operar un cluster aparte.
