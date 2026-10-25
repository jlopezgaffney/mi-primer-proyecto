José López Gaffney
student_id: jose1
escala: small

1)Con la red partida, ¿qué respondió la réplica C en modo CP y qué respondió en modo AP? ¿Qué propiedad de CAP sacrifica cada modo?

En modo CP, la réplica C respondió con error: la lectura dio ERROR: no disponible y la escritura ERROR: sin quórum. En modo AP respondió 100, un dato viejo, porque ya se había escrito 80 del otro lado. CP sacrifica disponibilidad y AP sacrifica consistencia.

2)En modo AP, ¿qué saldo quedó en las tres réplicas después de la reparación y qué escritura se perdió?

Quedó saldo = 50 en las tres réplicas. Se perdió la escritura saldo = 80 hecha en A: con last-write-wins ganó la versión más nueva (50, escrita en C) y esa compra desapareció.

3)Según el gráfico de latencia, ¿cuál es la p99 con W=1 y con W=3? ¿Qué se gana a cambio de esperar más réplicas? (PACELC)

La p99 es 9,6 ms con W=1 y 55,6 ms con W=3. A cambio de esperar a todas las réplicas (incluida la remota C, con +40 ms) se gana consistencia: toda lectura ve la última escritura. Es el trade-off latencia/consistencia de PACELC, incluso sin partición.

4)Con cinco réplicas partidas en ABC | DE, ¿qué lado pudo seguir escribiendo en modo CP y por qué?

Pudo seguir escribiendo el lado ABC, porque tiene 3 de 5 réplicas, que es la mayoría (quórum). El lado DE, con solo 2, recibió ERROR: sin quórum en la escritura y no disponible en la lectura

5)¿Cuántas veces más rápido fue el GET en memoria que el GET con Spark? ¿Por qué Redis guarda los datos en memoria?

El GET en memoria fue [COMPLETAR: ~N] veces más rápido que el GET con Spark. Redis guarda los datos en memoria porque su acceso es órdenes de magnitud más rápido que el de disco o el de un motor analítico, y una tabla hash da acceso O(1) por clave, algo necesario para responder en milisegundos.

6)¿Por qué para contar los clientes de AR hubo que leer y parsear todos los valores? Mencioná una ventaja y una desventaja del modelo clave-valor.

El país está dentro del valor JSON, que para la base es un texto opaco: no puede filtrar por él, así que hubo que parsear los 5.000 valores para encontrar los 992 clientes de AR. Ventaja: GET/SET por clave es O(1) y se reparte fácil entre nodos. Desventaja: no se puede consultar por el contenido del valor sin armar índices a mano.

7)Al pasar de 4 a 5 nodos, ¿qué porcentaje de claves se movió con hash mod N y con hashing consistente? ¿Por qué conviene el segundo?

Pasando de 4 a 5 nodos, hash mod N movió 79,6% de las claves y el hashing consistente 21,2% (≈ 1/(N+1)). Conviene el segundo porque al agregar un nodo solo se mueven las claves de su tramo del anillo, así que se transfieren muchos menos datos; con nodos virtuales además se reparte la carga parejo.

8)¿Cuántas filas devolvió la consulta relacional del cliente 42 y cuántos documentos la documental? ¿Qué datos quedaron embebidos dentro del documento?

La consulta relacional del cliente 42 devolvió 11 filas (3 tablas, 2 joins) y la documental 1 documento (0 joins). Quedaron embebidos el perfil (país, segmento, fecha de alta, email), las estadísticas (11 transacciones, 9.379,8 de monto, 1 fraude) y el arreglo de sus 11 transacciones, cada una con su producto (id, categoría, precio).

9)Al leer documentos con campos distintos, ¿qué hizo Spark con los campos que faltaban en algunos? ¿Por qué se dice que el modelo documental tiene esquema flexible?

Spark armó un esquema que es la unión de todos los campos y rellenó con null los que faltaban en cada documento. Se dice que el esquema es flexible porque cada documento puede traer campos distintos (loyalty, phones, preferences) sin declarar nada de antemano ni modificar una tabla: la estructura se infiere al leer (schema-on-read).

10)¿Cuánto pesa el documento más grande? ¿Qué problema aparece si un documento crece sin límite?

El documento más grande pesa 5,2 KB (≈ 225 bytes por transacción), y a ese ritmo llegaría a 16 MB con ≈ 74.599 transacciones. Si un documento crece sin límite se choca contra el máximo de MongoDB (16 MB) y cada lectura o actualización mueve un objeto cada vez más pesado; en esos casos conviene referenciar o partir el historial.

11)Para calcular el monto por categoría hubo que usar explode. ¿Qué tipo de consultas resuelve bien el modelo documental y cuáles le cuestan más?

Hubo que usar explode porque una pregunta que cruza documentos obliga a volver a filas. El modelo documental resuelve bien las consultas por entidad (todo el historial de un cliente sin joins, o filtros dentro del documento); le cuestan la analítica transversal (agregar por categoría entre todos los clientes), los joins entre colecciones y los datos duplicados al embeber (por ejemplo, el precio del producto repetido).

12)¿Cuántos nodos Customer, nodos Device y aristas USES tiene el grafo?

El grafo tiene 5.000 nodos Customer, 2.361 nodos Device y 5.400 aristas USES.

13)¿Cuántos joins necesitó SQL para el patrón de 2 saltos y para el de 4? ¿Por qué Neo4j recorre relaciones más eficientemente que una base relacional?

El patrón de 2 saltos necesitó 3 joins y el de 4 saltos 5 joins (cada flecha agrega un join). Neo4j recorre las relaciones más eficientemente porque cada nodo guarda punteros directos a sus vecinos (index-free adjacency): un salto cuesta seguir un puntero y no depende del tamaño total del grafo, mientras que en una base relacional cada salto es un join contra toda la tabla de aristas.

14)¿Cuántas iteraciones tardó en converger la búsqueda de componentes conexos y cuántos nodos tiene el componente más grande?

La propagación de etiquetas convergió en 14 iteraciones (en la 14 ya no cambió ningún nodo). El componente más grande tiene 23 nodos.

15)¿Qué porcentaje de aristas quedó entre máquinas distintas al repartir los nodos con hash(id) mod 4? ¿Por qué es difícil distribuir una base de grafos?

Con hash(id) mod 4 quedó entre máquinas distintas el [COMPLETAR: X]% de las aristas. Es difícil distribuir un grafo porque las aristas conectan nodos arbitrarios: al repartirlos por hash, los vecinos caen en máquinas distintas y cada salto cruza la red. Particionar por componentes corta menos aristas, pero en grafos reales hay componentes enormes y muy conectados que no se pueden separar sin cortar muchas relaciones.

16)¿Qué representa el vector de cada cliente y qué mide la similitud coseno?

El vector de cada cliente (10 dimensiones) describe su comportamiento de compra: la proporción de gasto en cada una de las 5 categorías, la proporción de transacciones por cada canal de pago, el importe promedio normalizado y la proporción de compras de ticket alto. La similitud coseno mide el ángulo entre dos vectores (1 = misma dirección), es decir, qué tan parecido es el perfil de comportamiento sin importar cuánto gastó cada cliente en total.

17)¿Qué porcentaje de clientes marcados hay entre los vecinos de clientes marcados y cuál es la tasa base? ¿Para qué sirve buscar "vecinos parecidos"?

Entre los 10 vecinos de los clientes marcados, el 31,6% también está marcado, contra una tasa base de 14,8% (y 13,2% entre los vecinos de no marcados). Buscar vecinos parecidos sirve para encontrar clientes con comportamiento similar a un sospechoso y priorizarlos para revisión. Ojo: como clasificador, la exactitud de k-NN (85,9%) no superó a predecir siempre “no marcado” (86,3%), así que sirve más para rankear candidatos que para decidir solo.

18)En la curva del índice IVF, ¿qué recall y qué porcentaje de vectores escaneados se obtienen con nprobe=4? ¿Qué se gana y qué se pierde con la búsqueda aproximada (ANN)?

Con nprobe=4 se obtiene un recall@10 de 0,872 (≈ 87%) escaneando solo el 6% de los vectores (5,985%). Se gana velocidad y menor costo (se comparan muchos menos vectores, y los clusters sirven para particionar); se pierde exactitud, porque ≈ 13% de los vecinos verdaderos quedan afuera. Es el trade-off recall vs costo: con nprobe=16 el recall llega a 1,0 pero se escanea el 24%.

19)¿Cuánto ocupan los datos en CSV, JSON y Parquet? ¿Qué columnas leyó Spark para la consulta de amount sobre Parquet (ReadSchema)? ¿Cuántos archivos se pueden saltear con los datos ordenados?

Parquet ocupa 9,4 veces menos que CSV y 22,8 veces menos que JSON (Parquet ≈ 5,3 MB; CSV ≈ 50 MB y JSON ≈ 121 MB aproximadamente; [COMPLETAR con los MB exactos del gráfico]). Para la consulta de amount, Spark leyó solo la columna amount (ReadSchema: struct<amount:decimal(12,2)>). Con los datos ordenados por amount se pueden saltear [COMPLETAR: X] de 8 archivos, contra 0 con los datos al azar.

20)¿Por qué el formato columnar es bueno para analítica y poco conveniente para modificar una fila por vez?

El formato columnar es bueno para analítica porque lee solo las columnas que la consulta necesita, comprime mejor (valores del mismo tipo juntos) y salta bloques enteros con las estadísticas min/max. Es poco conveniente para modificar una fila por vez porque esa fila está repartida entre todas las columnas y los archivos Parquet no se editan en sitio: un cambio obliga a reescribir archivos completos (como hace Delta), mientras que una base por filas toca un solo registro.
