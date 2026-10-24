José López Gaffney
student_id: jose1
escala: small

1)Con la red partida, ¿qué respondió la réplica C en modo CP y qué respondió en modo AP? ¿Qué propiedad de CAP sacrifica cada modo?

En modo CP, la réplica C respondió con error: la lectura dio ERROR: no disponible y la escritura ERROR: sin quórum. En modo AP respondió 100, un dato viejo, porque ya se había escrito 80 del otro lado. CP sacrifica disponibilidad y AP sacrifica consistencia.

2)En modo AP, ¿qué saldo quedó en las tres réplicas después de la reparación y qué escritura se perdió?
3)Según el gráfico de latencia, ¿cuál es la p99 con W=1 y con W=3? ¿Qué se gana a cambio de esperar más réplicas? (PACELC)
4)Con cinco réplicas partidas en ABC | DE, ¿qué lado pudo seguir escribiendo en modo CP y por qué?
5)¿Cuántas veces más rápido fue el GET en memoria que el GET con Spark? ¿Por qué Redis guarda los datos en memoria?
6)¿Por qué para contar los clientes de AR hubo que leer y parsear todos los valores? Mencioná una ventaja y una desventaja del modelo clave-valor.
7)Al pasar de 4 a 5 nodos, ¿qué porcentaje de claves se movió con hash mod N y con hashing consistente? ¿Por qué conviene el segundo?
8)¿Cuántas filas devolvió la consulta relacional del cliente 42 y cuántos documentos la documental? ¿Qué datos quedaron embebidos dentro del documento?
9)Al leer documentos con campos distintos, ¿qué hizo Spark con los campos que faltaban en algunos? ¿Por qué se dice que el modelo documental tiene esquema flexible?
10)¿Cuánto pesa el documento más grande? ¿Qué problema aparece si un documento crece sin límite?
11)Para calcular el monto por categoría hubo que usar explode. ¿Qué tipo de consultas resuelve bien el modelo documental y cuáles le cuestan más?
12)¿Cuántos nodos Customer, nodos Device y aristas USES tiene el grafo?
13)¿Cuántos joins necesitó SQL para el patrón de 2 saltos y para el de 4? ¿Por qué Neo4j recorre relaciones más eficientemente que una base relacional?
14)¿Cuántas iteraciones tardó en converger la búsqueda de componentes conexos y cuántos nodos tiene el componente más grande?
15)¿Qué porcentaje de aristas quedó entre máquinas distintas al repartir los nodos con hash(id) mod 4? ¿Por qué es difícil distribuir una base de grafos?
16)¿Qué representa el vector de cada cliente y qué mide la similitud coseno?
17)¿Qué porcentaje de clientes marcados hay entre los vecinos de clientes marcados y cuál es la tasa base? ¿Para qué sirve buscar "vecinos parecidos"?
18)En la curva del índice IVF, ¿qué recall y qué porcentaje de vectores escaneados se obtienen con nprobe=4? ¿Qué se gana y qué se pierde con la búsqueda aproximada (ANN)?
19)¿Cuánto ocupan los datos en CSV, JSON y Parquet? ¿Qué columnas leyó Spark para la consulta de amount sobre Parquet (ReadSchema)? ¿Cuántos archivos se pueden saltear con los datos ordenados?
20)¿Por qué el formato columnar es bueno para analítica y poco conveniente para modificar una fila por vez?
