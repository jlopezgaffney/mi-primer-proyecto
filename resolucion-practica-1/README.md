Nombre: José López Gaffney
srudent_id: jose1

1. Leer no es transformar:
  Un CSV es un archivo plano, por lo que esta todo pegado en un sting gigante.
  Un archivo Parquet tiene los tipos guardados dentro del mismo archivo
  El JSON es un archivo anidado, que tambien tiene formato texto, pero permite a Spark inferir los tipos

Experimento: Inferencia de tipos
  La inferencia tarda 10 segundos, 3 mas que el anterior. La inferencia intenta adivina el tipo de dato del CSV, por eso tarda eso 3 segundo extras. Puede ser intestable, ya que esta inferencia depende de los datos que esten presentes en el dataset. Puede ser que amount tenga 10(int), 1.5(float) o "no aplica"(sting). Entonces cuando se mergea todo, puede ser que tenga diferentes tipos de datos, y se rompa el modelo.
  En este caso aparece "N/A" en una celda, y por ello figura string en amount

3. Diagnostico Inicial de calidad
   Hay 50.011 filas, de las cuales 50.000 son únicos y 52 inválidos

4. Delta y plan de Accion
   El delta muestra la historia (En mi archivo hay 3 versiones en el Describe History porque corri 3 veces este codigo), la ubicacion, tamaño, un plan de ejecucion, etc. Se lo recubre de una capa de metadata, que ayuda con las propiedades ACID.
