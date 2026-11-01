Jose Lopez Gaffney
student_id: jose1

1) ¿Cuántas filas tiene cada split de ml_features y qué porcentaje de contracargos hay en cada uno? ¿Por qué se separa por fecha y no al azar?

train tiene 35.654 filas (11,05 % de contracargos), test 7.129 (10,38 %) y nuevas 7.165 (11,05 %). 

Se separa por fecha porque en la vida real el modelo se entrena con lo que ya pasó y se usa para el futuro. Si se mezcla al azar, el modelo termina viendo datos del futuro y la evaluación sale más linda de lo que va a ser en producción.


2) ¿Qué accuracy obtiene un "modelo" que siempre dice que no hay contracargo? ¿Por qué la práctica usa el PR-AUC como métrica principal?

Un modelo que siempre dice "no hay contracargo" tiene 89,6 % de accuracy y no detecta ninguno. Como son pocos contracargos (alrededor del 10 %), la accuracy no sirve y por eso se usa el PR-AUC, que mide qué tan bien ordena las transacciones de más a menos riesgosas.


3)¿Qué quedó registrado en el run 01_manual_logistica como parámetros, métricas y artefactos? ¿Qué diferencia hay entre un experimento y un run?

En 01_manual_logistica quedaron como parámetros el tipo de modelo (regresión logística), las variables (basicas), C = 1.0, class_weight = ninguno, el umbral 0,5 y la tabla y versión de los datos. Como métricas quedaron PR-AUC 0,339, ROC-AUC 0,794, accuracy 0,896, precision 0,493, recall 0,045, F1 0,082, 67 alertas y el PR-AUC de train (0,341); como artefactos, la matriz de confusión y el modelo con su firma. 

El experimento es la carpeta que junta todos los intentos del problema y cada run es un entrenamiento puntual dentro de ella.


4)¿Cuántos parámetros registró autolog y qué métricas? ¿Qué no registra autolog que sí registramos a mano?

Autolog registró 46 parámetros y 7 métricas, todas de entrenamiento (accuracy, f1, log loss, precision, recall, roc_auc y score). Lo que no registra y nosotros sí es todo lo que tiene que ver con test: el PR-AUC de test, el umbral, el conjunto de variables y la versión de los datos.


5)¿Dieron las mismas métricas 01_repro_a y 01_repro_b? Nombrá las cuatro cosas que hay que fijar para que un run sea reproducible.

Sí, dieron exactamente lo mismo (PR-AUC de test 0,5099 en los dos). 

Para que un run se pueda reproducir hay que fijar los datos, el código, la semilla y el entorno.


6)¿Qué pasó al llamar al modelo sin la columna amount? ¿Para qué sirve la firma (signature) del modelo?

Al sacarle amount el modelo tiró un error de "Failed to enforce schema" y no llegó a predecir nada. 

La firma sirve para eso: dice qué columnas y de qué tipo espera el modelo, así que si alguien le pasa datos que no corresponden lo frena en vez de dar cualquier cosa.


7)¿Cuál fue tu mejor run (modelo, variables, hiperparámetros y PR-AUC)? ¿Cuánto mejoró respecto de usar sólo rule_alert (solo_regla)?

Mi mejor run fue un gradient boosting con completas, sin class_weight, learning_rate = 0.03 y sin límite de profundidad, con PR-AUC de test 0,585.

Usando sólo rule_alert el PR-AUC fue 0,170, o sea que mejoró 0,415, casi 3,4 veces más.


8)Con el mismo modelo, ¿cuánto cambió el PR-AUC al pasar de basicas a cliente y a completas? ¿Qué variables resultaron más importantes en tu mejor run?

Con boosting, balanced y umbral 0,5, el PR-AUC fue 0,329 con basicas, 0,503 con cliente y 0,572 con completas, así que cada grupo de variables sumó bastante. 

En mi mejor run lo más importante fue amount (0,338), después payment_channel (0,185), hour (0,122), category (0,085) y account_age_days (0,057).


9)Con completas, ¿qué modelo anduvo mejor: logística, bosque o boosting? ¿Por qué un modelo de árboles puede aprovechar combinaciones de variables (por ejemplo, de noche y por transferencia) que una logística no ve?

Con completas ganó el boosting con 0,585, después la logística con 0,545 y último el bosque con 0,537. 

Los árboles pueden ir combinando condiciones una atrás de la otra, mientras que en cambio la logística le da un peso a cada variable por separado y esa combinación no la ve.


10)¿Qué cambió en precision, recall y cantidad de alertas al usar class_weight = balanced? ¿Cambió mucho el PR-AUC?

Con balanced (boosting, completas, umbral 0,2) el recall subió de 0,669 a 0,927, pero la precision cayó de 0,451 a 0,213. El PR-AUC casi no cambió, de 0,585 a 0,578.


11)¿Qué pasó al bajar el umbral? ¿Por qué el PR-AUC no depende del umbral y el F1 sí?

Al bajar el umbral de 0,3 a 0,2 las alertas pasaron de 788 a 1.105, el recall subió de 0,574 a 0,678 y la precision bajó de 0,539 a 0,454. 

El PR-AUC dio 0,585 en las dos porque se calcula con todos los umbrales posibles, y el F1 sí cambia (0,556 a 0,544) porque depende de las alertas que genera un umbral en particular.


12)¿Qué hiperparámetro barriste y con qué valor obtuviste el mejor PR-AUC de test? ¿Hubo valores con train_pr_auc mucho más alto que el de test? ¿Cómo se llama ese fenómeno?

El hiperparámetro que barrí fue learning_rate del boosting (0,03, 0,1 y 0,3) y el mejor PR-AUC de test fue con 0,03 (0,585). 

En el boosting train y test quedaron bastante cerca (0,64 contra 0,585), pero en el fortes sin límite de profundidad el PR-AUC de train fue 1,000 y el de test 0,535 a 0,544, y con profundidad 8 fue 0,632 contra 0,553. Eso es overfitting. 


13)¿Coincidieron tu mejor run por PR-AUC y tu mejor run por accuracy? Explicá por qué pueden no coincidir.

No coincidieron. El mejor por PR-AUC fue un boosting con 0,585 (accuracy 0,905) y el mejor por accuracy fue un bosque con 0,914, pero con PR-AUC 0,535 y recall de apenas 0,282. Pasa porque con tan pocos contracargos la accuracy premia a los modelos que casi no alertan, y el PR-AUC premia al que mejor ordena el riesgo.


14)En el gráfico de PR-AUC de train contra test, ¿qué runs quedan más lejos de la diagonal y qué indica eso?

Los que quedan más lejos son los forest sin límite de profundidad: train 1,000 contra 0,43 a 0,54 en test, y con basicas 0,989 contra 0,210. 

Quiere decir que se memorizaron el entrenamiento y no generalizan (overfitting).


15)¿Qué ventaja tiene consultar los runs con mlflow.search_runs en lugar de mirarlos en la interfaz? Escribí el filter_string que usarías para traer sólo tus bosques con PR-AUC de test mayor a 0,55.

Con search_runs podés filtrar y ordenar los runs desde código como si fueran una tabla, y eso sirve cuando tenés decenas de runs y mirarlos uno por uno en la interfaz no da. 

El filtro sería tags.student_id = 'bigdata_jose1' AND params.model_type = 'random_forest' AND metrics.test_pr_auc > 0.55.


16) En fraude, ¿qué cuesta más, una alerta falsa o un contracargo no detectado? Si el equipo de riesgo sólo puede revisar una cantidad fija de alertas por día, ¿cómo usarías el umbral?

Creo que suele costar más un contracargo que no detectás, porque perdés la plata de la compra, mientras que una alerta falsa te cuesta tiempo de revisión y alguna molestia al cliente. 

Si el equipo de riesgo sólo puede revisar una cantidad fija por día, ajustaría el umbral hasta que la cantidad de alertas entre en esa capacidad, así revisan primero las de mayor score.


17)¿Se pudo registrar el modelo en Unity Catalog? Indicá nombre, versión y alias (o el error, si falló). ¿Qué ventaja tiene cargar models:/...@champion en lugar de runs:/<run_id>/model?

Sí se pudo: quedó workspace.bigdata_jose1.fraud_model, versión 2, con alias champion

Usar models:/...@champion sirve porque quien usa el modelo no necesita saber el run_id; si sale un modelo mejor alcanza con mover el alias y el código que lo consume no se toca.


18)¿Cuántas transacciones nuevas se puntuaron y cuántas generaron alerta? ¿Qué columnas de ml_predictions permiten auditar cada decisión? ¿Por qué el umbral se guarda fuera del modelo?

Se puntuaron 7.165 transacciones nuevas y 1.165 generaron alerta, el 16,3 %. 

En ml_predictions quedan score, alerta, threshold, model_name, model_version, run_id y scored_at, que permiten saber qué modelo y qué umbral tomó cada decisión.

El umbral va afuera del modelo porque es una decisión de negocio, depende de cuántas alertas se pueden revisar, y se puede cambiar sin reentrenar.


19)Cuando llegaron los contracargos, ¿el PR-AUC sobre nuevas fue parecido al de test? En la campaña simulada, ¿qué pasó con el porcentaje de alertas? ¿Qué es el drift y por qué se puede detectar antes de tener las etiquetas?

Sí, el PR-AUC sobre nuevas fue 0,565, bastante parecido al de test (0,585). 

En la campaña simulada las alertas pasaron de 16,3 % a 30,1 %. 

El drift es cuando los datos que llegan cambian respecto de los que se usaron para entrenar, y se puede ver antes de tener las etiquetas porque alcanza con mirar las distribuciones de las entradas y de los scores, que las tenés en el momento; las métricas recién se pueden calcular cuando llegan los contracargos.


20)Ubicá lo que hiciste en el ciclo de vida de MLOps (datos, entrenamiento, tracking, registro, despliegue, monitoreo). ¿Qué faltaría para tener este modelo en producción real? Nombrá al menos dos cosas.

Arme los datos, entrene y registre los runs en MLflow. Elegir el mejor, lo registre con el alias champion, y lo use para puntuar las transacciones nuevas y monitorearlas. 

Para tenerlo en producción de verdad faltaría automatizar el scoring y el reentrenamiento con un job, y tener un monitoreo que avise solo cuando hay drift o cuando cae el rendimiento.
