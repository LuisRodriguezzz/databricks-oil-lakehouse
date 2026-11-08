# Decisiones del laboratorio

Cada decisión con su porqué y la alternativa que se descartó. Se agregan a medida que se
construye; si una cambia, se tacha y se escribe la nueva con su motivo.

## Plataforma: catálogo y landing

**D1 · Un catálogo por ambiente (`oil_dev`, `oil_prod`).**
Por qué: el catálogo es la frontera de permisos y se puede atar a un workspace; así prod queda
aislado de dev aunque alguien tenga permisos.
Descartado: un solo catálogo con esquemas `dev_` y `prod_`; es más simple, pero mezcla los
permisos de los dos ambientes en el mismo objeto.

**D2 · El catálogo no lo crea el bundle.**
Por qué: es de plataforma, lo comparten todos los proyectos y lo crea un administrador una sola
vez (en una empresa, con Terraform). En Free Edition, además, solo se crea desde la interfaz.
Descartado: crearlo desde el proyecto; cada repo tendría poder sobre un objeto compartido.
Aclaración (verificado en Microsoft Learn el 2026-11-07): un bundle **sí puede** crear catálogos
(recurso `catalog`, solo con el motor `direct`), esquemas y volumes. Dejarlo afuera es una
decisión, no un límite técnico. La alternativa senior es un **bundle de plataforma aparte**, o
Terraform, desplegado sin modo development y con `lifecycle.prevent_destroy: true`. El script es
la versión simple del laboratorio. En Free Edition el bundle chocaría con el mismo límite de la
API para crear el catálogo (deducción: usa la misma API).

**D3 · Landing es un volume managed en el esquema `landing`, compartido por todos.**
Por qué: los archivos crudos quedan gobernados por Unity Catalog (permisos y linaje desde el
origen), como recomienda Databricks. Es compartido porque todos los desarrolladores leen los
mismos archivos y ninguno los modifica.
Descartado: una ruta suelta en el storage, sin gobierno. En Chevron sería un *external location*
sobre ADLS Gen2, donde deja los archivos Data Factory.
Por qué no va en el bundle del proyecto:
- En `mode: development` el bundle le pone a cada esquema el prefijo del usuario, así que cada
  desarrollador tendría su propio landing; Data Factory necesita una dirección fija.
- `bundle destroy` borra lo que el bundle creó, y landing guarda la historia de entregas: tiene
  que sobrevivir al proyecto.

**D4 · Una carpeta por fuente y una subcarpeta por entrega: `produccion/ingest_date=AAAA-MM-DD/`.**
Por qué: Auto Loader reconoce cada archivo por su ruta; una corrección llega como archivo nuevo y
se procesa, y queda la historia de qué llegó y cuándo. El formato `clave=valor` hace que
`ingest_date` aparezca sola como columna.
Descartado: pisar siempre el mismo archivo; Auto Loader no lo vuelve a leer y la corrección se
pierde.

Límite de esta convención: una entrega por fuente y por día. Si la fuente pudiera mandar dos
correcciones el mismo día, la carpeta tendría que llevar la hora (`ingest_ts=...`).

**D5 · Los scripts de plataforma son idempotentes.**
Por qué: preguntan si el objeto existe antes de crearlo; correrlos dos veces da lo mismo, y se
pueden usar en un CI sin miedo.
Descartado: un script que falla si el objeto ya existe.

**D6 · `subir_a_landing.sh` se niega a pisar un archivo.**
Por qué: landing es inmutable (D4); el error obliga a usar otra fecha de entrega.
Descartado: `--overwrite`, que borraría la evidencia de la entrega anterior.

## Bundle y repo

**D7 · Todo lo del proyecto vive en un bundle con Git.**
Por qué: esquemas, pipelines y jobs quedan como archivos, se revisan en un pull request y se
despliegan igual en cada ambiente. Es la base del CI/CD.
Descartado: crear las cosas a mano en la interfaz; nadie sabe qué cambió ni se puede repetir.

**D8 · Un esquema por capa (`bronze`, `silver`, `gold`), declarado en `resources/schemas.yml`.**
Por qué: los permisos se dan por esquema; por ejemplo, los analistas leen solo gold.
Descartado: todas las tablas en un esquema; no se puede dar acceso a una capa sola.

**D9 · El target `dev` usa `mode: development`.**
Por qué: Databricks le agrega a cada recurso el usuario que despliega (`dev_<usuario>_bronze`,
`[dev <usuario>] oil_ingesta`) y pausa schedules y triggers. Cada desarrollador tiene su copia y
nadie pisa a otro.
Descartado: un esquema de dev compartido; dos personas desplegando a la vez se pisan las tablas.

**D10 · Se despliega con `DATABRICKS_BUNDLE_ENGINE=direct`.**
Por qué: la CLI instalada (v0.283.0) falla al bajar Terraform por una clave de firma vencida; el
motor directo no usa Terraform.
Pendiente: actualizar la CLI.

**D11 · `.gitattributes` con LF y `datos/` fuera de Git.**
Por qué: un `.sh` con fin de línea de Windows falla en el runner Linux del CI. Los datos de
muestra se suben a landing; el repo guarda código, no datos.

## Pipeline de ingesta

**D12 · Pipeline declarativo (Lakeflow Spark Declarative Pipelines), no notebooks sueltos.**
Por qué: uno declara las tablas y el pipeline deduce el orden, guarda el progreso de cada flujo,
reintenta y mide la calidad. Es lo que Databricks recomienda hoy para ingesta y medallion.
Descartado: notebooks orquestados con un job, que es lo que hiciste en el proyecto retail; ahí el
orden, los reintentos y el progreso los escribe uno. Los jobs se siguen usando, pero para
coordinar pipelines entre sí.

**D13 · Serverless.**
Por qué: no hay clúster que dimensionar ni dejar prendido; arranca en segundos. En Free Edition
es la única opción.

**D14 · La ruta de landing llega por configuración (`oil_landing`), no escrita en el código.**
Por qué: el mismo código lee el landing de dev o el de prod según el target. Lo mismo para los
esquemas (`oil_bronze`, `oil_silver`), que en dev llevan el prefijo del usuario.
Las claves van sin punto: el motor `direct` de la CLI v0.283.0 no resuelve una referencia a un
recurso (`${resources.schemas.silver.name}`) si la clave tiene punto (`oil.silver`).

**D15 · Bronze es una *streaming table* con Auto Loader.**
Por qué: procesa solo los archivos nuevos, cada uno una vez, y guarda su progreso; volver a
correr no duplica.
Descartado: leer la carpeta entera en cada corrida (batch); duplicaría filas o obligaría a borrar
y recargar todo. Otras opciones válidas: `COPY INTO` (SQL que saltea archivos ya cargados, sin
pipeline) y una *materialized view* con `read_files` para datos chicos de referencia. Para el
padrón (1,2 MB) una materialized view también servía; se eligió streaming table para que todas
las fuentes funcionen igual y bronze guarde cada entrega. Para bases de datos (SQL Server,
Postgres) no se leen archivos: se usa un conector de Lakeflow Connect.

**D16 · Todo entra como texto (`cloudFiles.inferColumnTypes=false`).**
Por qué: si bronze tipa, un valor raro se pierde antes de que alguien lo vea. Lo que no encaja en
las columnas va a `_rescued_data`. Tipar y validar es trabajo de silver.

**D17 · Tres columnas de linaje: `_archivo`, `_archivo_modificado_en`, `_cargado_en`.**
Por qué: cualquier fila se rastrea hasta el archivo de origen; sirve para auditar y para que
silver decida qué versión vale.

**D18 · Se limpia el BOM del nombre de la primera columna.**
Por qué: el CSV del padrón trae una marca invisible pegada a `idpozo`; sin limpiarla, la columna
se llamaría distinto y fallaría todo lo que la nombre.

**D19 · Una tabla por fuente (`produccion`, `padron`).**
Por qué: tienen columnas y grano distintos.

**D20 · El pipeline está escrito en Python.**
Por qué: permite una función común (`leer_entregas`) para todas las fuentes y, en silver, cargar
las reglas de calidad desde un archivo aparte. En SQL se haría con `read_files`, igual de válido.

## Silver

**D21 · El pipeline se ordena en `transformations/` y `utilities/`, con `root_path`.**
Por qué: es la estructura que propone Databricks. Solo `transformations/` define tablas; los
módulos de `utilities/` no se ejecutan como código del pipeline, se importan (la carpeta raíz
entra sola al `sys.path`).
Descartado: todo en un archivo (las reglas quedarían mezcladas con la lógica) o agregar rutas
al `sys.path` a mano.

**D22 · Silver va en el mismo pipeline que bronze; gold, en otro.**
Por qué: bronze y silver se actualizan juntos y el pipeline deduce el orden. Gold va aparte
porque una regla de calidad no frena lo que sigue dentro del mismo pipeline: para que gold no
corra si la ingesta falló, hace falta un job con dos tareas (paso 5).
Descartado: un pipeline por capa (orquestación de más entre bronze y silver) o todo en uno (no
habría forma de frenar gold).

**D23 · El estado vigente es una *materialized view*: la última entrega de cada año manda.**
Por qué: el portal republica el año entero en cada entrega, sin marcar borrados. Una
materialized view queda igual a lo que dice bronze en cada corrida: una fila corregida toma el
valor nuevo y una fila que el portal sacó desaparece. Databricks la recalcula de forma
incremental cuando puede.
Descartado, y cuándo convendría cada una:
- `AUTO CDC` por clave: no borra sin una marca de borrado. Conviene si la fuente manda cambios
  fila por fila (un ERP leído con Lakeflow Connect).
- `AUTO CDC FROM SNAPSHOT`: detecta borrados, pero espera la foto de toda la tabla y no admite
  expectations. Conviene si la fuente manda la tabla entera cada vez.
- Notebook con `MERGE ... WHEN NOT MATCHED BY SOURCE AND anio = 2024 THEN DELETE`: hace lo
  mismo con más código y orquestación propia.
Límite: la vista lee todo bronze en cada corrida. Con 20 años de entregas sigue siendo chico;
si bronze creciera mucho, convendría limpiar entregas viejas o pasar a `AUTO CDC FROM SNAPSHOT`.

**D24 · El año de una entrega sale del nombre del archivo, no de sus filas.**
Por qué: una fila con `anio = 2023` dentro del archivo de 2024 va a cuarentena
(`anio_de_la_entrega`). En AWS, `overwritePartitions()` habría reemplazado la partición 2023
entera con esa sola fila.

**D25 · Los tipos se aplican con `try_cast` y lo que no convierte queda anotado.**
Por qué: `try_cast` no rompe la corrida, pero devuelve nulo. Si el texto original no estaba vacío
y no convirtió, el nombre de la columna va a `_no_convierte` y la fila, a cuarentena
(`valores_convierten`). Así se cierra el hueco de AWS, donde un `12,5` se volvía nulo en silencio.
Descartado: `cast` (con ANSI activo, un solo valor raro rompe toda la corrida) o tipar en bronze.

**D26 · Una versión por clave, sin elegir al azar.**
Por qué: gana la `fechaingreso` más nueva (la rectificativa). Las copias idénticas quedan en una.
Si quedan dos versiones distintas con la misma fecha, las dos van a cuarentena
(`sin_versiones_en_conflicto`): no hay forma de saber cuál vale.
Cambio respecto de lo propuesto: se había dicho desempatar por otra columna. Se cambió porque
cualquier otra columna elige una versión arbitraria, con apariencia de regla.
Descartado: `row_number` sin desempate, como en AWS: con dos filas empatadas el resultado puede
cambiar de una corrida a otra.

**D27 · Las reglas viven en `utilities/contratos.py`, y nulo cuenta como falla.**
Por qué: Databricks recomienda guardar las reglas aparte del código (lo mismo que el ADR 0002
de AWS). En SQL, `NULL BETWEEN 0 AND 744` no es verdadero ni falso; cada regla se envuelve en
`coalesce(..., false)` para que un nulo no pase como válido.
Descartado: una tabla de reglas en Unity Catalog. Sirve cuando las reglas las cambia gente que no
toca el repo; acá las revisa el mismo pull request que el código. También un YAML como en AWS:
funcionaría, pero el pipeline tendría que leerlo desde una ruta del workspace; un módulo de
Python se importa directo y es el ejemplo que da Databricks.

**D28 · La cuarentena es una tabla con motivos; las expectations solo miden (modo *warn*).**
Por qué: `silver.produccion_cuarentena` guarda el texto original, los motivos y el archivo de
origen, y se consulta con SQL. Las mismas reglas se registran como expectations para que el
pipeline cuente cuántas filas pasan y fallan cada una.
Descartado: `expect_or_drop` (la fila se pierde sin rastro) y `expect_or_fail` (una sola fila
mala frena el flujo entero).

**D29 · El paso intermedio (`<fuente>_validada`) es privado.**
Por qué: es un cálculo interno; con `private=True` existe para el pipeline pero no se publica, y
nadie puede depender de él.

**D30 · El freno por porcentaje de cuarentena va en el job (paso 5).**
Por qué: una regla dentro del pipeline no frena a gold (D22). El job va a mirar el porcentaje
en cuarentena y no correr gold si pasa el umbral (1 %). Construido en D35 y D36.

**D31 · Una columna del contrato que falta en bronze hace fallar el pipeline.**
Por qué: silver selecciona cada columna por nombre; si una entrega nueva no la trae, la corrida
falla en vez de publicar una tabla incompleta. Pendiente de probar.

**Pendiente D32 · Qué pasa con una columna nueva en un CSV.** Hoy quedó el default sin decidirlo:
Auto Loader en modo `addNewColumns` frena el flujo, agrega la columna a bronze y sigue al
reiniciar; silver la ignora porque selecciona solo las columnas del contrato, y nadie se entera.
Opciones: `rescue` (la columna va a `_rescued_data` y la regla `fila_bien_formada` manda la
entrega a cuarentena), `failOnNewColumns` (frena hasta que alguien actualice el contrato) o
dejar `addNewColumns` y agregar en silver un control que avise si bronze tiene columnas fuera
del contrato. Falta probarlo con un CSV con una columna de más y ver si el pipeline se reinicia
solo.

**Hallazgo · unidad de `tef`.** En esta muestra de 2024, `tef` va de 0 a 31: parecen días. En
AWS se midió hasta 720 en otro recorte: parecían horas. La regla 0–744 deja pasar las dos. Es
una deducción: falta confirmarlo con el diccionario del portal antes de usar `tef` en gold.

**Prueba (2026-11-07): entrega corregida.** `scripts/simular_correccion.py` armó una segunda
entrega de 2024 (`ingest_date=2026-11-08`) con seis casos. Resultado: silver pasó de 45.943 a
45.939 filas, todas de la entrega nueva:
- El valor corregido quedó con el valor nuevo.
- La fila borrada salió de silver.
- La copia exacta quedó en una fila.
- Cuatro filas fueron a cuarentena: `12,5` por `valores_convierten`, `anio = 2023` por
  `anio_de_la_entrega` y las dos versiones en conflicto.
- Bronze conserva las dos entregas.
- El registro de eventos muestra esos conteos por regla.

## Gold y orquestación

**D33 · Gold tiene dos tablas: `produccion_pozo_mes` y `produccion_mensual_empresa`.**
Por qué: la primera es el hecho por pozo y mes, con la edad del pozo calculada desde el padrón
(como `fact_produccion_mensual` en AWS). La segunda es el resumen que leería un tablero. El cruce
con el padrón es *left join*: un pozo sin padrón conserva su producción, con la edad vacía (hoy
son 0 casos).
Pospuesto: la estrella con `dim_pozo` SCD2 y la curva tipo. Con un solo año de datos no muestran
nada.

**D34 · Gold son materialized views en su propio pipeline (`oil_gold`).**
Por qué: siguen a silver si corrige o saca filas. Es el mismo argumento de AWS ("reconstruir
cuesta menos que quedar desincronizado"), y además Databricks actualiza solo lo que cambió cuando
puede. Gold lee silver por nombre completo: es una tabla de otro pipeline, así que el orden lo
pone el job.
Descartado: streaming tables, que solo agregan filas y no corrigen un total ya calculado.

**D35 · El freno es `silver.control_calidad`, con `expect_all_or_fail`.**
Por qué: una fila por fuente con filas vigentes, filas en cuarentena y su porcentaje. Las reglas
(`silver_con_filas` y `cuarentena_bajo_umbral`, con umbral del 1 % por fuente) viven en
`contratos.py`. Si una falla, falla la corrida de ingesta, el job no corre gold, y gold queda
con su última versión buena. Silver se actualiza igual, porque cada flujo es independiente, y
solo tiene filas válidas. El flujo que falló se deshace: la tabla de control conserva su último
estado bueno.
Descartado: una tarea aparte del job que consulte y falle (más código y cómputo para lo mismo) o
`expect_or_fail` fila por fila en silver (tolerancia cero).

**D36 · El job encadena con una dependencia: gold corre solo si la ingesta terminó bien.**
Por qué: `run_if: ALL_SUCCESS` sobre la tarea `ingesta` es lo que hace funcionar el freno.
Descartado: un disparador por actualización de tabla. Rompería el freno, porque silver se
actualiza aunque el control falle.

**D37 · El job arranca cuando llega un archivo a landing.**
Por qué: *file arrival trigger* sobre `/Volumes/<catalogo>/landing/archivos/`. Revisa cada
minuto y espera 60 segundos sin archivos nuevos, así producción y padrón juntos disparan una sola
corrida. `max_concurrent_runs: 1` evita dos corridas en paralelo sobre las mismas tablas. En dev
queda pausado (D9) y se lanza a mano.
Descartado: un horario fijo, como EventBridge a las 12 en AWS. Corre aunque no llegue nada, y
llega tarde si la entrega cae un minuto después.

**D38 · Avisos por mail: si falla y si tarda más de 10 minutos.**
Por qué: el aviso de duración (`health`, `RUN_DURATION_SECONDS > 600`) llega antes del corte;
hoy corre en unos 2 minutos. Va a la dirección de quien despliega
(`${workspace.current_user.userName}`); en prod será una lista del equipo.
Alternativa: Teams o Slack con *notification destinations*, que configura un administrador.
Verificado: el mail de la corrida fallida llegó.

**D39 · Sin reintentos en el job; tope de 30 minutos por tarea.**
Por qué: una falla de calidad no se arregla repitiendo. El tope tiene margen sobre lo medido; en
AWS la ingesta tardaba 49 minutos con un tope de 60, demasiado justo.

**D40 · Reintentos del pipeline acotados.**
Lo que se encontró: lanzado desde un job, un pipeline triggered repite la corrida entera hasta 5
veces (`pipelines.numUpdateRetryAttempts`) y cada flujo hasta 3 veces por corrida
(`pipelines.maxFlowRetryAttempts`), y trata la falla del control como pasajera. Con la entrega
mala, la ingesta pasó 10 minutos repitiendo lo mismo, hasta que se canceló.
Decisión: ingesta con `numUpdateRetryAttempts: "0"` y el flujo de control con
`maxFlowRetryAttempts: "0"`: falla a la primera y avisa, y se reintenta con un repair. Gold con
un solo reintento, para errores pasajeros. Los demás flujos de la ingesta conservan sus
reintentos por flujo.
Descartado: dejar los valores por defecto (el aviso tarda decenas de minutos) o reintentar desde
el job (repetiría también la falla de calidad).

**Diseño para sumar fractura y reservas (propuesto, no construido).**
- **La matriz decide la agrupación:** qué fuentes necesita cada tabla de gold.
- **Un job por grupo con su propio disparador por archivo:** producción y padrón; fractura;
  reservas. Un disparador por archivo mira una sola carpeta y sus subcarpetas, así que landing
  conviene ordenarlo por grupo.
- **El job del mart de completación** corre con un disparador por actualización de tabla, en
  modo "cualquiera", sobre las tablas de gold de producción y de fractura: gold solo cambia si
  pasó el control. Mirar silver rompería el freno, como en D36.
- **Alternativa:** un solo job con ramas, cuando hay un solo equipo y la misma cadencia.

**Prueba (2026-11-07): romper y reparar.**
1. **Corrida normal:** ingesta 71 s y gold 51 s, todo en verde. Control de producción:
   0,0087 % en cuarentena. Gold: 45.939 filas, 0 pozos sin padrón, edad de 0 a 227 meses.
2. **Entrega mala** (`ingest_date=2026-11-09`, `scripts/simular_entrega_con_errores.py`, 5 %
   con coma decimal), primero con los reintentos por defecto: 10 minutos repitiendo y se canceló
   (D40). Después del ajuste: la ingesta falló en 72 s, en un intento, con
   `control_calidad failed an expectation check`, y gold quedó en `UPSTREAM_FAILED`. Silver pasó
   a 43.645 filas con 2.298 en cuarentena; gold siguió con 45.939.
3. **Entrega buena** (`ingest_date=2026-11-10`, el archivo real) y `jobs repair-run
   --rerun-all-failed-tasks`: las dos tareas pasaron en el intento 1, dentro de la misma corrida,
   en 3 minutos. Silver y gold quedaron con 45.943 filas, y el pozo 157982 volvió a 49,23: la
   última entrega manda.

## CI/CD y prod

**D41 · `prod` es otro target del mismo bundle, con su catálogo `oil_prod`.**
Por qué: el mismo código se despliega en los dos ambientes; solo cambian las variables. En Free
Edition hay un solo workspace. En una empresa, prod tendría su propio workspace y `oil_prod`
quedaría atado solo a ese workspace (*binding* ISOLATED).

**D42 · Prod corre como un service principal (`run_as`) y en `mode: production`.**
Por qué: no depende de una persona, que se puede ir de la empresa, y tiene solo los permisos que
necesita. El modo production no pone prefijos y deja los disparadores activos. El service
principal se busca por su nombre (`lookup`), así el repo no guarda su ID. Él tiene `CAN_MANAGE`
(es quien despliega) y las personas `CAN_RUN`: pueden correr y reparar, pero cambiarlo es
trabajo del CI. `workspace.root_path` apunta a la carpeta del service principal: hay una sola
copia de prod, y su estado (qué ID tiene cada recurso), quien sea que despliegue.
Descartado: correr prod como un usuario.

**D43 · El mail de aviso es una variable.**
Por qué: en prod despliega el service principal, que no tiene mail, así que
`${workspace.current_user.userName}` dejaría de servir. En dev es quien despliega; en prod, una
dirección fija (en una empresa, la lista del equipo).

**D44 · Permisos mínimos del service principal, a cargo de plataforma.**
Por qué: `plataforma.sh prod <application-id>` le da `USE_CATALOG` y `CREATE_SCHEMA` en
`oil_prod` (los esquemas los crea el bundle) y `USE_SCHEMA` + `READ_VOLUME` sobre landing (lo
lee Auto Loader y lo vigila el disparador). Nada más.

**D45 · Prod lo despliega solo el CD, al hacer merge a `main`.**
Por qué: todo lo que llega a prod pasó por un pull request y por los tests. El CD vuelve a
correr el CI sobre lo que quedó en `main`, porque un merge puede juntar cambios que nunca se
probaron juntos. `concurrency: prod` evita dos deploys a la vez. El deploy no corre el job: en
prod, el job corre cuando llega una entrega.

**D46 · GitHub entra con OAuth M2M (client id y secret del service principal).**
Por qué: lo ideal es OIDC (*workload identity federation*), sin secretos guardados, como el OIDC
del proyecto de AWS. Pero Databricks pide una política de federación en la cuenta, y Free
Edition no da acceso a la cuenta. Es una deducción: falta confirmarlo intentándolo. El secret lo
genera y lo carga en GitHub Luis, nunca pasa por el chat.
El secret es *scoped*: solo puede usar las APIs que el deploy necesita (`workspace`, `jobs`,
`pipelines`, `unity-catalog`, `scim` y `access-management`), no todas (`all-apis`, el valor por
defecto, que Databricks desaconseja). La lista sale de cruzar lo que hace un deploy con la tabla
de scopes de Databricks: es una deducción. Si al CI le falta uno, da error 403, y como los scopes
no se cambian después, hay que generar otro secret. Vence a los 90 días: hay que rotarlo.
Entitlements del service principal: solo *Workspace access*; no necesita SQL ni consumo.
En una empresa con Azure: OIDC con un service principal de Entra ID, o una service connection de
Azure DevOps con *workload identity federation* (`ci/azure-pipelines.yml`).

**D47 · Tests con PySpark local en cada pull request.**
Por qué: los seis casos que se probaron subiendo archivos a mano (corrección, borrado, coma,
otro año, copia y conflicto) corren ahora en segundos, sin Databricks: `tests/`. Otro test pasa
una fila real completa por el contrato, así un error de sintaxis en una regla falla en el CI y no
en el pipeline. `ruff` revisa el estilo. Las versiones van fijas en `requirements-dev.txt` y en
los workflows (`setup-cli@v1.23.0`).
Pospuesto: una prueba de punta a punta con un target `test`. Gastaría la cuota diaria, y Free
Edition permite un solo pipeline activo por tipo.

**D48 · Repo privado sin protección de rama obligatoria.**
Por qué: en una cuenta gratuita de GitHub, un repo privado no puede exigir que el CI esté en
verde antes del merge ni pedir una aprobación antes de prod. Queda como regla de trabajo. En un
plan pago, o en Azure DevOps con *branch policies* y *approvals*, se obliga.

**D49 · El equivalente en Azure DevOps queda escrito, no corrido.**
Por qué: el aviso de Chevron pide Azure DevOps. `ci/azure-pipelines.yml` hace lo mismo: CI en el
pull request y CD en `main`, con un *environment* que puede pedir aprobación.

**Pospuesto:** *liquid clustering* (las tablas son chicas) y actualizar la CLI local (v0.283.0;
el CI usa la 1.23.0).
