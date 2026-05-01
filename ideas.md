# Ideas / pendientes

Cosas detectadas pero aplazadas. Cada bloque indica el síntoma, una
solución posible, y por qué se dejó fuera del cambio actual.

## Sweeper de particiones huérfanas con pending=0

**Síntoma observado.** Tras un test que generó muchos buckets distintos
(`high_concurrency` con 10 001 particiones únicas), la mayoría quedaron
en la tabla con `pending_count = 0` y `total_admitted > 0`. Las
particiones se borran solas en `sweep_inactive_partitions!`, pero solo
después de `partition_inactive_after = 24h` de inactividad. Mientras
tanto inflan listados, queries de UI y la tabla de partitions.

**Posibles mejoras.**

- Bajar el TTL por defecto a algo más agresivo (¿1h? ¿15m?) — la única
  cosa que se pierde al borrar una partición vacía es su `gate_state`
  (token bucket), que se reconstruye al arrancar fresca.
- Añadir un botón en la UI "GC partitions" que llame a
  `Repository.sweep_inactive_partitions!(cutoff_seconds: 0)` para
  forzarlo bajo demanda.
- En el dashboard, mostrar "X partitions vacías candidatas a GC" como
  panel separado del "active w/ pending".

**Por qué se aplazó.** Cambiar el TTL global afecta a todos los
deployments y puede dar sorpresas (perder gate_state activo de
particiones temporalmente sin pending). Mejor pensarlo como:
configuración por policy + UI explícita.

## Revisar el sweeper y el intervalo de heartbeat

**Síntoma.** Tres números acoplados que no están bien justificados en el
código:

- `inflight_heartbeat_interval = 30s` — cada cuánto el InflightTracker
  refresca `heartbeat_at` mientras el job corre.
- `inflight_stale_after = 5 * 60` — cuándo el sweeper considera que
  una fila inflight es zombie y la borra.
- `sweep_every_ticks = 50` — cada cuántas iteraciones del TickLoop se
  invoca el sweeper.

Las relaciones que deberían cumplirse:

- `inflight_stale_after >> inflight_heartbeat_interval`. El ratio
  10× actual (300 / 30) deja margen para que un par de heartbeats
  fallen sin perder la fila. Documentar que tocar uno requiere tocar
  el otro.
- `sweep_every_ticks * idle_pause < inflight_stale_after`. Si el sweep
  corre cada 50 ticks y cada tick puede tardar `idle_pause = 0.5s`
  (en idle), el sweep corre cada ~25s en el peor caso. Bien para 5min.
  Pero si el operador sube `idle_pause` a 5s (por carga), el sweep
  corre cada 250s = 4min, casi al borde del stale cutoff. Validar.

**Posibles mejoras.**

- Validar al `DispatchPolicy.configure` que las relaciones se cumplen
  (warning en logger si están desbalanceadas).
- Cambiar `sweep_every_ticks` por `sweep_every_seconds` (tiempo, no
  iteraciones) — más estable frente a cambios de `idle_pause`.
- Exponer las tres métricas en la UI ("last sweep at", "stale rows
  pending sweep", "heartbeats in last minute") para que el operador
  vea si los valores están bien dimensionados para su carga.

**Por qué se aplazó.** Lo importante a corto plazo era arreglar el bug
de heartbeat que no se refrescaba durante el perform (el sweeper de
5min mataba inflight rows de jobs legítimamente largos). Una vez ese
está corregido, esto es afinación de defaults y monitoring.

## Bulk handoff: `ActiveJob.perform_all_later` en `Forwarder.dispatch`

**Síntoma.** Hoy `Forwarder.dispatch` (`forwarder.rb:23-44`) itera
`rows.each` y llama `job.enqueue` per-row. Con `admission_batch_size = 100`
y un adapter PG-backed, son 100 round-trips a Postgres por tick por
partición. El PR 18 (`7ea259ae`) en upstream lo bulkifica a una sola
llamada `ActiveJob.perform_all_later(jobs)`, que con good_job /
solid_queue produce un único `INSERT … VALUES (…), (…), …`.

**Solución.** Reemplazar el bucle por:

```ruby
jobs = rows.map { |row| Serializer.deserialize(row["job_data"]) }
Bypass.with { ActiveJob.perform_all_later(jobs) }
```

Con tres detalles a resolver:

**1. `wait_until` per-job no se soporta en bulk.** Hay que separar
filas con `scheduled_at` no nulo y mantenerlas en el path 1-a-1
(`job.set(wait_until:).enqueue`). Estructura:

```ruby
immediate, scheduled = rows.partition { |r| r["scheduled_at"].nil? }
bulk_dispatch(immediate) if immediate.any?
scheduled.each { |row| single_dispatch(row) }
```

En la práctica `scheduled_at` es la minoría — `perform_in / set(wait:)`
se usa puntualmente.

**2. Error handling per-row cambia.** Tras `perform_all_later`, cada
`ActiveJob` instance tiene `successfully_enqueued?` (Rails 7.1+). Hay
que mapear `job → row` (por `job.job_id` ↔ `row.dig("job_data",
"job_id")`) para saber cuáles fallaron, y hacer `unclaim!` +
`delete_inflight!` solo de esos. Si la llamada entera lanza excepción
(p.ej. PG connection drop), `unclaim!` todas las filas del batch.

Si esto se hace **junto con** la migración a TX-atomic (sección
anterior), el error handling se simplifica: cualquier fallo aborta
la TX → no hay `unclaim!` que mantener. `successfully_enqueued?` solo
hace falta si dejamos el path fuera de TX.

**3. Bypass + callbacks bajo `enqueue_all`.** Cuando el adapter
implementa `enqueue_all` nativamente (good_job y solid_queue **sí**
lo hacen), Rails se salta los callbacks `around_enqueue` por completo
— `Bypass.active?` ni se consulta, pero tampoco hace falta porque la
callback no corre. Para adapters sin `enqueue_all` nativo, Rails
hace fallback a un loop interno que sí dispara callbacks, y ahí
`Bypass.active?` sigue siendo necesaria. Mantener `Bypass.with`
envolviendo la llamada cubre ambos casos.

**Test que añadir.** Encolar N jobs vía `Forwarder.dispatch` con
adapter good_job (y otro con solid_queue), assertar:
(a) cero filas nuevas en `staged_jobs` (no hay re-stage),
(b) N filas nuevas en `good_jobs` / `solid_queue_jobs`,
(c) un único `INSERT` por adapter (capturable con `ActiveSupport::Notifications`
escuchando `sql.active_record`).

**Por qué se aplaza.** Es perf, no correctness. Y conviene hacerlo
en el mismo cambio que mover a TX-atomic, porque ambos tocan
`Forwarder.dispatch` y el contrato de error handling cambia en los
dos. Si los hiciéramos en orden inverso (bulk primero, TX después)
estaríamos reescribiendo el rescue dos veces.

## Bulk `record_partition_evaluation!`: N UPDATEs → 1 UPDATE con VALUES

**Síntoma.** En `tick.rb` `admit_partition`, tras evaluar gates y
admitir/denegar, llamamos a `Repository.record_partition_evaluation!`
(o equivalente) **una vez por partición**. Con
`partition_batch_size = 50` son 50 statements por tick. El PR 18
upstream lo bulkifica en un único `UPDATE … FROM (VALUES …)`.

**Solución.** Acumular los cambios de cada partición en un array y
emitir un solo statement al final del tick:

```sql
UPDATE dispatch_policy_partitions p
SET    total_admitted   = p.total_admitted + v.delta,
       last_admit_at    = v.last_admit,
       next_eligible_at = v.next_eligible,
       gate_state       = p.gate_state || v.gate_state_patch::jsonb,
       updated_at       = now()
FROM   (VALUES (1, 5, now(), NULL, '{}'::jsonb),
               (2, 0, NULL, '...'::timestamptz, '{"throttle":{...}}'::jsonb),
               …) AS v(id, delta, last_admit, next_eligible, gate_state_patch)
WHERE  p.id = v.id;
```

Probablemente más limpio: **dos statements bulk separados**, uno por
rama (admit vs deny), cada uno con su VALUES list. Evita tener que
pasar NULLs por columnas que esa fila no quiere tocar.

**Por qué es correctness-safe.**

- Cada fila del VALUES es un update independiente (no hay agregación
  cross-partición), así que un único statement produce el mismo
  estado final que N statements secuenciales.
- Las filas de `partitions` ya están bloqueadas por el
  `FOR UPDATE SKIP LOCKED` previo de `claim_partitions` dentro de la
  misma TX. El bulk UPDATE las re-toca bajo los mismos locks.
- Atomicidad equivalente: estamos dentro de una transacción Rails;
  hoy un fallo en la fila #25 revierte 1-24 porque no ha commiteado;
  en bulk el fallo aborta el statement completo. Mismo efecto.

**Las dos zonas frágiles.**

- **JSONB merge de `gate_state`.** Hay que usar `||` (concat top-level)
  como hoy. Test: una partición con
  `gate_state = {"throttle":{...},"foo":"bar"}` debe conservar `foo`
  tras la admisión. Fácil meter un bug si se sustituye por
  `jsonb_set` con path equivocado.
- **Tipos en la VALUES table.** Construirla con
  `ActiveRecord::Base.connection.quote` o vía bind params, no por
  interpolación. Un `Time` mal casteado o un Hash sin serializar
  rompe silenciosamente.

**Test que añadir.** Comparar estado final de N particiones tras
`record_partition_evaluation!` vía loop vs vía bulk. Con varias
combinaciones: admits, denies, mezcla, gate_state con claves
preexistentes que NO debe perder.

**Por qué se aplaza.** Es perf, no correctness. Y conviene hacerlo
junto al move-into-TX y al bulk handoff (las tres tocan
`Tick#admit_partition` y `Forwarder.dispatch`); reescribir tres veces
el mismo código sería derroche.

