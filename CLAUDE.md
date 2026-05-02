# dispatch_policy — guía para futuras sesiones

Resumen mínimo para retomar el proyecto sin recargar la memoria entera.
La verdad está en el código + git log + README; este archivo es
**solo** lo que NO es derivable leyendo eso.

## Qué es

Gema Rails que actúa como **admission control por partición** sobre
ActiveJob, persistido en Postgres. Intercepta `perform_later`, stagea
en una tabla intermedia, y un *tick loop* periódico decide cuántos jobs
liberar al adapter real (`good_job` / `solid_queue`) según gates
declarados (`throttle`, `concurrency`).

Ver `README.md` para la API y los ejemplos.

## Estado

v0.1 (en master). Todo el flujo principal está implementado y testeado.
Lo pendiente está en `ideas.md` con su porqué.

101 tests / 217 assertions. `bundle exec rake test` desde la raíz.

## Arquitectura — 4 tablas

```
dispatch_policy_staged_jobs      jobs interceptados, esperando admisión
dispatch_policy_partitions       una fila por (policy, partition_key)
                                 — gate_state (token bucket), shard,
                                 last_checked_at, next_eligible_at, …
dispatch_policy_inflight_jobs    jobs admitidos que están corriendo
                                 — heartbeat_at lo refresca un thread
dispatch_policy_tick_samples     una fila por Tick.run para métricas
                                 (operator decisions panel)
```

## Flujo

1. `MyJob.perform_later(args)` → `JobExtension.around_enqueue_for` →
   `Repository.stage!` (INSERT staged + UPSERT partition con ctx
   refrescado y shard pinned-on-first-write).
2. Un `DispatchTickLoopJob` corre `TickLoop.run(policy_name:, shard:)`.
3. Cada Tick: `Repository.claim_partitions` → para cada partición,
   `Pipeline.call(ctx, partition)` → **una sola TX** que hace
   `Repository.claim_staged_jobs!` (DELETE … RETURNING) +
   pre-INSERT en `inflight_jobs` + `Forwarder.dispatch` (re-enqueue
   al adapter con `Bypass.with`). El adapter PG-backed comparte la
   conexión, así que su INSERT entra en la misma TX.
4. El worker del adapter ejecuta el job: `InflightTracker.track`
   (around_perform) hace INSERT idempotente en `inflight_jobs`,
   spawn un thread de heartbeat, en `ensure` lo cancela y DELETE.

## Invariantes — no romper sin pensar

- **`partition_key` identifica una partición; `shard` es metadata
  de routing.** El shard se pinea en el primer write
  (`COALESCE(EXCLUDED.shard, partitions.shard)`) para que las
  particiones no salten entre tick workers.
- **`partition_by` recomendado a nivel policy.** Cuando se declara
  con `partition_by ->(ctx) { … }` en el bloque de la policy, el
  partition_key del staged_job y la `inflight_partition_key` del
  gate de concurrency comparten el mismo valor canónico → ningún
  gate sufre dilución de scope. El `partition_by:` per-gate sigue
  funcionando (backwards-compat) y es el path heredado: hace que
  partition_key se construya como concatenación de los `partition_by`
  de TODOS los gates → el bucket de throttle se divide N veces si
  los gates tienen scopes distintos. Si los dos están definidos a
  la vez, gana el de policy y se loguea un warning. Para necesidades
  reales de gates con scopes distintos, usar policies separadas.
- **`partitions.context` se refresca en cada `perform_later`** vía
  UPSERT. Los gates leen ese ctx, no el de `staged_jobs.context`
  (que es histórico). Esto permite que un cambio en la DB del host
  (p.ej. nuevo `max_per_account`) tome efecto al siguiente enqueue.
- **`shard_by` debe ser ≥ tan grueso como el `partition_by` del
  throttle más restrictivo.** Si no, el bucket se duplica entre
  shards y el rate efectivo es `rate × N_shards`.
- **`BulkEnqueue.perform_all_later` chequea `Bypass.active?`** y delega
  a `super` cuando está activo. Sin esto, la llamada que hace
  `Forwarder.dispatch` (deserializa + `perform_all_later` bajo Bypass)
  re-stageaba en bucle infinito. El fix vive en `job_extension.rb`;
  hay un test de regresión en `test/integration/tick_atomic_test.rb`
  (`test_full_tick_with_kwargs_does_not_re_stage`) que falla si lo
  quitas.
- **`JobExtension.ensure_arguments_materialized!(job)`** se llama
  antes de leer `job.arguments` tanto en el path single como en el
  bulk. Razón: `klass.deserialize(payload)` solo setea
  `@serialized_arguments`; el getter público `arguments` es un
  `attr_accessor` puro que devuelve `@arguments = []` hasta que
  `perform_now` dispara la materialización privada. Sin esta defensa
  el context proc recibía `[]` y caía a sus defaults.
- **`Forwarder.dispatch` corre dentro de la TX de admisión.** El
  adapter (good_job / solid_queue) usa
  `ActiveRecord::Base.connection`, así que su INSERT en `good_jobs`
  / `solid_queue_jobs` participa en la misma transacción que el
  DELETE de `staged_jobs` y el INSERT de `inflight_jobs`. Cualquier
  excepción (deserialize, adapter, network) revierte todo
  atómicamente — no hay ventana de pérdida entre el commit de
  admisión y el enqueue al adapter. **No reintroduzcas `unclaim!`
  ni `preinserted_inflight_ids`**: el rollback hace ese trabajo.
  Si soportas un adapter no-PG en el futuro, antes piensa cómo
  garantizar at-least-once sin esta invariante.
- **Adapter no-PG = warning al boot, no hard-fail.** El railtie
  llama a `DispatchPolicy.warn_unsupported_adapter` en
  `after_initialize`. Si el host usa Sidekiq/Resque, se loguea un
  warning explicando que la atomicidad se pierde. Es deliberado:
  un adapter PG-backed custom (no detectado) puede seguir
  funcionando, y queremos no romper su deploy.
- **`config.database_role`**: para Rails multi-DB (p.ej.
  solid_queue con DB separada), define el role contra el que se
  abre la TX de admisión. `Repository.with_connection` envuelve la
  TX en `connected_to(role:)` cuando está fijado. Las tablas de
  staging y la del adapter deben estar en la misma DB para que la
  atomicidad funcione.
- **Todos los jobs admitidos crean una fila en `inflight_jobs`**,
  tengan o no gate de concurrency. La key cambia: con concurrency,
  la key del gate (coarse, agrega cross-staged-partition); sin
  concurrency, la `partition_key` del staged. La UI cuenta por
  `policy_name` y siempre da un valor real.
- **Fairness intra-tick = orden + cap, NO se mezcla con la
  selección.** `claim_partitions` sigue ordenando por
  `last_checked_at NULLS FIRST, id` (anti-stagnation: cada partición
  con pending se procesa cada ⌈N/B⌉ ticks). Una vez claimed, el Tick
  las reordena en memoria por `decayed_admits ASC` (EWMA con
  half_life = 60s default) y aplica `fair_share = ceil(tick_cap / N)`
  como techo per partición. **No reintroduzcas decayed_admits en el
  ORDER BY del SELECT FOR UPDATE** — eso rompe la garantía
  anti-stagnation cuando hay > batch_size particiones frescas.
- **El cap global del tick gana al floor anti-stagnation per-tick.**
  Si `tick_admission_budget < N_claimed`, algunas particiones admiten
  0 en este tick. NO se les fuerza un floor de 1 (eso rompería el
  cap). La fairness viene de claim_partitions: como sus
  `last_checked_at` se bumpean al ser claimed, el siguiente tick las
  pone al frente.
- **El decay update se hace en la misma TX que el admit.** En
  `record_partition_admit!`, si `half_life_seconds` está fijado, el
  UPDATE incluye `decayed_admits = decayed_admits * exp(-Δt/τ) +
  admitted` y `decayed_admits_at = now()`. Mismo lock de fila que ya
  teníamos. `bulk_record_partition_denies!` NO toca el decay (en
  deny no hubo admisión).
- **`claim_staged_jobs!` requiere `limit > 0`** (ahora es la
  vía solo-admit). El path de deny puro va por
  `Repository.bulk_record_partition_denies!`: el Tick acumula
  todos los deny del lote y los flushea en un único
  `UPDATE…FROM(VALUES…)` al final, en vez de N statements per
  partición. La equivalencia per-fila (sin agregación
  cross-partición) preserva la corrección. Lo crítico es no
  perder el `gate_state || patch` (jsonb merge) — un test de
  integración bloquea el caso "el patch sobreescribe claves
  pre-existentes".

## Cosas que romper rompe la UI

- El layout no usa Turbo (lo removí por una colisión con
  `<meta http-equiv="refresh">`). El usuario añadió un picker de
  auto-refresh en sessionStorage. Si reintroduces Turbo, recuerda
  que el setTimeout vanilla puede competir con `Turbo.visit`.
- `lib/` NO se autorrecarga en Rails dev. Cualquier cambio en
  `lib/dispatch_policy/*` requiere reiniciar foreman.
- Foreman pone `PORT=5000` por defecto. En macOS el puerto 5000
  es AirPlay → 403. El Procfile tiene `-p 3000` literal.

## Cómo desarrollar

```bash
# Arrancar dummy app (web + worker + tick) con foreman
bin/dummy setup good_job        # crea DB y migra
DUMMY_ADAPTER=good_job bundle exec foreman start

# Endpoints útiles
http://localhost:3000/                       # forms para encolar
http://localhost:3000/dispatch_policy        # dashboard

# Tests
bundle exec rake test                        # 61 runs / 137 asserts

# Si añades una columna o tabla:
#   1. Edita db/migrate/20260501000001_create_dispatch_policy_tables.rb
#   2. Edita lib/generators/.../create_dispatch_policy_tables.rb.tt
#   3. Para el dummy en vivo, ALTER TABLE manualmente (no hay migración
#      incremental porque el v0.1 es una sola migración)
#   4. test/integration/repository_test.rb#schema_present? detecta drift
#      por columnas conocidas; añade la nueva al check.
```

## Queries de debug útiles

```sql
-- Distribución de partitions por policy/shard, con pending y lifetime
SELECT policy_name, shard, status, count(*) AS partitions,
       sum(pending_count) AS pending, sum(total_admitted) AS lifetime
FROM dispatch_policy_partitions
GROUP BY policy_name, shard, status
ORDER BY pending DESC;

-- Particiones en backoff ahora mismo
SELECT policy_name, partition_key,
       gate_state -> 'throttle' ->> 'tokens' AS tokens,
       (next_eligible_at - now()) AS time_left
FROM dispatch_policy_partitions
WHERE next_eligible_at > now();

-- Tick samples del último minuto
SELECT policy_name, count(*) AS ticks, sum(jobs_admitted) AS admitted,
       avg(duration_ms)::int AS avg_ms
FROM dispatch_policy_tick_samples
WHERE sampled_at > now() - interval '1 minute'
GROUP BY policy_name;
```

## Qué hay en `ideas.md`

Cosas detectadas pero aplazadas con su razonamiento. Léelo antes de
proponer una mejora "nueva" — probablemente ya está anotada.

Hoy contiene:
- Sweeper más agresivo de particiones huérfanas con `pending=0`
- Revisar acoplamiento entre `inflight_heartbeat_interval`,
  `inflight_stale_after` y `sweep_every_ticks`

## Convenciones del repo

- Tests unit en `test/unit/`, integration (con Postgres) en
  `test/integration/`. Los integration se skipean si no hay DB.
- Mensajes de commit en inglés, en cuerpo se explica el **por qué**
  no solo el qué. Co-Author tag al final.
- El usuario edita la dummy app (jobs de stress, layout) entre mis
  commits — respeta sus modificaciones.
