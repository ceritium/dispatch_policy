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

61 tests / 137 assertions. `bundle exec rake test` desde la raíz.

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
   `Pipeline.call(ctx, partition)` → `Repository.claim_staged_jobs!`
   (DELETE … RETURNING) → pre-INSERT en `inflight_jobs` →
   `Forwarder.dispatch` (re-enqueue al adapter con `Bypass.with`).
4. El worker del adapter ejecuta el job: `InflightTracker.track`
   (around_perform) hace INSERT idempotente en `inflight_jobs`,
   spawn un thread de heartbeat, en `ensure` lo cancela y DELETE.

## Invariantes — no romper sin pensar

- **`partition_key` identifica una partición; `shard` es metadata
  de routing.** El shard se pinea en el primer write
  (`COALESCE(EXCLUDED.shard, partitions.shard)`) para que las
  particiones no salten entre tick workers.
- **`partitions.context` se refresca en cada `perform_later`** vía
  UPSERT. Los gates leen ese ctx, no el de `staged_jobs.context`
  (que es histórico). Esto permite que un cambio en la DB del host
  (p.ej. nuevo `max_per_account`) tome efecto al siguiente enqueue.
- **`shard_by` debe ser ≥ tan grueso como el `partition_by` del
  throttle más restrictivo.** Si no, el bucket se duplica entre
  shards y el rate efectivo es `rate × N_shards`.
- **`Forwarder.dispatch` recibe `preinserted_inflight_ids`**: si el
  enqueue al adapter falla, además de `unclaim!` borra la fila
  inflight pre-insertada para que el cupo de concurrency no se
  sobre-cuente hasta el sweep.
- **Todos los jobs admitidos crean una fila en `inflight_jobs`**,
  tengan o no gate de concurrency. La key cambia: con concurrency,
  la key del gate (coarse, agrega cross-staged-partition); sin
  concurrency, la `partition_key` del staged. La UI cuenta por
  `policy_name` y siempre da un valor real.
- **`unclaim!` preserva `enqueued_at`** vía
  `COALESCE($n, now())` para mantener orden FIFO si el forward falla.
- **`claim_staged_jobs!(limit: 0, retry_after:)` SÍ persiste**
  `next_eligible_at` y `gate_state` aunque no admita filas. Sin
  esto, los gates "deny + retry_after" no producían backoff
  efectivo y el tick reentraba en bucle.

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
