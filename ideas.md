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

