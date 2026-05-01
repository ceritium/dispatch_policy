# dispatch policy

Esto es una gema

Per-partition admission control for ActiveJob. Stages perform_later into a dedicated table, runs a tick loop that admits jobs through declared gates (throttle, concurrency, global_cap, fair_interleave, adaptive_concurrency), then forwards survivors to the real adapter.

se basa en postgres, compatible con gemas background jobs basados en ActiveJob compatibles con postgres, como good job y solid queue

debe funcionar con perform_later y perform_later_all
considerar los retry y perform in

el dsl será algo así como:

dispatch_policy do
    # este bloque genera el contexto basado en los argumentos del job
    context ->(args) {
      event = args.first
      { endpoint_id: event.endpoint_id, rate_limit: event.endpoint.rate_limit }
    }

    gate :throttle,
         rate:         ->(ctx) { ctx[:rate_limit] },
         per:          1.minute,
         partition_by: ->(ctx) { ctx[:endpoint_id] }

    gate :concurrency,
        max:          ->(ctx) { ctx[:max_per_account] || 5 },
        partition_by: ->(ctx) { "acct:#{ctx[:account_id]}" }
end


super importante:
- las operaciones para evitar perder jobs
- la gema provee de una ui como rails engine
- para no tener que montar un sistema nuevo, el sistema estará embebido como un job periodico, puede ser algo así:

ejemplo para good job

class DispatchTickLoopJob < ApplicationJob
  include GoodJob::ActiveJobExtensions::Concurrency
  good_job_control_concurrency_with(
    total_limit: 1,
    key: -> { "dispatch_tick_loop:#{arguments.first || 'all'}" }
  )

  def perform(policy_name = nil)
    deadline = Time.current + DispatchPolicy.config.tick_max_duration
    DispatchPolicy::TickLoop.run(
      policy_name: policy_name,
      stop_when:   -> {
        GoodJob.current_thread_shutting_down? || Time.current >= deadline
      }
    )
    # Self-chain so the next run starts immediately; cron below is a safety net.
    DispatchTickLoopJob.set(wait: 1.second).perform_later(policy_name)
  end
end


lo mas importante del sistema, con miles o millones de jobs encolados con distintos partitions keys,
pueden ser una decena o miles de partitions keys
creo que lo mejor es mantener una tabla con los partitions keys
con los atributos a tener en cuenta
el sistema recorre la tabla, posiblemente ordenada por un atributo como: last checked at o algo así, siendo null los primeros
esta tabla se tiene que mantener actulizada y tener un sistema para borrar o ignorar partitions sin actividad

necesitamos un sistema que permita al operador ver el estado del sistema y tomar decisiones como por ejemplo incrementar el batch size de las queries
o particionar el sistema, por ejemplo, tener dos "DispatchTickLoopJob" cada uno centrado en una policy_name o subconjunto de partitions
podemos hacer que el queue_name se use para esto

la gema debe incorporar una dummy app para poner a prueba el sistema con varios jobs predefinidos y una ui para encolar los jobs
