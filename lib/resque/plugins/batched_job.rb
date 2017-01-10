module Resque
  def assemble_batched_jobs id
    puts "assembling batch job..."
    redis.set batch_in_progress_key(id), 1
    yield
    redis.del batch_in_progress_key(id)
  end

  def batch_count_key(id)
    "#{id}_count"
  end

  def batch_in_progress_key(id)
    "#{id}_in_progress"
  end


  module Plugin

    # This hook is really the meaning of our adventure.
    def after_batch_hooks(job)
      job.methods.grep(/^after_batch/).sort
    end

  end

  module Plugins

    module BatchedJob

      # Helper method used to generate the batch key.
      #
      # @param [Object, #to_s] id Batch identifier. Any Object that responds to #to_s
      # @return [String] Used to identify batch Redis List key
      def batch(id)
        "batch:#{id}"
      end

      # Resque hook that handles batching the job. (closes #2)
      #
      # @param [Object, #to_s] id Batch identifier. Any Object that responds to #to_s
      def after_enqueue_batch(id, *args)
        puts "incrementing #{ Resque::batch_count_key(id)}"
        redis.incr Resque::batch_count_key(id)
      end

      # After the job is performed, remove it from the batched job list.  If the
      # current job is the last in the batch to be performed, invoke the after_batch
      # hooks.
      #
      # @param id (see Resque::Plugins::BatchedJob#after_enqueue_batch)
      def after_perform_batch(id, *args)
        puts "decrementing #{ Resque::batch_count_key(id)}"
        count = redis.decr Resque::batch_count_key(id)
        if count.zero? && !batch_assembly_in_progress?(id)
          after_batch_hooks = Resque::Plugin.after_batch_hooks(self)
          after_batch_hooks.each do |hook|
            send(hook, id, *args)
          end
        end
      end

      # After a job is removed, also remove it from the batch.
      #
      # Checks the size of the batched job list and returns true if the list is
      # empty or if the key does not exist.
      #
      def batch_assembly_in_progress?(id)
        !!redis.get(::Resque.batch_in_progress_key(id))
      end

      # Remove a job from the batch list. (closes #6)

      private

        # Handle either Resque 1-x-stable or 2.0.0.pre
        def redis
          Resque.respond_to?(:redis) ? Resque.redis : Resque.backend.store
        end
    end
  end

end
