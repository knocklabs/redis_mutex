defmodule RedisMutex.Lock do
  @moduledoc """
  This module contains the actual Redis locking business logic. The `with_lock`
  macro is generally the only function that should be used from this module, as it
  will handle the logic for setting and removing key/values in Redis.
  """

  @default_timeout :timer.seconds(40)
  @default_expiry :timer.seconds(20)
  @unlock_script """
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
  else
    return 0
  end
  """

  @doc """
    This macro takes in a key and a timeout.

    A key might be be an id or a resource identifier that will
    lock a particular resource. A good example might be an email or phone
    number for a user, and you might want lock a db insert so that
    multiple users aren't created for one email or phone number.

    The timeout is in milliseconds and defaults to 40000 milliseconds.
    There is a key expiration of 20 seconds, so the timeout should always
    be greater than 20 seconds. The 20 second expiry allows the key to expire
    in case the logic inside the `with_lock` macro throws an error or fails
    to complete within 20 seconds, thereby freeing up the key so the lock
    can be obtained by another request or resource.

    The lock will be released after the logic inside the `with_lock` has
    completed, or the timeout, whichever comes first. The return value
    of the macro will be the return value of the logic inside
    the 'with_lock' macro.

    ```elixir
    defmodule PossumLodge do
      use RedisMutex
      alias PossumLodge.{Repo, Member}

      def add_member(params) do
        with_lock(params.phone_number) do
          %Member{}
          |> Member.changeset(params)
          |> Repo.insert_or_update!
        end
      end
    end
    ```
  """
  defmacro with_lock(key, opts \\ [], do: block) do
    default_timeout = @default_timeout
    default_expiry = @default_expiry

    quote do
      key = unquote(key)

      opts = unquote(opts)
      timeout = Keyword.get(opts, :timeout, unquote(default_timeout))
      expiry = Keyword.get(opts, :expiry, unquote(default_expiry))
      retry_delay = Keyword.get(opts, :retry_delay)

      uuid = UUID.uuid1()

      RedisMutex.Lock.take_lock(key, uuid, timeout, expiry, retry_delay)

      result = unquote(block)

      RedisMutex.Lock.unlock(key, uuid)

      result
    end
  end

  @doc """
  This function takes in a key, unique string, and a timeout in milliseconds.
  It will call itself recursively until it is able to set a lock
  or the timeout expires.
  """
  def take_lock(key, uuid, timeout, expiry, retry_delay) do
    start = System.monotonic_time(:microsecond)
    finish = DateTime.add(DateTime.utc_now(), timeout, :millisecond)

    attempt_count = take_lock(key, uuid, timeout, expiry, retry_delay, start, finish, 1)

    stop = System.monotonic_time(:microsecond)

    :telemetry.execute([:redis_mutex, :take_lock, :success], %{duration: stop - start}, %{
      attempt_count: attempt_count
    })

    :ok
  rescue
    e in RedisMutex.Error ->
      :telemetry.execute([:redis_mutex, :take_lock, :exception], %{})

      reraise e, __STACKTRACE__
  end

  defp take_lock(key, uuid, timeout, expiry, retry_delay, start, finish, attempt) do
    if DateTime.compare(finish, DateTime.utc_now()) == :lt do
      raise RedisMutex.Error, message: "Unable to obtain lock."
    end

    if lock(key, uuid, expiry) do
      attempt
    else
      # Sleep for a period before retrying lock acquisition if a retry delay is given
      if is_integer(retry_delay) do
        delay = retry_delay + :rand.uniform(floor(retry_delay / 2))
        Process.sleep(delay)
      end

      take_lock(key, uuid, timeout, expiry, retry_delay, start, finish, attempt + 1)
    end
  end

  @doc """
  This function takes in a key and a unique identifier to set it in Redis.
  This is how a lock is identified in Redis. If a key/value pair is able to be
  set in Redis, `lock` returns `true`. If it isn't able to set in Redis, `lock`
  returns `false`.
  """
  def lock(key, value, expiry) do
    client = Process.whereis(:redis_mutex_connection)

    case Redix.command!(client, ["SET", key, value, "NX", "PX", "#{expiry}"]) do
      "OK" -> true
      nil -> false
    end
  end

  @doc """
  This function takes in the key/value pair that are to be released in Redis
  """
  def unlock(key, value) do
    client = Process.whereis(:redis_mutex_connection)

    case Redix.command!(client, ["EVAL", @unlock_script, 1, key, value]) do
      1 -> true
      0 -> false
    end
  end
end
