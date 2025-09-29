using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Investimentos.Silo.Streaming.Redis.Storage;
using Orleans.Streams;
using StackExchange.Redis;

namespace Orleans.Investimentos.Silo.Streaming.Redis
{
    public class RedisQueueAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly IRedisStreamStorage streamStorage;
        private readonly QueueId queueId;
        private readonly TimeProvider timeProvider;
        private readonly ILogger<RedisQueueAdapterReceiver> logger;

        private readonly QueueId _queueId;
        private readonly IDatabase _database;
        private readonly ILogger<RedisQueueAdapterReceiver> _logger;
        private string _lastId = "0";
        private Task? pendingTasks;
        private DateTimeOffset _lastTrimTime;

        private TimeProvider _timeProvider;
        private readonly RedisQueueAdapterReceiverOptions _receiverOptions; // Added options field

        // Changed: Constructor to accept TimeProvider and IOptions<RedisStreamReceiverOptions>
        public RedisQueueAdapterReceiver(QueueId queueId,
                                 IDatabase database,
                                 ILogger<RedisQueueAdapterReceiver> logger,
                                 TimeProvider? timeProvider = null,
                                 IOptions<RedisQueueAdapterReceiverOptions>? receiverOptions = null)
        {
            _queueId = queueId;
            _database = database ?? throw new ArgumentNullException(nameof(database));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _timeProvider = timeProvider ?? TimeProvider.System;
            _receiverOptions = receiverOptions?.Value ?? new RedisQueueAdapterReceiverOptions(); // Use provided options or default
            _lastTrimTime = _timeProvider.GetUtcNow();
        }

        public RedisQueueAdapterReceiver(IRedisStreamStorage streamStorage,
            QueueId queueId, TimeProvider timeProvider,
            ILogger<RedisQueueAdapterReceiver> logger)
        {
            this.streamStorage = streamStorage;
            this.queueId = queueId;
            this.timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        // This method might be less relevant if options are passed via constructor, 
        // but kept for now if direct TimeProvider manipulation is still needed for some tests.
        public void SetTimeProvider(TimeProvider timeProvider)
        {
            _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
            _lastTrimTime = _timeProvider.GetUtcNow();
        }

        public async Task<IList<IBatchContainer>?> GetQueueMessagesAsync(int maxCount)
        {
            try
            {
                var streamEntriesTask = streamStorage
                    .GetEntriesAsync(_queueId.ToString(), "consumer", _queueId.ToString(), _lastId, maxCount);

                pendingTasks = streamEntriesTask;
                _lastId = ">";

                var batches = (await streamEntriesTask)
                    .Select(e => new RedisBatchContainer(e))
                    .ToList<IBatchContainer>();

                await TrimStreamIfNeeded();

                return batches;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error reading from stream {QueueId}", _queueId);
                return default;
            }
            finally
            {
                pendingTasks = null;
            }


        }

        public virtual async Task TrimStreamIfNeeded()
        {
            // Changed: Use _timeProvider.GetUtcNow() and options for trim parameters
            //if (_timeProvider.GetUtcNow() - _lastTrimTime > TimeSpan.FromMinutes(_receiverOptions.TrimTimeMinutes))
            //{
            //    try
            //    {
            //        var trim = await _database.StreamTrimAsync(_queueId.ToString(), _receiverOptions.MaxStreamLength, useApproximateMaxLength: true);
            //        _lastTrimTime = _timeProvider.GetUtcNow();
            //        _logger.LogDebug("Trimmed stream {QueueId} to {MaxStreamLength} entries at {Time}", _queueId, _receiverOptions.MaxStreamLength, _lastTrimTime);
            //    }
            //    catch (Exception ex)
            //    {
            //        _logger.LogError(ex, "Error trimming stream {QueueId}", _queueId);
            //    }
            //}
        }

        public async Task Initialize(TimeSpan timeout)
        {
            await Task.CompletedTask;
            return;

            try
            {
                using (var cts = new CancellationTokenSource(timeout))
                {
                    var task = _database.StreamCreateConsumerGroupAsync(_queueId.ToString(), "consumer", "$", true);
                    await task.WaitAsync(timeout, cts.Token);
                }
            }
            catch (Exception ex) when (ex.Message.Contains("name already exists")) { }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error initializing stream {QueueId}", _queueId);
            }
        }

        public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            try
            {
                foreach (var message in messages)
                {
                    if (message is RedisBatchContainer container)
                    {
                        var ackTask = streamStorage.EntryDeliveredAsync(_queueId.ToString(), "consumer", container.StreamEntryId);
                        pendingTasks = ackTask;
                        await ackTask;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error acknowledging messages in stream {QueueId}", _queueId);
            }
            finally
            {
                pendingTasks = null;
            }
        }

        public async Task Shutdown(TimeSpan timeout)
        {
            using (var cts = new CancellationTokenSource(timeout))
            {

                if (pendingTasks is not null)
                {
                    await pendingTasks.WaitAsync(timeout, cts.Token);
                }
            }
            _logger.LogInformation("Shutting down stream {QueueId}", _queueId);
        }
    }
}
