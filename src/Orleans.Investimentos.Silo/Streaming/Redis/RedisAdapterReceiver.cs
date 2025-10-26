using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Investimentos.Silo.Streaming.Redis.Storage;
using Orleans.Streams;
using StackExchange.Redis;

namespace Orleans.Investimentos.Silo.Streaming.Redis
{
    public class RedisAdapterReceiver : IQueueAdapterReceiver
    {
        private RedisStorage? streamStorage;
        private readonly QueueId queueId;
        private readonly TimeProvider timeProvider;
        private readonly ILogger<RedisAdapterReceiver> logger;

        private Task? outstandingTask;
        private string lastId = "0";

        
        private DateTimeOffset _lastTrimTime;        

        //private TimeProvider _timeProvider;
        //private readonly RedisQueueAdapterReceiverOptions _receiverOptions; // Added options field

        // Changed: Constructor to accept TimeProvider and IOptions<RedisStreamReceiverOptions>
        //public RedisQueueAdapterReceiver(QueueId queueId,
        //                         IDatabase database,
        //                         ILogger<RedisQueueAdapterReceiver> logger,
        //                         TimeProvider? timeProvider = null,
        //                         IOptions<RedisQueueAdapterReceiverOptions>? receiverOptions = null)
        //{
        //    _queueId = queueId;
        //    _database = database ?? throw new ArgumentNullException(nameof(database));
        //    _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        //    _timeProvider = timeProvider ?? TimeProvider.System;
        //    _receiverOptions = receiverOptions?.Value ?? new RedisQueueAdapterReceiverOptions(); // Use provided options or default
        //    _lastTrimTime = _timeProvider.GetUtcNow();
        //}

        internal static IQueueAdapterReceiver Create(RedisStorage storage, 
            QueueId queueId, TimeProvider timeProvider, ILoggerFactory loggerFactory)
        {            
            if (queueId.IsDefault) throw new ArgumentNullException(nameof(queueId));
            ArgumentNullException.ThrowIfNull(timeProvider);
            ArgumentNullException.ThrowIfNull(loggerFactory);
                        
            return new RedisAdapterReceiver(storage, queueId, timeProvider, loggerFactory.CreateLogger<RedisAdapterReceiver>());            
        }

        private RedisAdapterReceiver(
            RedisStorage streamStorage,
            QueueId queueId, TimeProvider timeProvider,
            ILogger<RedisAdapterReceiver> logger)
        {
            this.streamStorage = streamStorage;
            this.queueId = queueId;
            this.timeProvider = timeProvider;
            this.logger = logger;
        }

        // This method might be less relevant if options are passed via constructor, 
        // but kept for now if direct TimeProvider manipulation is still needed for some tests.
        public void SetTimeProvider(TimeProvider timeProvider)
        {
            //_timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
            //_lastTrimTime = _timeProvider.GetUtcNow();
        }

        public async Task<IList<IBatchContainer>?> GetQueueMessagesAsync(int maxCount)
        {
            try
            {
                var streamStorageRef = streamStorage; // store direct ref, in case we are somehow asked to shutdown while we are receiving.
                if (streamStorageRef == null) return [];

                var task = streamStorageRef
                    .GetEntriesAsync(lastId, maxCount);

                outstandingTask = task;
                lastId = ">";

                var streamMessages = (await task)
                    .Select(streamEntry => new RedisBatchContainer(streamEntry))
                    .ToList<IBatchContainer>();

                await TrimStreamIfNeeded();

                return streamMessages;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error reading from stream {QueueId}", queueId);
                return default;
            }
            finally
            {
                outstandingTask = null;
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

        public Task Initialize(TimeSpan timeout)
        {
            if (streamStorage != null) // check in case we already shut it down.
            {
                return streamStorage.InitAsync();
            }
            return Task.CompletedTask;

            //try
            //{
            //    using (var cts = new CancellationTokenSource(timeout))
            //    {
            //        var task = _database.StreamCreateConsumerGroupAsync(_queueId.ToString(), "consumer", "$", true);
            //        await task.WaitAsync(timeout, cts.Token);
            //    }
            //}
            //catch (Exception ex) when (ex.Message.Contains("name already exists")) { }
            //catch (Exception ex)
            //{
            //    _logger.LogError(ex, "Error initializing stream {QueueId}", _queueId);
            //}
        }

        public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            try
            {
                foreach (var message in messages)
                {
                    if (message is RedisBatchContainer container)
                    {
                        var ackTask = streamStorage
                            .EntryDeliveredAsync(container.StreamEntryId);
                        outstandingTask = ackTask;
                        await ackTask;
                    }
                }
            }
            catch (Exception ex)
            {
                //logger.LogError(ex, "Error acknowledging messages in stream {QueueId}", _queueId);
            }
            finally
            {
                outstandingTask = null;
            }
        }

        public async Task Shutdown(TimeSpan timeout)
        {
            try
            {
                // await the last storage operation, so after we shutdown and stop this receiver we don't get async operation completions from pending storage operations.
                if (outstandingTask != null)
                    await outstandingTask;
            }
            finally
            {
                // remember that we shut down so we never try to read from the queue again.
                streamStorage = null;
            }

            //using (var cts = new CancellationTokenSource(timeout))
            //{

            //    if (outstandingTask is not null)
            //    {
            //        await outstandingTask.WaitAsync(timeout, cts.Token);
            //    }
            //}
            //logger.LogInformation("Shutting down stream {QueueId}", queueId);
        }        
    }
}
