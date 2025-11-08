using Orleans.Providers.Streams.Common;
using Orleans.Streams;
using StackExchange.Redis;
using System.Text.Json;

namespace Orleans.Investimentos.Silo.Streaming.Redis;

public class RedisStreamBatchContainer : IBatchContainer
{
    [Id(0)]
    public StreamId StreamId { get; }

    [Id(1)]
    public StreamSequenceToken SequenceToken { get; }

    [Id(2)]
    public string EventType { get; }

    [Id(3)]
    public string Data { get; }

    [Id(4)]
    public string StreamEntryId { get; }

    public RedisStreamBatchContainer(StreamEntry streamEntry)
    {
        var streamNamespace = streamEntry.Values[0].Value;
        var streamKey = streamEntry.Values[1].Value;
        var eventType = streamEntry.Values[2].Value;
        var data = streamEntry.Values[3].Value;

        ArgumentNullException.ThrowIfNullOrWhiteSpace(streamEntry.Id);
        ArgumentNullException.ThrowIfNullOrWhiteSpace(streamNamespace);
        ArgumentNullException.ThrowIfNullOrWhiteSpace(streamKey);
        ArgumentNullException.ThrowIfNullOrWhiteSpace(eventType);
        ArgumentNullException.ThrowIfNullOrWhiteSpace(data);

        StreamEntryId = streamEntry.Id.ToString();
        StreamId = StreamId.Create(streamNamespace!, streamKey!);
        SequenceToken = CreateStreamSequenceToken(streamEntry.Id);
        EventType = eventType!;
        Data = data!;
    }

    public StreamSequenceToken CreateStreamSequenceToken(RedisValue id)
    {        
        var redisValueId = id.ToString();

        var splitIndex = redisValueId.IndexOf('-');
        if (splitIndex < 0)
        {   
            throw new ArgumentException(message: $"Invalid {nameof(id)}", paramName: nameof(id));
        }

        var sequenceNumber = long.Parse(redisValueId.AsSpan(0, splitIndex));
        var eventIndex = int.Parse(redisValueId.AsSpan(splitIndex + 1));

        return new EventSequenceTokenV2(sequenceNumber, eventIndex);
    }

    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
    {
        List<Tuple<T, StreamSequenceToken>> events = [];
        var eventType = typeof(T).Name;
        if (eventType == EventType)
        {
            var data = Data;
            var @event = JsonSerializer.Deserialize<T>(data);
            events.Add(new(@event!, SequenceToken));
        }
        return events;
    }

    public bool ImportRequestContext()
    {
        return false;
    }

    internal static IEnumerable<NameValueEntry[]> ToStreamEntries<T>(StreamId streamId, IEnumerable<T> events)
    {
        foreach (var @event in events)
        {
            NameValueEntry streamNamespaceEntry = new("streamNamespace", streamId.Namespace);
            NameValueEntry streamKeyEntry = new("streamKey", streamId.Key);
            NameValueEntry eventTypeEntry = new("eventType", @event!.GetType().Name);
            NameValueEntry dataEntry = new("data", JsonSerializer.Serialize(@event));

            yield return [streamNamespaceEntry, streamKeyEntry, eventTypeEntry, dataEntry];
        }
    }
}
