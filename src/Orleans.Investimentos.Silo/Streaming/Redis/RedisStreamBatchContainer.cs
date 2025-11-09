using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;
using StackExchange.Redis;
using System.Text.Json;

namespace Orleans.Investimentos.Silo.Streaming.Redis;

[GenerateSerializer]
[Alias(nameof(RedisStreamBatchContainer))]
public class RedisStreamBatchContainer : IBatchContainer
{
    [Id(0)]
    public StreamId StreamId { get; set; }

    [Id(1)]
    public EventSequenceTokenV2 SequenceTokenV2 { get; set; }

    [Id(2)]
    public List<object> Events { get; set; }

    [Id(3)]
    public Dictionary<string, object> RequestContext { get; set; }   

    [NonSerialized]
    internal RedisValue StreamEntryId;

    public StreamSequenceToken SequenceToken => SequenceTokenV2;

    private RedisStreamBatchContainer(StreamId streamId, List<object> events, Dictionary<string, object> requestContext)
    {
        StreamId = streamId;
        Events = events ?? throw new ArgumentNullException(nameof(events), "Message contains no events");
        RequestContext = requestContext;
    }   

    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
    {
        return Events
            .OfType<T>()
            .Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, SequenceTokenV2.CreateSequenceTokenForEvent(i)));     
    }

    public bool ImportRequestContext()
    {
        if (RequestContext != null)
        {
            RequestContextExtensions.Import(RequestContext);
            return true;
        }
        return false;
    }

    internal static EventSequenceTokenV2 GetSequenceTokenFromStreamEntryId(RedisValue streamEntryId)
    {
        var redisValueId = streamEntryId.ToString();

        var splitIndex = redisValueId.IndexOf('-');
        if (splitIndex < 0)
        {
            throw new ArgumentException(message: $"Invalid {nameof(streamEntryId)}", paramName: nameof(streamEntryId));
        }

        var sequenceNumber = long.Parse(redisValueId.AsSpan(0, splitIndex));
        return new EventSequenceTokenV2(sequenceNumber);
    }

    internal static NameValueEntry[] ToStreamEntry<T>(StreamId streamId, Serializer<RedisStreamBatchContainer> serializer, IEnumerable<T> events, Dictionary<string, object> requestContext)
    {
        var redisStreamBatchContainer = new RedisStreamBatchContainer(streamId, [.. events.Cast<object>()], requestContext);
        var rawBytes = serializer.SerializeToArray(redisStreamBatchContainer);
        var base64String = Convert.ToBase64String(rawBytes);

        NameValueEntry streamNamespaceEntry = new("namespace", streamId.Namespace);
        NameValueEntry streamKeyEntry = new("key", streamId.Key);
        NameValueEntry dataEntry = new("data", (RedisValue)base64String);

        return [streamNamespaceEntry, streamKeyEntry, dataEntry];
    }

    internal static RedisStreamBatchContainer FromStreamEntry(StreamEntry streamEntry, Serializer<RedisStreamBatchContainer> serializer)
    {
        var dataEntry = streamEntry.Values.FirstOrDefault(v => v.Name == "data");
        if (dataEntry.Equals(default))
        {
            throw new ArgumentException("Stream entry does not contain 'data' field.", nameof(streamEntry));
        }
        var base64String = (string)dataEntry.Value;
        var rawBytes = Convert.FromBase64String(base64String);
        var redisStreamBatchContainer = serializer.Deserialize(rawBytes);
        redisStreamBatchContainer.StreamEntryId = streamEntry.Id;
        redisStreamBatchContainer.SequenceTokenV2 = GetSequenceTokenFromStreamEntryId(streamEntry.Id);

        return redisStreamBatchContainer;
    }

    internal static IEnumerable<NameValueEntry[]> ToStreamEntries<T>(StreamId streamId, Serializer<RedisStreamBatchContainer> serializer, IEnumerable<T> events, Dictionary<string, object> requestContext)
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
