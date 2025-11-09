using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
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

    //[Id(3)]
    //public string Data { get; }

    //[Id(4)]
    //public string StreamEntryId { get; }

    [NonSerialized]
    internal RedisValue StreamEntryId;

    public StreamSequenceToken SequenceToken => SequenceTokenV2;

    public RedisStreamBatchContainer(StreamEntry streamEntry)
    {
        //var streamNamespace = streamEntry.Values[0].Value;
        //var streamKey = streamEntry.Values[1].Value;
        //var eventType = streamEntry.Values[2].Value;
        //var data = streamEntry.Values[3].Value;

        //ArgumentNullException.ThrowIfNullOrWhiteSpace(streamEntry.Id);
        //ArgumentNullException.ThrowIfNullOrWhiteSpace(streamNamespace);
        //ArgumentNullException.ThrowIfNullOrWhiteSpace(streamKey);
        //ArgumentNullException.ThrowIfNullOrWhiteSpace(eventType);
        //ArgumentNullException.ThrowIfNullOrWhiteSpace(data);

        //StreamEntryId = streamEntry.Id.ToString();
        //StreamId = StreamId.Create(streamNamespace!, streamKey!);
        //SequenceToken = CreateStreamSequenceToken(streamEntry.Id);
        //EventType = eventType!;
        //Data = data!;
    }

    private RedisStreamBatchContainer(StreamId streamId, List<object> events, Dictionary<string, object> requestContext)
    {
        StreamId = streamId;
        Events = events ?? throw new ArgumentNullException(nameof(events), "Message contains no events");
        RequestContext = requestContext;
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
        return Events
            .OfType<T>()
            .Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, SequenceTokenV2.CreateSequenceTokenForEvent(i)));

        //List<Tuple<T, StreamSequenceToken>> events = [];
        //var eventType = typeof(T).Name;
        //if (eventType == EventType)
        //{
        //    var data = Data;
        //    var @event = JsonSerializer.Deserialize<T>(data);
        //    events.Add(new(@event!, SequenceToken));
        //}
        //return events;
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

    internal static NameValueEntry[] ToStreamEntry<T>(StreamId streamId, Serializer<RedisStreamBatchContainer> serializer, IEnumerable<T> events, Dictionary<string, object> requestContext)
    {
        var redisStreamBatchContainer = new RedisStreamBatchContainer(streamId, [.. events.Cast<object>()], requestContext);
        var rawBytes = serializer.SerializeToArray(redisStreamBatchContainer);

        NameValueEntry streamNamespaceEntry = new("streamNamespace", streamId.Namespace);
        NameValueEntry streamKeyEntry = new("streamKey", streamId.Key);
        NameValueEntry dataEntry = new("data", (RedisValue)rawBytes);

        return [streamNamespaceEntry, streamKeyEntry, dataEntry];
    }

    internal static RedisStreamBatchContainer FromStreamEntry(StreamEntry streamEntry, Serializer<RedisStreamBatchContainer> serializer, long sequenceId)
    {
        var dataEntry = streamEntry.Values.FirstOrDefault(v => v.Name == "data");
        if (dataEntry.Equals(default))
        {
            throw new ArgumentException("Stream entry does not contain 'data' field.", nameof(streamEntry));
        }
        var rawBytes = (byte[])dataEntry.Value;
        var redisStreamBatchContainer = serializer.Deserialize(rawBytes);
        redisStreamBatchContainer.StreamEntryId = streamEntry.Id;
        redisStreamBatchContainer.SequenceTokenV2 = new EventSequenceTokenV2(sequenceId);

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
