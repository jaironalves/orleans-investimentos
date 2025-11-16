using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;
using StackExchange.Redis;

namespace Orleans.Investimentos.Silo.Streaming.Redis;

[GenerateSerializer]
[Alias(nameof(RedisStreamBatchContainer))]
internal class RedisStreamBatchContainer : IBatchContainer
{
    [Id(0)]
    public StreamId StreamId { get; set; }

    [Id(1)]
    public StreamSequenceToken SequenceToken { get; private set; }

    [Id(2)]
    public List<object> Events { get; set; }

    [Id(3)]
    public Dictionary<string, object> RequestContext { get; set; }

    [NonSerialized]
    internal EventSequenceTokenV2 sequenceTokenV2;

    internal EventSequenceTokenV2 RealSequenceToken
    {
        set
        {
            sequenceTokenV2 = value;
            SequenceToken = sequenceTokenV2;
        }
    }

    public RedisStreamBatchContainer() { }

    public RedisStreamBatchContainer(StreamId streamId, List<object> events, Dictionary<string, object> requestContext)
    {
        StreamId = streamId;
        Events = events ?? throw new ArgumentNullException(nameof(events), "Message contains no events");
        RequestContext = requestContext;
    }

    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
    {
        return Events
            .OfType<T>()
            .Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, sequenceTokenV2.CreateSequenceTokenForEvent(i)));
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
}
