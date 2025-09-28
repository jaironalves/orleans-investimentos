using Orleans.Streams;
using StackExchange.Redis;

namespace Orleans.Investimentos.Silo.Streaming.Redis;

public class RedisSequenceToken : StreamSequenceToken
{
    [Id(0)]
    public sealed override long SequenceNumber { get; protected set; }
    [Id(1)]
    public sealed override int EventIndex { get; protected set; }

    public RedisSequenceToken(RedisValue id)
    {
        [System.Diagnostics.CodeAnalysis.DoesNotReturn] static void ThrowArgumentException() => throw new ArgumentException(message: $"Invalid {nameof(id)}", paramName: nameof(id));
        var redisValueId = id.ToString();

        var splitIndex = redisValueId.IndexOf('-');
        if (splitIndex < 0)
            ThrowArgumentException();
        SequenceNumber = long.Parse(redisValueId.AsSpan(0, splitIndex));
        EventIndex = int.Parse(redisValueId.AsSpan(splitIndex + 1));
    }
    public RedisSequenceToken(long sequenceNumber, int eventIndex)
    {
        SequenceNumber = sequenceNumber;
        EventIndex = eventIndex;
    }
    public override int CompareTo(StreamSequenceToken other)
    {
        if (other is null) throw new ArgumentNullException(nameof(other));
        if (other is RedisSequenceToken token)
        {
            if (SequenceNumber == token.SequenceNumber)
            {
                return EventIndex.CompareTo(token.EventIndex);
            }
            return SequenceNumber.CompareTo(token.SequenceNumber);
        }
        throw new ArgumentException("Invalid token type", nameof(other));
    }

    public override bool Equals(StreamSequenceToken? other)
    {
        var token = other as RedisSequenceToken;
        return token != null && SequenceNumber == token.SequenceNumber && EventIndex == token.EventIndex;
    }
}