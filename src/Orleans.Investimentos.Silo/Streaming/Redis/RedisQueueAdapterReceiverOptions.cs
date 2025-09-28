namespace Orleans.Investimentos.Silo.Streaming.Redis
{
    public class RedisQueueAdapterReceiverOptions
    {
        public int MaxStreamLength { get; set; } = 1000; // Default value from previous const
        public int TrimTimeMinutes { get; set; } = 5;   // Default value from previous const
    }
}
