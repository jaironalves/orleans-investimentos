using Microsoft.Extensions.Options;
using StackExchange.Redis;
using StackExchange.Redis.Maintenance;
using StackExchange.Redis.Profiling;
using System.Net;

namespace Orleans.Investimentos.Silo.Storage;

public class ConnectionMultiplexerWrapper(IConnectionMultiplexer inner, int? defaultDatabase) : IConnectionMultiplexer
{
    /// <summary>
    /// Obtain an interactive connection to a database inside redis.
    /// </summary>
    /// <param name="db">The database ID to get.</param>
    /// <param name="asyncState">The async state to pass to the created <see cref="IDatabase"/>.</param>
    IDatabase IConnectionMultiplexer.GetDatabase(int db, object? asyncState)
    {
        var dbGet = db == -1 ? defaultDatabase.GetValueOrDefault(-1) : db;
        return inner.GetDatabase(dbGet, asyncState);
    }

    string IConnectionMultiplexer.ClientName => inner.ClientName;
    string IConnectionMultiplexer.Configuration => inner.Configuration;
    int IConnectionMultiplexer.TimeoutMilliseconds => inner.TimeoutMilliseconds;
    long IConnectionMultiplexer.OperationCount => inner.OperationCount;
    bool IConnectionMultiplexer.PreserveAsyncOrder
    {
        get => inner.PreserveAsyncOrder;
        set => inner.PreserveAsyncOrder = value;
    }
    bool IConnectionMultiplexer.IsConnected => inner.IsConnected;
    bool IConnectionMultiplexer.IsConnecting => inner.IsConnecting;
    bool IConnectionMultiplexer.IncludeDetailInExceptions
    {
        get => inner.IncludeDetailInExceptions;
        set => inner.IncludeDetailInExceptions = value;
    }
    int IConnectionMultiplexer.StormLogThreshold
    {
        get => inner.StormLogThreshold;
        set => inner.StormLogThreshold = value;
    }

    event EventHandler<RedisErrorEventArgs>? IConnectionMultiplexer.ErrorMessage
    {
        add => inner.ErrorMessage += value;
        remove => inner.ErrorMessage -= value;
    }
    event EventHandler<ConnectionFailedEventArgs>? IConnectionMultiplexer.ConnectionFailed
    {
        add => inner.ConnectionFailed += value;
        remove => inner.ConnectionFailed -= value;
    }
    event EventHandler<InternalErrorEventArgs>? IConnectionMultiplexer.InternalError
    {
        add => inner.InternalError += value;
        remove => inner.InternalError -= value;
    }
    event EventHandler<ConnectionFailedEventArgs>? IConnectionMultiplexer.ConnectionRestored
    {
        add => inner.ConnectionRestored += value;
        remove => inner.ConnectionRestored -= value;
    }
    event EventHandler<EndPointEventArgs>? IConnectionMultiplexer.ConfigurationChanged
    {
        add => inner.ConfigurationChanged += value;
        remove => inner.ConfigurationChanged -= value;
    }
    event EventHandler<EndPointEventArgs>? IConnectionMultiplexer.ConfigurationChangedBroadcast
    {
        add => inner.ConfigurationChangedBroadcast += value;
        remove => inner.ConfigurationChangedBroadcast -= value;
    }
    event EventHandler<ServerMaintenanceEvent>? IConnectionMultiplexer.ServerMaintenanceEvent
    {
        add => inner.ServerMaintenanceEvent += value;
        remove => inner.ServerMaintenanceEvent -= value;
    }
    event EventHandler<HashSlotMovedEventArgs>? IConnectionMultiplexer.HashSlotMoved
    {
        add => inner.HashSlotMoved += value;
        remove => inner.HashSlotMoved -= value;
    }

    void IConnectionMultiplexer.AddLibraryNameSuffix(string suffix) => inner.AddLibraryNameSuffix(suffix);
    void IConnectionMultiplexer.Close(bool allowCommandsToComplete) => inner.Close(allowCommandsToComplete);
    Task IConnectionMultiplexer.CloseAsync(bool allowCommandsToComplete) => inner.CloseAsync(allowCommandsToComplete);
    bool IConnectionMultiplexer.Configure(TextWriter? log) => inner.Configure(log);
    Task<bool> IConnectionMultiplexer.ConfigureAsync(TextWriter? log) => inner.ConfigureAsync(log);
    void IDisposable.Dispose() => ((IDisposable)inner).Dispose();
    ValueTask IAsyncDisposable.DisposeAsync() => ((IAsyncDisposable)inner).DisposeAsync();
    void IConnectionMultiplexer.ExportConfiguration(Stream destination, ExportOptions options) => inner.ExportConfiguration(destination, options);
    ServerCounters IConnectionMultiplexer.GetCounters() => inner.GetCounters();
    EndPoint[] IConnectionMultiplexer.GetEndPoints(bool configuredOnly) => inner.GetEndPoints(configuredOnly);
    int IConnectionMultiplexer.GetHashSlot(RedisKey key) => inner.GetHashSlot(key);
    IServer IConnectionMultiplexer.GetServer(string host, int port, object? asyncState) => inner.GetServer(host, port, asyncState);
    IServer IConnectionMultiplexer.GetServer(string hostAndPort, object? asyncState) => inner.GetServer(hostAndPort, asyncState);
    IServer IConnectionMultiplexer.GetServer(IPAddress host, int port) => inner.GetServer(host, port);
    IServer IConnectionMultiplexer.GetServer(EndPoint endpoint, object? asyncState) => inner.GetServer(endpoint, asyncState);
    IServer[] IConnectionMultiplexer.GetServers() => inner.GetServers();
    string IConnectionMultiplexer.GetStatus() => inner.GetStatus();
    void IConnectionMultiplexer.GetStatus(TextWriter log) => inner.GetStatus(log);
    string? IConnectionMultiplexer.GetStormLog() => inner.GetStormLog();
    ISubscriber IConnectionMultiplexer.GetSubscriber(object? asyncState) => inner.GetSubscriber(asyncState);
    int IConnectionMultiplexer.HashSlot(RedisKey key) => inner.HashSlot(key);
    long IConnectionMultiplexer.PublishReconfigure(CommandFlags flags) => inner.PublishReconfigure(flags);
    Task<long> IConnectionMultiplexer.PublishReconfigureAsync(CommandFlags flags) => inner.PublishReconfigureAsync(flags);
    void IConnectionMultiplexer.RegisterProfiler(Func<ProfilingSession?> profilingSessionProvider) => inner.RegisterProfiler(profilingSessionProvider);
    void IConnectionMultiplexer.ResetStormLog() => inner.ResetStormLog();
    string IConnectionMultiplexer.ToString() => inner.ToString();
    void IConnectionMultiplexer.Wait(Task task) => inner.Wait(task);
    T IConnectionMultiplexer.Wait<T>(Task<T> task) => inner.Wait(task);
    void IConnectionMultiplexer.WaitAll(params Task[] tasks) => inner.WaitAll(tasks);
}
