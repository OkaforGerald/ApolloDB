using System.Threading.Channels;
using ApolloDB.Storage.Buffer;

namespace ApolloDB.Storage.Disk;

public class DiskScheduler : IAsyncDisposable
{
    readonly Channel<DiskRequest> _channel;
    readonly DiskManager _diskManager;
    readonly CancellationTokenSource _cts;
    readonly Task _workerTask;

    public DiskScheduler(DiskManager diskManager)
    {
        _diskManager = diskManager;
        _channel = Channel.CreateUnbounded<DiskRequest>(new UnboundedChannelOptions
        {
            SingleReader = true
        });
        _cts = new CancellationTokenSource();
        _workerTask = ProcessQueueAsync(_cts.Token);
    }

    public void Schedule(DiskRequest request)
    {
        if (!_channel.Writer.TryWrite(request))
        {
            request.Tcs.TrySetException(new InvalidOperationException("Scheduler has been disposed"));
        }
    }

    public async ValueTask ScheduleAsync(DiskRequest request, CancellationToken cancellationToken = default)
    {
        await _channel.Writer.WriteAsync(request, cancellationToken);
    }

    async Task ProcessQueueAsync(CancellationToken cancellationToken)
    {
        await foreach (var request in _channel.Reader.ReadAllAsync(cancellationToken))
        {
            try
            {
                switch (request.Operation)
                {
                    case DiskOperation.Write:
                        await _diskManager.WritePageAsync(request.PageId, request.Data);
                        request.Tcs.TrySetResult(true);
                        break;
                    case DiskOperation.Read:
                        await _diskManager.ReadPageAsync(request.PageId, request.Data);
                        request.Tcs.TrySetResult(true);
                        break;
                }
            }
            catch (Exception ex)
            {
                request.Tcs.TrySetException(ex);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.Complete();
        await _cts.CancelAsync();

        try
        {
            await _workerTask;
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }

        _cts.Dispose();
    }
}

public enum DiskOperation
{
    Read,
    Write
}

public readonly struct DiskRequest
{
    public DiskOperation Operation { get; init; }
    public PageId PageId { get; init; }
    public byte[] Data { get; init; }
    public TaskCompletionSource<bool> Tcs { get; init; }
}
