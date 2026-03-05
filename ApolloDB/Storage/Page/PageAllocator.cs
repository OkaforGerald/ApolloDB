using System.Collections.Concurrent;
using System.Numerics;
using System.Runtime.InteropServices;

namespace ApolloDB.Storage.Page;

public class PageAllocator
{
    private readonly ConcurrentDictionary<uint, ulong[]> _fileBitmaps = new();
    private readonly ConcurrentDictionary<uint, object> _fileLocks = new();
    private readonly int _maxPagesPerFile;

    public PageAllocator(int maxPagesPerFile = 65536)
    {
        _maxPagesPerFile = maxPagesPerFile;
    }

    public void RestoreFile(uint fileId, ulong[] savedBitmap)
    {
        _fileBitmaps[fileId] = savedBitmap;
        _fileLocks.TryAdd(fileId, new object());
    }

    public int AllocatePage(uint fileId)
    {
        int wordCount = (_maxPagesPerFile + 63) / 64;
        var bitmap = _fileBitmaps.GetOrAdd(fileId, _ => new ulong[wordCount]);
        var fileLock = _fileLocks.GetOrAdd(fileId, _ => new object());

        lock (fileLock)
        {
            for (int i = 0; i < bitmap.Length; i++)
            {
                if (bitmap[i] == ulong.MaxValue) continue;

                ulong freeBits = ~bitmap[i];
                int bitIndex = BitOperations.TrailingZeroCount(freeBits);
                bitmap[i] |= (1UL << bitIndex);
                return (i * 64) + bitIndex;
            }
        }

        return -1; // File is full
    }

    public void DeallocatePage(uint fileId, int pageNumber)
    {
        if (!_fileBitmaps.TryGetValue(fileId, out var bitmap) ||
            !_fileLocks.TryGetValue(fileId, out var fileLock))
            return; // Never allocated from, nothing to deallocate

        int wordIndex = pageNumber / 64;
        int bitIndex = pageNumber % 64;
        lock (fileLock)
        {
            bitmap[wordIndex] &= ~(1UL << bitIndex);
        }
    }

    // Locks each file's bitmap in turn and clones it — safe for the flusher to call concurrently.
    public Dictionary<uint, ulong[]> SnapshotBitmaps()
    {
        var snapshot = new Dictionary<uint, ulong[]>(_fileBitmaps.Count);
        foreach (var (fileId, bitmap) in _fileBitmaps)
        {
            if (!_fileLocks.TryGetValue(fileId, out var fileLock)) continue;
            lock (fileLock)
            {
                snapshot[fileId] = (ulong[])bitmap.Clone();
            }
        }
        return snapshot;
    }
}

public class BitMaskFlusher : IAsyncDisposable
{
    private readonly PeriodicTimer _timer;
    private readonly CancellationTokenSource _cts = new();
    private readonly PageAllocator _allocator;
    private readonly string _bitmapFilePath;
    private Task? _timerTask;

    public BitMaskFlusher(PageAllocator allocator, string bitmapFilePath, TimeSpan interval)
    {
        _allocator = allocator;
        _bitmapFilePath = bitmapFilePath;
        _timer = new PeriodicTimer(interval);
    }

    public void Start()
    {
        _timerTask = RunFlushingLoopAsync();
    }

    private async Task RunFlushingLoopAsync()
    {
        try
        {
            while (await _timer.WaitForNextTickAsync(_cts.Token))
            {
                await PerformFlushingAsync();
            }
        }
        catch (OperationCanceledException) { /* Normal shutdown */ }
    }

    private async Task PerformFlushingAsync()
    {
        var snapshot = _allocator.SnapshotBitmaps();

        await using var fs = new FileStream(
            _bitmapFilePath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.None,
            bufferSize: 65536,
            useAsync: true);

        foreach (var (fileId, bitmap) in snapshot)
        {
            // Layout per file: [fileId: 4B][wordCount: 4B][bitmap: wordCount * 8B]
            await fs.WriteAsync(BitConverter.GetBytes(fileId));
            await fs.WriteAsync(BitConverter.GetBytes(bitmap.Length));
            await fs.WriteAsync(MemoryMarshal.AsBytes(bitmap.AsSpan()).ToArray());
        }
    }

    // Call on startup to restore state into a PageAllocator.
    public static async Task LoadIntoAsync(string bitmapFilePath, PageAllocator allocator)
    {
        if (!File.Exists(bitmapFilePath)) return;

        await using var fs = new FileStream(
            bitmapFilePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 65536,
            useAsync: true);

        var header = new byte[8];
        while (await fs.ReadAsync(header.AsMemory(0, 8)) == 8)
        {
            uint fileId = BitConverter.ToUInt32(header, 0);
            int wordCount = BitConverter.ToInt32(header, 4);

            var bitmapBytes = new byte[wordCount * sizeof(ulong)];
            await fs.ReadExactlyAsync(bitmapBytes);

            var bitmap = MemoryMarshal.Cast<byte, ulong>(bitmapBytes.AsSpan()).ToArray();
            allocator.RestoreFile(fileId, bitmap);
        }
    }

    public async Task StopAsync()
    {
        _cts.Cancel();
        if (_timerTask != null)
            await _timerTask;

        await PerformFlushingAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _timer.Dispose();
        _cts.Dispose();
    }
}
