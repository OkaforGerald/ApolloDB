using System.Buffers;
using System.Collections.Concurrent;
using ApolloDB.Storage.Disk;

namespace ApolloDB.Storage.Buffer;

public class BufferPoolManager
{
    const int MAX_BUFFER_SIZE = 128;
    const int FRAME_SIZE = 8192;
    private readonly Frame[] _frames;
    private readonly ConcurrentDictionary<PageId, int> _pageTable;
    private readonly IReplacer _replacer;
    private readonly DiskScheduler _diskScheduler;
    private readonly ArrayPool<byte> _bytePool;
    private readonly ConcurrentQueue<int> _freePageIndices;

    public BufferPoolManager()
    {
        _frames = new Frame[MAX_BUFFER_SIZE];
        _bytePool = ArrayPool<byte>.Shared;
        for (int i = 0; i < MAX_BUFFER_SIZE; i++)
        {
            _frames[i] = new Frame
            {
                Data = _bytePool.Rent(FRAME_SIZE),
                PageId = default,
                PinCount = 0,
                IsDirty = false
            };
        }

        _pageTable = new ConcurrentDictionary<PageId, int>();
        _replacer = new ArcReplacer(MAX_BUFFER_SIZE);
        _freePageIndices = new ConcurrentQueue<int>(Enumerable.Range(0, MAX_BUFFER_SIZE));
        _diskScheduler = new DiskScheduler(new DiskManager());
    }

    public class Frame
    {
        public byte[] Data { get; set; }
        public PageId PageId { get; set; }
        public uint PinCount { get; set; }
        public bool IsDirty { get; set; }
        public ReaderWriterLockSlim Latch { get; } = new();
    }

    // Removes a page from the database, both on disk and in memory.
    public async Task<bool> DeletePage(PageId pageId)
    {
        if (_pageTable.TryGetValue(pageId, out var frameIdx))
        {
            var frame = _frames[frameIdx];
            
            frame.Latch.EnterWriteLock();
            if(frame.PinCount > 0) return false;
            _replacer.SetEvictable(pageId, true);
            frame.Latch.ExitWriteLock();
            
            var request = new DiskRequest
            {
                Operation = DiskOperation.Write,
                PageId = pageId,
                Data = Array.Empty<byte>(),
                Tcs = new TaskCompletionSource<bool>(),
            };
            await _diskScheduler.ScheduleAsync(request);
            await request.Tcs.Task;  
            
            _freePageIndices.Enqueue(frameIdx);
            _pageTable.TryRemove(pageId, out _);
        }

        return false;
    }

    public async Task<Frame> ReadPage(PageId pageId)
    {
        Frame frame;
        int? targetFrameIdx;
        bool isInMemory = true;
        if (_pageTable.TryGetValue(pageId, out var pageIdx))
        {
            targetFrameIdx = pageIdx;
        }
        else
        {
            // Not in buffer; Read from Disk
            isInMemory = false;
            if (_freePageIndices.TryDequeue(out int frameIdx))
            {
                targetFrameIdx = frameIdx;
            }
            else
            {
                // Evict something
                var victimPageId = _replacer.Evict();
                if (victimPageId.HasValue)
                {
                    var victimFrameIdx = _pageTable[victimPageId.Value];
                    var victimFrame = _frames[victimFrameIdx];

                    if (victimFrame.IsDirty)
                    {
                        await FlushPageAsync(victimPageId.Value);
                    }
                    
                    _pageTable.TryRemove(victimPageId.Value, out _);
                    targetFrameIdx = victimFrameIdx;
                }
                else
                {
                    throw new InvalidOperationException("All frames in memory have been pinned");
                }
            }
        }
        if (targetFrameIdx.HasValue)
        {
            frame = _frames[targetFrameIdx.Value];
            frame.Latch.EnterWriteLock();
            if (!isInMemory)
            {
                var request = new DiskRequest
                {
                    Operation = DiskOperation.Read,
                    PageId = pageId,
                    Data = _frames[targetFrameIdx.Value].Data,
                    Tcs = new TaskCompletionSource<bool>(),
                };
                await _diskScheduler.ScheduleAsync(request);
                await request.Tcs.Task; 
                _pageTable.TryAdd(pageId, targetFrameIdx.Value);
            }
            frame.PinCount++;
            _replacer.RecordAccess(pageId);
            _replacer.SetEvictable(pageId, false);
            frame.Latch.ExitWriteLock();

            return frame;
        }
        else
        {
            throw new InvalidOperationException("Failed to obtain frame");
        }
    }
    
    public void UnpinPage(PageId pageId, bool isDirty)
    {
        if (!_pageTable.TryGetValue(pageId, out var frameIdx))
        {
            return;
        }

        var frame = _frames[frameIdx];
    
        frame.Latch.EnterWriteLock();
        try
        {
            if (frame.PinCount > 0)
            {
                frame.PinCount--;
            
                if (isDirty)
                {
                    frame.IsDirty = true;
                }
                
                if (frame.PinCount == 0)
                {
                    _replacer.SetEvictable(pageId, true);
                }
            }
        }
        finally
        {
            frame.Latch.ExitWriteLock();
        }
    }

    public async Task FlushPageAsync(PageId pageId)
    {
    }

    public async Task FlushAllPages()
    {
    }

    public uint GetPinCount(PageId pageId)
    {
        if (!_pageTable.TryGetValue(pageId, out var frameIdx))
            return 0;
        
        var frame = _frames[frameIdx];
        frame.Latch.EnterReadLock();
        try
        {
            return frame.PinCount;
        }
        finally
        {
            frame.Latch.ExitReadLock();
        }
    }
}

public readonly struct PageId : IEquatable<PageId>
{
    public uint FileId { get; }
    public uint PageNumber { get; }

    public PageId(uint fileId, uint pageNumber)
    {
        FileId = fileId;
        PageNumber = pageNumber;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(FileId, PageNumber);
    }

    public bool Equals(PageId other)
    {
        return FileId == other.FileId && PageNumber == other.PageNumber;
    }
}