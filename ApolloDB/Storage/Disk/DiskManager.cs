using System.Collections.Concurrent;
using ApolloDB.Storage.Buffer;
using Microsoft.Win32.SafeHandles;

namespace ApolloDB.Storage.Disk;

public class DiskManager : IAsyncDisposable
{
    private const int FRAME_SIZE = 8192;
    private readonly CatalogManager _catalogManager;
    private readonly ConcurrentDictionary<uint, SafeFileHandle> _openFiles;
    private readonly SemaphoreSlim _fileOpenLock = new SemaphoreSlim(1, 1);
    
    public DiskManager(CatalogManager catalogManager)
    {
        _catalogManager = catalogManager;
        _openFiles = new ConcurrentDictionary<uint, SafeFileHandle>();
    }
    
    public async Task WritePageAsync(PageId pageId, byte[] data)
    {
        var handle = await GetOrOpenFileAsync(pageId.FileId);
        var offset = (long)pageId.PageNumber * FRAME_SIZE;
        
        await RandomAccess.WriteAsync(handle, data, offset);
    }

    public async Task ReadPageAsync(PageId pageId, byte[] buffer)
    {
        var handle = await GetOrOpenFileAsync(pageId.FileId);
        var offset = (long)pageId.PageNumber * FRAME_SIZE;

        var bytesRead = await RandomAccess.ReadAsync(handle, buffer, offset);
        
        if (bytesRead < buffer.Length)
        {
            Array.Clear(buffer, bytesRead, buffer.Length - bytesRead);
        }
    }

    public async Task FlushAsync(PageId pageId)
    {
        var handle = await GetOrOpenFileAsync(pageId.FileId);
        
        RandomAccess.FlushToDisk(handle);
    }

    private async Task<SafeFileHandle> GetOrOpenFileAsync(uint fileId)
    {
        SafeFileHandle safeHandle;
        if (_openFiles.TryGetValue(fileId, out var handle))
            return handle;

        try
        {
            await _fileOpenLock.WaitAsync();
            var fileHandle = _catalogManager.GetFile(fileId);

            safeHandle = File.OpenHandle(
                fileHandle.FilePath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.None,
                FileOptions.RandomAccess | FileOptions.Asynchronous
            );
        }
        finally
        {
            _fileOpenLock.Release();
        }
        
        _openFiles[fileId] = safeHandle;
        return safeHandle;
    }

    public ValueTask DisposeAsync()
    {
        foreach (var handle in _openFiles.Values)
            handle.Dispose();
        _fileOpenLock.Dispose();
        return ValueTask.CompletedTask;
    }
}