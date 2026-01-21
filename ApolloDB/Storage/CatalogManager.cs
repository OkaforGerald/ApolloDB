using System.Collections.Concurrent;

namespace ApolloDB.Storage;

public class CatalogManager
{
    private readonly ConcurrentDictionary<uint, FileHandle> _files;
    private uint _nextFileId = 0;
    
    public class FileHandle
    {
        public uint FileId { get; }
        public string FilePath { get; }
        public FileType Type { get; }  // Heap, Index, etc.

        public FileHandle(uint fileId, string filePath, FileType type)
        {
            FileId = fileId;
            FilePath = filePath;
            Type = type;
        }
    }
    
    public uint CreateTable(string tableName)
    {
        var fileId = Interlocked.Increment(ref _nextFileId);
        var filePath = $"data/{tableName}.db";
        var handle = new FileHandle(fileId, filePath, FileType.Heap);
        _files[fileId] = handle;
        return fileId;
    }
    
    public FileHandle GetFile(uint fileId)
    {
        return _files[fileId];
    }
}

public enum FileType
{
    Heap,
    Index
}