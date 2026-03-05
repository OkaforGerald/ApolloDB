using System.Collections.Concurrent;

namespace ApolloDB.Storage;

public class CatalogManager
{
    private readonly ConcurrentDictionary<string, FileHandle> _filesByName;
    private readonly ConcurrentDictionary<uint, FileHandle> _filesById;
    private uint _nextFileId;

    public CatalogManager()
    {
        _filesByName = new ConcurrentDictionary<string, FileHandle>();
        _filesById = new ConcurrentDictionary<uint, FileHandle>();
        _nextFileId = 0;
    }

    public class FileHandle
    {
        public string Name { get; }
        public uint FileId { get; }
        public string FilePath { get; }
        public FileType Type { get; }

        public FileHandle(string name, uint fileId, string filePath, FileType type)
        {
            Name = name;
            FileId = fileId;
            FilePath = filePath;
            Type = type;
        }
    }
    
    public FileHandle RegisterFile(string name, string filePath, FileType type)
    {
        var fileId = Interlocked.Increment(ref _nextFileId);
        var handle = new FileHandle(name, fileId, filePath, type);

        if (!_filesByName.TryAdd(name, handle))
            throw new InvalidOperationException($"File '{name}' is already registered.");

        _filesById[fileId] = handle;
        return handle;
    }
    
    public FileHandle GetFile(string name)
    {
        if (_filesByName.TryGetValue(name, out var handle))
            return handle;

        throw new KeyNotFoundException($"No file registered with name '{name}'.");
    }
    
    public FileHandle GetFile(uint fileId)
    {
        if (_filesById.TryGetValue(fileId, out var handle))
            return handle;

        throw new KeyNotFoundException($"No file registered with FileId {fileId}.");
    }

    public bool TryGetFile(string name, out FileHandle? handle)
    {
        return _filesByName.TryGetValue(name, out handle);
    }

    public bool Contains(string name) => _filesByName.ContainsKey(name);
}

public enum FileType
{
    Heap,
    Index
}
