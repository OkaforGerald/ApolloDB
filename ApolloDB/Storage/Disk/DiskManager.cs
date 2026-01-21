using ApolloDB.Storage.Buffer;

namespace ApolloDB.Storage.Disk;

public class DiskManager
{
    const int FRAME_SIZE = 8192;
    private CatalogManager _catalogManager = new CatalogManager();

    private FileStreamOptions _writeStreamOptions = new()
    {
        BufferSize = FRAME_SIZE,
        Access = FileAccess.Write,
        Mode = FileMode.Open,
        Options = FileOptions.WriteThrough | FileOptions.RandomAccess // Direct IO and Yeah
    };

    private FileStreamOptions _readStreamOptions = new()
    {
        BufferSize = FRAME_SIZE,
        Access = FileAccess.Read,
        Mode = FileMode.Open,
        Options = FileOptions.RandomAccess
    };
    
    public async Task WritePageAsync(PageId pageId, byte[] data)
    {
        if (data.Length != FRAME_SIZE)
            throw new ArgumentException($"Data must be exactly {FRAME_SIZE} bytes", nameof(data));

        var fileHandle = _catalogManager.GetFile(pageId.FileId);
    
        using (var fStream = File.Open(fileHandle.FilePath, _writeStreamOptions))
        {
            fStream.Seek((long)(FRAME_SIZE * pageId.PageNumber), SeekOrigin.Begin);
            await fStream.WriteAsync(data, 0, data.Length);
            await fStream.FlushAsync();
        }
    }

    public async Task ReadPageAsync(PageId pageId, byte[] buffer)
    {
        if (buffer.Length != FRAME_SIZE)
            throw new ArgumentException($"Buffer must be exactly {FRAME_SIZE} bytes", nameof(buffer));

        var fileHandle = _catalogManager.GetFile(pageId.FileId);
    
        using (var fStream = File.Open(fileHandle.FilePath, _readStreamOptions))
        {
            fStream.Seek((long)(FRAME_SIZE * pageId.PageNumber), SeekOrigin.Begin);
        
            int totalRead = 0;
            while (totalRead < buffer.Length)
            {
                int bytesRead = await fStream.ReadAsync(buffer, totalRead, buffer.Length - totalRead);
                if (bytesRead == 0)
                    throw new EndOfStreamException($"Unexpected end of file when reading page {pageId}");
                totalRead += bytesRead;
            }
        }
    }
}
