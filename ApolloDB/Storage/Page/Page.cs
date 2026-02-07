using System.Runtime.InteropServices;

namespace ApolloDB.Storage.Page;

// 7 bytes
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct PageHeader
{
    public byte HAS_FREE_SPACE;
    public ushort PG_LOWER_OFFSET; // Offset to start of free space
    public ushort PG_UPPER_OFFSET; // Offset to end of free space
    public ushort PG_SPECIAL;
}

public static class PageExtensions
{
    public static int HeaderSize => 7;
    
    public static unsafe Span<ushort> GetSlots(this ref PageHeader header)
    {
        fixed (PageHeader* ptr = &header)
        {
            byte* pageStart = (byte*)ptr;
            int slotCount = (header.PG_LOWER_OFFSET - sizeof(PageHeader)) / sizeof(ushort);
            return new Span<ushort>(pageStart + sizeof(PageHeader), slotCount);
        }
    }
    public static unsafe ref ushort GetSlot(this ref PageHeader header, int index)
    {
        return ref header.GetSlots()[index];
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

public class Frame
{
    public byte[] Data { get; set; }
    public PageId PageId { get; set; }
    public uint PinCount { get; set; }
    public bool IsDirty { get; set; }
    public ReaderWriterLockSlim Latch { get; } = new();
}
