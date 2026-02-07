using System.Runtime.InteropServices;

namespace ApolloDB.Storage.Page;

/// <summary>
/// Record Identifier - points to a specific tuple in a page.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly struct Rid
{
    public readonly uint PageNumber;
    public readonly ushort SlotNumber;

    public Rid(uint pageNumber, ushort slotNumber)
    {
        PageNumber = pageNumber;
        SlotNumber = slotNumber;
    }

    public static readonly Rid Invalid = new(uint.MaxValue, ushort.MaxValue);
    public bool IsValid => PageNumber != uint.MaxValue;
}

/// <summary>
/// Special area at the end of B+tree pages.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct BTreePageSpecial
{
    public byte PageType;       // 0 = leaf, 1 = internal
    public byte Level;          // 0 for leaves, increases toward root
    public uint NextPageNumber; // Right sibling (leaves only, 0 = none)
    public uint PrevPageNumber; // Left sibling (leaves only, 0 = none)

    public const int Size = 10;
    public const byte LeafType = 0;
    public const byte InternalType = 1;
}

public ref struct BPlusTreeLeafPage
{
    public const int PageSize = 8192;
    public const int TupleSize = 6 + 8; // RID + Key

    private readonly Span<byte> _data;

    public BPlusTreeLeafPage(Span<byte> data)
    {
        _data = data;
    }

    public static BPlusTreeLeafPage From(Frame frame) => new(frame.Data);

    public bool IsLeaf => true;

    private ref PageHeader GetHeader()
    {
        return ref MemoryMarshal.AsRef<PageHeader>(_data[..PageExtensions.HeaderSize]);
    }

    private ref BTreePageSpecial GetSpecial()
    {
        ref var header = ref GetHeader();
        return ref MemoryMarshal.AsRef<BTreePageSpecial>(
            _data.Slice(header.PG_SPECIAL, BTreePageSpecial.Size));
    }

    private Span<ushort> GetSlotArray()
    {
        ref var header = ref GetHeader();
        int slotCount = (header.PG_LOWER_OFFSET - PageExtensions.HeaderSize) / sizeof(ushort);
        return MemoryMarshal.Cast<byte, ushort>(
            _data.Slice(PageExtensions.HeaderSize, slotCount * sizeof(ushort)));
    }

    public int KeyCount => GetSlotArray().Length;

    public int FreeSpace
    {
        get
        {
            ref var header = ref GetHeader();
            return header.PG_UPPER_OFFSET - header.PG_LOWER_OFFSET;
        }
    }

    public void Initialize(byte level = 0)
    {
        ref var header = ref GetHeader();
        header.PG_SPECIAL = (ushort)(PageSize - BTreePageSpecial.Size);
        header.PG_UPPER_OFFSET = header.PG_SPECIAL;
        header.PG_LOWER_OFFSET = (ushort)PageExtensions.HeaderSize;
        header.HAS_FREE_SPACE = 1;

        ref var special = ref GetSpecial();
        special.PageType = BTreePageSpecial.LeafType;
        special.Level = level;
        special.NextPageNumber = 0;
        special.PrevPageNumber = 0;
    }

    public long GetKey(int index)
    {
        if (index < 0 || index >= KeyCount)
            throw new ArgumentOutOfRangeException(nameof(index));

        var slots = GetSlotArray();
        ushort offset = slots[index];

        return MemoryMarshal.Read<long>(_data.Slice(offset + 6, 8));
    }

    public void SetKey(int index, long key)
    {
        if (index < 0 || index >= KeyCount)
            throw new ArgumentOutOfRangeException(nameof(index));

        var slots = GetSlotArray();
        ushort offset = slots[index];

        MemoryMarshal.Write(_data.Slice(offset + 6, 8), in key);
    }

    public Rid GetValue(int index)
    {
        if (index < 0 || index >= KeyCount)
            throw new ArgumentOutOfRangeException(nameof(index));

        var slots = GetSlotArray();
        ushort offset = slots[index];

        uint pageNum = MemoryMarshal.Read<uint>(_data.Slice(offset, 4));
        ushort slotNum = MemoryMarshal.Read<ushort>(_data.Slice(offset + 4, 2));

        return new Rid(pageNum, slotNum);
    }

    public void SetValue(int index, Rid rid)
    {
        if (index < 0 || index >= KeyCount)
            throw new ArgumentOutOfRangeException(nameof(index));

        var slots = GetSlotArray();
        ushort offset = slots[index];

        MemoryMarshal.Write(_data.Slice(offset, 4), in rid.PageNumber);
        MemoryMarshal.Write(_data.Slice(offset + 4, 2), in rid.SlotNumber);
    }

    public int FindKeyIndex(long searchKey)
    {
        int low = 0;
        int high = KeyCount;

        while (low < high)
        {
            int mid = low + (high - low) / 2;
            long midKey = GetKey(mid);

            if (midKey < searchKey)
                low = mid + 1;
            else
                high = mid;
        }

        return low;
    }

    public bool Insert(long key, Rid rid)
    {
        if (FreeSpace < TupleSize + sizeof(ushort))
            return false;

        int insertIdx = FindKeyIndex(key);
        
        ref var header = ref GetHeader();
        header.PG_UPPER_OFFSET -= TupleSize;
        ushort tupleOffset = header.PG_UPPER_OFFSET;
        
        MemoryMarshal.Write(_data.Slice(tupleOffset, 4), in rid.PageNumber);
        MemoryMarshal.Write(_data.Slice(tupleOffset + 4, 2), in rid.SlotNumber);
        MemoryMarshal.Write(_data.Slice(tupleOffset + 6, 8), in key);
        
        MemoryMarshal.Write(_data.Slice(header.PG_LOWER_OFFSET), in tupleOffset);
        header.PG_LOWER_OFFSET += sizeof(ushort);
        
        var slots = GetSlotArray();
        ushort newSlotValue = slots[slots.Length - 1];

        for (int i = slots.Length - 1; i > insertIdx; i--)
        {
            slots[i] = slots[i - 1];
        }
        slots[insertIdx] = newSlotValue;

        return true;
    }

    public uint GetNextLeaf()
    {
        return GetSpecial().NextPageNumber;
    }

    public void SetNextLeaf(uint pageNumber)
    {
        GetSpecial().NextPageNumber = pageNumber;
    }

    public uint GetPrevLeaf()
    {
        return GetSpecial().PrevPageNumber;
    }

    public void SetPrevLeaf(uint pageNumber)
    {
        GetSpecial().PrevPageNumber = pageNumber;
    }
}

public ref struct BPlusTreeInternalPage
{
    public const int PageSize = 8192;
    public const int TupleSize = 8 + 4; // Key + ChildPtr
    public const int P0Size = 4;        // Leftmost child pointer

    private readonly Span<byte> _data;

    public BPlusTreeInternalPage(Span<byte> data)
    {
        _data = data;
    }

    public static BPlusTreeInternalPage From(Frame frame) => new(frame.Data);

    public bool IsLeaf => false;

    private ref PageHeader GetHeader()
    {
        return ref MemoryMarshal.AsRef<PageHeader>(_data[..PageExtensions.HeaderSize]);
    }

    private ref BTreePageSpecial GetSpecial()
    {
        ref var header = ref GetHeader();
        return ref MemoryMarshal.AsRef<BTreePageSpecial>(
            _data.Slice(header.PG_SPECIAL, BTreePageSpecial.Size));
    }

    private Span<ushort> GetSlotArray()
    {
        ref var header = ref GetHeader();
        int slotCount = (header.PG_LOWER_OFFSET - PageExtensions.HeaderSize) / sizeof(ushort);
        return MemoryMarshal.Cast<byte, ushort>(
            _data.Slice(PageExtensions.HeaderSize, slotCount * sizeof(ushort)));
    }

    public int KeyCount => GetSlotArray().Length;

    public int FreeSpace
    {
        get
        {
            ref var header = ref GetHeader();
            return header.PG_UPPER_OFFSET - header.PG_LOWER_OFFSET;
        }
    }

    public void Initialize(byte level)
    {
        if (level == 0)
            throw new ArgumentException("Internal pages must have level > 0", nameof(level));

        ref var header = ref GetHeader();
        header.PG_SPECIAL = (ushort)(PageSize - BTreePageSpecial.Size);
        header.PG_UPPER_OFFSET = header.PG_SPECIAL;
        header.PG_LOWER_OFFSET = (ushort)PageExtensions.HeaderSize;
        header.HAS_FREE_SPACE = 1;

        ref var special = ref GetSpecial();
        special.PageType = BTreePageSpecial.InternalType;
        special.Level = level;
        special.NextPageNumber = 0;
        special.PrevPageNumber = 0;
        
        header.PG_UPPER_OFFSET -= P0Size;
        
        uint zero = 0;
        MemoryMarshal.Write(_data.Slice(header.PG_UPPER_OFFSET, P0Size), in zero);
    }

    public uint GetLeftmostChild()
    {
        ref var header = ref GetHeader();
        int p0Offset = header.PG_SPECIAL - P0Size;
        return MemoryMarshal.Read<uint>(_data.Slice(p0Offset, P0Size));
    }

    public void SetLeftmostChild(uint pageNumber)
    {
        ref var header = ref GetHeader();
        int p0Offset = header.PG_SPECIAL - P0Size;
        MemoryMarshal.Write(_data.Slice(p0Offset, P0Size), in pageNumber);
    }

    public long GetKey(int index)
    {
        if (index < 0 || index >= KeyCount)
            throw new ArgumentOutOfRangeException(nameof(index));

        var slots = GetSlotArray();
        ushort offset = slots[index];

        return MemoryMarshal.Read<long>(_data.Slice(offset, 8));
    }

    public void SetKey(int index, long key)
    {
        if (index < 0 || index >= KeyCount)
            throw new ArgumentOutOfRangeException(nameof(index));

        var slots = GetSlotArray();
        ushort offset = slots[index];

        MemoryMarshal.Write(_data.Slice(offset, 8), in key);
    }
    
    public uint GetChild(int index)
    {
        if (index < 0 || index > KeyCount)
            throw new ArgumentOutOfRangeException(nameof(index));

        if (index == 0)
            return GetLeftmostChild();

        var slots = GetSlotArray();
        ushort offset = slots[index - 1];

        return MemoryMarshal.Read<uint>(_data.Slice(offset + 8, 4));
    }

    public void SetChild(int index, uint pageNumber)
    {
        if (index < 0 || index > KeyCount)
            throw new ArgumentOutOfRangeException(nameof(index));

        if (index == 0)
        {
            SetLeftmostChild(pageNumber);
            return;
        }

        var slots = GetSlotArray();
        ushort offset = slots[index - 1];

        MemoryMarshal.Write(_data.Slice(offset + 8, 4), in pageNumber);
    }

    public int FindKeyIndex(long searchKey)
    {
        int low = 0;
        int high = KeyCount;

        while (low < high)
        {
            int mid = low + (high - low) / 2;
            long midKey = GetKey(mid);

            if (midKey < searchKey)
                low = mid + 1;
            else
                high = mid;
        }

        return low;
    }

    public bool Insert(long key, uint rightChild)
    {
        if (FreeSpace < TupleSize + sizeof(ushort))
            return false;

        int insertIdx = FindKeyIndex(key);

        // Allocate tuple space
        ref var header = ref GetHeader();
        header.PG_UPPER_OFFSET -= TupleSize;
        ushort tupleOffset = header.PG_UPPER_OFFSET;
        
        MemoryMarshal.Write(_data.Slice(tupleOffset, 8), in key);
        MemoryMarshal.Write(_data.Slice(tupleOffset + 8, 4), in rightChild);
        
        MemoryMarshal.Write(_data.Slice(header.PG_LOWER_OFFSET), in tupleOffset);
        header.PG_LOWER_OFFSET += sizeof(ushort);
        
        var slots = GetSlotArray();
        ushort newSlotValue = slots[slots.Length - 1];

        for (int i = slots.Length - 1; i > insertIdx; i--)
        {
            slots[i] = slots[i - 1];
        }
        slots[insertIdx] = newSlotValue;

        return true;
    }
}
