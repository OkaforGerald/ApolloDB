namespace ApolloDB.Storage;

// 7 bytes
public struct PageHeader
{
    public byte HAS_FREE_SPACE;
    public ushort PG_LOWER_OFFSET;
    public ushort PG_UPPER_OFFSET;
    public ushort PG_SPECIAL;
}
