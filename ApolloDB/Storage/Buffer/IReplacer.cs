using ApolloDB.Storage.Page;

namespace ApolloDB.Storage.Buffer;

public interface IReplacer
{
    void SetEvictable(PageId pageId, bool evictable);
    AccessResult RecordAccess(PageId pageId);
    PageId? Evict();
    void Remove(PageId frameId);
}