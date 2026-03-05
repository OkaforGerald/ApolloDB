using System.Runtime.InteropServices;
using ApolloDB.Storage.Buffer;
using ApolloDB.Storage.Page;

namespace ApolloDB.Storage.Index;

public class BPlusTree
{
    private const int MaxLeafKeyCount     = (BPlusTreeLeafPage.PageSize     - 7 - BTreePageSpecial.Size) / (BPlusTreeLeafPage.TupleSize     + sizeof(ushort));
    private const int MaxInternalKeyCount = (BPlusTreeInternalPage.PageSize - 7 - BTreePageSpecial.Size - BPlusTreeInternalPage.P0Size) / (BPlusTreeInternalPage.TupleSize + sizeof(ushort));

    private readonly BufferPoolManager _manager;
    private readonly CatalogManager _catalog;
    private readonly string _name;
    private Frame _frame; // header page — stays pinned for the lifetime of the tree

    private BPlusTree(string name, BufferPoolManager manager, CatalogManager catalog)
    {
        _manager = manager;
        _catalog = catalog;
        _name    = name;
    }

    /// <summary>
    /// Creates a new, empty B+ tree. The file must already be registered in the catalog.
    /// Allocates the header page (page 0) and an initial empty root leaf (page 1).
    /// </summary>
    public static async Task<BPlusTree> Create(string name, BufferPoolManager manager, CatalogManager catalog)
    {
        var tree   = new BPlusTree(name, manager, catalog);
        var handle = catalog.GetFile(name);

        // Page 0 — header
        var headerFrame = await manager.AllocatePage(handle.FileId);
        tree._frame = headerFrame;

        // Page 1 — initial root leaf
        var rootFrame = await manager.AllocatePage(handle.FileId);
        BPlusTreeLeafPage.From(rootFrame).Initialize(0);

        uint rootPageId = rootFrame.PageId.PageNumber;
        MemoryMarshal.Write(headerFrame.Data.AsSpan()[..4], in rootPageId);
        headerFrame.IsDirty = true;

        manager.UnpinPage(rootFrame.PageId, isDirty: true);
        return tree;
    }
    
    public static async Task<BPlusTree> Open(string name, BufferPoolManager manager, CatalogManager catalog)
    {
        var tree   = new BPlusTree(name, manager, catalog);
        var handle = catalog.GetFile(name);
        tree._frame = await manager.ReadPage(new PageId(handle.FileId, 0));
        return tree;
    }

    public uint GetRootPageId() =>
        MemoryMarshal.Read<uint>(_frame.Data.AsSpan()[..4]);
    

    public async Task<Rid> Search(long key)
    {
        var handle = _catalog.GetFile(_name);
        var pointerFrame = await _manager.ReadPage(new PageId(handle.FileId, GetRootPageId()));

        while (true)
        {
            var special = MemoryMarshal.AsRef<BTreePageSpecial>(pointerFrame.Data.AsSpan()[^BTreePageSpecial.Size..]);
            if (special.Level == 0) break;

            uint child;
            { var node = BPlusTreeInternalPage.From(pointerFrame); child = node.GetChild(node.FindKeyIndex(key)); }
            _manager.UnpinPage(pointerFrame.PageId, isDirty: false);
            pointerFrame = await _manager.ReadPage(new PageId(handle.FileId, child));
        }

        var leaf  = BPlusTreeLeafPage.From(pointerFrame);
        var idx   = leaf.FindKeyIndex(key);
        var found = idx < leaf.KeyCount && leaf.GetKey(idx) == key
            ? leaf.GetValue(idx)
            : Rid.Invalid;

        _manager.UnpinPage(pointerFrame.PageId, isDirty: false);
        return found;
    }
    

    /// <summary>
    /// Returns true if the key was inserted, false if it already exists.
    /// </summary>
    public async Task<bool> Insert(long key, Rid value)
    {
        var handle = _catalog.GetFile(_name);

        // Traverse to leaf, keeping ancestors pinned so we can propagate splits.
        var ancestors    = new Stack<(Frame frame, int childIndex)>();
        var pointerFrame = await _manager.ReadPage(new PageId(handle.FileId, GetRootPageId()));

        while (true)
        {
            var special = MemoryMarshal.AsRef<BTreePageSpecial>(pointerFrame.Data.AsSpan()[^BTreePageSpecial.Size..]);
            if (special.Level == 0) break;

            int index; uint childPage;
            { var node = BPlusTreeInternalPage.From(pointerFrame); index = node.FindKeyIndex(key); childPage = node.GetChild(index); }
            ancestors.Push((pointerFrame, index));
            pointerFrame = await _manager.ReadPage(new PageId(handle.FileId, childPage));
        }

        // Duplicate check + no-split insert (scoped so the ref struct doesn't cross the await below)
        {
            var leaf    = BPlusTreeLeafPage.From(pointerFrame);
            var leafIdx = leaf.FindKeyIndex(key);
            if (leafIdx < leaf.KeyCount && leaf.GetKey(leafIdx) == key)
            {
                UnpinAll(pointerFrame, ancestors, isDirty: false);
                return false;
            }

            // No split needed
            if (leaf.FreeSpace >= BPlusTreeLeafPage.TupleSize + sizeof(ushort))
            {
                leaf.Insert(key, value);
                _manager.UnpinPage(pointerFrame.PageId, isDirty: true);
                UnpinAncestors(ancestors, isDirty: false);
                return true;
            }
        }

        // Leaf is full — split and insert, then propagate up
        var (rightLeafFrame, promotedKey) = await SplitLeafAndInsert(pointerFrame, key, value, handle.FileId);
        _manager.UnpinPage(pointerFrame.PageId,     isDirty: true);
        _manager.UnpinPage(rightLeafFrame.PageId,   isDirty: true);

        uint  rightChildPage   = rightLeafFrame.PageId.PageNumber;
        uint  leftChildPage    = pointerFrame.PageId.PageNumber;
        byte  newRootLevel     = 1; // if we end up creating a new root from a leaf split

        while (ancestors.TryPop(out var ancestor))
        {
            var parentSpecial = MemoryMarshal.AsRef<BTreePageSpecial>(ancestor.frame.Data.AsSpan()[^BTreePageSpecial.Size..]);
            newRootLevel = (byte)(parentSpecial.Level + 1);

            bool parentHasSpace;
            {
                var parent = BPlusTreeInternalPage.From(ancestor.frame);
                parentHasSpace = parent.FreeSpace >= BPlusTreeInternalPage.TupleSize + sizeof(ushort);
                if (parentHasSpace) parent.Insert(promotedKey, rightChildPage);
            }
            if (parentHasSpace)
            {
                _manager.UnpinPage(ancestor.frame.PageId, isDirty: true);
                UnpinAncestors(ancestors, isDirty: false);
                return true;
            }

            // Internal node also full
            leftChildPage = ancestor.frame.PageId.PageNumber;
            var (rightInternalFrame, newPromoted) =
                await SplitInternalAndInsert(ancestor.frame, promotedKey, rightChildPage, handle.FileId);

            _manager.UnpinPage(ancestor.frame.PageId,    isDirty: true);
            _manager.UnpinPage(rightInternalFrame.PageId, isDirty: true);

            rightChildPage = rightInternalFrame.PageId.PageNumber;
            promotedKey    = newPromoted;
        }

        // Ancestors exhausted — root was split; create new root
        var newRootFrame = await _manager.AllocatePage(handle.FileId);
        var newRoot      = BPlusTreeInternalPage.From(newRootFrame);
        newRoot.Initialize(newRootLevel);
        newRoot.SetLeftmostChild(leftChildPage);
        newRoot.Insert(promotedKey, rightChildPage);

        uint newRootPageId = newRootFrame.PageId.PageNumber;
        MemoryMarshal.Write(_frame.Data.AsSpan()[..4], in newRootPageId);
        _frame.IsDirty = true;

        _manager.UnpinPage(newRootFrame.PageId, isDirty: true);
        return true;
    }
    

    /// <summary>
    /// Deletes the entry for the given key. Returns true if found and removed.
    /// Note: does not rebalance underflowing nodes.
    /// </summary>
    public async Task<bool> Remove(long key)
    {
        var handle       = _catalog.GetFile(_name);
        var pointerFrame = await _manager.ReadPage(new PageId(handle.FileId, GetRootPageId()));

        while (true)
        {
            var special = MemoryMarshal.AsRef<BTreePageSpecial>(pointerFrame.Data.AsSpan()[^BTreePageSpecial.Size..]);
            if (special.Level == 0) break;

            uint child;
            { var node = BPlusTreeInternalPage.From(pointerFrame); child = node.GetChild(node.FindKeyIndex(key)); }
            _manager.UnpinPage(pointerFrame.PageId, isDirty: false);
            pointerFrame = await _manager.ReadPage(new PageId(handle.FileId, child));
        }

        var leaf = BPlusTreeLeafPage.From(pointerFrame);
        var idx  = leaf.FindKeyIndex(key);
        if (idx >= leaf.KeyCount || leaf.GetKey(idx) != key)
        {
            _manager.UnpinPage(pointerFrame.PageId, isDirty: false);
            return false;
        }

        leaf.Delete(idx);
        _manager.UnpinPage(pointerFrame.PageId, isDirty: true);
        return true;
    }
    

    private async Task<(Frame rightFrame, long promotedKey)> SplitLeafAndInsert(
        Frame leftFrame, long insertKey, Rid insertValue, uint fileId)
    {
        (long key, Rid rid)[] all;
        int total, leftCount;
        uint oldNext;

        // Collect all data from the left leaf before any await
        {
            var leftLeaf  = BPlusTreeLeafPage.From(leftFrame);
            int keyCount  = leftLeaf.KeyCount;
            int insertIdx = leftLeaf.FindKeyIndex(insertKey);

            all = new (long key, Rid rid)[keyCount + 1];
            for (int i = 0; i < insertIdx; i++)
                all[i] = (leftLeaf.GetKey(i), leftLeaf.GetValue(i));
            all[insertIdx] = (insertKey, insertValue);
            for (int i = insertIdx; i < keyCount; i++)
                all[i + 1] = (leftLeaf.GetKey(i), leftLeaf.GetValue(i));

            total     = keyCount + 1;
            leftCount = total / 2;
            oldNext   = leftLeaf.GetNextLeaf();
        }

        // Allocate and build right leaf
        var rightFrame = await _manager.AllocatePage(fileId);
        var rightLeaf  = BPlusTreeLeafPage.From(rightFrame);
        rightLeaf.Initialize(0);
        for (int i = leftCount; i < total; i++)
            rightLeaf.Insert(all[i].key, all[i].rid);

        // Rebuild left leaf in-place
        var leftLeafRebuilt = BPlusTreeLeafPage.From(leftFrame);
        leftLeafRebuilt.Initialize(0);
        for (int i = 0; i < leftCount; i++)
            leftLeafRebuilt.Insert(all[i].key, all[i].rid);

        // Stitch sibling chain
        leftLeafRebuilt.SetNextLeaf(rightFrame.PageId.PageNumber);
        rightLeaf.SetPrevLeaf(leftFrame.PageId.PageNumber);
        rightLeaf.SetNextLeaf(oldNext);

        return (rightFrame, all[leftCount].key);
    }

    private async Task<(Frame rightFrame, long promotedKey)> SplitInternalAndInsert(
        Frame leftFrame, long insertKey, uint insertRightChild, uint fileId)
    {
        long[] allKeys;
        uint[] allChildren;
        int total, midIdx;
        long promoted;
        byte level;

        // Collect all data from the left node before any await
        {
            var leftNode = BPlusTreeInternalPage.From(leftFrame);
            int keyCount = leftNode.KeyCount;

            allKeys     = new long[keyCount + 1];
            allChildren = new uint[keyCount + 2];
            allChildren[0] = leftNode.GetLeftmostChild();
            for (int i = 0; i < keyCount; i++)
            {
                allKeys[i]         = leftNode.GetKey(i);
                allChildren[i + 1] = leftNode.GetChild(i + 1);
            }

            // Insert new key in sorted position
            int pos = 0;
            while (pos < keyCount && allKeys[pos] < insertKey) pos++;
            for (int i = keyCount; i > pos; i--)
            {
                allKeys[i]         = allKeys[i - 1];
                allChildren[i + 1] = allChildren[i];
            }
            allKeys[pos]         = insertKey;
            allChildren[pos + 1] = insertRightChild;

            total    = keyCount + 1;
            midIdx   = total / 2;
            promoted = allKeys[midIdx];
            level    = MemoryMarshal.AsRef<BTreePageSpecial>(leftFrame.Data.AsSpan()[^BTreePageSpecial.Size..]).Level;
        }

        // Build right internal node
        var rightFrame = await _manager.AllocatePage(fileId);
        var rightNode  = BPlusTreeInternalPage.From(rightFrame);
        rightNode.Initialize(level);
        rightNode.SetLeftmostChild(allChildren[midIdx + 1]);
        for (int i = midIdx + 1; i < total; i++)
            rightNode.Insert(allKeys[i], allChildren[i + 1]);

        // Rebuild left internal node in-place
        var leftNodeRebuilt = BPlusTreeInternalPage.From(leftFrame);
        leftNodeRebuilt.Initialize(level);
        leftNodeRebuilt.SetLeftmostChild(allChildren[0]);
        for (int i = 0; i < midIdx; i++)
            leftNodeRebuilt.Insert(allKeys[i], allChildren[i + 1]);

        return (rightFrame, promoted);
    }
    

    private void UnpinAll(Frame leaf, Stack<(Frame frame, int childIndex)> ancestors, bool isDirty)
    {
        _manager.UnpinPage(leaf.PageId, isDirty);
        UnpinAncestors(ancestors, isDirty);
    }

    private void UnpinAncestors(Stack<(Frame frame, int childIndex)> ancestors, bool isDirty)
    {
        while (ancestors.TryPop(out var anc))
            _manager.UnpinPage(anc.frame.PageId, isDirty);
    }
}
