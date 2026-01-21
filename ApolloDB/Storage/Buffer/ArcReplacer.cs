using ApolloDB.Utils;
using Microsoft.Extensions.ObjectPool;

namespace ApolloDB.Storage.Buffer;

public class ArcReplacer : IReplacer
{
    private uint bufferSize; // 128 for now
    private Dictionary<PageId, (DoublyLinkedList<PageId>.Node, uint)> nodes;
    private DoublyLinkedList<PageId> T1;
    private DoublyLinkedList<PageId> T2;
    private DoublyLinkedList<PageId> B1;
    private DoublyLinkedList<PageId> B2;
    private readonly ObjectPool<DoublyLinkedList<PageId>.Node> _nodePool;

    private uint _p;
    public ArcReplacer(uint maxBufferSize)
    {
        bufferSize = maxBufferSize;
        nodes = new Dictionary<PageId, (DoublyLinkedList<PageId>.Node, uint)>();
        _p = 0;
        _nodePool = new DefaultObjectPool<DoublyLinkedList<PageId>.Node>(new DoublyLinkedList<PageId>.NodePoolPolicy());
        T1 = new(_nodePool); T2 = new(_nodePool);
        B1 = new(_nodePool); B2 = new(_nodePool);
    }
    public void SetEvictable(PageId frameId, bool evictable) => nodes[frameId].Item1.IsEvictable = evictable;

    public AccessResult RecordAccess(PageId pageId)
    {
        if (nodes.TryGetValue(pageId, out var node))
        {
            uint delta = 0;
            // frame is in memory
            switch (node.Item2)
            {
                case 1: // T1
                    T1.Remove(node.Item1);
                    T2.Insert(node.Item1);
                    nodes[pageId] = (node.Item1, 2);
                    return AccessResult.Hit;
                case 2: // T2
                    T2.MoveToBottom(node.Item1);
                    return AccessResult.Hit;
                case 3: // B1
                    delta = Math.Max(1, B2.Size / Math.Max(1, B1.Size));
                    _p = Math.Min(_p + delta, bufferSize);
                    if (T1.Size + T2.Size >= bufferSize)
                        Replace(pageId, isInB1: true, isInB2: false);
                    B1.Remove(node.Item1);
                    T2.Insert(node.Item1);
                    nodes[pageId] = (node.Item1, 2);
                    return AccessResult.GhostB1;
                case 4: // B2
                    delta = Math.Max(1, B1.Size / Math.Max(1, B2.Size));
                    _p = (uint)Math.Max(0, (int)_p - (int)delta);
                    if (T1.Size + T2.Size >= bufferSize)
                        Replace(pageId, isInB1: false, isInB2: true);
                    B2.Remove(node.Item1);
                    T2.Insert(node.Item1);
                    nodes[pageId] = (node.Item1, 2);
                    return AccessResult.GhostB2;
            }
        }
        else
        {
            if (T1.Size + T2.Size >= bufferSize)
            {
                Replace(pageId, isInB1: false, isInB2: false);
            }

            var newNode = T1.Insert(pageId);
            nodes.TryAdd(pageId, (newNode, 1));

            return AccessResult.Miss;
        }
        return AccessResult.Miss;
    }

    public PageId? Evict()
    {
        var victim = T1.RemoveLRU();
        if (victim != null)
        {
            var pageId = victim.Value;
            
            var ghostNode = B1.Insert(pageId);
            nodes[pageId] = (ghostNode, 3);
            
            if (B1.Size > bufferSize)
            {
                var oldGhost = B1.RemoveLRU();
                if (oldGhost != null)
                {
                    nodes.Remove(oldGhost.Value, out _);
                    _nodePool.Return(oldGhost);
                }
            }
            
            return pageId;
        }
        
        victim = T2.RemoveLRU();
        if (victim != null)
        {
            var pageId = victim.Value;
            
            var ghostNode = B2.Insert(pageId);
            nodes[pageId] = (ghostNode, 4);
            
            if (B2.Size > bufferSize)
            {
                var oldGhost = B2.RemoveLRU();
                if (oldGhost != null)
                {
                    nodes.Remove(oldGhost.Value, out _);
                    _nodePool.Return(oldGhost);
                }
            }
            
            return pageId;
        }
        
        return null;
    }

    public void Remove(PageId pageId)
    {
        if (!nodes.TryGetValue(pageId, out var entry))
            return;
        
        var node = entry.Item1;
        var location = entry.Item2;
        
        switch (location)
        {
            case 1:
                T1.Remove(node);
                break;
            case 2:
                T2.Remove(node);
                break;
            case 3:
                B1.Remove(node);
                break;
            case 4:
                B2.Remove(node);
                break;
        }
        nodes.Remove(pageId, out _);
        _nodePool.Return(node);
    }
    
    private void Replace(PageId pageId, bool isInB1, bool isInB2)
    {
        if (T1.Size > 0 && (T1.Size > _p || isInB2))
        {
            // Evict from T1
            var victim = T1.RemoveLRU();
            if (victim != null)
            {
                var ghostNode = B1.Insert(victim.Value);
                nodes[victim.Value] = (ghostNode, 3);
                
                if (B1.Size > bufferSize)
                {
                    var oldGhost = B1.RemoveLRU();
                    if (oldGhost != null)
                    {
                        nodes.Remove(oldGhost.Value, out _);
                        _nodePool.Return(oldGhost);
                    }
                }
            }
        }
        else  // Evict from T2
        {
            var victim = T2.RemoveLRU();
            if (victim != null)
            {
                var ghostNode = B2.Insert(victim.Value);
                nodes[victim.Value] = (ghostNode, 4);
                
                if (B2.Size > bufferSize)
                {
                    var oldGhost = B2.RemoveLRU();
                    if (oldGhost != null)
                    {
                        nodes.Remove(oldGhost.Value, out _);
                        _nodePool.Return(oldGhost);
                    }
                }
            }
        }
    }
}

public enum AccessResult {
    Hit,
    Miss,
    GhostB1,
    GhostB2
}