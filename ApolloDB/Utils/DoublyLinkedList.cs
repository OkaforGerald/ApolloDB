using Microsoft.Extensions.ObjectPool;

namespace ApolloDB.Utils;

public class DoublyLinkedList<T>
{
    Node? Head { get; set; }
    Node? Current { get; set; }
    public uint Size { get; set; }

    private readonly ObjectPool<Node> _pool;

    public class Node
    {
        public T? Value { get; set; }
        public Node? Next { get; set; }
        public Node? Previous { get; set; }
        public bool IsEvictable { get; set; } = true;
    }
    
    public class NodePoolPolicy : IPooledObjectPolicy<Node>
    {
        public Node Create() => new Node();

        public bool Return(Node obj)
        {
            obj.Next = obj.Previous = null;
            obj.Value = default;
            obj.IsEvictable = false;
            return true;
        }
    }

    public DoublyLinkedList(ObjectPool<Node> pool)
    {
        _pool = pool;
    }

    public Node? RemoveLRU()
    {
        if (Size == 0) return null;
    
        Node? current = Head;
        
        while (current != null && !current.IsEvictable)
        {
            current = current.Next;
        }
    
        if (current == null) return null; 
        
        if (current == Head)
        {
            Head = current.Next;
            if (Head != null)
                Head.Previous = null;
        }
        else
        {
            current.Previous!.Next = current.Next;
        }
    
        if (current == Current)
        {
            Current = current.Previous;
        }
        else if (current.Next != null)
        {
            current.Next.Previous = current.Previous;
        }
    
        Size--;
        return current;
    }

    public Node Insert(T value)
    {
        var newNode = _pool.Get();
        newNode.Value = value;
        if (Head == null)
        {
            Head = Current = newNode; 
        }
        else
        {
            Current!.Next = newNode;
            newNode.Previous = Current;
            Current = newNode;   
        }
        Size++;

        return newNode;
    }

    public void Insert(Node value)
    {
        value.Next = null;
        value.Previous = Current;

        if (Head == null)
        {
            Head = Current = value;
        }
        else
        {
            Current!.Next = value;
            Current = value;
        }
        Size++;
    }

    public void MoveToBottom(T value)
    {
        var node = Remove(value);
        if (node is null) return;

        node.Next = null;
        Current!.Next = node;
        node.Previous = Current;
        
        Current = node;
        Size++;
    }
    
    public void MoveToBottom(Node node)
    {
        Remove(node);
    
        node.Next = null;
        node.Previous = Current;
    
        if (Current != null)
            Current.Next = node;
        else
            Head = node;
        
        Current = node;
        Size++;
    }
    
    public Node? Remove(T value)
    {
        var pointer = Head;
    
        while (pointer != null)
        {
            if (pointer.Value!.Equals(value))
            {
                return Remove(pointer);
            }
            pointer = pointer.Next;
        }
    
        return null;
    }
    
    public Node Remove(Node node)
    {
        if (node.Previous != null)
            node.Previous.Next = node.Next;
        else
            Head = node.Next;
        
        if (node.Next != null)
            node.Next.Previous = node.Previous;
        else
            Current = node.Previous;
    
        Size--;
        return node;
    }
    
    public void ReturnNode(Node node) => _pool.Return(node);
}