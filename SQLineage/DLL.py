import logging

logger = logging.getLogger(__name__)

class Node:
    def __init__(self, data):
        self.data = data
        self.next = None
        self.prev = None

class DLL:
    def __init__(self):
        self.head = None
    
    #Insert new node of data to the front of the list
    def push(self, data):
        new = Node(data)
        new.next = self.head
        
        if self.head is not None:
            self.head.prev = new
        
        self.head = new
    
    #Give a node as previous_node, insert a new node after the given node
    def insertAfter(self, previous_node, data):
        if previous_node is None:
            logger.error("Cannot insert when previous node is None")
            return
        
        new = Node(data)
        new.next = previous_node.next
        previous_node.next = new
        new.prev = previous_node
        
        if new.next is not None:
            new.next.prev = new
    
    #Given the head node of the list, inserts a new node at the end
    def append(self, data):
        new = Node(data)
        new.next = None
        
        if self.head is None:
            new.prev = None
            self.head = new
            return
        
        last = self.head
        while(last.next is not None):
            last = last.next
            
        last.next = new
        new.prev = last
        return

    #Convert list to array
    def toArray(self, head):
        arr = []
        while(head is not None):
            arr.append(head.data)
            head = head.next
        return arr
    
    #Output list left to right and right to left  
    def printList(self, node):
        print("\nForward Traversal.")
        while(node is not None):
            print(str(node.data))
            last = node
            node = node.next
            
        print("\nReverse Traversal.")
        while(last is not None):
            print(str(last.data))
            last = last.prev