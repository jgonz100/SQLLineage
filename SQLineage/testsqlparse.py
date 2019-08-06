from anytree import Node, RenderTree

a = Node("a")
poh = Node("POH", parent=a)
pol = Node("POL", parent=a)
pohcustomer = Node("POH.customerid", parent=poh)
ids = Node("POH.id", parent=poh)
customer = Node("POH_customerid", parent=pohcustomer)

for pre, fill, node in RenderTree(a):
    print("%s%s" % (pre, node.name))