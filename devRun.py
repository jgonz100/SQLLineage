from sqltrace import SQLTrace, SQLineage, SQLTable
from anytree import Node, RenderTree
import pandas as pd

tracer = SQLTrace(r'C:\Users\jgonzalez1\Projects\SQLineage\apvendor.txt')
data = tracer.parseSQL().drop_duplicates()
print(data)

lineage = SQLineage(data)
parents = lineage.getParents()

tables = []
for parent in parents:
    try:
        if parent[0] == 'root':
            tables.append(parent)
        else:
            tables.append(parent.strip())
    except:
        tables.append(parent)
        
parents = []
for item in tables:
    if item == '':
        pass
    else:
        parents.append(item)
print(parents)

#Get sub operations of tables, child tables/columns
children = lineage.getChildTables()
#print(children)
#Tag child columns/tables with their parent tables and set to dataframe
chain = lineage.mergeParentstoChild(parents, children)
lin = pd.DataFrame(chain, columns = ['Parent_Table', 'Parent_Column', 'Child_Column'])
paths = []

#print(lin)
#lin.to_csv(r'C:\Users\jgonzalez1\Projects\SQLineage\linex.csv', index = False)

if parents[0][0] == 'root':
    root = Node(parents[0][1])
    parents.remove(parents[0])
    for i, row in lin.iterrows():
        if lin.at[i, 'Parent_Table'] == root.name:
            rcol = Node(str(lin.at[i, 'Parent_Column'].strip()), parent = root)
            try:
                rsubcol = Node(str(lin.at[i, 'Child_Column'].strip()), parent = rcol)
            except:
                pass
            
    subroot = Node(str(parents[len(parents)-1]), parent = root)
    parents.remove(parents[len(parents)-1])
    
    for item in parents:
        child_table = Node(str(item), parent = subroot)
        for i, row in lin.iterrows():
            if lin.at[i, 'Parent_Table'] == child_table.name:
                ccol = Node(str(lin.at[i, 'Parent_Column'].strip()), parent = child_table)
                subcol = Node(str(lin.at[i, 'Child_Column'].strip()), parent = ccol)
                paths.append(subcol.path)
else:
    if len(parents) % 2 != 0:
        root = Node(str(parents[len(parents)-1]))
        parents.remove(parents[len(parents)-1])
    
        #Add node of subtables of root
        for item in parents:
            c = Node(str(item), parent = root)
            #get subcolumns of parents
            for i, row in lin.iterrows():
                if lin.at[i, 'Parent_Table'] == c.name:
                    ccol = Node(str(lin.at[i, 'Parent_Column'].strip()), parent=c)
                    subcol = Node(str(lin.at[i, 'Child_Column'].strip()), parent =ccol)
                    paths.append(subcol.path)
            
print(lin)
#print(paths)
                    
#for pre, fill, node in RenderTree(root):
#    print("%s%s" % (pre, node.name))
    
"""  
if len(parents % 2) != 0:
    root = Node(str(parents[len(parents)-1]))
    parents.remove(parents[len(parents)-1])
    
    #Add node of subtables of root
    for item in parents:
        c = Node(str(item), parent = root)
        #get subcolumns of parents
        for i, row in lin.iterrows():
            if lin.at[i, 'Parent_Table'] == c.name:
                ccol = Node(str(lin.at[i, 'Parent_Column'].strip()), parent=c)
                subcol = Node(str(lin.at[i, 'Child_Column'].strip()), parent =ccol)
                paths.append(subcol.path)
"""