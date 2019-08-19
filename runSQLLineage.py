from sqltrace import SQLTrace, SQLineage, SQLTable
from anytree import Node, RenderTree
import pandas as pd


#read in SQL code as text, formats code for output and parsing
tracer = SQLTrace(r'C:\Users\jgonzalez1\Projects\SQLineage\sqlexample.txt')
#return dataframe of tagged SQL statements
data = tracer.parseSQL().drop_duplicates()
print(data)
#Create lineage object to begin convert to DLL then array for tree input
lineage = SQLineage(data)
parents = lineage.getParents()
tables = []
#Get names of parent tables
for parent in parents:
    if parent != "":
        tables.append(SQLTable(parent.strip()))       
parents = []
for item in tables:
    parents.append(item.values[0])
print(parents)

#Get sub operations of tables, child tables/columns
children = lineage.getChildTables()
#Tag child columns/tables with their parent tables and set to dataframe
chain = lineage.mergeParentstoChild(parents, children)
lin = pd.DataFrame(chain, columns = ['Parent_Table', 'Parent_Column', 'Child_Column'])
paths = []

#test for single root table and creates table heirarchy
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
                
#each item in parents is a root node
else:
    for item in parents:
        p = Node(str(item))
        for i, row in lin.iterrows():
            if lin.at[i, 'Parent_Table'] == p.name:
                ccol = Node(str(lin.at[i, 'Parent_Column']), parent=p)
                subcol = Node(str(lin.at[i, 'Child_Column']), parent =ccol)

#print tree representation as per anytree documentation
for pre, fill, node in RenderTree(root):
    print("%s%s" % (pre, node.name))

vals = [] 
for item in paths:
    temp = []
    for i in range(len(item)):
        temp.append(item[i].name)
    vals.append(temp)
    
out = pd.DataFrame(vals, columns = ["Parent_Table", "Sub_Table", "Parent_Column", "Target_Column"])
print(out)