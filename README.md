# SQL Data Lineage Parser
This module attempts to read a SQL query as a text file, parse it, then generate a lineage tree that traces the tables and columns changed within the query. It outputs a tree (anytree package) representation in the console and writes the lineage to a CSV where each row is one traceback from a leaf to the root table. The sqltrace module contains several classes that assist in parsing a query.

- Class SQLTrace
> This class is initialized with the file path to a SQL query. It contains printing functions to output the query and a parse function which parses each line into a DataFrame and tags it with its appropriate function.
```python
   def parseSQL():
   #Parses query into a DataFrame by tagging each statement with its function
   def printSQL():
   #prints the entire query as a single string
   def ppSQL():
   #prints the entire query, preserving newlines and tabs
```
- Class SQLineage
> This class contains functions for building the relationships between columns and tables that are changed by the SQL query. The lineage is built by using the DataFrame from the SQLTrace class. Each statement that contains a column or table name is taken and the names are parsed out. They are inserted into a linked list (Was used to check lineage was working properly. Can be set to an array to simplify process) which is converted to an array after the DataFrame has been parsed.
```python
   def match_parens(string):
   #Regex function to capture everything inside parentheses
   def match_case_stmts(string):
   #Regex to parse names out of a CASE statement
   def getParents():
   #Parses a DataFrame and appends table names to an array, the target table is appended as a tuple in the form
   #('root', table_name)
   def getChildTables():
   #Parses a DataFrame and appends columns and sub-columns to an array
   def mergeParentstoChild(parents, children):
   #Takes input that was generated from the previous function and generates their relationships into
   #a linked list, the returned list is an array that contains the relationships of column/tables from the query
   class SQLTable:
   #transformation of DataFrame to Linked List, will be removed
```
- The anytree module is used to convert parent tables and child columns into their lineage tree
   
   
