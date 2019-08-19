import re
import logging
from DLL import DLL
import pandas as pd

logger = logging.getLogger(__name__)

#Parsing and printing SQL
class SQLTrace:
    sql = []
    df = pd.DataFrame()
    #value to maintain sql query structure upon output
    ppsql = ""
    keywords = ["ALTER", "TRUNCATE", "INSERT", "SELECT", "NULL", "FROM", "AS", "CASE", "WHEN", "THEN", "END", "ELSE", "IF", "CONVERT", "LEFT", "OUTER", "JOIN", "ON", "AND", "(", ")"]
    
    #SQLTrace object initialized by passing the file path to the SQL query
    def __init__(self, sql_text_path):
        #save sql in given format
        with open(sql_text_path, 'r') as infile:
            self.ppsql = infile.readlines()
            self.ppsql = " ".join(self.ppsql)
            
        #save each sql line in an array, replace tabs, new lines, whitespace
        with open(sql_text_path, 'r') as infile:
            for line in infile:
                self.sql.append(line.replace("\n", "").replace("\t", "").strip())
                logging.info("SQL query passed in and read")

        if self.sql == []:
            logging.error("Cannot parse an empty file.")
    
    #Calling parseSQL will iterate through the sql variable of a SQLTrace object
    #With the SQL keywords, it will tag each statement and append the tuple (tag, stament) to an array
    #that array is turned into a DataFrame which is then returned
    def parseSQL(self):
        self.values = []
        for item in self.sql:
            for word in self.keywords:
                if word in item:
                    if word == 'TRUNCATE':
                        self.values.append(('truncate', item))
                    elif word == 'ON':
                        if 'AS' in item:
                            pass
                        else:
                            self.values.append(('join target', item))
                    elif word == 'ALTER':
                        self.values.append(('alter', item))
                    elif word == 'INSERT':
                        self.values.append(('target', item))
                    elif word == 'SELECT':
                        if 'FROM' in item:
                            pass
                        else:
                            self.values.append(('select', item))
                    elif word == 'NULL':
                        self.values.append(('new column', item))
                    elif word == 'CONVERT':
                        self.values.append(('convert', item))
                    elif word == 'FROM':
                        if '(' or ')' in item:
                            pass
                        else:
                            self.values.append(('from', item))
                    elif word == 'AS':
                        if 'ALTER' in item:
                            pass
                        elif 'NULL' in item:
                            pass
                        elif 'CASE' in item:
                            self.values.append(('case column', item))
                        else:
                            self.values.append(('columns', item))
                    elif word == 'LEFT':
                        self.values.append(('join', item))
                    elif word == '(':
                        if 'ON' in item:
                            pass
                        elif '(' in item and ')'  in item:
                            self.values.append(('process', item))
                        else:
                            self.values.append(('begin process', item))
                    elif word == ')':
                        if 'ON' in item:
                            pass
                        elif '(' in item and ')'  in item:
                            self.values.append(('process', item))
                        else:
                            self.values.append(('final process', item))
                    else:
                        if ')' or '(' in item:
                            pass
                        else:
                            self.values.append(('column subset', item))
                        
        logging.debug("SQL query successfully parsed")
        self.df = pd.DataFrame(self.values)
        return self.df
    
    #print SQL query statements
    def printSQL(self):
        logging.info("Print SQL called.")
        print(self.sql)
    
    #print entire SQL query, preserving structure
    def prettyprintSQL(self):
        logging.info("Print SQL called.")
        print(self.ppsql)
        
class SQLineage:    
    lineage = pd.DataFrame()
    keywords = ["ALTER", "TRUNCATE", "INSERT", "SELECT", "NULL", "FROM", "AS", "CASE", "WHEN", "THEN", "END", "ELSE", "IF", "CONVERT", "LEFT", "OUTER", "JOIN", "ON", "AND", "(", ")"]
    
    def __init__(self, dataframe):
        self.lineage = dataframe
        
    def match_parens(string):
        pattern = re.compile(r'\((.*)\)|(.*)\)')
        result = pattern.search(string)
        try:
            return str(result.group())
        except:
            return None
    
    def match_case_stmts(string):
        pattern1 = re.compile(r'(?<=WHEN )[A-Za-z.]+\b')
        pattern2 = re.compile(r'(?<=AS )[A-Za-z_]+\b')
        first = pattern1.search(string)
        second = pattern2.search(string)
        try:
            return str(first.group()), str(second.group())
        except:
            return None, None
              
    def getParents(self):
        self.vals = []
        self.tempvals = []
        
        for i, row in self.lineage.iterrows():
            if row[0] == 'target':
                self.tempvals.append(('root',row[1].replace('INSERT INTO ', '')))
            elif row[0] == 'end process' or row[0] == 'final process' or row[0] == 'process':
                self.vals.append(row[1])
        
        self.temp = ""
        
        for item in self.vals:
            self.temp = item
            self.match = SQLineage.match_parens(item)
            
            if self.match is not None:
                item = item.replace(self.match, "")
                self.tempvals.append(item)    
        return self.tempvals
    
    def getChildTables(self):
        self.vals = []
        for i, row in self.lineage.iterrows():
            if row[0] == 'new column':
                self.vals.append(row[1].split('AS'))
            elif row[0] == 'convert':
                self.vals.append((['NULL', re.search(r'(?<=AS )[A-Za-z_.]+\b', row[1]).group()]))
            elif row[0] == 'columns':
                self.vals.append(row[1].split('AS'))
            elif row[0] == 'case column':
                self.f, self.s = SQLineage.match_case_stmts(str(row[1]))
                self.case = []
                self.case.append(self.f.replace('WHEN', ''))
                self.case.append(self.s.replace('AS', ''))
                self.vals.append(self.case)
        return self.vals
    
    def mergeParentstoChild(self, parents, children):
        self.returnlist = []
        for parent in parents:
            for child in children:
                try:
                    if parent[0] == 'root':
                        if 'NULL' in child[0]:
                            if '.' not in child[1]:
                                self.chain = DLL()
                                self.chain.append(parent[1])
                                self.chain.append(child[1].replace(',', ''))
                                self.returnlist.append(self.chain.toArray(self.chain.head))
                        elif 'CONVERT' in child[0]:
                                self.chain = DLL()
                                self.chain.append(parent[1])
                                self.chain.append(child[1].replace(',', ''))
                                self.returnlist.append(self.chain.toArray(self.chain.head))
                        else:
                            if '.' not in child[1]:
                                self.chain = DLL()
                                self.chain.append(parent[1])
                                self.chain.append(child[0])
                                self.chain.append(child[1].replace(',', ''))
                                self.returnlist.append(self.chain.toArray(self.chain.head))
                    else:
                        if parent in child[0]:
                            self.chain = DLL()
                            self.chain.append(parent)
                            self.chain.append(child[0])
                            self.chain.append(child[1].replace(',', ''))
                            self.returnlist.append(self.chain.toArray(self.chain.head))
                        else:
                            continue
                except:
                    pass
        return self.returnlist

class SQLTable:
    def __init__(self, data):
        self.table = DLL()
        self.table.append(data)
        self.values = self.table.toArray(self.table.head)
        
        
