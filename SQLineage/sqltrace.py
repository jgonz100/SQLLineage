import re
import logging
from DLL import DLL
import pandas as pd

logger = logging.getLogger(__name__)

class SQLTrace:
    sql = []
    df = pd.DataFrame()
    ppsql = ""
    keywords = ["SELECT", "FROM", "AS", "CASE", "WHEN", "THEN", "END", "ELSE", "IF", "LEFT", "OUTER", "JOIN", "ON", "AND", "(", ")"]
    
    def __init__(self, sql_text_path):
        with open(sql_text_path, 'r') as infile:
            self.ppsql = infile.readlines()
        with open(sql_text_path, 'r') as infile:
            for line in infile:
                self.sql.append(line.replace("\n", "").replace("\t", "").strip())
            
        self.ppsql = " ".join(self.ppsql)
        if self.sql == []:
            logging.error("Cannot parse an empty file.")
    
    #Given a sql keyword, will return all statements until next keyword
    def parseSQL(self):
        self.values = []
        for item in self.sql:
            for word in self.keywords:
                if word in item:
                    if word == 'SELECT':
                        self.values.append(('select', item))
                    if word == 'FROM':
                        self.values.append(('from', item))
                    if word == 'AS':
                        if 'CASE' in item:
                            self.values.append(('case column', item))
                        else:
                            self.values.append(('columns', item))
                    if word == 'LEFT':
                        self.values.append(('join', item))
                    if word == '(':
                        self.values.append(('begin process', item))
                    if word == ')':
                        self.values.append(('final process', item))
                    else:
                        self.values.append(('column subset', item))
        self.df = pd.DataFrame(self.values)
        return self.df
           
    def printSQL(self):
        print(self.sql)
        
    def prettyprintSQL(self):
        print(self.ppsql)
        
class SQLineage:    
    lineage = pd.DataFrame()
    keywords = ["SELECT", "FROM", "AS", "CASE", "WHEN", "THEN", "END", "ELSE", "IF", "LEFT", "OUTER", "JOIN", "ON", "AND", "(", ")"]
    
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
        pattern1 = re.compile(r'(?:WHEN )[A-Za-z.]+\b')
        pattern2 = re.compile(r'(?:AS )[A-Za-z_]+\b')
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
            if row[0] == 'end process' or row[0] == 'final process':
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
            if row[0] == 'columns':
                self.vals.append(row[1].split('AS'))
            if row[0] == 'case column':
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
                if parent in child[0]:
                    self.chain = DLL()
                    self.chain.append(parent)
                    self.chain.append(child[0])
                    self.chain.append(child[1].replace(',', ''))
                    self.returnlist.append(self.chain.toArray(self.chain.head))
                else:
                    continue
        return self.returnlist

class SQLTable:
    def __init__(self, data):
        self.table = DLL()
        self.table.append(data)
        self.values = self.table.toArray(self.table.head)
        
        
