import csv
import sqlparse
import sys
import re
import traceback
from collections import OrderedDict
from prettytable import PrettyTable

def read_file(file):
    formattedLines = []
    with open(file,'r') as f:
        lines = f.readlines()
        for line in lines:
            formattedLines.append(line.strip())
    return formattedLines

def create_tableDict(formattedLines):
    flag = 0
    tableDict = {}
    for line in formattedLines:
        if "<begin_table>" in line:
            flag = 1
            continue
        if flag == 1:
            tableName = line
            tableDict[tableName] = OrderedDict()
            flag = 0
        elif "<end_table>" not in line:
            tableDict[tableName][line] = []

    tableDict['prodTable'] = OrderedDict()
    return tableDict

def read_csv_file(tableName, tableDict):
    columnDict = tableDict[tableName]
    columnNames = []
    for key in columnDict.keys():
        columnNames.append(key)

    fname = tableName + '.csv'
    with open(fname,'r') as f:
        for row in csv.reader(f, delimiter=','):
            for i,x in enumerate(row):
                tableDict[tableName][columnNames[i]].append(int(x.strip('\'"')))

def cartesian_product(table, tableDict):
    colNames = []
    if len(tableDict['prodTable'].keys()) == 0:
        for col in tableDict[table].keys():
            tableDict['prodTable'][table + "." + col] = tableDict[table][col]

    else:
        rows1 = []
        rows2 = []

        k1 = list(tableDict['prodTable'].keys())[0]
        k1 = len(tableDict['prodTable'][k1])
        
        k2 = list(tableDict[table].keys())[0]
        k2 = len(tableDict[table][k2])
        # print(k1, k2)

        for col in tableDict['prodTable'].keys():
            tmp = []
            for i in tableDict['prodTable'][col]:
                tmp += [int(i)] * k1
            tableDict['prodTable'][col] = tmp
            rows1.append(tmp)

        for col in tableDict[table].keys():
            tableDict['prodTable'][table + "." + col] = tableDict[table][col] * k2
            rows2.append(tableDict[table][col] * k2)

def processQuery(query, tableDict):
    distinctFlag = False
    query = (re.sub(' +',' ',query)).strip()
    if "distinct(" in query or "distinct (" in query :
        q = query.split("distinct")
        temp = q[1].strip().split("(")
        temp = temp[1].split(")")
        colName = temp[0].strip()
        query = q[0] + "distinct " + colName + temp[1]

    query = sqlparse.parse(query.strip())[0]
    #print(query.tokens)

    if not str(query.tokens[0]).lower() == "select":
            raise NotImplementedError('Only select query type is supported')

    if not str(query.tokens[4]).lower() == "from" and not str(query.tokens[6]).lower() == "from":
            raise Exception('From clause is not present in query')

    i = 2
    if str(query.tokens[i]).lower() == "distinct":
        distinctFlag = True
        i += 2

    tables = []
    
    # Only one table is present
    if type(query.tokens[i+4]).__name__ == "Identifier":
        tables = [str(query.tokens[i+4])]

    #Multiple tables
    else:
        tables = list(query.tokens[i+4].get_identifiers())
        tables = [str(x) for x in tables]
    
    for table in tables:
        if table not in tableDict.keys():
            raise Exception('Invalid table ' + table)

    for table in tables:
        cartesian_product(table, tableDict)

    columns = []
    aggfuncs = []
    aggcols = []
    if str(query.tokens[i]) == '*':
        for table in tables:
            for columnName in tableDict[table].keys():
                columns.append(table + "." + columnName)
    elif type(query.tokens[i]).__name__  == "Identifier":
        colName = str(format_col(str(query.tokens[i]), tables, tableDict))
        columns.append(colName)
    elif type(query.tokens[i]).__name__  == "Function":
        func, colName = processAggregate(query.tokens[i])
        aggfuncs.append(func)
        colName = str(format_col(str(colName), tables, tableDict))
        aggcols.append(colName)
        columns.append(colName)
    elif type(query.tokens[i]).__name__  == "IdentifierList":
        cols = list(query.tokens[i].get_identifiers())
        if type(cols[0]).__name__ == "Function":
            if any(type(x).__name__ != "Function" for x in cols):
                raise Exception('Syntax Error in using Aggregation functions')
            for col in cols:
                # print(col)
                func, colName = processAggregate(col)
                aggfuncs.append(func)
                colName = str(format_col(str(colName), tables, tableDict))
                aggcols.append(colName)
                columns.append(colName)
        elif type(cols[0]).__name__ == "Identifier":
            if any(type(x).__name__ != "Identifier" for x in cols):
                raise Exception('Syntax Error in using Aggregation functions')
            for col in cols:
                colName = str(format_col(str(col), tables, tableDict))
                columns.append(colName)

    curTable = tableDict['prodTable'].copy()
    tmpcolumns = curTable.keys()
    redundantColumns = []
    # print(tmpcolumns)
    for col in tmpcolumns:
        if col not in columns:
            redundantColumns.append(col)

    # for col in redundantColumns:
        # del curTable[col]

    

    if len(query.tokens) > i + 6:
        whereTokens = query.tokens[i+6].tokens
        if not str(whereTokens[0]).lower() == "where":
            raise NotImplementedError('Only where is supported')
        whereTokens = whereTokens[2:]
        idxs = processWhere(whereTokens, tables, tableDict, curTable)
        cols = list(curTable.keys())
        for k in cols:
            curTable[k] = [tableDict['prodTable'][k][p] for p in idxs]
        if len(aggfuncs) >= 1:
            applyAggregate(aggfuncs, aggcols, curTable)
        else:
            for col in redundantColumns:
                del curTable[col]
            if distinctFlag:
                s = set()
                idxs = []
                cols = list(curTable.keys())
                nrows = len(curTable[cols[0]])
                for p in range(nrows):
                    tmp = []
                    for k in cols:
                        tmp.append(curTable[k][p])
                    tp = str(tmp)
                    if not tp in s:
                        s.add(tp)
                        idxs.append(p)

                # print(idxs)
                for k in cols:
                    curTable[k] = [tableDict['prodTable'][k][p] for p in idxs]

            display_table(curTable)
    else:
        if len(aggfuncs) >= 1:
            applyAggregate(aggfuncs, aggcols, curTable)
        else:
            for col in redundantColumns:
                del curTable[col]
            if distinctFlag:
                s = set()
                idxs = []
                cols = list(curTable.keys())
                nrows = len(curTable[cols[0]])
                for p in range(nrows):
                    tmp = []
                    for k in cols:
                        tmp.append(curTable[k][p])
                    tp = str(tmp)
                    if not tp in s:
                        s.add(tp)
                        idxs.append(p)

                # print(idxs)
                for k in cols:
                    curTable[k] = [tableDict['prodTable'][k][p] for p in idxs]
            # print("DISPLAY TABLE")
            display_table(curTable)
        # display_table(curTable)

def applyAggregate(aggfuncs, aggcols, curTable):
    colNames = []
    row = []
    for i, funcName in enumerate(aggfuncs):
        col = aggcols[i]
        colNames.append(funcName + "(" + col + ")")
        tmp = curTable[col]
        if funcName == "max":
            v = str(max(tmp))
        elif funcName == "min":
            v = str(min(tmp))
        elif funcName == "sum":
            v = str(sum(tmp))
        elif funcName == "avg":
            v = str(sum(tmp)/len(tmp))
        else:
            raise NotImplementedError('Unknown Aggregation function ' + funcName)
        row.append(v)

    x = PrettyTable()
    x.field_names = colNames
    x.add_row(row)
    print(x)

def intersect(ls1, ls2):
    return set(set(ls1) & set(ls2))

def processWhere(whereTokens, tables, tableDict, curTable):
    # print(whereTokens)
    prevand = True
    prevIdx = set()
    flag = 1
    for condition in whereTokens:
        # print(type(condition).__name__)
        if type(condition).__name__ == "Comparison":
            tokens = condition.tokens
            iden1 = None
            iden2 = None
            op = None
            value = None
            for token in tokens:
                # print(token,type(token).__name__)
                if type(token).__name__ == 'Identifier':
                    if iden1 is None:
                        iden1 = format_col(str(token), tables, tableDict)
                        # print(iden1,"DFsd")
                    else:
                        iden2 = format_col(str(token), tables, tableDict)
                else:
                    if str(token.ttype) == 'Token.Operator.Comparison':
                        op = str(token)
                    elif str(token.ttype).startswith('Token.Literal'):
                        value = int(str(token))
            if iden2 is not None:
                ls1 = curTable[iden1]
                ls2 = curTable[iden2]
                idxs = applyOp(ls1, op, ls2)
            else:
                ls1 = curTable[iden1]
                ls2 = [value] * len(ls1)
                idxs = applyOp(ls1, op, ls2)
            # print("idxs12",idxs)

            if prevand is False:
                for i in idxs:
                    # print(i)
                    if i not in prevIdx:
                        prevIdx.add(i)
            else:
                if flag:
                    prevIdx = set(idxs)
                    flag = 0
                else:
                    prevIdx = intersect(prevIdx, idxs)

        elif type(condition).__name__ == "Parenthesis":
            # print(c)
            idxs = processWhere(condition.tokens[1:-1], tables, tableDict, curTable)
            if prevand is False:
                for i in idxs:
                    # print(i)
                    if i not in prevIdx:
                        prevIdx.add(i)
            else:
                if flag:
                    prevIdx = set(idxs)
                    flag = 0
                else:
                    prevIdx = intersect(prevIdx, idxs)

        
        elif str(condition.ttype) == 'Token.Keyword':
            if str(condition).lower() == "or":
                prevand = False
            if str(condition).lower() == "and":
                prevand = True

    return list(prevIdx)
        

def applyOp(ls1, op, ls2):
    # print(ls1)
    # print(ls2)
    idxs = []

    if op == "=":
        for i in range(len(ls1)):
            if(ls1[i] == ls2[i]):
                idxs.append(i)
    elif op == ">":
        for i in range(len(ls1)):
            if(ls1[i] > ls2[i]):
                idxs.append(i)
    elif op == "<":
        for i in range(len(ls1)):
            if(ls1[i] < ls2[i]):
                idxs.append(i)
    elif op == "<=":
        for i in range(len(ls1)):
            if(ls1[i] <= ls2[i]):
                idxs.append(i)
    elif op == ">=":
        for i in range(len(ls1)):
            if(ls1[i] >= ls2[i]):
                idxs.append(i)
    else:
        raise NotImplementedError(op + ' operator not recognized')
    return idxs

def display_table(tableDict):
    cols = list(tableDict.keys())
    nrows = len(tableDict[cols[0]])
    x = PrettyTable()
    x.field_names = cols
    for i in range(nrows):
        row = []
        for col in cols:
            row.append(tableDict[col][i])
        x.add_row(row)
    print(x)

def format_col(col, tables, tableDict):
    formattedName = ""
    if len(col.split(".")) == 1:
        cnt = 0
        # Tables have already been validated till this point
        for i in tables:
            if col in tableDict[i].keys():
                cnt += 1
                formattedName = i + "." + col
        if cnt == 0:
            raise Exception("Invalid column " + col)
        if cnt > 1:
            raise Exception("Column Name " + col + " is Ambiguous, exists in multiple tables")
    else:
        # print(col)
        colx = col.split(".")
        # print(colx,"colx")
        if not colx[0] in tables:
            raise Exception('Invalid column ' + col)
        if not colx[1] in tableDict[colx[0]]:
            raise Exception('Invalid column ' + col)
        formattedName = col
    # print(formattedName)
    return formattedName

def processAggregate(querytoken):
    func = ""
    colName = ""
    for token in querytoken.tokens:
        if type(token).__name__ == 'Identifier':
            func = str(token).lower()
        elif type(token).__name__ == 'Parenthesis':
            colName = str(token.tokens[1:-1][0])
    return func, colName

def main():
    formattedLines = read_file('./metadata.txt')
    tableDict = create_tableDict(formattedLines)
    read_csv_file('table1', tableDict)
    read_csv_file('table2', tableDict)
    try:
        query = sys.argv[1].strip("'\"")
        q = processQuery(query, tableDict)
    except Exception:
        traceback.print_exc()

main()