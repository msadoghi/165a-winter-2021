from template.table import *

def makeTable():
    x = Table('students', 5, 1)
    print(x)
    print(x.book)
    for pr in x.book:
        print('Page Range:\n')
        print(f'{pr}\n')
        for bp in x.book[0].pages:
            print(' Base Page:\n')
            print(f'    {bp}\n')
            print('     Columns:\n')
            for column in bp.columns :
                # print(f'{column} : {column.data}\n')
                print(f'        {column}\n')

makeTable()