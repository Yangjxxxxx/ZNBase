
import Pythonbepi
import decimal
def decimal_type(handler, a):
    print(type(a))
    print(a)
    return a
def mytable_update(handler, id, desc):
    ret = False
    print("=====================")
    sql="UPDATE mytable SET description = '%s' WHERE id=%d;"%(desc,id)
    print(sql)
    ret=Pythonbepi.BEPI_execute(handler, sql)
    return ret
def inta(handler,a ):
    print(a)
    return a
def floata(handler,a ):
    print(a)
    return a
def stra(handler,a ):
    print(a)
    return a
def boola(handler,a ):
    print(a)
    return a
def float_type(handler,a ):
    print(a)
    return [a,a,a,a]
def str_type(handler ,a):
    return a

def int_type(handler,a):
    a = False
    print(a)
    return a
def array_type(handler, p1):
    a=p1
    return a
def bool_type(handler, x):
    if x == True:
        x=False
    else:
        x=True
    return x
def bytes_type(handler, p1):
    print("==========================")

    a=p1
    print(a)
    return a

def testdate(handler, date):
    print(type(date)," : ",date)
    print(date)
    return date
def testtime(handler, date):
    print(type(date)," : ",date)
    print(date)
    return date
def testts(handler, date):
    print(type(date)," : ",date)
    print(date)
    return date
def testtstz(handler, date):
    print(type(date)," : ",date)
    print(date)
    return date
def add(handler,a,b):
    print(a)
    print(b)
    return a + b

def test_proc1(handler):
    print("This is test_proc1() !")
    pass

def testselect(handler) :
    # stmt = 'SELECT * FROM testcursor1 where b = nulls;'
    # planIndex = Pythonbepi.BEPI_prepare(handler, stmt)
    # print("planIndex : ",planIndex)
    # rv = Pythonbepi.BEPI_execute_plan_with_return(handler, planIndex)
    # print("rv :",rv)
    rv = Pythonbepi.BEPI_execute_with_return(handler,"SELECT * FROM testtime;");
    for i in rv:print(i)
    return ""
def test_proc2(handler):
    a = [4]
    return a[1]

def test_proc3(handler, x):
    Pythonbepi.BEPI_execute(handler, "INSERT INTO test1 VALUES (%s)" % x)

def stupid(handler):
    return "FFF"

def insertanytimes(handler,times):
    affRows = 0
    for i in range(times):
        if i % 2 == 0:
            Pythonbepi.BEPI_execute(handler, "insert into t1 values(%s) ;" % (1000 + i))
        else:
            Pythonbepi.BEPI_execute(handler, "insert into t1 values(%s) ;" % (1000 + i))
        affRows += 1
    return affRows


def sumtwotables(handler, tableA, tableB):
    result = 0
    rv = Pythonbepi.BEPI_execute_with_return(handler, "select count(*) from " + tableA + " ;")
    result += rv[0]['count']
    rv = Pythonbepi.BEPI_execute_with_return(handler, "select count(*) from " + tableB + " ;")
    result += rv[0]['count']
    return result

# def createcursor(handler):
#     i = Pythonbepi.BEPI_cursor_declare(handler,'testcursor','select ** from cursortest;')
#     print(i)
#     return i

def createcursor(handler):
    i = Pythonbepi.BEPI_cursor_declare(handler,'testcursor','(x int)','select * from cursortest where id =x ;')
    print(i)
    return i
def opencursor(handler):
    j = Pythonbepi.BEPI_cursor_open_with_paramlist(handler,'testcursor','(x=1)',False)
    print(j)
    return j

def testcursor(handler):
    err = ""
    if Pythonbepi.BEPI_cursor_find(handler,'testcursor'):
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.FORWARD,3):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv)
        if Pythonbepi.BEPI_cursor_move(handler,'testcursor',Pythonbepi.FORWARD,4) :
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.FORWARD,2):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv)
    return str(rv)

def testscrollcursor(handler):
    err = ""
    if Pythonbepi.BEPI_cursor_find(handler,'testcursor'):
        if Pythonbepi.BEPI_scroll_cursor_fetch(handler,'testcursor',6,3):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv)
        if Pythonbepi.BEPI_scroll_cursor_move(handler,'testcursor',6,4) :
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv)
        if Pythonbepi.BEPI_scroll_cursor_fetch(handler,'testcursor',6,2):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv)
        return str(rv)
    return err

def closecursor(handler):
    err = Pythonbepi.BEPI_cursor_close(handler,'testcursor')
    print("err is " + err)
    return 0


def testc(handler):
    all_columns = "*"
    input_table = "customer_churn_train"
    rv = Pythonbepi.BEPI_execute_with_return(handler, 'SELECT %s FROM %s order by rownumber desc limit 900;' % (all_columns,input_table))
    for i in rv:
        print(i)
    return 0

def testdeclareopen(handler):
    i = Pythonbepi.BEPI_cursor_declare(handler,'testcursor1','','select * from testtime ;')
    j = Pythonbepi.BEPI_cursor_open_with_paramlist(handler,'testcursor1','',False)
    return j
def testscrollcursor3(handler):
    err = ""
    if Pythonbepi.BEPI_cursor_find(handler,'testcursor1'):
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor1',Pythonbepi.FORWARD,50):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            for i in rv:
                print(i)
            return str(rv)
    return ""

def testscrollcursor8(handler):
    err = ""
    if Pythonbepi.BEPI_cursor_find(handler,'testcursor'):
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',5,0):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',5,5):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',5,1):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',5,-1):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',5,-6):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',6,2):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',5,0):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',5,1):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv)
    return ""

def hello(handler):
    print("world")
    return "miao ~"

def testscrollcursor7(handler) :
    err = ""
    if Pythonbepi.BEPI_cursor_find(handler,'testcursor'):
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.NEXT):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 1)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.NEXT):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 1)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.NEXT):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 1)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.NEXT):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 1)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.FIRST):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 2)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.FORWARD,1):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 3)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.FORWARD,1):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 4)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.FORWARD,1):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 5)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.FORWARD,1):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 6)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.FORWARD,1):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 7)
    return ""


def testscrollcursor2(handler) :
    err = ""
    if Pythonbepi.BEPI_cursor_find(handler,'testcursor'):
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.PRIOR):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 1)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.FORWARD, 2):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 1)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.PRIOR):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 1)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.PRIOR):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 1)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.PRIOR):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 2)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.FORWARD,1):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 3)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.FORWARD,1):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 4)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.FORWARD,1):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 5)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.FORWARD,1):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 6)
        if Pythonbepi.BEPI_cursor_fetch(handler,'testcursor',Pythonbepi.FORWARD,1):
            rv = Pythonbepi.BEPI_cursor_get_fetch_row(handler)
            print(rv, " ", 7)
    return ""