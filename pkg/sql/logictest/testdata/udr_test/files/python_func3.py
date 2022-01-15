# coding=UTF-8
# encoding = utf-8
from _pickle import dumps
from _pickle import loads

import Pythonbepi
import pandas as pd
# from pandas import DataFrame
from sklearn.cluster import KMeans

def testlist(handler,l):
    print(l)
    return l

def kmeans(handler, input_table, columns, clus_num):
    stmt = 'select %s from %s ;' % (columns, input_table)

    planIndex = Pythonbepi.BEPI_prepare(handler, stmt)
    rv = Pythonbepi.BEPI_execute_plan_with_return(handler, planIndex)
    # print(rv)
    with open('a.txt','w+',encoding='utf-8') as f:
        for i in rv:
            f.write(str(i) + '\n')
    frame = []
    for i in rv:
        frame.append(i)
    df = pd.DataFrame(frame)
    kmeans = KMeans(n_clusters=clus_num, random_state=0).fit(df._get_numeric_data())
    return dumps(kmeans)

def get_kmeans_centroids(handler, model_table, model_column, model_id):
    stmt = 'select %s from %s where id = %s ;' % (model_column, model_table, model_id)

    planIndex = Pythonbepi.BEPI_prepare(handler, stmt)
    rv = Pythonbepi.BEPI_execute_plan_with_return(handler, planIndex)
    model = loads(rv[0][model_column])

    ret = map(list, model.cluster_centers_)
    ret = list(ret)
    return ret

def predict_kmeans(handler, model_table, model_column, model_id, input_values):  # input_values real[]
    stmt = 'select %s from %s where id = %s' % (model_column, model_table, model_id)
    planIndex = Pythonbepi.BEPI_prepare(handler, stmt)
    rv = Pythonbepi.BEPI_execute_plan_with_return(handler, planIndex)
    model = loads(rv[0][model_column])
    ret = model.predict([input_values])
    return ret

def sklearn_train_pp(handler):
    from pandas import DataFrame
    all_columns = "*"
    input_table = "powerplant1"
    stmt = 'SELECT %s FROM %s;' % (all_columns, input_table)
    planIndex = Pythonbepi.BEPI_prepare(handler, stmt)
    rv = Pythonbepi.BEPI_execute_plan_with_return(handler, planIndex)
    frame = []
    for i in rv:
        frame.append(i)
    data_raw = pd.DataFrame(frame)
    print(data_raw)
    X = data_raw[["temperaturecelcius", "exhaustvaccumhg", "ambientpressuremillibar", "relativehumidity"]]
    y = data_raw[["hourlyenergyoutputmw"]]
    from sklearn.model_selection import train_test_split

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2,
                                                        random_state=1)
    import autosklearn.regression
    cls = autosklearn.regression.AutoSklearnRegressor(time_left_for_this_task=30, per_run_time_limit=30, ml_memory_limit=40960)
    cls.fit(X_train, y_train)
    from _pickle import dumps
    model = dumps(cls)
    print(len(model))
    return model

def sklearn_test_pp(handler,model_table,model_id):
    import pandas as pd
    from pandas import DataFrame
    from _pickle import loads
    all_columns = "*"
    input_table = "powerplant1"
    stmt = 'SELECT %s FROM %s;' % (all_columns, input_table)
    planIndex = Pythonbepi.BEPI_prepare(handler, stmt)
    rv = Pythonbepi.BEPI_execute_plan_with_return(handler, planIndex)
    frame = []
    for i in rv:
        frame.append(i)

    data_raw = DataFrame(frame)

    from sklearn.preprocessing import LabelEncoder
    X = data_raw[["temperaturecelcius", "exhaustvaccumhg", "ambientpressuremillibar", "relativehumidity"]]
    y = data_raw[["hourlyenergyoutputmw"]]

    from sklearn.model_selection import train_test_split

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=1)
    model_column = "model"
    # model_table = "models_pp"
    # model_id = 1

    stmt = 'SELECT %s FROM %s WHERE id = %s;' %(model_column, model_table, model_id)
    planIndex = Pythonbepi.BEPI_prepare(handler, stmt)
    rv = Pythonbepi.BEPI_execute_plan_with_return(handler, planIndex)

    model = loads(rv[0][model_column])
    predictions = model.predict(X_test)
    return list(predictions)

def sklearn_train_cr(handler):
    import pandas as pd
    from pandas import DataFrame
    all_columns = "*"
    input_table = "creditcard2"
    stmt = 'SELECT %s FROM %s;' % (all_columns, input_table)
    planIndex = Pythonbepi.BEPI_prepare(handler, stmt)
    rv = Pythonbepi.BEPI_execute_plan_with_return(handler, planIndex)
    frame = []
    for i in rv:
        frame.append(i)
    data_raw = DataFrame(frame)

    from sklearn.preprocessing import LabelEncoder
    X =data_raw.iloc[: , 0:30]
    y = data_raw[["class"]]
    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=1)
    import autosklearn.classification
    cls =autosklearn.classification.AutoSklearnClassifier(time_left_for_this_task=30, per_run_time_limit=30,ml_memory_limit=40960)
    cls.fit(X_train, y_train)

    from _pickle import dumps
    model = dumps(cls)
    print(len(model))
    return model

def sklearn_test_cr(handler,model_table, model_id):
    import pandas as pd
    from pandas import DataFrame
    from _pickle import loads
    all_columns = "*"
    input_table = "creditcard2"
    stmt = 'SELECT %s FROM %s order by rowid desc limit 900;' %(all_columns, input_table)
    planIndex = Pythonbepi.BEPI_prepare(handler, stmt)
    rv = Pythonbepi.BEPI_execute_plan_with_return(handler, planIndex)
    frame = []
    for i in rv:
        frame.append(i)
    data_raw = DataFrame(frame)
    from sklearn.preprocessing import LabelEncoder
    X =data_raw.iloc[: , 0:30]
    y = data_raw.iloc[:,30]
    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=1)
    model_column = "model"
    # model_table = "cc_models"
    # model_id = 1
    stmt = 'SELECT %s FROM %s WHERE id = %s;' %(model_column, model_table, model_id)
    planIndex = Pythonbepi.BEPI_prepare(handler, stmt)
    rv = Pythonbepi.BEPI_execute_plan_with_return(handler, planIndex)
    model = loads(rv[0][model_column])
    predictions = model.predict(X_test)
    return list(predictions)


##################################################################################

def sklearn_train_cc(handler):
    import pandas as pd
    from pandas import DataFrame
    all_columns = "*"
    input_table = "customer_churn_train"
    stmt = 'SELECT %s FROM %s;' % (all_columns, input_table)
    print("------------------------------")
    print(stmt)
    print("------------------------------")
    planIndex = Pythonbepi.BEPI_prepare(handler, stmt)
    rv = Pythonbepi.BEPI_execute_plan_with_return(handler, planIndex)
    frame = []
    for i in rv:
        frame.append(i)

    data_raw = DataFrame(frame)
    from sklearn.preprocessing import LabelEncoder
    try:
        X = data_raw[["creditscore", "geography", "gender", "age", "tenure",
                      "balance", "numofproducts", "hascrcard", "isactivemember",
                      "estimatedsalary"]]
        le = LabelEncoder()
        le.fit(X.geography.drop_duplicates())
        X.geography = le.transform(X.geography)
        le.fit(X.gender.drop_duplicates())
        X.gender = le.transform(X.gender)
        y = data_raw[["exited"]]
        # print(y)
    except Exception as e :
        print("-----------------------------")
        print(e)
        print("-----------------------------")


    from sklearn.model_selection import train_test_split
    print("-----------------------------------------")
    print("from sklearn.model_selection import train_test_split")
    print("-----------------------------------------")
    try:
        print("---------biaozhuanhua---------------")
        print("---------biaozhuanhua---------------")
        X = (X - X.min())/(X.max()- X.min())
        #y = (y - y.min())/(y.max()- y.min())
        print(X)
        print(y)
        print("---------biaozhuanhua---------------")
        print("---------biaozhuanhua---------------")
    except Exception as e :
        print(e)


    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2,
                                                        random_state=1)
    print(X_train)
    print(y_train)
    import autosklearn.classification
    cls =autosklearn.classification.AutoSklearnClassifier(time_left_for_this_task=30, per_run_time_limit=30,ml_memory_limit=40960)
    cls.fit(X_train, y_train)
    from _pickle import dumps
    model = dumps(cls)
    #print(model)
    print("---------------------------")
    print(len(model))
    print("---------------------------")
    return model

def sklearn_test(handler):
    import pandas as pd
    from pandas import DataFrame

    stmt = 'SELECT model FROM models_cc where id = 1;'
    print("------------------------------")
    print(stmt)
    print("------------------------------")
    planIndex = Pythonbepi.BEPI_prepare(handler, stmt)
    rv = Pythonbepi.BEPI_execute_plan_with_return(handler, planIndex)
    print("================================")
    print(len(rv[0]['model']))
    print("================================")
    return ""