from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import pandas as pd
import numpy as np
from sklearn import tree
from sklearn import metrics
from sklearn.model_selection import KFold

if __name__ == "__main__":
    session = SparkSession.builder.appName("predict returning users").getOrCreate()
    # sc = pyspark.SparkContext(pyspark.SparkConf)
    
    path_log = "user_log_format1.csv"
    path_info = "user_info_format1.csv"
    path_train = "train_format1.csv"
    path_test = "test_format1.csv"
    
    #读取为dataframe且丢弃缺失值
    user_info = session.read.option("header", True).csv(path_info).dropna()
    user_log = session.read.option("header", True).csv(path_log).dropna()
    df_train = session.read.option("header", True).csv(path_train).dropna()
    
    
    '''构建特征
    需要建立的特征如下：
    用户的年龄(age_range)
    用户的性别(gender)
    某用户在该商家日志的总条数(total_logs)
    用户浏览的商品的数目，就是浏览了多少个商品(unique_item_ids)
    浏览的商品的种类的数目，就是浏览了多少种商品(categories)
    用户浏览的天数(browse_days)
    用户单击的次数(one_clicks)
    用户添加购物车的次数(shopping_carts)
    用户购买的次数(purchase_times)
    用户收藏的次数(favourite_times)'''
    
    #age_range,gender特征添加
    df_train = df_train.join(user_info, on="user_id", how="left")
    #total_logs特征添加
    total_logs_temp = user_log.groupby("user_id", "seller_id").count()
    total_logs_temp = total_logs_temp.withColumnRenamed("count", "total_logs")
    total_logs_temp = total_logs_temp.withColumnRenamed("seller_id", "merchant_id")
    df_train = df_train.join(total_logs_temp, on=["user_id", "merchant_id"], how="left")
    #unique_item_ids特征添加
    unique_item_ids = user_log.groupBy("user_id", "seller_id", "item_id").count()
    unique_item_ids = unique_item_ids.groupBy("user_id", "seller_id").count()
    unique_item_ids = unique_item_ids.withColumnRenamed("seller_id", "merchant_id")
    unique_item_ids = unique_item_ids.withColumnRenamed("count", "unique_item_ids")
    df_train = df_train.join(unique_item_ids, on=["user_id", "merchant_id"], how="left")
    #categories特征构建
    categories = user_log.groupBy("user_id", "seller_id", "cat_id").count()
    categories = categories.groupBy("user_id", "seller_id").count()
    categories = categories.withColumnRenamed("seller_id", "merchant_id")
    categories = categories.withColumnRenamed("count", "categories")
    df_train = df_train.join(categories, on=["user_id", "merchant_id"], how="left")
    df_train = df_train.withColumn("categories", when(df_train.categories.isNull(), lit("0")).otherwise(df_train.categories))
    #browse_days特征构建
    browse_days_temp = user_log.groupby("user_id", "seller_id", "time_stamp").count()
    browse_days_temp = browse_days_temp.drop("count")
    browse_days_temp1 = browse_days_temp.groupby("user_id", "seller_id").count()
    browse_days_temp1 = browse_days_temp1.withColumnRenamed("seller_id", "merchant_id")
    browse_days_temp1 = browse_days_temp1.withColumnRenamed("count", "browse_days")
    df_train = df_train.join(browse_days_temp1, on=["user_id", "merchant_id"], how="left")
    #one_clicks、shopping_carts、purchase_times、favourite_times特征构建
    one_clicks_temp = user_log.filter("action_type == 0")
    one_clicks_temp = one_clicks_temp.groupby("user_id", "seller_id").count()
    one_clicks_temp = one_clicks_temp.withColumnRenamed("seller_id", "merchant_id")
    one_clicks_temp = one_clicks_temp.withColumnRenamed("count", "one_clicks")
    shopping_carts_temp = user_log.filter("action_type == 1")
    shopping_carts_temp = shopping_carts_temp.groupby("user_id", "seller_id").count()
    shopping_carts_temp = shopping_carts_temp.withColumnRenamed("seller_id", "merchant_id")
    shopping_carts_temp = shopping_carts_temp.withColumnRenamed("count", "shopping_carts")
    purchase_times_temp = user_log.filter("action_type == 2")
    purchase_times_temp = purchase_times_temp.groupby("user_id", "seller_id").count()
    purchase_times_temp = purchase_times_temp.withColumnRenamed("seller_id", "merchant_id")
    purchase_times_temp = purchase_times_temp.withColumnRenamed("count", "purchase_times")
    favourite_times_temp = user_log.filter("action_type == 3")
    favourite_times_temp = favourite_times_temp.groupby("user_id", "seller_id").count()
    favourite_times_temp = favourite_times_temp.withColumnRenamed("seller_id", "merchant_id")
    favourite_times_temp = favourite_times_temp.withColumnRenamed("count", "favourite_times")
    df_train = df_train.join(one_clicks_temp, on=["user_id", "merchant_id"], how="left")
    df_train = df_train.withColumn("one_clicks", when(df_train.one_clicks.isNull(), lit('0')).otherwise(df_train.one_clicks))
    df_train = df_train.join(shopping_carts_temp, on=["user_id", "merchant_id"], how="left")
    df_train = df_train.withColumn("shopping_carts",
                                   when(df_train.shopping_carts.isNull(), lit('0')).otherwise(df_train.shopping_carts))
    df_train = df_train.join(purchase_times_temp, on=["user_id", "merchant_id"], how="left")
    df_train = df_train.withColumn("purchase_times",
                             when(df_train.purchase_times.isNull(), lit('0')).otherwise(df_train.purchase_times))
    df_train = df_train.join(favourite_times_temp, on=["user_id", "merchant_id"], how="left")
    df_train = df_train.withColumn("favourite_times",
                             when(df_train.favourite_times.isNull(), lit('0')).otherwise(df_train.favourite_times))
    df_train.repartition(1).write.csv("train10features", encoding="utf-8", header=True)
    session.stop()

    #以下调用随机森林算法，并利用5折交叉验证，在训练集上进行训练，并以auc为度量
    if os.path.exists("train.csv"):
        #读取训练集，丢弃缺失值
        df_train=pd.read_csv("train.csv").dropna()
        #将数据集分为特征和标签,user_id和merchant_id行不考虑
        Xtrain=np.array(df_train.iloc[:,3:])
        Ytrain=df_train.iloc[:,2]
        # 将标签转换为-1和1
        Ytrain = Ytrain.apply(lambda x: -1 if x == 0 else 1).values

        #调用决策树，每棵树的每次划分从k个属性中选最佳属性
        k=3
        dtc = tree.DecisionTreeClassifier(criterion="entropy",max_features=k,random_state=0)

        kf = KFold(n_splits=5,random_state=0,shuffle=True)

        df_num_auc=pd.DataFrame(columns=['num_voters','AUC','acc'],dtype=float)

        #投票人数从3到max_num_voters都试一遍,取奇数
        max_num_voters=15

        #num指投票人数
        #在训练集上划分,训练,测试，以得到最佳的num
        for num in range(3, max_num_voters+1,2):
            auc ,acc= 0,0
            #5-fold交叉验证
            for train_index, test_index in kf.split(Xtrain):
                train_X, train_y =Xtrain[train_index], Ytrain[train_index]
                test_X, test_y = Xtrain[test_index], Ytrain[test_index]
                len_train, len_test = len(train_index), len(test_index)
                preds_test = np.zeros(len_test)
                for i in range(num):
                    #bootstrap有放回采样
                    bagging_index=np.random.choice(len_train,len_train)
                    bagging_X,bagging_y=train_X[bagging_index],train_y[bagging_index]
                    #训练
                    dtc.fit(bagging_X, bagging_y)
                    #测试
                    pred_test = dtc.predict(test_X)
                    #利用标签是1和-1，通过与0比较大小来体现投票结果
                    preds_test = preds_test +pred_test
                preds_test= (preds_test>0)*2-1
                #计算roc和auc
                acc=metrics.accuracy_score(test_y,preds_test)
                fpr, tpr, thresholds = metrics.roc_curve(test_y, preds_test,pos_label=1)
                auc+=metrics.auc(fpr,tpr)
            #5-fold交叉验证，所以取平均
            auc/=5
            df_num_auc.loc[(num-3)/2,'num_voters']=num
            df_num_auc.loc[(num-3)/2,'AUC']=float(auc)
            df_num_auc.loc[(num-3)/2,'acc']=acc

        print(df_num_auc)

        #根据auc表现，选出最好的num
        idx=df_num_auc['AUC'].idxmax()
        bestNum=df_num_auc.loc[idx,'num_voters']
        print('when we use {} base learners, we have the best auc {}, while the accuracy is {}'.format(bestNum,df_num_auc.loc[idx,'AUC'],df_num_auc.loc[idx,'acc']))