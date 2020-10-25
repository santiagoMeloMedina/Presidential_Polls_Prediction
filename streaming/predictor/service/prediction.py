
from pyspark.ml.regression import LinearRegression, GeneralizedLinearRegression, IsotonicRegression
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.evaluation import RegressionEvaluator
import numpy as np

def linearRegression(df, label, features, adjust):
    """ This function returns the rmse and the predictions form the applied generalized 
        regression model on the dataframe with the speficied feature columns """
    ## Columns with non numerical values are adjusted
    for col in adjust:
        indexer=StringIndexer(inputCol=col,outputCol="{}_num".format(col)) 
        features.append("{}_num".format(col))
        df=indexer.fit(df).transform(df)
    ## Features vector configured from dataframe for model processing
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    assembled = assembler.transform(df)
    lr = LinearRegression(featuresCol ='features', labelCol=label, maxIter=10, regParam=0.3, elasticNetParam=0.8)
    lrModel = lr.fit(assembled)
    ## Summary for rsme estimation
    trainingSummary = lrModel.summary
    predictions = lrModel.transform(assembled)
    result = {
        "RMSE": trainingSummary.rootMeanSquaredError,
        "predictions": [r["prediction"] for r in predictions.select("prediction").collect()]
    }
    return result

def generalizeRegression(df, label, features, adjust):
    """ This function returns the rmse and the predictions form the applied generalized 
        regression model on the dataframe with the speficied feature columns """
    ## Columns with non numerical values are adjusted
    for col in adjust:
        indexer=StringIndexer(inputCol=col,outputCol="{}_num".format(col)) 
        features.append("{}_num".format(col))
        df=indexer.fit(df).transform(df)
    ## Features vector configured from dataframe for model processing
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    assembled = assembler.transform(df)
    gr = GeneralizedLinearRegression(featuresCol ='features', labelCol=label, regParam=0.3, family="poisson")
    grModel=gr.fit(assembled)
    predictions = grModel.transform(assembled)
    ## Evaluator required for rmse estimation
    evaluator = RegressionEvaluator(labelCol=label, metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    result = {
        "RMSE": rmse,
        "predictions": [r["prediction"] for r in predictions.select("prediction").collect()]
    }
    return result

def isotonicRegression(df, label, features, adjust):
    """ This function returns the rmse and the predictions form the applied isotonic 
        regression model on the dataframe with the speficied feature columns """
    ## Columns with non numerical values are adjusted
    for col in adjust:
        indexer=StringIndexer(inputCol=col,outputCol="{}_num".format(col)) 
        features.append("{}_num".format(col))
        df=indexer.fit(df).transform(df)
    ## Features vector configured from dataframe for model processing
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    assembled = assembler.transform(df)
    ir = IsotonicRegression(featuresCol ='features', labelCol=label)
    irModel=ir.fit(assembled)
    predictions = irModel.transform(assembled)
    ## Evaluator required for rmse estimation
    evaluator = RegressionEvaluator(labelCol=label, metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    result = {
        "RMSE": rmse,
        "predictions": [r["prediction"] for r in predictions.select("prediction").collect()]
    }
    return result

def compareResult(results):
    """ This function compares the rmse of all the resulting models and returns the 
        minimum one """
    bestRMSE = [None, ""]
    for i in range(len(results)):
        rmse = results[i][0]["RMSE"]
        if bestRMSE[0] != None:
            if rmse < bestRMSE[0]:
                bestRMSE[0] = rmse
                bestRMSE[1] = results[i][1]
        else:
            bestRMSE[0] = rmse
            bestRMSE[1] = results[i][1]
    return { "RMSE": bestRMSE }