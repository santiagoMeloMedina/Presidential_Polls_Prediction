
from configuration.connection import spark
from pyspark.sql.types import FloatType, StructType, StructField
from constant.data import COUPLE, INDIVIDUAL, COLS, DIVISION, CASTTYPE
from functools import reduce

def coupleCast(df):
    """ This funtions creates a subset of the dataframe which contains the column couple of 
        a category, divided by candidates with the purpose of evalutating their values of a 
        0-100% scale """
    for couple in COUPLE:
        fname, sname, scale = couple
        firstCalc = lambda f, s, standard, scale: standard/(f*scale+s*scale)*(f*scale)
        secondCalc = lambda f, s, standard, scale: standard/(f*scale+s*scale)*(s*scale)
        ### A temporal column used for replacing values mantaining consistency
        df = df.withColumn("tmp", firstCalc(df[fname], df[sname], 100, scale))
        df = df.withColumn(sname, secondCalc(df[fname], df[sname], 100, scale))
        df = df.drop(fname)
        df = df.withColumnRenamed("tmp", fname)
    return df

def individualCast(df):
    """ This funtions creates a subset of the dataframe which contains an individual column 
        configure its value on a 0-100% scale """
    for individual in INDIVIDUAL:
        name, scale = individual
        scaleNum = lambda num, scale: (num*scale)
        df = df.withColumn(name, scaleNum(df[name], scale))
    return df

def dropNull(df):
    """ This returns a subset of the dataframe which contains the columns specified with rows 
        dropped in which there exist null values"""
    for col in COLS:
        df = df.filter(df[col].isNotNull())
    return df

def castType(df):
    """ This function returns a subset of the dataframe with the casted values to float on certains 
        specified columns"""
    for col in CASTTYPE:
        df = df.withColumn(col, df[col]+0.0)
    return df

def castingData(df):
    """ This function returns a subset of the dataframe with all the cleaning with respect to 
        casting data and stablishing scales for data """
    df = dropNull(df)
    df = castType(df)
    df = coupleCast(df)
    df = individualCast(df)
    return df

def selectColumns(df):
    """ This function returns the subsets of the dataframe with the structure specified for 
        each candidate """
    general = df.select(COLS)
    incumbent = general.select(DIVISION["INC"])
    challenger = general.select(DIVISION["CHAL"])
    return incumbent, challenger

def dataCleaning(df):
    """ This function recieves a dataframe and applied all cleaning modifications of data such as 
        casting and selection of data """
    df = castingData(df)
    return selectColumns(df)
    