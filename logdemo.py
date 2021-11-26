import pandas as pd
from datetime import datetime
import uuid
from util.Logger import Logger
import util.DBUtility as DBUtil
import util.ExcelUtility as ExcelUtil

class ImportEtch:

    uuid = None
    excelFilename = None
    inputSeq = None
    logger = None
    excelUtil = None
    dBUtil = None

    def InsertExcelFile(self):
        tableName = 'dbo.excel_file'
        now = datetime.now()
        nowStr = now.strftime("%Y-%m-%d %H:%M:%S")
        sqlStr = "INSERT INTO {tableName} (PmId, FileType, FileName, Module, InputSeq, UpdateTime) VALUES ('{pmId}', 'Summary', '{fileName}', 'ETCH', {inputSeq}, CONVERT(datetime,'{updateTime}'))".format(tableName=tableName, 
                    pmId=self.uuid, fileName=self.excelFilename, inputSeq=self.inputSeq, updateTime=nowStr)
        self.dBUtil.Execute(sqlStr)
        self.logger.info(sqlStr)
        sqlStr = "INSERT INTO excel_file (PmId, FileType, FileName, Module, InputSeq, UpdateTime) VALUES ('{pmId}', 'EQ', '{fileName}', 'ETCH', {inputSeq}, CONVERT(datetime,'{updateTime}'))".format(pmId=self.uuid, 
                    fileName=self.excelUtil.filenameEqG, inputSeq=self.inputSeq, updateTime=nowStr)
        self.dBUtil.Execute(sqlStr)

    def InsertSummary(self):
        tableName = 'dbo.eq_summary'
        df = self.excelUtil.LoadSummaryData()
        df['PmId'] = self.uuid
        # print(df)
        sqlStr = "INSERT INTO {tableName} (JudgeRatio, Fab, EqG, Vendor, Model, EqGG, PmId) VALUES (%s, %s, %s, %s, %s, %s, %s) ".format(tableName=tableName)
        self.dBUtil.InsertDBFromDataFrame(sqlStr, df)

    def InsertEqChange(self):
        tableName = 'dbo.eq_change'
        df = self.excelUtil.LoadEqChangeData()
        df['PmId'] = self.uuid
        print(df)
        sqlStr = "INSERT INTO {tableName} (Fab, EqG, EqChange, PmId) VALUES (%s, %s, %s, %s) ".format(tableName=tableName)
        self.dBUtil.InsertDBFromDataFrame(sqlStr, df)

    def InsertEqCross(self):
        tableName = 'dbo.eq_cross'
        df = self.excelUtil.LoadEqCrossData()
        df['PmId'] = self.uuid
        print(df)
        sqlStr = "INSERT INTO {tableName} (FabTo, EqGTo, VendorTo, ModelTo, Ratio, FabFrom, EqGFrom, VendorFrom, ModelFrom, PmId) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ".format(tableName=tableName)
        self.dBUtil.InsertDBFromDataFrame(sqlStr, df)

    def InsertEqFlow(self):
        tableName = 'dbo.eq_flow'
        dfP12 = self.excelUtil.LoadP12FlowData()
        dfP3 = self.excelUtil.LoadP3FlowData()
        df = pd.concat([dfP12,dfP3])
        df['PmId'] = self.uuid
        # print(df)
        # print(df.isnull().sum())
        sqlStr = "INSERT INTO {tableName} (OpFab, Product, OpNo, EqG, LRCP, TC, RunTime, Loss, UpTime, BatchSize, Fab, BackupFlag, PmId) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ".format(tableName=tableName)
        self.dBUtil.InsertDBFromDataFrame(sqlStr, df)

    def InsertEqDualDirect(self):
        tableName = 'dbo.eq_dualdirect'
        df = self.excelUtil.LoadEqDualDirectData()
        df['PmId'] = self.uuid
        # print(df)
        sqlStr = "INSERT INTO {tableName} (BgId, Fab, EqG, PmId) VALUES (%s, %s, %s, %s) ".format(tableName=tableName)
        self.dBUtil.InsertDBFromDataFrame(sqlStr, df)

    def InsertEqModel(self):
        tableName = 'dbo.eq_model'
        dfP12 = self.excelUtil.LoadP12EqData()
        dfP3 = self.excelUtil.LoadP3EqData()
        df = pd.concat([dfP12,dfP3])
        df['PmId'] = self.uuid
        # print(df)
        # print(df.isnull().sum())
        sqlStr = "INSERT INTO {tableName} (Fab, EqG, Vendor, Model, EqBase, EqCount, PmId) VALUES (%s, %s, %s, %s, %s, %s, %s) ".format(tableName=tableName)
        self.dBUtil.InsertDBFromDataFrame(sqlStr, df)

    def InsertEqMove(self):
        tableName = 'dbo.eq_move'
        df = self.excelUtil.LoadP123BackupData()
        df['PmId'] = self.uuid
        # print(df)
        sqlStr = "INSERT INTO {tableName} (EqGP12, StepP12, CapP12, EqGP3, StepP3, CapP3, PmId) VALUES (%s, %s, %s, %s, %s, %s, %s) ".format(tableName=tableName)
        self.dBUtil.InsertDBFromDataFrame(sqlStr, df)

    def InsertEqUniDirect(self):
        tableName = 'dbo.eq_unidirect'
        df = self.excelUtil.LoadEqUniDirectData()
        df['PmId'] = self.uuid
        # print(df)
        sqlStr = "INSERT INTO {tableName} (FabTo, EqGTo, FabFrom, EqGFrom, PmId) VALUES (%s, %s, %s, %s, %s) ".format(tableName=tableName)
        self.dBUtil.InsertDBFromDataFrame(sqlStr, df)

    def InsertProdInput(self):
        tableName = 'dbo.prod_input'
        dfP12 = self.excelUtil.LoadP12InputData()
        dfP3  = self.excelUtil.LoadP3InputData()
        df = pd.concat([dfP12,dfP3])
        df['PmId'] = self.uuid
        # print(df)
        sqlStr = "INSERT INTO {tableName} (Product, Input, Fab, PmId) VALUES (%s, %s, %s, %s) ".format(tableName=tableName)
        self.dBUtil.InsertDBFromDataFrame(sqlStr, df)


    def InsertEqInvest(self):
        tableName = 'dbo.eq_invest'
        df = self.excelUtil.LoadEqInvestData()
        df['PmId'] = self.uuid
        print(df)
        sqlStr = "INSERT INTO {tableName} (Fab, EqG, EqInvest, PmId) VALUES (%s, %s, %s, %s) ".format(tableName=tableName)
        self.dBUtil.InsertDBFromDataFrame(sqlStr, df)


    def ImportData(self):
        # self.InsertExcelFile()
        # self.InsertSummary()
        # self.InsertEqChange()
        # self.InsertEqCross()
        # self.InsertEqFlow()
        # self.InsertEqDualDirect()
        # self.InsertEqModel()
        # self.InsertEqMove()
        # self.InsertEqUniDirect()
        # self.InsertProdInput()
        self.InsertEqInvest()


    def __init__(self,filename, inputSeq):
        self.excelFilename = filename
        self.inputSeq = inputSeq
        dbConfigName = 'AIoProductMix'
        now = datetime.now()
        nowStr = now.strftime("%Y%m%d")
        loggerFileName = filename.replace('.xlsx', '_' + nowStr + '.log').replace(r'/data/',r'/log/')
        self.logger = Logger(name=loggerFileName)
        self.excelUtil = ExcelUtil.ExcelUtility(filename)
        self.dBUtil = DBUtil.DBUtitily(dbConfigName)
        self.uuid = uuid.uuid4()

if __name__ == "__main__": 
    file_name = './data/ETCH.xlsx'
    inputSeq = 1
    importEtch = ImportEtch(file_name, inputSeq)
    importEtch.ImportData()
