using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using JAC_DataBase;
using Logger;

namespace testDll {
    class Program {
        static void Main(string[] args) {
            LoggerClass log = new LoggerClass("./log", EnumLogLevel.LogLevelAll, true);
            DB_Class db = new DB_Class("./DB_Config.xml");

            /*
            log.TraceInfo("GetTableName()");
            db.GetTableName();

            Console.WriteLine();
            log.TraceWarning("GetTableColumns(\"BrakeResult\")");
            db.GetTableColumns("BrakeResult");

            Console.WriteLine();
            log.TraceError("GetRecords(\"VehicleInfo\", 1, 4)");
            db.GetRecords("VehicleInfo", 1, 4);
            */

            Console.WriteLine();
            log.TraceInfo("GetRecordsCount(\"VehicleInfo\")");
            int count = db.GetRecordsCount("VehicleInfo");
            Console.WriteLine("There is {0} record(s) in \"VehicleInfo\"", count);

            // UpdateDB
            Console.WriteLine();
            log.TraceFatal("ModifyDB(\"VehicleInfo\", new string[] { \"3\", \"4\" }, strArr)");
            string[,] strArr = new string[,] { { "testvincode332345", "IEV33" }, { "testvincode442345", "IEV44" } };
            int iRet = db.ModifyDB("VehicleInfo", new string[] { "3", "4" }, strArr);
            Console.WriteLine("ModifyDB(), {0} Row(s) affected", iRet);

            // UpdateDB and InsertDB
            Console.WriteLine();
            log.TraceFatal("ModifyDB(\"VehicleInfo\", new string[] { \"3\" }, strArr)");
            strArr = new string[,] { { "testvincode332345", "IEV33" }, { "testvincode992299", "IEV992299" } };
            iRet = db.ModifyDB("VehicleInfo", new string[] { "3" }, strArr);
            Console.WriteLine("ModifyDB(), {0} Row(s) affected", iRet);

            // InsertDB
            Console.WriteLine();
            log.TraceFatal("ModifyDB(\"VehicleInfo\", new string[] { }, strArr)");
            strArr = new string[,] { { "testvincode993399", "IEV993399" } };
            iRet = db.ModifyDB("VehicleInfo", new string[] { }, strArr);
            Console.WriteLine("ModifyDB(), {0} Row(s) affected", iRet);

            // DeleteDB
            Console.WriteLine();
            log.TraceFatal("ModifyDB(\"VehicleInfo\", new string[] { \"22\", \"23\" }, strArr)");
            iRet = db.DeleteDB("VehicleInfo", new string[] { "22", "23" });
            Console.WriteLine("DeleteDB(), {0} Row(s) affected", iRet);

            // Select
            Console.WriteLine();
            log.TraceError("SelectRecord(\"CaliProcResult\", \"Result\", \"X\", out count)");
            db.SelectRecord("CaliProcResult", "Result", "X", out count);
            Console.WriteLine("SelectRecord(), There is {0} record(s)", count);
        }
    }
}
