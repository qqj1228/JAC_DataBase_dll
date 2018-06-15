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
            db.GetTableName();
            Console.WriteLine();
            log.TraceInfo("GetTableName()");
            db.GetTableColumns("BrakeResult");
            Console.WriteLine();
            log.TraceWarning("GetTableColumns(\"BrakeResult\")");
            db.GetRecords("VehicleInfo", 1, 4);
            Console.WriteLine();
            log.TraceError("GetRecords(\"VehicleInfo\", 1, 4)");
            string[,] strArr = new string[,] { { "testvincode332345", "IEV33" }, { "testvincode442345", "IEV44" } };
            int iRet = db.ModifyDB("VehicleInfo", new string[] { "3", "4" }, strArr);
            Console.WriteLine();
            Console.WriteLine("{0} Row(s) affected", iRet);
            log.TraceFatal("ModifyDB(\"VehicleInfo\", new string[] { \"3\", \"4\" }, strArr)");
            int count = db.GetRecordsCount("VehicleInfo");
            Console.WriteLine();
            Console.WriteLine("There is {0} record(s)", count);
            log.TraceInfo("GetRecordsCount(\"VehicleInfo\")");
        }
    }
}
