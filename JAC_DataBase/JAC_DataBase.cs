using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using Logger;

namespace JAC_DataBase {
    public class DB_Class {
        struct DBConfig {
            public string DB_IP { get; set; }
            public string DB_Port { get; set; }
            public string DB_Name { get; set; }
            public string DB_UserID { get; set; }
            public string DB_Pwd { get; set; }
        }

        DBConfig DBCfg;
        LoggerClass Log { get; set; }
        string StrConn { get; set; }

        public DB_Class(string strConfigFile) {
            this.DBCfg = new DBConfig();
            LoadConfig(strConfigFile);
            string strPath = Path.GetDirectoryName(strConfigFile);
            this.Log = new LoggerClass(strPath + "\\log", EnumLogLevel.LogLevelAll, true);

            StrConn = "user id=" + DBCfg.DB_UserID + ";";
            StrConn += "password=" + DBCfg.DB_Pwd + ";";
            StrConn += "database=" + DBCfg.DB_Name + ";";
            StrConn += "data source=" + DBCfg.DB_IP + "," + DBCfg.DB_Port;

            Log.TraceInfo("================ JAC_DataBase Start ================");
            Log.TraceInfo(StrConn);
        }

        void LoadConfig(string strConfigFile) {
            try {
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.Load(strConfigFile);
                XmlNode xn = xmlDoc.SelectSingleNode("DB_Config");
                XmlNodeList xnl = xn.ChildNodes;

                foreach (XmlNode item in xnl) {
                    if (item.Name == "DB_IP") {
                        DBCfg.DB_IP = item.InnerText;
                    } else if (item.Name == "DB_Port") {
                        DBCfg.DB_Port = item.InnerText;
                    } else if (item.Name == "DB_Name") {
                        DBCfg.DB_Name = item.InnerText;
                    } else if (item.Name == "DB_UserID") {
                        DBCfg.DB_UserID = item.InnerText;
                    } else if (item.Name == "DB_Pwd") {
                        DBCfg.DB_Pwd = item.InnerText;
                    }
                }
            } catch (Exception e) {
                Console.Error.WriteLine(e.Message);
                Log.TraceError(e.Message);
            }
        }

        public string[] GetTableName() {
            try {
                using (SqlConnection sqlConn = new SqlConnection(StrConn)) {
                    sqlConn.Open();
                    DataTable schema = sqlConn.GetSchema("Tables");
                    int count = schema.Rows.Count;
                    string[] tableName = new string[count + 1];
                    tableName[0] = "请选择...";
                    for (int i = 0; i < count; i++) {
                        DataRow row = schema.Rows[i];
                        foreach (DataColumn col in schema.Columns) {
                            if (col.Caption == "TABLE_NAME") {
                                if (col.DataType.Equals(typeof(DateTime))) {
                                    tableName[i + 1] = string.Format("{0:d}", row[col]);
                                } else if (col.DataType.Equals(typeof(Decimal))) {
                                    tableName[i + 1] = string.Format("{0:C}", row[col]);
                                } else {
                                    tableName[i + 1] = string.Format("{0}", row[col]);
                                }
                            }
                        }
                    }
                    sqlConn.Close();
                    foreach (var item in tableName) {
                        Console.WriteLine(item);
                    }
                    return tableName;
                }
            } catch (Exception e) {
                Console.Error.WriteLine(e.Message);
                Log.TraceError(e.Message);
            }
            return new string[] { "" };
        }

        public string[] GetTableColumns(string strTableName) {
            try {
                using (SqlConnection sqlConn = new SqlConnection(StrConn)) {
                    sqlConn.Open();
                    DataTable schema = sqlConn.GetSchema("Columns", new string[] { null, null, strTableName });
                    schema.DefaultView.Sort = "ORDINAL_POSITION";
                    schema = schema.DefaultView.ToTable();
                    int count = schema.Rows.Count;
                    string[] tableName = new string[count];
                    for (int i = 0; i < count; i++) {
                        DataRow row = schema.Rows[i];
                        foreach (DataColumn col in schema.Columns) {
                            if (col.Caption == "COLUMN_NAME") {
                                if (col.DataType.Equals(typeof(DateTime))) {
                                    tableName[i] = string.Format("{0:d}", row[col]);
                                } else if (col.DataType.Equals(typeof(Decimal))) {
                                    tableName[i] = string.Format("{0:C}", row[col]);
                                } else {
                                    tableName[i] = string.Format("{0}", row[col]);
                                }
                            }
                        }
                    }
                    sqlConn.Close();
                    foreach (var item in tableName) {
                        Console.WriteLine(item);
                    }
                    return tableName;
                }
            } catch (Exception e) {
                Console.Error.WriteLine(e.Message);
                Log.TraceError(e.Message);
            }
            return new string[] { "" };
        }

        void ShowDataTable(DataTable table, Int32 length) {
            foreach (DataColumn col in table.Columns) {
                Console.Write("{0,-" + length + "}", col.ColumnName);
            }
            Console.WriteLine();

            foreach (DataRow row in table.Rows) {
                foreach (DataColumn col in table.Columns) {
                    if (col.DataType.Equals(typeof(DateTime)))
                        Console.Write("{0,-" + length + ":d}", row[col]);
                    else if (col.DataType.Equals(typeof(Decimal)))
                        Console.Write("{0,-" + length + ":C}", row[col]);
                    else
                        Console.Write("{0,-" + length + "}", row[col]);
                }
                Console.WriteLine();
            }
        }

        void ShowDataTable(DataTable table) {
            ShowDataTable(table, 14);
        }

        void ShowColumns(DataTable columnsTable) {
            var selectedRows = from info in columnsTable.AsEnumerable()
                               select new {
                                   TableCatalog = info["TABLE_CATALOG"],
                                   TableSchema = info["TABLE_SCHEMA"],
                                   TableName = info["TABLE_NAME"],
                                   ColumnName = info["COLUMN_NAME"],
                                   DataType = info["DATA_TYPE"]
                               };

            Console.WriteLine("{0,-15}{1,-15}{2,-15}{3,-15}{4,-15}", "TableCatalog", "TABLE_SCHEMA",
                "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE");
            foreach (var row in selectedRows) {
                Console.WriteLine("{0,-15}{1,-15}{2,-15}{3,-15}{4,-15}", row.TableCatalog,
                    row.TableSchema, row.TableName, row.ColumnName, row.DataType);
            }
        }

        void ShowIndexColumns(DataTable indexColumnsTable) {
            var selectedRows = from info in indexColumnsTable.AsEnumerable()
                               select new {
                                   TableSchema = info["table_schema"],
                                   TableName = info["table_name"],
                                   ColumnName = info["column_name"],
                                   ConstraintSchema = info["constraint_schema"],
                                   ConstraintName = info["constraint_name"],
                                   KeyType = info["KeyType"]
                               };

            Console.WriteLine("{0,-14}{1,-11}{2,-14}{3,-18}{4,-16}{5,-8}", "table_schema", "table_name", "column_name", "constraint_schema", "constraint_name", "KeyType");
            foreach (var row in selectedRows) {
                Console.WriteLine("{0,-14}{1,-11}{2,-14}{3,-18}{4,-16}{5,-8}", row.TableSchema,
                    row.TableName, row.ColumnName, row.ConstraintSchema, row.ConstraintName, row.KeyType);
            }
        }

        public int GetRecordsCount(string strTableName) {
            string strSQL = "select count(*) from " + strTableName;
            Log.TraceInfo("SQL: " + strSQL);
            int count = 0;
            try {
                using (SqlConnection sqlConn = new SqlConnection(StrConn)) {
                    SqlCommand sqlCmd = new SqlCommand(strSQL, sqlConn);
                    sqlConn.Open();
                    count = (int)sqlCmd.ExecuteScalar();
                    sqlConn.Close();
                }
            } catch (Exception e) {
                Console.Error.WriteLine(e.Message);
                Log.TraceError(e.Message);
            }
            return count;
        }

        public string[,] GetRecords(string strTableName, int page, int perPage = 100) {
            int offset = page * perPage;
            string strSQL = string.Format("select top {2} * from {0} where id not in (select top {1} ID from {0} order by ID asc)", strTableName, offset, perPage);
            Log.TraceInfo("SQL: " + strSQL);
            return SelectDB(strSQL);
        }

        public int ModifyDB(string strTableName, string[] strID, string[,] strValue) {
            int iRet = 0;
            int iRowNum = strValue.GetLength(0);
            int iColNum = strValue.GetLength(1);
            int iIDNum = strID.Length;
            Log.TraceInfo(string.Format("iRowNum:{0}, iIDNum:{1}", iRowNum, iIDNum));
            if (iRowNum == iIDNum) {
                iRet += UpdateDB(strTableName, strID, strValue);
            } else if (iRowNum > iIDNum) {
                string[,] strUpdate = new string[iIDNum, iColNum];
                Array.Copy(strValue, 0, strUpdate, 0, iIDNum * iColNum);
                iRet += UpdateDB(strTableName, strID, strUpdate);

                string[,] strInsert = new string[iRowNum - iIDNum, iColNum];
                Array.Copy(strValue, iIDNum * iColNum, strInsert, 0, (iRowNum - iIDNum) * iColNum);
                iRet += InsertDB(strTableName, strInsert);
            } else {
                iRet = -1;
            }
            return iRet;
        }

        int UpdateDB(string strTableName, string[] strID, string[,] strValue) {
            int iRet = 0;
            int iRowNum = strValue.GetLength(0);
            int iColNum = strValue.GetLength(1);
            if (iRowNum * strID.Length == 0) {
                return 0;
            }
            string[] strColumns = GetTableColumns(strTableName);
            try {
                using (SqlConnection sqlConn = new SqlConnection(StrConn)) {
                    sqlConn.Open();
                    for (int i = 0; i < iRowNum; i++) {
                        string strSQL = "update ";
                        string strSet = "set ";
                        for (int j = 1; j < strColumns.Length; j++) {
                            strSet += strColumns[j] + " = '" + strValue[i, j - 1] + "', ";
                        }
                        strSet = strSet.Remove(strSet.Length - 2);
                        strSQL += string.Format("{0} {1} where ID = '{2}'", strTableName, strSet, strID[i]);
                        Log.TraceInfo("SQL: " + strSQL);
                        SqlCommand sqlCmd = new SqlCommand(strSQL, sqlConn);
                        iRet += sqlCmd.ExecuteNonQuery();
                    }
                    sqlConn.Close();
                }
            } catch (Exception e) {
                Console.Error.WriteLine(e.Message);
                Log.TraceError(e.Message);
            }
            return iRet;
        }

        int InsertDB(string strTableName, string[,] strValue) {
            int iRet = 0;
            int iRowNum = strValue.GetLength(0);
            int iColNum = strValue.GetLength(1);
            if (iRowNum * iColNum == 0) {
                return 0;
            }
            try {
                using (SqlConnection sqlConn = new SqlConnection(StrConn)) {
                    sqlConn.Open();
                    for (int i = 0; i < iRowNum; i++) {
                        string strSQL = "insert " + strTableName + " values (";
                        for (int j = 0; j < iColNum; j++) {
                            strSQL += "'" + strValue[i, j] + "', ";
                        }
                        strSQL = strSQL.Remove(strSQL.Length - 2);
                        strSQL += ")";
                        Log.TraceInfo("SQL: " + strSQL);
                        SqlCommand sqlCmd = new SqlCommand(strSQL, sqlConn);
                        iRet += sqlCmd.ExecuteNonQuery();
                    }
                    sqlConn.Close();
                }
            } catch (Exception e) {
                Console.Error.WriteLine(e.Message);
                Log.TraceError(e.Message);
            }
            return iRet;
        }

        public int DeleteDB(string strTableName, string[] strID) {
            int iRet = 0;
            int length = strID.Length;
            try {
                using (SqlConnection sqlConn = new SqlConnection(StrConn)) {
                    sqlConn.Open();
                    for (int i = 0; i < length; i++) {
                        string strSQL = "delete from " + strTableName + " where ID = '" + strID[i] + "'";
                        Log.TraceInfo("SQL: " + strSQL);
                        SqlCommand sqlCmd = new SqlCommand(strSQL, sqlConn);
                        iRet += sqlCmd.ExecuteNonQuery();
                    }
                    sqlConn.Close();
                }
            } catch (Exception e) {
                Console.Error.WriteLine(e.Message);
                Log.TraceError(e.Message);
            }
            return iRet;
        }

        string[,] SelectDB(string strSQL) {
            try {
                int count = 0;
                List<string[]> rowList;
                using (SqlConnection sqlConn = new SqlConnection(StrConn)) {
                    SqlCommand sqlCmd = new SqlCommand(strSQL, sqlConn);
                    sqlConn.Open();
                    SqlDataReader sqlData = sqlCmd.ExecuteReader();
                    count = sqlData.FieldCount;
                    rowList = new List<string[]>();
                    while (sqlData.Read()) {
                        string[] items = new string[count];
                        for (int i = 0; i < count; i++) {
                            object obj = sqlData.GetValue(i);
                            if (obj.GetType() == typeof(DateTime)) {
                                items[i] = ((DateTime)obj).ToString("yyyy-MM-dd");
                            } else {
                                items[i] = obj.ToString();
                            }
                        }
                        rowList.Add(items);
                    }
                    sqlConn.Close();
                }
                string[,] records = new string[rowList.Count, count];
                for (int i = 0; i < rowList.Count; i++) {
                    for (int j = 0; j < count; j++) {
                        records[i, j] = rowList[i][j];
                    }
                }
                return records;
            } catch (Exception e) {
                Console.Error.WriteLine(e.Message);
                Log.TraceError(e.Message);
            }
            return new string[,] { { "" }, { "" } };
        }

        public string[,] SelectRecord(string strTableName, string strColumn, string strValue, out int iCount) {
            string strSQL = "select * from " + strTableName + " where " + strColumn + " = '" + strValue + "'";
            Log.TraceInfo("SQL: " + strSQL);
            string[,] strArr = SelectDB(strSQL);
            iCount = strArr.GetLength(0);
            return strArr;
        }
    }
}
