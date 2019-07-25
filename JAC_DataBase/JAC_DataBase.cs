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
            public List<string[]> TableList;
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
                    } else if (item.Name == "TableList") {
                        DBCfg.TableList = new List<string[]>();
                        foreach (var node in item.ChildNodes) {
                            string key = ((XmlElement)node).GetAttribute("Name");
                            string value = ((XmlElement)node).GetAttribute("Description");
                            DBCfg.TableList.Add(new string[] { key, value });
                        }
                    }
                }
            } catch (Exception e) {
                Console.Error.WriteLine(e.Message);
                Log.TraceError(e.Message);
            }
        }

        public string[] GetTableName() {
            using (SqlConnection sqlConn = new SqlConnection(StrConn)) {
                try {
                    sqlConn.Open();
                    DataTable schema = sqlConn.GetSchema("Tables");
                    int count = DBCfg.TableList.Count;
                    string[] tableName = new string[count + 1];
                    tableName[0] = "请选择...";
                    foreach (DataColumn col in schema.Columns) {
                        if (col.Caption == "TABLE_NAME") {
                            for (int i = 0; i < schema.Rows.Count; i++) {
                                DataRow row = schema.Rows[i];
                                for (int j = 0; j < count; j++) {
                                    if (string.Format("{0}", row[col]) == DBCfg.TableList[j][0]) {
                                        tableName[j + 1] = DBCfg.TableList[j][1];
                                    }
                                }
                            }
                        }
                    }
                    return tableName;
                } catch (Exception e) {
                    Console.Error.WriteLine(e.Message);
                    Log.TraceError(e.Message);
                } finally {
                    sqlConn.Close();
                }
                return new string[] { };
            }
        }

        public string[] GetTableColumns(int index) {
            if (index < 1) {
                return new string[] { };
            }
            string strTableName = DBCfg.TableList[index - 1][0];
            using (SqlConnection sqlConn = new SqlConnection(StrConn)) {
                try {
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
                                } else if (col.DataType.Equals(typeof(decimal))) {
                                    tableName[i] = string.Format("{0:C}", row[col]);
                                } else {
                                    tableName[i] = string.Format("{0}", row[col]);
                                }
                            }
                        }
                    }
                    return tableName;
                } catch (Exception e) {
                    Console.Error.WriteLine(e.Message);
                    Log.TraceError(e.Message);
                } finally {
                    sqlConn.Close();
                }
            }
            return new string[] { };
        }

        public int GetRecordsCount(int index) {
            if (index < 1) {
                return -1;
            }
            string strTableName = DBCfg.TableList[index - 1][0];
            string strSQL = "select count(*) from " + strTableName;
            Log.TraceInfo("SQL: " + strSQL);
            int count = 0;
            using (SqlConnection sqlConn = new SqlConnection(StrConn)) {
                try {
                    SqlCommand sqlCmd = new SqlCommand(strSQL, sqlConn);
                    sqlConn.Open();
                    count = (int)sqlCmd.ExecuteScalar();
                } catch (Exception e) {
                    Console.Error.WriteLine(e.Message);
                    Log.TraceError(e.Message);
                } finally {
                    sqlConn.Close();
                }
            }
            return count;
        }

        public string[,] GetRecords(int index, int page, int perPage = 100) {
            if (index < 1) {
                return new string[,] { };
            }
            string strTableName = DBCfg.TableList[index - 1][0];
            if (page < 0) {
                page = 0;
            }
            int offset = page * perPage;
            string strSQL = string.Format("select top {2} * from {0} where id not in (select top {1} ID from {0} order by ID asc)", strTableName, offset, perPage);
            Log.TraceInfo("SQL: " + strSQL);
            return SelectDB(strSQL);
        }

        public int ModifyDB(int index, string[] strID, string[,] strValue) {
            if (index < 1) {
                return -1;
            }
            string strTableName = DBCfg.TableList[index - 1][0];
            int iRet = 0;
            int iRowNum = strValue.GetLength(0);
            int iColNum = strValue.GetLength(1);
            int iIDNum = strID.Length;
            Log.TraceInfo(string.Format("iRowNum:{0}, iIDNum:{1}", iRowNum, iIDNum));
            if (iRowNum == iIDNum) {
                iRet += UpdateDB(index, strID, strValue);
            } else if (iRowNum > iIDNum) {
                string[,] strUpdate = new string[iIDNum, iColNum];
                Array.Copy(strValue, 0, strUpdate, 0, iIDNum * iColNum);
                iRet += UpdateDB(index, strID, strUpdate);

                string[,] strInsert = new string[iRowNum - iIDNum, iColNum];
                Array.Copy(strValue, iIDNum * iColNum, strInsert, 0, (iRowNum - iIDNum) * iColNum);
                iRet += InsertDB(index, strInsert);
            } else {
                iRet = -1;
            }
            return iRet;
        }

        int UpdateDB(int index, string[] strID, string[,] strValue) {
            if (index < 1) {
                return -1;
            }
            string strTableName = DBCfg.TableList[index - 1][0];
            int iRet = 0;
            int iRowNum = strValue.GetLength(0);
            int iColNum = strValue.GetLength(1);
            if (iRowNum * strID.Length == 0) {
                return 0;
            }
            string[] strColumns = GetTableColumns(index);
            using (SqlConnection sqlConn = new SqlConnection(StrConn)) {
                try {
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
                } catch (Exception e) {
                    Console.Error.WriteLine(e.Message);
                    Log.TraceError(e.Message);
                } finally {
                    sqlConn.Close();
                }
            }
            return iRet;
        }

        int InsertDB(int index, string[,] strValue) {
            if (index < 1) {
                return -1;
            }
            string strTableName = DBCfg.TableList[index - 1][0];
            int iRet = 0;
            int iRowNum = strValue.GetLength(0);
            int iColNum = strValue.GetLength(1);
            if (iRowNum * iColNum == 0) {
                return 0;
            }
            using (SqlConnection sqlConn = new SqlConnection(StrConn)) {
                try {
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
                } catch (Exception e) {
                    Console.Error.WriteLine(e.Message);
                    Log.TraceError(e.Message);
                } finally {
                    sqlConn.Close();
                }
            }
            return iRet;
        }

        public int DeleteDB(int index, string[] strID) {
            if (index < 1) {
                return -1;
            }
            string strTableName = DBCfg.TableList[index - 1][0];
            int iRet = 0;
            int length = strID.Length;
            using (SqlConnection sqlConn = new SqlConnection(StrConn)) {
                try {
                    sqlConn.Open();
                    for (int i = 0; i < length; i++) {
                        string strSQL = "delete from " + strTableName + " where ID = '" + strID[i] + "'";
                        Log.TraceInfo("SQL: " + strSQL);
                        SqlCommand sqlCmd = new SqlCommand(strSQL, sqlConn);
                        iRet += sqlCmd.ExecuteNonQuery();
                    }
                } catch (Exception e) {
                    Console.Error.WriteLine(e.Message);
                    Log.TraceError(e.Message);
                } finally {
                    sqlConn.Close();
                }
            }
            return iRet;
        }

        string[,] SelectDB(string strSQL) {
            int count = 0;
            List<string[]> rowList;
            using (SqlConnection sqlConn = new SqlConnection(StrConn)) {
                try {
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
                } finally {
                    sqlConn.Close();
                }
            }
            return new string[,] { };
        }

        public string[,] SelectRecord(int index, string strColumnDesc, string strValue, out int iCount) {
            if (index < 1) {
                iCount = -1;
                return new string[,] { };
            }
            string strTableName = DBCfg.TableList[index - 1][0];
            string[] strColumns = GetTableColumns(index);
            string[] strColDesc = GetTableColumnDesc(index);
            string strColumn = "";
            for (int i = 0; i < strColDesc.Length; i++) {
                if (strColDesc[i] == strColumnDesc) {
                    strColumn = strColumns[i];
                }
            }
            string strSQL = "select * from " + strTableName + " where " + strColumn + " like '%" + strValue + "%'";
            Log.TraceInfo("SQL: " + strSQL);
            string[,] strArr = SelectDB(strSQL);
            iCount = strArr.GetLength(0);
            return strArr;
        }

        public string[] GetTableColumnDesc(int index) {
            if (index < 1) {
                return new string[] { };
            }
            string strTableName = DBCfg.TableList[index - 1][0];
            using (SqlConnection sqlConn = new SqlConnection(StrConn)) {
                try {
                    string strSQL = "select value from sys.extended_properties where major_id=OBJECT_ID('" + strTableName + "')";
                    SqlCommand sqlCmd = new SqlCommand(strSQL, sqlConn);
                    sqlConn.Open();
                    SqlDataReader sqlData = sqlCmd.ExecuteReader();
                    List<string> valueList = new List<string>();
                    while (sqlData.Read()) {
                        valueList.Add(sqlData.GetString(0));
                    }
                    string[] strDesc = new string[valueList.Count];
                    for (int i = 0; i < strDesc.Length; i++) {
                        strDesc[i] = valueList[i];
                    }
                    return strDesc;
                } catch (Exception e) {
                    Console.Error.WriteLine(e.Message);
                    Log.TraceError(e.Message);
                } finally {
                    sqlConn.Close();
                }
            }
            return new string[] { };
        }
    }
}
