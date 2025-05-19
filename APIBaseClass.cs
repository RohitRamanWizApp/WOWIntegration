using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace WOWIntegration
{
    public class APIBaseClass : IDisposable
    {

        String _Con = "";
        SqlConnection con;
        SqlCommand SQLCmd;
        SqlDataAdapter da;
        SqlTransaction SQLTran;
        public String _DatabaseName
        {
            get
            {
                if (con == null)
                    return "";
                else
                    return Convert.ToString(con.Database);
            }
        }
        public String _ImageDatabaseName
        {
            get
            {
                if (con == null)
                    return "";
                else
                    return Convert.ToString(con.Database) + "_IMAGE";
            }
        }

        public APIBaseClass(string Conn)
        {
            _Con = Conn;
            con = new SqlConnection(_Con);
        }
        public APIBaseClass(SqlConnection SqlCon,SqlTransaction SqlTran)
        {
            con =SqlCon;
            SQLTran = SqlTran;
            _Con = con.ConnectionString;
        }
        public APIBaseClass()
        {

        }
        public String SelectCmdToSql(DataSet dset, String cExpr, String cTableAlias)
        {
            String cRetVal = "";
            try
            {
                SQLCmd = new SqlCommand(cExpr, con);
                SQLCmd.CommandTimeout = 0;
                da = new SqlDataAdapter(SQLCmd);
                DataSet dset1 = new DataSet();
                if (dset.Tables.Contains(cTableAlias)) dset.Tables[cTableAlias].Rows.Clear();
                da.Fill(dset, cTableAlias);
                //da.Fill(dset1);//, cTableAlias);
            }
            catch (Exception ex) { cRetVal = "SelectCmdToSql(DataSet dset, String cExpr, String cTableAlias)\n" + ex.Message.ToString(); }

            return cRetVal;
        }
        public String SelectCmdToSql(DataTable dt, String cExpr, String cTableAlias)
        {
            String cRetVal = "";
            SqlConnection con1 = new SqlConnection(_Con);
            try
            {
                using (SqlCommand SqlCmd1 = new SqlCommand(cExpr))
                {
                    SqlCmd1.Connection = con1;
                    SqlCmd1.CommandTimeout = 0;
                    con1.Open();
                    using (SqlDataReader sqldr = SqlCmd1.ExecuteReader())
                    {

                        dt.Load(sqldr);
                    }
                }
            }
            catch (Exception ex)
            {
                cRetVal = "SelectCmdToSql(DataTable dt, String cExpr, String cTableAlias)\n" + ex.Message.ToString();
            }
            finally
            {
                if (con1.State != ConnectionState.Closed)
                {
                    con1.Close();
                }
            }
            return cRetVal;
        }

        public String SelectCmdToSql_New(DataSet dset, String cExpr, String cTableAlias, out String cError)
        {
            String cRetVal = "";
            SqlConnection con1 = new SqlConnection(_Con);
            cError = "";
            try
            {

                using (SqlCommand SqlCmd1 = new SqlCommand(cExpr))
                {
                    SqlCmd1.Connection = con1;
                    SqlCmd1.CommandTimeout = 0;
                    con1.Open();
                    if (dset.Tables.Contains(cTableAlias))
                    {
                        dset.Tables[cTableAlias].Rows.Clear();
                        using (SqlDataReader sqldr = SqlCmd1.ExecuteReader())
                        {
                            //DataTable dt= sqldr.GetSchemaTable();
                            //while (sqldr.Read())
                            //{ 

                            //}

                            dset.Tables[cTableAlias].Load(sqldr);
                        }
                    }
                    else
                    {
                        DataTable dt = new DataTable(cTableAlias);
                        using (SqlDataReader sqldr = SqlCmd1.ExecuteReader())
                        {

                            dt.Load(sqldr);
                        }
                        dset.Tables.Add(dt);
                    }
                }
            }
            catch (Exception ex)
            {
                cError = ex.Message;
                cRetVal = "SelectCmdToSql_New(DataSet dset, String cExpr, String cTableAlias, out String cError)\n" + ex.Message.ToString();
            }
            finally
            {
                if (con1.State != ConnectionState.Closed)
                {
                    con1.Close();
                }
            }
            return cRetVal;
        }
        public object ExecuteScalar(String cExpr)
        {
            object obj;
            try
            {
                con = new SqlConnection(_Con);
                con.Open();
                SQLCmd = new SqlCommand(cExpr, con);
                SQLCmd.CommandTimeout = 0;
                obj = SQLCmd.ExecuteScalar();
            }
            catch (Exception) { obj = null; }
            finally { con.Close(); }
            return obj;
        }

        public object ExecuteScalar(String cExpr, out String cExpr_Err)
        {
            cExpr_Err = "";
            object obj;
            try
            {
                con = new SqlConnection(_Con);
                con.Open();
                SQLCmd = new SqlCommand(cExpr, con);
                SQLCmd.CommandTimeout = 0;
                obj = SQLCmd.ExecuteScalar();
            }
            catch (Exception ex) { cExpr_Err = ex.Message; obj = null; }
            finally { con.Close(); }
            return obj;
        }
        public void ExecuteNonQuery(String cExpr)
        {
            try
            {
                con = new SqlConnection(_Con);
                con.Open();
                SQLCmd = new SqlCommand(cExpr, con);
                SQLCmd.CommandTimeout = 0;
                SQLCmd.ExecuteNonQuery();
            }
            catch (Exception ex) { }
            finally { con.Close(); }
        }

        public void ExecuteNonQuery(String cExpr, out String cExpr_Err)
        {
            cExpr_Err = "";
            try
            {
                con = new SqlConnection(_Con);
                con.Open();
                SQLCmd = new SqlCommand(cExpr, con);
                SQLCmd.CommandTimeout = 0;
                SQLCmd.ExecuteNonQuery();
            }
            catch (Exception ex) { cExpr_Err = ex.Message; }
            finally { con.Close(); }
        }

        public void RunSQLCommandWithSQLTran(String cExpr,SqlConnection sqlcon, SqlTransaction sqlTran, out String cExpr_Err)
        {
            cExpr_Err = "";
            try
            {
                SqlCommand _SqlCmd = new SqlCommand(cExpr, sqlcon,sqlTran);
                _SqlCmd.CommandTimeout = 0;
                _SqlCmd.ExecuteNonQuery();
            }
            catch (Exception ex) { cExpr_Err = ex.Message; }
            finally {  }
        }
        public void RunSQLCommand(DataSet dset, String cExpr,String cTableAlias, Boolean bCloseConnection, out String cError)
        {
            cError = "";
            //try
            //{
            //    SqlCommand _SqlCmd = new SqlCommand(cExpr, sqlcon);
            //    _SqlCmd.CommandTimeout = 0;
            //    _SqlCmd.ExecuteNonQuery();
            //}
            //catch (Exception ex) { cError = ex.Message; }
            //finally { }

            {
                String cRetVal = "";
                SqlConnection con1 = new SqlConnection(_Con);
                cError = "";
                try
                {

                    using (SqlCommand SqlCmd1 = new SqlCommand(cExpr))
                    {
                        SqlCmd1.Connection = con1;
                        SqlCmd1.CommandTimeout = 0;
                        con1.Open();
                        if (dset.Tables.Contains(cTableAlias))
                        {
                            dset.Tables[cTableAlias].Rows.Clear();
                            using (SqlDataReader sqldr = SqlCmd1.ExecuteReader())
                            {
                                //DataTable dt= sqldr.GetSchemaTable();
                                //while (sqldr.Read())
                                //{ 

                                //}

                                dset.Tables[cTableAlias].Load(sqldr);
                            }
                        }
                        else
                        {
                            DataTable dt = new DataTable(cTableAlias);
                            using (SqlDataReader sqldr = SqlCmd1.ExecuteReader())
                            {

                                dt.Load(sqldr);
                            }
                            dset.Tables.Add(dt);
                        }
                    }
                }
                catch (Exception ex)
                {
                    //cError = ex.Message;
                    cRetVal = "SelectCmdToSql_New(DataSet dset, String cExpr, String cTableAlias, out String cError)\n" + ex.Message.ToString();
                    cError = cRetVal;
                }
                finally
                {
                    if (con1.State != ConnectionState.Closed)
                    {
                        con1.Close();
                    }
                }
                //return cRetVal;
            }
        }

        public double ConvertValue(string cValue)
        {
            double nValue = 0;
            bool bCheck = double.TryParse(cValue, out nValue);

            if (bCheck) return nValue;
            else return 0;
        }
        public DateTime ConvertDateTime(object val)
        {
            string dt = Convert.ToString(val);
            DateTime dtValue = new DateTime(1900, 1, 1);

            if (string.IsNullOrEmpty(dt) == false)
                DateTime.TryParse(dt, out dtValue);

            return dtValue;
        }
        public double ConvertDouble(object ob)
        {
            string cVal = Convert.ToString(ob);
            double nValue = 0;

            if (cVal.Length > 0)
                double.TryParse(cVal, out nValue);

            return nValue;
        }

        public Decimal ConvertDecimal(object ob)
        {
            string cVal = Convert.ToString(ob);
            Decimal nValue = 0M;

            if (cVal.Length > 0) Decimal.TryParse(cVal, out nValue);

            return nValue;
        }

        public bool ConvertBool(object Value)
        {
            bool bValue = true;
            string cValue = Convert.ToString(Value);

            if (cValue == "")
                bValue = false;
            else if (cValue == "0")
                bValue = false;
            else if (cValue.ToUpper() == "FALSE")
                bValue = false;
            else if (cValue == "1")
                bValue = true;
            else if (cValue.ToUpper() == "TRUE")
                bValue = true;

            return bValue;
        }
        public Int32 ConvertInt(object cVal)
        {
            string cValue = Convert.ToString(cVal);

            Int32 nValue = 0;
            bool bCheck = true;
            double dbValue = 0;

            if (string.IsNullOrEmpty(cValue.Trim()) == false)
                bCheck = double.TryParse(cValue, out dbValue);

            if (bCheck)
            {
                dbValue = Math.Floor(dbValue);

                if (dbValue != 0)
                    bCheck = Int32.TryParse(Convert.ToString(dbValue), out nValue);
            }

            return nValue;
        }

        public void AddDataForEditCols(DataTable dtSourceTable, DataTable dtTargetTable, String cSource, ref String cError)
        {
            try
            {
                int iRowCount = dtSourceTable.Rows.Count;
                int iNullRowCount = 0;


                foreach (DataColumn dcol in dtSourceTable.Columns)
                {
                    Type t = dcol.DataType;


                    iNullRowCount = 0;
                    //if (Type.GetTypeCode(t) == TypeCode.String)
                    //    iNullRowCount = dtSourceTable.Select(dcol.ColumnName + "=''").Length;
                    //else
                    //if (Type.GetTypeCode(t) == TypeCode.Int16 || Type.GetTypeCode(t) == TypeCode.Int32)
                    //    iNullRowCount = dtSourceTable.Select(dcol.ColumnName + "=0").Length;
                    //else
                    //if (Type.GetTypeCode(t) == TypeCode.Decimal)
                    //    iNullRowCount = dtSourceTable.Select(dcol.ColumnName + "=0.00").Length;
                    //else
                    iNullRowCount = dtSourceTable.Select(dcol.ColumnName + " IS NULL").Length;

                    if (iNullRowCount != iRowCount) //(!dtSourceTable.Rows.OfType<DataRow>().Any(r => r.IsNull(dcol)))
                    {
                        DataRow drNew = dtTargetTable.NewRow();

                        String cOrgCol = dcol.ColumnName.Trim();
                        drNew["TableName"] = cSource;
                        drNew["columnName"] = cOrgCol;
                        dtTargetTable.Rows.Add(drNew);
                    }
                }



            }
            catch (Exception ex)
            {
                cError = ex.Message;
            }
        }

        #region IDisposable Members

        public void Dispose()
        {
            //this.Dispose();
        }

        #endregion
    }
}
