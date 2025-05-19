using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;

namespace WOWIntegration
{
    public class clsSALE_PS
    {
        Boolean _bAllowNegative = false;
        String LoggedLocation, LoggedUserCode, LoggedUserAlias, LoggedBin, cUserForBINs;
        //Boolean bApplyFlatschemesOnly;
        //public bool BApplyFlatschemesOnly { get => bApplyFlatschemesOnly; set => bApplyFlatschemesOnly = value; }

        public string _cUserForBINs { get => cUserForBINs; set => cUserForBINs = value; }
        //public Boolean _AddMode = false;
        //DataTable dtRedeemCoupon, dtAPP_MST, dtAPP_DET, dtEditBackUp;
        //internal Boolean bRoundOff_Total = true, bRoundOff_Item = true, BSaleSetUp = false;
        //internal String cRoundOff_Total_At = "1", cRoundOff_Item_At = "1", cSpidNew = "";
        //internal Decimal dexchange_tolerance_discount_diff_pct = 0;
        //String cPARA_NAME_FOR_DISCOUNT = "";
        //Int32 _DISCOUNT_PICKMODE_SLR = 0;
        //internal string cCmdRowId = "";
        //Boolean bProcessReturnItems = true, bALLOW_NEG_STOCK = false;
        //Boolean lShown = false;
        //DataTable dtApplicableBarcodes, dtACTIVE_SCHEMES_CLONE, dtACTIVE_SCHEMES1_CLONE, dtACTIVE_SCHEMES2_CLONE, dtACTIVE_SCHEMES3_CLONE, dtACTIVE_SCHEMES_BARCODE_CLONE;
        //String _CM_ID;
        //DateTime _EOSS_SERVER_DATETIME = new DateTime();
        //Decimal nHAPPYHOURS_MAXDISCOUNT = 0;
        public DataTable CreateDataTableWithDataType<T>(T items)
        {
            DataTable dataTable = new DataTable(typeof(T).Name);
            //Get all the properties
            PropertyInfo[] Props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (PropertyInfo prop in Props)
            {
                //Setting column names as Property names
                dataTable.Columns.Add(prop.Name, prop.PropertyType);
                dataTable.Columns[prop.Name].AllowDBNull = true;
            }
            //foreach (T item in items)
            //{

            //    var values = new object[Props.Length];
            //    for (int i = 0; i < Props.Length; i++)
            //    {
            //        //inserting property values to datatable rows
            //        values[i] = Props[i].GetValue(item, null);
            //    }

            //    dataTable.Rows.Add(values);
            //}
            ////put a breakpoint here and check datatable
            //if (!AppAIVAL.dset.Tables.Contains(dataTable.TableName))
            //    AppAIVAL.dset.Tables.Add(dataTable);
            //else
            //    AppAIVAL.dset.Tables[dataTable.TableName].Clear();
            return dataTable;
        }
        public clsSALE_PS(Boolean bAllowNegative, String _LoggedLocation, String _LoggedUserCode, String _LoggedUserAlias, String _LoggedBin)
        {
            _bAllowNegative = bAllowNegative;
            LoggedLocation = _LoggedLocation;
            LoggedUserCode = _LoggedUserCode;
            LoggedUserAlias = _LoggedUserAlias;
            LoggedBin = _LoggedBin;
        }
        String BadRequest(String cMsg)
        {
            return cMsg;
        }

        public String AllocateBatchBarcode(String cConStr, String cMemoID, DataTable tINVDET)
        {
            String lRetVal = "";
            DataSet dset = new DataSet();
            Int32 iCountTotalBeforeAllocation = tINVDET.Rows.Count;
            Int32 iCountTotalAfterAllocation = 0;

            if (!tINVDET.Columns.Contains("SrNo"))
            {
                tINVDET.Columns.Add("SrNo", typeof(System.Int32));
            }

            APIBaseClass clsCommon = new APIBaseClass();
            DataTable dtSave = tINVDET.Clone();
            DataTable dtSave_UPLOAD = tINVDET.Clone();
            StringBuilder sb = new StringBuilder();
            StringBuilder sbPC = new StringBuilder();
            SqlCommand cmd = new SqlCommand();
            SqlDataAdapter sda = new SqlDataAdapter();
            SqlConnection con = new SqlConnection(cConStr);
            cmd = new SqlCommand();
            cmd.Connection = con;
            try
            {
                if (con.State != ConnectionState.Open)
                    con.Open();

                foreach (DataRow drow in tINVDET.Rows)
                {
                    if (clsCommon.ConvertDecimal(drow["QUANTITY"]) > 0 && clsCommon.ConvertInt(drow["CODING_SCHEME"]) == 1
                        /*&& clsCommon.ConvertInt(drow["UOM_TYPE"]) == 1 */&& !clsCommon.ConvertBool(drow["STOCK_NA"])
                        && !Convert.ToString(drow["product_code"]).Contains("@"))
                    {
                        sb.Append("'");
                        sb.Append(Convert.ToString(drow["ROW_ID"]));
                        sb.Append("',");
                        DataRow[] drowexists = dtSave.Select("product_code='" + Convert.ToString(drow["product_code"]) + "' AND BIN_ID='" + Convert.ToString(drow["BIN_ID"]) + "' AND MRP=" + clsCommon.ConvertDecimal(drow["MRP"]) + "");
                        Int32 drowexistsSrNo = drowexists.Length;
                        //Decimal nDiscAmount = clsCommon.ConvertDecimal(drow["DISCOUNT_AMOUNT"]);
                        Decimal nQuantity = clsCommon.ConvertDecimal(drow["QUANTITY"]);
                        if (drowexistsSrNo > 0)
                        {
                            drowexists[0]["QUANTITY"] = clsCommon.ConvertDecimal(drowexists[0]["QUANTITY"]) + nQuantity;
                        }
                        else
                        {
                            sbPC.Append("'");
                            sbPC.Append(Convert.ToString(drow["PRODUCT_CODE"]));
                            sbPC.Append("',");

                            DataRow drNew1 = dtSave.NewRow();
                            drNew1.ItemArray = drow.ItemArray;
                            dtSave.Rows.Add(drNew1);
                        }
                    }
                }
                String cStrRowID = sb.ToString().TrimEnd(',');
                String cStrProductCode = sbPC.ToString().TrimEnd(',');
                if (!String.IsNullOrEmpty(cStrRowID))
                {
                    DataRow[] drowDelete = tINVDET.Select("ROW_ID IN (" + cStrRowID + ")");
                    foreach (DataRow drowdel in drowDelete)
                    {
                        tINVDET.Rows.Remove(drowdel);
                    }
                    Decimal nAllowNegativeStockQuantity = 0.00M;
                    if (_bAllowNegative) nAllowNegativeStockQuantity = 99999.00M;

                    String cStrQuery = @"IF OBJECT_ID('tempdb..#WSLDTSAVE','U') is not null
	                                                DROP TABLE #WSLDTSAVE
                                                SELECT DISTINCT product_code INTO #WSLDTSAVE FROM SKU where PRODUCT_CODE IN (" + cStrProductCode.ToString() + ")";


                    cmd.CommandText = cStrQuery;
                    cmd.ExecuteNonQuery();



                    //using (SqlBulkCopy bulkCopy = new SqlBulkCopy(cConStr))
                    //{
                    //    bulkCopy.BulkCopyTimeout = 50000;
                    //    bulkCopy.BatchSize = 5000;
                    //    bulkCopy.DestinationTableName = "#WSLDTSAVE";

                    //    //clsCommon.SelectCmdToSql(dset, "SELECT * FROM #WSLDTSAVE WHERE 1=2", "TCURSOR_WSLDTSAVE");
                    //    if (dset.Tables.Contains("TCURSOR_WSLDTSAVE"))
                    //        dset.Tables["TCURSOR_WSLDTSAVE"].Clear();
                    //    cmd.CommandText = "SELECT * FROM #WSLDTSAVE WHERE 1=2";
                    //    sda = new SqlDataAdapter(cmd);
                    //    sda.Fill(dset, "TCURSOR_WSLDTSAVE");
                    //    foreach (DataColumn dcol in dset.Tables["TCURSOR_WSLDTSAVE"].Columns)
                    //        bulkCopy.ColumnMappings.Add(dcol.ColumnName, dcol.ColumnName);

                    //    try
                    //    {
                    //        bulkCopy.WriteToServer(dtSave);
                    //    }
                    //    catch (Exception ex)
                    //    {
                    //        String cErr = "Record Not Updated in #WSLDTDAVE Table SQL Error : " + ex.Message.ToString();
                    //        //MessageBox.Show("Error in Normalization : " + cErr, Application.ProductName, MessageBoxButtons.OK, MessageBoxIcon.Warning);
                    //        return cErr;
                    //    }

                    //}
                    cStrQuery = @";WITH STK
                                AS
                                (
                                    SELECT LEFT(A.PRODUCT_CODE, ISNULL(NULLIF(CHARINDEX ('@',A.PRODUCT_CODE)-1,-1),LEN(A.PRODUCT_CODE ))) AS BASE_PRODUCT_CODE,
                                    A.product_code," + (!_bAllowNegative ? " A.quantity_in_stock " : nAllowNegativeStockQuantity.ToString()) + @" AS quantity_in_stock,
                                    A.DEPT_ID,A.BIN_ID,C.MRP,C.HSN_CODE
                                    ,C.article_code,C.para1_code,C.para2_code,C.para3_code,C.para4_code,C.para5_code,C.para6_code,C.para7_code
	                                ,C1.SECTION_NAME,C1.SUB_SECTION_NAME,C1.article_no,C1.para1_name,C1.para2_name,C1.para3_name,C1.para4_name,C1.para5_name,C1.para6_name,C1.para7_name
                                    FROM PMT01106 A (NOLOCK)
                                    JOIN SKU C (NOLOCK) ON C.PRODUCT_CODE=A.PRODUCT_CODE
                                    JOIN sku_names C1 (NOLOCK) ON C1.PRODUCT_CODE=C.PRODUCT_CODE
                                    JOIN BIN B (NOLOCK) ON A.BIN_ID =B.BIN_ID 
                                    JOIN #WSLDTSAVE D ON D.PRODUCT_CODE=LEFT(A.PRODUCT_CODE, ISNULL(NULLIF(CHARINDEX ('@',A.PRODUCT_CODE)-1,-1),LEN(A.PRODUCT_CODE ))) 
                                    where ISNULL( A.bo_order_id,'')='' " + (!_bAllowNegative ? " AND quantity_in_stock>0 " : "") +
                                        //AND LEFT(A.PRODUCT_CODE, ISNULL(NULLIF(CHARINDEX('@', A.PRODUCT_CODE) - 1, -1), LEN(A.PRODUCT_CODE))) IN
                                        //   (" + cStrProductCode + @")" +
                                        (LoggedUserCode != "0000000" ? " AND B.major_bin_id IN(" + _cUserForBINs + @") " : "") +
                                    @" AND DEPT_ID='" + LoggedLocation + @"'
                                    UNION ALL
                                    SELECT LEFT(A.PRODUCT_CODE, ISNULL(NULLIF(CHARINDEX ('@',A.PRODUCT_CODE)-1,-1),LEN(A.PRODUCT_CODE ))) AS BASE_PRODUCT_CODE,
                                    A.product_code,A.quantity quantity_in_stock,A.DEPT_ID,A.BIN_ID,A.MRP,A.HSN_CODE
                                    ,C.article_code,C.para1_code,C.para2_code,C.para3_code,C.para4_code,C.para5_code,C.para6_code,C.para7_code
	                                ,C1.SECTION_NAME,C1.SUB_SECTION_NAME,C1.article_no,C1.para1_name,C1.para2_name,C1.para3_name,C1.para4_name,C1.para5_name,C1.para6_name,C1.para7_name
                                    FROM CMD01106 A (NOLOCK)
                                    JOIN SKU C (NOLOCK) ON C.PRODUCT_CODE=A.PRODUCT_CODE
                                    JOIN sku_names C1 (NOLOCK) ON C1.PRODUCT_CODE=C.PRODUCT_CODE
                                    WHERE CM_ID='" + cMemoID + @"'
                                )
                                SELECT cast(ISNULL(NULLIF(CHARINDEX ('@',PRODUCT_CODE)-1,-1),-99) as NUMERIC(3)) AS  SRNO,BASE_PRODUCT_CODE,
                                CAST((CASE WHEN NULLIF(CHARINDEX ('@',PRODUCT_CODE)-1,-1) IS NULL THEN 0 ELSE RIGHT(PRODUCT_CODE, LEN(PRODUCT_CODE )-CHARINDEX ('@',PRODUCT_CODE)) END) AS NUMERIC) AS BATCH_NO,
                                product_code,DEPT_ID,BIN_ID,MRP
                                ,article_code,para1_code,para2_code,para3_code,para4_code,para5_code,para6_code,para7_code
                                ,section_name,sub_section_name,article_no,para1_name,para2_name,para3_name,para4_name,para5_name,para6_name,para7_name
                                ,cast(0 as NUMERIC(14,3)) AS allocatedQty,SUM(quantity_in_stock) AS quantity_in_stock,HSN_CODE
                                FROM STK
                                GROUP BY BASE_PRODUCT_CODE,product_code,DEPT_ID,BIN_ID,MRP,HSN_CODE ,article_code,para1_code,para2_code,para3_code,para4_code,para5_code,para6_code,para7_code
                                ,section_name,sub_section_name,article_no,para1_name,para2_name,para3_name,para4_name,para5_name,para6_name,para7_name " +
                                (_bAllowNegative ?
                                @"UNION 
								SELECT cast(ISNULL(NULLIF(CHARINDEX ('@',A.PRODUCT_CODE)-1,-1),-99) as NUMERIC(3)) AS  SRNO,LEFT(A.PRODUCT_CODE, ISNULL(NULLIF(CHARINDEX ('@',A.PRODUCT_CODE)-1,-1),LEN(A.PRODUCT_CODE ))) AS BASE_PRODUCT_CODE,
								CAST((CASE WHEN NULLIF(CHARINDEX ('@',A.PRODUCT_CODE)-1,-1) IS NULL THEN 0 ELSE RIGHT(A.PRODUCT_CODE, LEN(A.PRODUCT_CODE )-CHARINDEX ('@',A.PRODUCT_CODE)) END) AS NUMERIC) AS BATCH_NO,
                                A.product_code,'" + LoggedLocation + @"' DEPT_ID,'" + 000 + @"' BIN_ID,A.MRP
                                ,A.article_code,A.para1_code,A.para2_code,A.para3_code,A.para4_code,A.para5_code,A.para6_code,A.para7_code
                                ,C1.section_name,C1.sub_section_name, C1.article_no,C1.para1_name,C1.para2_name,C1.para3_name,C1.para4_name,C1.para5_name,C1.para6_name,C1.para7_name
                                ,cast(0 as NUMERIC(14,3)) AS allocatedQty,cast(" + nAllowNegativeStockQuantity.ToString() + @" as NUMERIC(14,3)) AS  quantity_in_stock
                                ,A.HSN_CODE
								FROM SKU A (NOLOCK)
                                JOIN sku_names C1 (NOLOCK) ON C1.PRODUCT_CODE=A.PRODUCT_CODE
                                JOIN #WSLDTSAVE D1 ON D1.PRODUCT_CODE=LEFT(A.PRODUCT_CODE, ISNULL(NULLIF(CHARINDEX ('@',A.PRODUCT_CODE)-1,-1),LEN(A.PRODUCT_CODE ))) 
								LEFT OUTER JOIN PMT01106 C (NOLOCK) ON C.PRODUCT_CODE=A.PRODUCT_CODE AND C.DEPT_ID='" + LoggedLocation + @"'" +
                                (LoggedUserCode != "0000000" ? " AND C.bin_id IN(" + _cUserForBINs + @") " : "") +
                                @" LEFT OUTER JOIN STK D on D.product_code=A.product_code
								where C.product_code IS NULL AND D.product_code IS NULL 
                                "
                                : "") + " ORDER BY 1,2,3";
                    //System.IO.File.WriteAllText(+ "\\logs\\NormalizationQuery.txt", cStrQuery);
                    if (dset.Tables.Contains("tNormalizeItems"))
                        dset.Tables["tNormalizeItems"].Clear();
                    cmd.CommandText = cStrQuery;
                    sda = new SqlDataAdapter(cmd);
                    sda.Fill(dset, "tNormalizeItems");
                    //if (clsCommon.SelectCmdToSql(dset, cStrQuery, "tNormalizeItems")=="")
                    {
                        if (dset.Tables.Contains("tNormalizeItems"))
                        {
                            if (dset.Tables["tNormalizeItems"].Rows.Count > 0)
                            {
                                foreach (DataRow drowdtSave in dtSave.Rows)
                                {
                                    Decimal nQtyScanned = clsCommon.ConvertDecimal(drowdtSave["quantity"]);
                                    Decimal nDiscAmount = clsCommon.ConvertDecimal(drowdtSave["BASIC_DISCOUNT_AMOUNT"]);
                                    Decimal nDiscAmount_PerQty = 0, nDiscAmount_Alloted = 0;
                                    Decimal nCardDiscAmount = clsCommon.ConvertDecimal(drowdtSave["CARD_DISCOUNT_AMOUNT"]);
                                    Decimal nCardDiscAmount_PerQty = 0, nCardDiscAmount_Alloted = 0;
                                    Decimal nManualDiscAmount = clsCommon.ConvertDecimal(drowdtSave["MANUAL_DISCOUNT_AMOUNT"]);
                                    Decimal nManualDiscAmount_PerQty = 0, nManualDiscAmount_Alloted = 0;
                                    if (nDiscAmount > 0) nDiscAmount_PerQty = Math.Round(clsCommon.ConvertDecimal(nDiscAmount) / nQtyScanned);
                                    if (nCardDiscAmount > 0) nCardDiscAmount_PerQty = Math.Round(clsCommon.ConvertDecimal(nCardDiscAmount) / nQtyScanned);
                                    if (nManualDiscAmount > 0) nManualDiscAmount_PerQty = Math.Round(clsCommon.ConvertDecimal(nManualDiscAmount) / nQtyScanned);

                                    try
                                    {
                                        DataRow[] drowNormalizeItems = dset.Tables["tNormalizeItems"].Select("BASE_PRODUCT_CODE = '" + Convert.ToString(drowdtSave["product_code"]) + "' AND BIN_ID = '" + Convert.ToString(drowdtSave["BIN_ID"]) + "' AND MRP=" + clsCommon.ConvertDecimal(drowdtSave["MRP"]) + " AND quantity_in_stock>allocatedqty", "SRNO DESC");
                                        if (drowNormalizeItems.Length > 0)
                                        {
                                            DataTable dtPMT = drowNormalizeItems.CopyToDataTable();
                                            Decimal nQtyInstock = clsCommon.ConvertDecimal(dtPMT.Compute("SUM(quantity_in_stock)", ""));
                                            Decimal nQtyAllocated = clsCommon.ConvertDecimal(dtPMT.Compute("SUM(allocatedQty)", ""));
                                            {
                                                foreach (DataRow drowPMT in drowNormalizeItems)
                                                {
                                                    nQtyInstock = clsCommon.ConvertDecimal(drowPMT["quantity_in_stock"]);
                                                    nQtyAllocated = clsCommon.ConvertDecimal(drowPMT["allocatedQty"]);
                                                    if ((nQtyInstock - nQtyAllocated) > 0)
                                                    {
                                                        if ((nQtyInstock - nQtyAllocated) >= nQtyScanned)
                                                        {
                                                            DataRow drowNew = dtSave_UPLOAD.NewRow();
                                                            drowNew.ItemArray = drowdtSave.ItemArray;
                                                            drowNew["HSN_CODE"] = Convert.ToString(drowPMT["HSN_CODE"]);
                                                            drowNew["MRP"] = Convert.ToString(drowPMT["MRP"]);
                                                            drowNew["bin_id"] = Convert.ToString(drowPMT["bin_id"]);
                                                            drowNew["row_id"] = GetNewRowID(LoggedLocation);
                                                            drowNew["product_code"] = Convert.ToString(drowPMT["product_code"]);
                                                            drowNew["org_product_code"] = Convert.ToString(drowPMT["product_code"]);
                                                            drowNew["Quantity"] = nQtyScanned;
                                                            if (clsCommon.ConvertInt(drowNew["UOM_TYPE"]) == 1)
                                                            {
                                                                drowNew["pcs_Quantity"] = nQtyScanned;
                                                                drowNew["mtr_Quantity"] = 1;
                                                            }
                                                            else
                                                            {
                                                                drowNew["pcs_Quantity"] = 1;
                                                                drowNew["mtr_Quantity"] = nQtyScanned;
                                                            }
                                                            drowNew["basic_discount_amount"] = nDiscAmount;
                                                            drowNew["card_discount_amount"] = nCardDiscAmount;
                                                            drowNew["manual_discount_amount"] = nManualDiscAmount;
                                                            drowNew["discount_amount"] = (nDiscAmount + nCardDiscAmount + nManualDiscAmount);
                                                            drowNew["NET"] = (clsCommon.ConvertDecimal(drowNew["QUANTITY"]) * clsCommon.ConvertDecimal(drowNew["MRP"])) - clsCommon.ConvertDecimal(drowNew["discount_amount"]);
                                                            drowNew["row_id"] = GetNewRowID(LoggedLocation);
                                                            if (drowNew.Table.Columns.Contains("section_name")) drowNew["section_name"] = drowPMT["section_name"];
                                                            if (drowNew.Table.Columns.Contains("sub_section_name")) drowNew["sub_section_name"] = drowPMT["sub_section_name"];
                                                            if (drowNew.Table.Columns.Contains("article_no")) drowNew["article_no"] = drowPMT["article_no"];
                                                            if (drowNew.Table.Columns.Contains("para1_name")) drowNew["para1_name"] = drowPMT["para1_name"];
                                                            if (drowNew.Table.Columns.Contains("para2_name")) drowNew["para2_name"] = drowPMT["para2_name"];
                                                            if (drowNew.Table.Columns.Contains("para3_name")) drowNew["para3_name"] = drowPMT["para3_name"];
                                                            if (drowNew.Table.Columns.Contains("para4_name")) drowNew["para4_name"] = drowPMT["para4_name"];
                                                            if (drowNew.Table.Columns.Contains("para5_name")) drowNew["para5_name"] = drowPMT["para5_name"];
                                                            if (drowNew.Table.Columns.Contains("para6_name")) drowNew["para6_name"] = drowPMT["para6_name"];
                                                            if (drowNew.Table.Columns.Contains("para7_name")) drowNew["para7_name"] = drowPMT["para7_name"];
                                                            if (drowNew.Table.Columns.Contains("article_code")) drowNew["article_code"] = drowPMT["article_code"];
                                                            if (drowNew.Table.Columns.Contains("para1_code")) drowNew["para1_code"] = drowPMT["para1_code"];
                                                            if (drowNew.Table.Columns.Contains("para2_code")) drowNew["para2_code"] = drowPMT["para2_code"];
                                                            if (drowNew.Table.Columns.Contains("para3_code")) drowNew["para3_code"] = drowPMT["para3_code"];
                                                            if (drowNew.Table.Columns.Contains("para4_code")) drowNew["para4_code"] = drowPMT["para4_code"];
                                                            if (drowNew.Table.Columns.Contains("para5_code")) drowNew["para5_code"] = drowPMT["para5_code"];
                                                            if (drowNew.Table.Columns.Contains("para6_code")) drowNew["para6_code"] = drowPMT["para6_code"];
                                                            if (drowNew.Table.Columns.Contains("para7_code")) drowNew["para7_code"] = drowPMT["para7_code"];
                                                            dtSave_UPLOAD.Rows.Add(drowNew);
                                                            drowPMT["allocatedQty"] = clsCommon.ConvertDecimal(drowPMT["allocatedQty"]) + nQtyScanned;
                                                            nQtyScanned = 0;
                                                            nDiscAmount = 0;
                                                            break;
                                                        }
                                                        else
                                                        {
                                                            nDiscAmount_Alloted = (nQtyInstock - nQtyAllocated) * nDiscAmount_PerQty;
                                                            nCardDiscAmount_Alloted = (nQtyInstock - nQtyAllocated) * nCardDiscAmount_PerQty;
                                                            nManualDiscAmount_Alloted = (nQtyInstock - nQtyAllocated) * nManualDiscAmount_PerQty;
                                                            DataRow drowNew = dtSave_UPLOAD.NewRow();
                                                            drowNew.ItemArray = drowdtSave.ItemArray;
                                                            drowNew["HSN_CODE"] = Convert.ToString(drowPMT["HSN_CODE"]);
                                                            drowNew["MRP"] = Convert.ToString(drowPMT["MRP"]);
                                                            drowNew["bin_id"] = Convert.ToString(drowPMT["bin_id"]);
                                                            drowNew["row_id"] = GetNewRowID(LoggedLocation);
                                                            drowNew["product_code"] = Convert.ToString(drowPMT["product_code"]);
                                                            drowNew["org_product_code"] = Convert.ToString(drowPMT["product_code"]);
                                                            drowNew["Quantity"] = (nQtyInstock - nQtyAllocated);
                                                            if (clsCommon.ConvertInt(drowNew["UOM_TYPE"]) == 1)
                                                            {
                                                                drowNew["pcs_Quantity"] = (nQtyInstock - nQtyAllocated);
                                                                drowNew["mtr_Quantity"] = 1;
                                                            }
                                                            else
                                                            {
                                                                drowNew["pcs_Quantity"] = 1;
                                                                drowNew["mtr_Quantity"] = (nQtyInstock - nQtyAllocated);
                                                            }
                                                            drowNew["basic_discount_amount"] = nDiscAmount_Alloted;
                                                            drowNew["card_discount_amount"] = nCardDiscAmount_Alloted;
                                                            drowNew["manual_discount_amount"] = nManualDiscAmount_Alloted;
                                                            drowNew["discount_amount"] = (nDiscAmount_Alloted + nCardDiscAmount_Alloted + nManualDiscAmount_Alloted);
                                                            drowNew["NET"] = (clsCommon.ConvertDecimal(drowNew["QUANTITY"]) * clsCommon.ConvertDecimal(drowNew["MRP"])) - clsCommon.ConvertDecimal(drowNew["discount_amount"]);
                                                            if (drowNew.Table.Columns.Contains("section_name")) drowNew["section_name"] = drowPMT["section_name"];
                                                            if (drowNew.Table.Columns.Contains("sub_section_name")) drowNew["sub_section_name"] = drowPMT["sub_section_name"];
                                                            if (drowNew.Table.Columns.Contains("article_no")) drowNew["article_no"] = drowPMT["article_no"];
                                                            if (drowNew.Table.Columns.Contains("para1_name")) drowNew["para1_name"] = drowPMT["para1_name"];
                                                            if (drowNew.Table.Columns.Contains("para2_name")) drowNew["para2_name"] = drowPMT["para2_name"];
                                                            if (drowNew.Table.Columns.Contains("para3_name")) drowNew["para3_name"] = drowPMT["para3_name"];
                                                            if (drowNew.Table.Columns.Contains("para4_name")) drowNew["para4_name"] = drowPMT["para4_name"];
                                                            if (drowNew.Table.Columns.Contains("para5_name")) drowNew["para5_name"] = drowPMT["para5_name"];
                                                            if (drowNew.Table.Columns.Contains("para6_name")) drowNew["para6_name"] = drowPMT["para6_name"];
                                                            if (drowNew.Table.Columns.Contains("para7_name")) drowNew["para7_name"] = drowPMT["para7_name"];
                                                            if (drowNew.Table.Columns.Contains("article_code")) drowNew["article_code"] = drowPMT["article_code"];
                                                            if (drowNew.Table.Columns.Contains("para1_code")) drowNew["para1_code"] = drowPMT["para1_code"];
                                                            if (drowNew.Table.Columns.Contains("para2_code")) drowNew["para2_code"] = drowPMT["para2_code"];
                                                            if (drowNew.Table.Columns.Contains("para3_code")) drowNew["para3_code"] = drowPMT["para3_code"];
                                                            if (drowNew.Table.Columns.Contains("para4_code")) drowNew["para4_code"] = drowPMT["para4_code"];
                                                            if (drowNew.Table.Columns.Contains("para5_code")) drowNew["para5_code"] = drowPMT["para5_code"];
                                                            if (drowNew.Table.Columns.Contains("para6_code")) drowNew["para6_code"] = drowPMT["para6_code"];
                                                            if (drowNew.Table.Columns.Contains("para7_code")) drowNew["para7_code"] = drowPMT["para7_code"];
                                                            dtSave_UPLOAD.Rows.Add(drowNew);
                                                            drowPMT["allocatedQty"] = clsCommon.ConvertDecimal(drowPMT["allocatedQty"]) + (nQtyInstock - nQtyAllocated);
                                                            nQtyScanned = nQtyScanned - (nQtyInstock - nQtyAllocated);
                                                            nDiscAmount = nDiscAmount - nDiscAmount_Alloted;
                                                            nCardDiscAmount = nCardDiscAmount - nCardDiscAmount_Alloted;
                                                            nManualDiscAmount = nManualDiscAmount - nManualDiscAmount_Alloted;
                                                        }


                                                    }

                                                }

                                            }
                                        }
                                        if (nQtyScanned > 0)
                                        {
                                            drowNormalizeItems = dset.Tables["tNormalizeItems"].Select("BASE_PRODUCT_CODE = '" + Convert.ToString(drowdtSave["product_code"]) + "' AND BIN_ID <> '" + Convert.ToString(drowdtSave["BIN_ID"]) + "' AND MRP=" + clsCommon.ConvertDecimal(drowdtSave["MRP"]) + " AND quantity_in_stock>allocatedqty", "SRNO DESC");
                                            if (drowNormalizeItems.Length > 0)
                                            {
                                                DataTable dtPMT = drowNormalizeItems.CopyToDataTable();
                                                Decimal nQtyInstock = clsCommon.ConvertDecimal(dtPMT.Compute("SUM(quantity_in_stock)", ""));
                                                Decimal nQtyAllocated = clsCommon.ConvertDecimal(dtPMT.Compute("SUM(allocatedQty)", ""));

                                                {
                                                    foreach (DataRow drowPMT in drowNormalizeItems)
                                                    {
                                                        nQtyInstock = clsCommon.ConvertDecimal(drowPMT["quantity_in_stock"]);
                                                        nQtyAllocated = clsCommon.ConvertDecimal(drowPMT["allocatedQty"]);
                                                        if ((nQtyInstock - nQtyAllocated) > 0)
                                                        {
                                                            if ((nQtyInstock - nQtyAllocated) >= nQtyScanned)
                                                            {
                                                                DataRow drowNew = dtSave_UPLOAD.NewRow();
                                                                drowNew.ItemArray = drowdtSave.ItemArray;
                                                                drowNew["HSN_CODE"] = Convert.ToString(drowPMT["HSN_CODE"]);
                                                                drowNew["MRP"] = Convert.ToString(drowPMT["MRP"]);
                                                                drowNew["bin_id"] = Convert.ToString(drowPMT["bin_id"]);
                                                                drowNew["row_id"] = GetNewRowID(LoggedLocation);
                                                                drowNew["product_code"] = Convert.ToString(drowPMT["product_code"]);
                                                                drowNew["org_product_code"] = Convert.ToString(drowPMT["product_code"]);
                                                                drowNew["Quantity"] = nQtyScanned;
                                                                if (clsCommon.ConvertInt(drowNew["UOM_TYPE"]) == 1)
                                                                {
                                                                    drowNew["pcs_Quantity"] = nQtyScanned;
                                                                    drowNew["mtr_Quantity"] = 1;
                                                                }
                                                                else
                                                                {
                                                                    drowNew["pcs_Quantity"] = 1;
                                                                    drowNew["mtr_Quantity"] = nQtyScanned;
                                                                }
                                                                drowNew["basic_discount_amount"] = nDiscAmount;
                                                                drowNew["card_discount_amount"] = nCardDiscAmount;
                                                                drowNew["manual_discount_amount"] = nManualDiscAmount;
                                                                drowNew["discount_amount"] = (nDiscAmount + nCardDiscAmount + nManualDiscAmount);
                                                                drowNew["NET"] = (clsCommon.ConvertDecimal(drowNew["QUANTITY"]) * clsCommon.ConvertDecimal(drowNew["MRP"])) - clsCommon.ConvertDecimal(drowNew["discount_amount"]);
                                                                if (drowNew.Table.Columns.Contains("section_name")) drowNew["section_name"] = drowPMT["section_name"];
                                                                if (drowNew.Table.Columns.Contains("sub_section_name")) drowNew["sub_section_name"] = drowPMT["sub_section_name"];
                                                                if (drowNew.Table.Columns.Contains("article_no")) drowNew["article_no"] = drowPMT["article_no"];
                                                                if (drowNew.Table.Columns.Contains("para1_name")) drowNew["para1_name"] = drowPMT["para1_name"];
                                                                if (drowNew.Table.Columns.Contains("para2_name")) drowNew["para2_name"] = drowPMT["para2_name"];
                                                                if (drowNew.Table.Columns.Contains("para3_name")) drowNew["para3_name"] = drowPMT["para3_name"];
                                                                if (drowNew.Table.Columns.Contains("para4_name")) drowNew["para4_name"] = drowPMT["para4_name"];
                                                                if (drowNew.Table.Columns.Contains("para5_name")) drowNew["para5_name"] = drowPMT["para5_name"];
                                                                if (drowNew.Table.Columns.Contains("para6_name")) drowNew["para6_name"] = drowPMT["para6_name"];
                                                                if (drowNew.Table.Columns.Contains("para7_name")) drowNew["para7_name"] = drowPMT["para7_name"];
                                                                if (drowNew.Table.Columns.Contains("article_code")) drowNew["article_code"] = drowPMT["article_code"];
                                                                if (drowNew.Table.Columns.Contains("para1_code")) drowNew["para1_code"] = drowPMT["para1_code"];
                                                                if (drowNew.Table.Columns.Contains("para2_code")) drowNew["para2_code"] = drowPMT["para2_code"];
                                                                if (drowNew.Table.Columns.Contains("para3_code")) drowNew["para3_code"] = drowPMT["para3_code"];
                                                                if (drowNew.Table.Columns.Contains("para4_code")) drowNew["para4_code"] = drowPMT["para4_code"];
                                                                if (drowNew.Table.Columns.Contains("para5_code")) drowNew["para5_code"] = drowPMT["para5_code"];
                                                                if (drowNew.Table.Columns.Contains("para6_code")) drowNew["para6_code"] = drowPMT["para6_code"];
                                                                if (drowNew.Table.Columns.Contains("para7_code")) drowNew["para7_code"] = drowPMT["para7_code"];
                                                                dtSave_UPLOAD.Rows.Add(drowNew);
                                                                drowPMT["allocatedQty"] = clsCommon.ConvertDecimal(drowPMT["allocatedQty"]) + nQtyScanned;
                                                                nQtyScanned = nDiscAmount = nCardDiscAmount = nManualDiscAmount = 0;
                                                                break;
                                                            }
                                                            else
                                                            {
                                                                nDiscAmount_Alloted = (nQtyInstock - nQtyAllocated) * nDiscAmount_PerQty;
                                                                nCardDiscAmount_Alloted = (nQtyInstock - nQtyAllocated) * nCardDiscAmount_PerQty;
                                                                nManualDiscAmount_Alloted = (nQtyInstock - nQtyAllocated) * nManualDiscAmount_PerQty;
                                                                DataRow drowNew = dtSave_UPLOAD.NewRow();
                                                                drowNew.ItemArray = drowdtSave.ItemArray;
                                                                drowNew["HSN_CODE"] = Convert.ToString(drowPMT["HSN_CODE"]);
                                                                drowNew["MRP"] = Convert.ToString(drowPMT["MRP"]);
                                                                drowNew["bin_id"] = Convert.ToString(drowPMT["bin_id"]);
                                                                drowNew["row_id"] = GetNewRowID(LoggedLocation);
                                                                drowNew["product_code"] = Convert.ToString(drowPMT["product_code"]);
                                                                drowNew["org_product_code"] = Convert.ToString(drowPMT["product_code"]);
                                                                drowNew["Quantity"] = (nQtyInstock - nQtyAllocated);
                                                                if (clsCommon.ConvertInt(drowNew["UOM_TYPE"]) == 1)
                                                                {
                                                                    drowNew["pcs_Quantity"] = (nQtyInstock - nQtyAllocated);
                                                                    drowNew["mtr_Quantity"] = 1;
                                                                }
                                                                else
                                                                {
                                                                    drowNew["pcs_Quantity"] = 1;
                                                                    drowNew["mtr_Quantity"] = (nQtyInstock - nQtyAllocated);
                                                                }
                                                                drowNew["basic_discount_amount"] = nDiscAmount_Alloted;
                                                                drowNew["card_discount_amount"] = nCardDiscAmount_Alloted;
                                                                drowNew["manual_discount_amount"] = nManualDiscAmount_Alloted;
                                                                drowNew["discount_amount"] = (nDiscAmount_Alloted + nCardDiscAmount_Alloted + nManualDiscAmount_Alloted);
                                                                drowNew["NET"] = (clsCommon.ConvertDecimal(drowNew["QUANTITY"]) * clsCommon.ConvertDecimal(drowNew["MRP"])) - clsCommon.ConvertDecimal(drowNew["discount_amount"]);
                                                                if (drowNew.Table.Columns.Contains("section_name")) drowNew["section_name"] = drowPMT["section_name"];
                                                                if (drowNew.Table.Columns.Contains("sub_section_name")) drowNew["sub_section_name"] = drowPMT["sub_section_name"];
                                                                if (drowNew.Table.Columns.Contains("article_no")) drowNew["article_no"] = drowPMT["article_no"];
                                                                if (drowNew.Table.Columns.Contains("para1_name")) drowNew["para1_name"] = drowPMT["para1_name"];
                                                                if (drowNew.Table.Columns.Contains("para2_name")) drowNew["para2_name"] = drowPMT["para2_name"];
                                                                if (drowNew.Table.Columns.Contains("para3_name")) drowNew["para3_name"] = drowPMT["para3_name"];
                                                                if (drowNew.Table.Columns.Contains("para4_name")) drowNew["para4_name"] = drowPMT["para4_name"];
                                                                if (drowNew.Table.Columns.Contains("para5_name")) drowNew["para5_name"] = drowPMT["para5_name"];
                                                                if (drowNew.Table.Columns.Contains("para6_name")) drowNew["para6_name"] = drowPMT["para6_name"];
                                                                if (drowNew.Table.Columns.Contains("para7_name")) drowNew["para7_name"] = drowPMT["para7_name"];
                                                                if (drowNew.Table.Columns.Contains("article_code")) drowNew["article_code"] = drowPMT["article_code"];
                                                                if (drowNew.Table.Columns.Contains("para1_code")) drowNew["para1_code"] = drowPMT["para1_code"];
                                                                if (drowNew.Table.Columns.Contains("para2_code")) drowNew["para2_code"] = drowPMT["para2_code"];
                                                                if (drowNew.Table.Columns.Contains("para3_code")) drowNew["para3_code"] = drowPMT["para3_code"];
                                                                if (drowNew.Table.Columns.Contains("para4_code")) drowNew["para4_code"] = drowPMT["para4_code"];
                                                                if (drowNew.Table.Columns.Contains("para5_code")) drowNew["para5_code"] = drowPMT["para5_code"];
                                                                if (drowNew.Table.Columns.Contains("para6_code")) drowNew["para6_code"] = drowPMT["para6_code"];
                                                                if (drowNew.Table.Columns.Contains("para7_code")) drowNew["para7_code"] = drowPMT["para7_code"];
                                                                dtSave_UPLOAD.Rows.Add(drowNew);
                                                                drowPMT["allocatedQty"] = clsCommon.ConvertDecimal(drowPMT["allocatedQty"]) + (nQtyInstock - nQtyAllocated);
                                                                nQtyScanned = nQtyScanned - (nQtyInstock - nQtyAllocated);
                                                                nDiscAmount = nDiscAmount - nDiscAmount_Alloted;
                                                                nCardDiscAmount = nCardDiscAmount - nCardDiscAmount_Alloted;
                                                                nManualDiscAmount = nManualDiscAmount - nManualDiscAmount_Alloted;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        if (nQtyScanned > 0)
                                        {
                                            // MessageBox.Show("Error in Normalization : PRODUCT_CODE = '" + Convert.ToString(drowdtSave["product_code"]) + "' WITH MRP = " + clsCommon.ConvertDecimal(drowdtSave["MRP"]) + " not normalized properly. Quantity" + nQtyScanned + " remain not allocated.", Application.ProductName, MessageBoxButtons.OK, MessageBoxIcon.Warning);
                                            return "Error in Normalization : PRODUCT_CODE = '" + Convert.ToString(drowdtSave["product_code"]) + "' WITH MRP = " + clsCommon.ConvertDecimal(drowdtSave["MRP"]) + " not normalized properly. Quantity" + nQtyScanned + " remain not allocated.";
                                        }
                                    }
                                    catch (Exception EX)
                                    {
                                        //MessageBox.Show("Error in Normalization : " + EX.Message, Application.ProductName, MessageBoxButtons.OK, MessageBoxIcon.Warning);
                                        return "Error in Normalization : " + EX.Message;
                                    }
                                }
                            }
                            else
                            {
                                //MessageBox.Show("Error in Normalization : Batch Barcode not found.", Application.ProductName, MessageBoxButtons.OK, MessageBoxIcon.Warning);
                                return "Error in Normalization : Batch Barcode not found.";
                            }
                        }
                        else
                        {
                            //MessageBox.Show("Error in Normalization : Cursor Not Returned by Query.", Application.ProductName, MessageBoxButtons.OK, MessageBoxIcon.Warning);
                            return "Error in Normalization : Cursor Not Returned by Query.";
                        }
                    }

                    foreach (DataRow drowAdd in dtSave_UPLOAD.Rows)
                    {
                        DataRow drNew1 = tINVDET.NewRow();
                        drNew1.ItemArray = drowAdd.ItemArray;
                        //drNew1["quantity"] = 1;
                        tINVDET.Rows.Add(drNew1);
                    }


                }
                lRetVal = "";
                iCountTotalAfterAllocation = tINVDET.Rows.Count;
                if (iCountTotalAfterAllocation < iCountTotalBeforeAllocation)
                {
                    lRetVal = "Batch Allocation is not done properly...";
                }
            }
            catch (Exception ex)
            {
                lRetVal = nameof(AllocateBatchBarcode) + " : " + ex.Message;
            }
            finally
            {
                if (con.State != ConnectionState.Closed)
                    con.Close();

            }
            return lRetVal;
        }
        
        public String SavePackSlip(Int32 iMode, String cMemoID, String cConStr, String LoggedLocation, String LoggedUserCode, String LoggedUserAlias, DataTable tCM_MST, DataTable tCM_DET, out DataTable dtResult)
        {
            String cRetVal = "";
            dtResult = new DataTable("TDATA");
            dtResult.Columns.Add("ERRMSG", typeof(System.String));
            dtResult.Columns.Add("MEMO_ID", typeof(System.String));
            if (iMode == 1)
            {
                cRetVal = AddRPS(cConStr, LoggedLocation, LoggedUserCode, LoggedUserAlias, tCM_MST, tCM_DET, out dtResult);
            }
            else if (iMode == 2)
            {
                cRetVal = UpdateRPS(cConStr, LoggedLocation, LoggedUserCode, LoggedUserAlias, tCM_MST, tCM_DET, out dtResult);
            }
            else if (iMode == 3)
            {
                cRetVal = CancelRPS(cConStr, LoggedLocation, LoggedUserCode, LoggedUserAlias, cMemoID, out dtResult);
            }
            return cRetVal;
        }

        public String AddRPS(String cConStr, String LoggedLocation, String LoggedUserCode, String LoggedUserAlias, DataTable tCM_MST, DataTable tCM_DET, out DataTable dtResult)
        {
            dtResult = new DataTable("TDATA");
            dtResult.Columns.Add("ERRMSG", typeof(System.String));
            dtResult.Columns.Add("MEMO_ID", typeof(System.String));

            try
            {
                //String cErr = "";
                //DataTable DtM = new DataTable();

                if (string.IsNullOrEmpty(cConStr))
                    return BadRequest("Connection String Not Found");

                if (string.IsNullOrEmpty(LoggedLocation))
                    return BadRequest("Logged Location ID should not be Empty");

                if (string.IsNullOrEmpty(LoggedUserCode))
                    return BadRequest("Logged User Code should not be Empty");

                DataSet dset = new DataSet();

                String result = "";

                //string serializedObject = Newtonsoft.Json.JsonConvert.SerializeObject(Body, Newtonsoft.Json.Formatting.Indented);

                //CMM cmm = Newtonsoft.Json.JsonConvert.DeserializeObject<CMM>(serializedObject);

                //commonMethods globalMethods = new commonMethods();

                //APIBaseClass clsBase = new APIBaseClass(cConStr);

                APIBaseClass globalMethods = new APIBaseClass(cConStr);

                result = "";
                using (SqlConnection con = new SqlConnection(cConStr))
                {
                    SqlCommand cmd = new SqlCommand();
                    SqlDataAdapter sda = new SqlDataAdapter();


                    //DataSet dset = new DataSet();
                    String cQueryStr = @"";

                    {
                        if (!tCM_DET.Columns.Contains("SrNo"))
                        {
                            tCM_DET.Columns.Add("SrNo", typeof(System.Int32));
                        }
                        //if (iTemTytpe == 1)
                        {
                            DataTable dtSave = tCM_DET.Clone();
                            DataTable dtSave_UPLOAD = tCM_DET.Clone();
                            StringBuilder sb = new StringBuilder();
                            StringBuilder sbPC = new StringBuilder();

                            String cStrRowID = sb.ToString().TrimEnd(',');
                            String cStrProductCode = sbPC.ToString().TrimEnd(',');

                        }
                    }

                    StringBuilder sbInsertUniqueBarcode = new StringBuilder();
                    StringBuilder sbInsertDetValues = new StringBuilder();

                    if (con.State == ConnectionState.Closed)
                        con.Open();
                    SqlTransaction sqlTran = con.BeginTransaction();
                    cmd = new SqlCommand();
                    cmd.Connection = con;
                    cmd.Transaction = sqlTran;

                    result = "";

                    String cMemoNo = "", cPrefix = "";
                    DateTime dtCMDT = globalMethods.ConvertDateTime(tCM_MST.Rows[0]["cm_dt"]);
                    String cFinYear = (dtCMDT.Month >= 4 && dtCMDT.Month <= 12 ? dtCMDT.AddYears(1).Year.ToString().Substring(2) : dtCMDT.Year.ToString().Substring(2));
                    //CUSERALIAS = Convert.ToString(dtUserSetting.Rows[0]["user_alias"]);
                    String CKEYSTABLE = "KEYS_CMM";
                    cPrefix = LoggedLocation + LoggedUserAlias;


                    cPrefix = cPrefix + "-";
                    try
                    {
                        do
                        {
                            cQueryStr = @"
                                DECLARE @CMEMONOVAL VARCHAR(20)
                                EXEC GETNEXTKEY_OPT @CTABLENAME='RPS_MST', @CCOLNAME='CM_NO',@NWIDTH= 12, @CPREFIX='" + cPrefix + @"', @NLZEROS=1,@CFINYEAR='011" + cFinYear + @"' ,@NROWCOUNT=0,@CKEYSTABLE='" + CKEYSTABLE + @"',@CNEWKEYVAL= @CMEMONOVAL OUTPUT
                                SELECT @CMEMONOVAL AS NewMemoNo
                                ";
                            cmd.CommandText = cQueryStr;
                            cmd.CommandType = CommandType.Text;
                            cMemoNo = Convert.ToString(cmd.ExecuteScalar());
                            if (!String.IsNullOrEmpty(cMemoNo))
                            {
                                cQueryStr = @"SELECT COUNT(*) FROM RPS_MST(NOLOCK) 
					                WHERE CM_NO='" + cMemoNo + @"' AND FIN_YEAR = '011" + cFinYear + @"' ";

                                cmd.CommandText = cQueryStr;
                                cmd.CommandType = CommandType.Text;
                                if (globalMethods.ConvertInt(cmd.ExecuteScalar()) > 0)
                                    cMemoNo = "";
                            }

                        } while (String.IsNullOrEmpty(cMemoNo));
                        tCM_MST.Rows[0]["CM_NO"] = cMemoNo;
                        String cMemoID = LoggedLocation + "011" + cFinYear + (new String('0', 15 - cMemoNo.Length)) + cMemoNo;
                        tCM_MST.Rows[0]["CM_ID"] = cMemoID;
                        tCM_MST.Rows[0]["last_update"] = DateTime.Now;
                        tCM_MST.Rows[0]["bin_id"] = "000";
                        tCM_MST.Rows[0]["fin_year"] = "011" + cFinYear;
                        tCM_MST.Rows[0]["user_code"] = LoggedUserCode;

                        sbInsertDetValues = new StringBuilder();
                        StringBuilder sbInsertMstValues = new StringBuilder();
                        foreach (DataColumn dcol in tCM_MST.Columns)
                        {
                            Object objVal = tCM_MST.Rows[0][dcol.ColumnName];
                            if (sbInsertMstValues.Length > 0)
                            {
                                sbInsertMstValues.Append(",");
                                sbInsertDetValues.Append(",");
                            }
                            sbInsertMstValues.Append(dcol.ColumnName);
                            sbInsertDetValues.Append(ColTypeValue(dcol, objVal));
                        }
                        //if (sbInsertDetValues.Length > 0)
                        {
                            //cQueryStr = @"INSERT INTO  TCMM01106(" + sbInsertMstValues.ToString() + @" )
                            //        VALUES(" + sbInsertDetValues + @")";

                            //cmd.CommandText = cQueryStr;
                            //cmd.CommandType = CommandType.Text;
                            //cmd.ExecuteNonQuery();
                            tCM_DET.Select("").ToList<DataRow>().ForEach(r => r["DEPT_ID"] = LoggedLocation);
                            tCM_DET.Select("").ToList<DataRow>().ForEach(r => r["CM_ID"] = cMemoID);
                            tCM_DET.Select("").ToList<DataRow>().ForEach(r => r["ROW_ID"] = GetNewRowID(LoggedLocation));

                        }
                        ChangeDBNull(tCM_MST, "RPS_MST");
                        ChangeDBNull(tCM_DET, "RPS_DET");
                        var options = SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.CheckConstraints;

                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(con, options, sqlTran))
                        {
                            bulkCopy.BulkCopyTimeout = 50000;
                            bulkCopy.BatchSize = 5000;
                            bulkCopy.DestinationTableName = "RPS_MST";
                            globalMethods.SelectCmdToSql(dset, "SELECT * FROM RPS_MST(NOLOCK) WHERE 1=2", "TCURSOR_CMM");
                            foreach (DataColumn dcol in dset.Tables["TCURSOR_CMM"].Columns)
                                bulkCopy.ColumnMappings.Add(dcol.ColumnName, dcol.ColumnName);

                            try
                            {
                                bulkCopy.WriteToServer(tCM_MST);
                            }
                            catch (Exception ex)
                            {
                                sqlTran.Rollback();
                                dtResult.Rows.Add((new String[] { "Error! Record Not Updated SQL Error : " + ex.Message.ToString(), "" }));
                                return BadRequest("Error! Record Not Updated SQL Error : " + ex.Message.ToString());
                            }
                            finally
                            {

                            }
                        }

                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(con, options, sqlTran))
                        {
                            bulkCopy.BulkCopyTimeout = 50000;
                            bulkCopy.BatchSize = 5000;
                            bulkCopy.DestinationTableName = "RPS_DET";
                            globalMethods.SelectCmdToSql(dset, "SELECT * FROM RPS_DET(NOLOCK) WHERE 1=2", "TCURSOR_CMD");
                            foreach (DataColumn dcol in dset.Tables["TCURSOR_CMD"].Columns)
                                bulkCopy.ColumnMappings.Add(dcol.ColumnName, dcol.ColumnName);

                            try
                            {
                                bulkCopy.WriteToServer(tCM_DET);
                            }
                            catch (Exception ex)
                            {
                                sqlTran.Rollback();
                                dtResult.Rows.Add((new String[] { "Error! Record Not Updated SQL Error : " + ex.Message.ToString(), "" }));
                                return BadRequest("Error! Record Not Updated SQL Error : " + ex.Message.ToString());
                            }
                            finally
                            {

                            }
                        }



                        cQueryStr = @"
                                ;with det
                                AS
                                (
                                    SELECT A.PRODUCT_CODE,A.BIN_ID,C.location_code AS DEPT_ID,SUM(A.QUANTITY) AS QUANTITY 
                                    from RPS_DET A(NOLOCK) 
                                    JOIN RPS_MST C (NOLOCK) ON C.CM_ID=A.CM_ID
                                    WHERE C.cm_id='" + cMemoID + @"'
                                    GROUP BY A.PRODUCT_CODE,C.location_code,A.BIN_ID
                                )
                                UPDATE a SET a.quantity_in_stock=a.quantity_in_stock-b.QUANTITY 
        						from pmt01106 a WITH (ROWLOCK)
                                JOIN DET b ON B.PRODUCT_CODE=a.product_code and a.DEPT_ID=b.DEPT_ID AND b.BIN_ID=a.BIN_ID
                                ";
                        cmd.CommandText = cQueryStr;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();

                        sqlTran.Commit();

                        dtResult.Rows.Add((new String[] { "", cMemoID }));
                        return result = "";
                        //return Ok("Record Saved Sucessfully");

                    }
                    catch (Exception ex)
                    {
                        sqlTran.Rollback();
                        dtResult.Rows.Add((new String[] { "Error! " + ex.Message.ToString(), "" }));
                        return BadRequest("Error! " + ex.Message.ToString());
                    }
                }

                //return result;

            }
            catch (Exception ex)
            {
                dtResult.Rows.Add((new String[] { "Error! " + ex.Message.ToString(), "" }));
                return BadRequest(ex.Message.ToString());
            }
        }

        public String UpdateRPS(String cConStr, String LoggedLocation, String LoggedUserCode, String LoggedUserAlias, DataTable tCM_MST, DataTable tCM_DET, out DataTable dtResult)
        {
            dtResult = new DataTable("TDATA");
            dtResult.Columns.Add("ERRMSG", typeof(System.String));
            dtResult.Columns.Add("MEMO_ID", typeof(System.String));

            try
            {
                //String cErr = "";
                //DataTable DtM = new DataTable();

                if (string.IsNullOrEmpty(cConStr))
                    return BadRequest("Connection String Not Found");

                if (string.IsNullOrEmpty(LoggedLocation))
                    return BadRequest("Logged Location ID should not be Empty");

                if (string.IsNullOrEmpty(LoggedUserCode))
                    return BadRequest("Logged User Code should not be Empty");

                DataSet dset = new DataSet();

                String result = "";
                String cMemoID = "";

                //string serializedObject = Newtonsoft.Json.JsonConvert.SerializeObject(Body, Newtonsoft.Json.Formatting.Indented);

                //CMM cmm = Newtonsoft.Json.JsonConvert.DeserializeObject<CMM>(serializedObject);

                APIBaseClass globalMethods = new APIBaseClass(cConStr);

                //APIBaseClass clsBase = new APIBaseClass(cConStr);



                result = "";
                using (SqlConnection con = new SqlConnection(cConStr))
                {
                    SqlCommand cmd = new SqlCommand();
                    SqlDataAdapter sda = new SqlDataAdapter();


                    //DataSet dset = new DataSet();
                    String cQueryStr = @"";

                    cMemoID = Convert.ToString(tCM_MST.Rows[0]["CM_ID"]);

                    if (!tCM_DET.Columns.Contains("SrNo"))
                    {
                        tCM_DET.Columns.Add("SrNo", typeof(System.Int32));
                    }

                    //StringBuilder sbBarcode = new StringBuilder();
                    //foreach (DataRow drow in tCM_DET.Rows)
                    //{
                    //    if (sbBarcode.Length > 0) { sbBarcode.Append(","); }
                    //    sbBarcode.Append("'");
                    //    sbBarcode.Append(Convert.ToString(drow["product_code"]));
                    //    sbBarcode.Append("'");
                    //}



                    //StringBuilder sbInsertUniqueBarcode = new StringBuilder();
                    //StringBuilder sbInsertDetValues = new StringBuilder();


                    if (!_bAllowNegative)
                    {
                        cQueryStr = @"
                                ;with det
                                AS
                                (
                                    SELECT A.PRODUCT_CODE,A.BIN_ID,C.location_code AS DEPT_ID,SUM(A.QUANTITY) AS QUANTITY 
                                    from RPS_DET A(NOLOCK) 
                                    JOIN RPS_MST C (NOLOCK) ON C.CM_ID=A.CM_ID
                                    WHERE C.cm_id='" + cMemoID + @"'
                                    GROUP BY A.PRODUCT_CODE,C.location_code,A.BIN_ID
                                )
                                select a.product_code as productCode,a.quantity_in_stock - b.QUANTITY  as stockQuantity ,
                                b.QUANTITY as grnQuantity,'Stock is going negative.' as errMsg 
                                from pmt01106 a (NOLOCK)
                                JOIN DET b ON B.PRODUCT_CODE=a.product_code and a.DEPT_ID=b.DEPT_ID AND b.BIN_ID=a.BIN_ID
                                
                                ";

                        globalMethods.SelectCmdToSql(dset, cQueryStr, "tNegativeStock");
                        DataTable dtNegativeStock = dset.Tables["tNegativeStock"].Clone();
                        if (dset.Tables["tNegativeStock"].Rows.Count > 0)
                        {
                            DataRow[] drowNegative = dset.Tables["tNegativeStock"].Select("stockQuantity<0");
                            foreach (DataRow drowN in drowNegative)
                            {
                                DataRow[] drowDet = tCM_DET.Select("PRODUCT_CODE='" + Convert.ToString(drowN["PRODUCTCODE"]) + "'");
                                //if (drowDet.Length == 0)
                                //{
                                //    dtNegativeStock.Rows.Add(drowN.ItemArray);
                                //}
                                //else
                                 if (drowDet.Length > 0)
                                {
                                    Decimal nQty = globalMethods.ConvertDecimal(drowDet[0]["quantity"]);
                                    Decimal nStockQty = globalMethods.ConvertDecimal(drowN["stockQuantity"]);
                                    if (nQty + nStockQty < 0)
                                    {
                                        dtNegativeStock.Rows.Add(drowN.ItemArray);
                                    }
                                }
                            }

                        }
                        if (dtNegativeStock.Rows.Count > 0)
                        {
                            //result.ERRMSG = "Stock is going negative.";
                            //result.NegativeStock = dtNegativeStock;
                            dtResult.Rows.Add((new String[] { Convert.ToString(dset.Tables["tNegativeStock"].Rows[0]["ERRMSG"]), "" }));
                            return BadRequest(Convert.ToString(dset.Tables["tNegativeStock"].Rows[0]["ERRMSG"]));
                        }
                    }


                    if (con.State == ConnectionState.Closed)
                        con.Open();
                    SqlTransaction sqlTran = con.BeginTransaction();
                    cmd = new SqlCommand();
                    cmd.Connection = con;
                    cmd.Transaction = sqlTran;

                    result = "";


                    try
                    {
                        cQueryStr = @"
                        ;with det
                        AS
                        (
                            SELECT A.PRODUCT_CODE,A.BIN_ID,C.location_code AS DEPT_ID,SUM(A.QUANTITY) AS QUANTITY 
                                    from RPS_DET A(NOLOCK) 
                                    JOIN RPS_MST C (NOLOCK) ON C.CM_ID=A.CM_ID
                                    WHERE C.cm_id='" + cMemoID + @"'
                                    GROUP BY A.PRODUCT_CODE,C.location_code,A.BIN_ID
                        )
                        UPDATE a SET a.quantity_in_stock=a.quantity_in_stock+b.QUANTITY 
        				from pmt01106 a WITH (ROWLOCK)
                        JOIN DET b ON B.PRODUCT_CODE=a.product_code and a.DEPT_ID=b.DEPT_ID AND b.BIN_ID=a.BIN_ID
                        ";
                        cmd.CommandText = cQueryStr;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();


                        cQueryStr = @"UPDATE A SET CM_ID='XXXXXXXXXX'
                                    from RPS_DET A WITH (ROWLOCK)
                                    WHERE CM_id='" + cMemoID + "'";

                        cmd.CommandText = cQueryStr;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();

                        tCM_MST.Rows[0]["last_update"] = DateTime.Now;



                        cQueryStr = @"Update A SET 
                            atd_charges = " + globalMethods.ConvertDecimal(tCM_MST.Rows[0]["atd_charges"]) + @",
                            CUSTOMER_CODE ='" + Convert.ToString(tCM_MST.Rows[0]["CUSTOMER_CODE"]) + @"',
                            CM_DT = '" + globalMethods.ConvertDateTime(tCM_MST.Rows[0]["cm_dt"]).ToString("yyyy-MM-dd") + @"',
                            SUBTOTAL = " + globalMethods.ConvertDecimal(tCM_MST.Rows[0]["SUBTOTAL"]) + @",
                            NET_AMOUNT =  " + globalMethods.ConvertDecimal(tCM_MST.Rows[0]["NET_AMOUNT"]) + @",
                            USER_CODE =  '" + LoggedUserCode + @"',
                            LAST_UPDATE =  GETDATE(),  
                            computer_name =   '" + Environment.MachineName.Replace("\'", "") + @"',   
                            DISCOUNT_PERCENTAGE =" + globalMethods.ConvertDecimal(tCM_MST.Rows[0]["DISCOUNT_PERCENTAGE"]) + @", 
                            DISCOUNT_AMOUNT =  " + globalMethods.ConvertDecimal(tCM_MST.Rows[0]["DISCOUNT_AMOUNT"]) + @", 
                            REMARKS =  '" + Convert.ToString(tCM_MST.Rows[0]["REMARKS"]) + @"',
                            Manual_discount = " + (globalMethods.ConvertBool(tCM_MST.Rows[0]["Manual_discount"]) ? "1" : "0") + @",   
                            total_quantity = " + globalMethods.ConvertDecimal(tCM_MST.Rows[0]["total_quantity"]) + @", 
                            mrp_exchange_bill =  " + (globalMethods.ConvertBool(tCM_MST.Rows[0]["mrp_exchange_bill"]) ? "1" : "0") + @",  
                            ROUND_OFF = " + globalMethods.ConvertDecimal(tCM_MST.Rows[0]["ROUND_OFF"]) + @"
                            from RPS_MST A WITH (ROWLOCK)
                            WHERE CM_id='" + cMemoID + "'";

                        cmd.CommandText = cQueryStr;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                        tCM_DET.Select().ToList<DataRow>().ForEach(r => r["DEPT_ID"] = LoggedLocation);
                        tCM_DET.Select().ToList<DataRow>().ForEach(r => r["ROW_ID"] = GetNewRowID(LoggedLocation));
                        ChangeDBNull(tCM_DET, "");
                        var options = SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.CheckConstraints;

                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(con, options, sqlTran))
                        {
                            bulkCopy.BulkCopyTimeout = 50000;
                            bulkCopy.BatchSize = 5000;
                            bulkCopy.DestinationTableName = "RPS_DET";
                            globalMethods.SelectCmdToSql(dset, "SELECT * FROM RPS_DET (NOLOCK) WHERE 1=2", "TCURSOR_CMD");
                            foreach (DataColumn dcol in dset.Tables["TCURSOR_CMD"].Columns)
                                bulkCopy.ColumnMappings.Add(dcol.ColumnName, dcol.ColumnName);

                            try
                            {
                                bulkCopy.WriteToServer(tCM_DET);
                            }
                            catch (Exception ex)
                            {
                                sqlTran.Rollback();
                                dtResult.Rows.Add((new String[] { "Error! Record Not Updated SQL Error : " + ex.Message.ToString(), "" }));
                                return BadRequest("Error! Record Not Updated SQL Error : " + ex.Message.ToString());
                            }
                            finally
                            {

                            }
                        }


                        cQueryStr = @"
                                ;with det
                                AS
                                (
                                    SELECT A.PRODUCT_CODE,A.BIN_ID,C.location_code AS DEPT_ID,SUM(A.QUANTITY) AS QUANTITY 
                                    from RPS_DET A(NOLOCK) 
                                    JOIN RPS_MST C (NOLOCK) ON C.CM_ID=A.CM_ID
                                    WHERE C.cm_id='" + cMemoID + @"'
                                    GROUP BY A.PRODUCT_CODE,C.location_code,A.BIN_ID
                                )
                                UPDATE a SET a.quantity_in_stock=a.quantity_in_stock-b.QUANTITY 
        						from pmt01106 a WITH (ROWLOCK)
                                JOIN DET b ON B.PRODUCT_CODE=a.product_code and a.DEPT_ID=b.DEPT_ID AND b.BIN_ID=a.BIN_ID
                                ";
                        cmd.CommandText = cQueryStr;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();

                        sqlTran.Commit();

                        dtResult.Rows.Clear();
                        dtResult.Rows.Add((new String[] { "", cMemoID }));
                        return result = "";

                    }
                    catch (Exception ex)
                    {
                        sqlTran.Rollback();
                        dtResult.Rows.Add((new String[] { "Error! Record Not Updated SQL Error : " + ex.Message.ToString(), "" }));
                        return BadRequest("Error! " + ex.Message.ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                dtResult.Rows.Add((new String[] { "Error! " + ex.Message.ToString(), "" }));
                return BadRequest(ex.Message.ToString());

            }
        }

        public String CancelRPS(String cConStr, String LoggedLocation, String LoggedUserCode, String LoggedUserAlias, String MemoID, out DataTable dtResult)
        {
            dtResult = new DataTable("TDATA");
            dtResult.Columns.Add("ERRMSG", typeof(System.String));
            dtResult.Columns.Add("MEMO_ID", typeof(System.String));

            try
            {
                String cErr = "";
                DataTable DtM = new DataTable();

                if (string.IsNullOrEmpty(cConStr))
                    return BadRequest("Connection String Not Found");

                if (string.IsNullOrEmpty(MemoID))
                    return BadRequest("Memo ID should not be Empty");

                if (string.IsNullOrEmpty(LoggedLocation))
                    return BadRequest("Logged Location ID should not be Empty");

                if (string.IsNullOrEmpty(LoggedUserCode))
                    return BadRequest("Logged User Code should not be Empty");

                //SqlConnection con = new SqlConnection(cConStr);
                //SqlCommand cmd = new SqlCommand();
                //SqlDataAdapter sda = new SqlDataAdapter();
                //DataSet dset = new DataSet();

                APIBaseClass globalMethods = new APIBaseClass(cConStr);

                String result = "";
                using (SqlConnection con = new SqlConnection(cConStr))
                {
                    SqlCommand cmd = new SqlCommand();
                    SqlDataAdapter sda = new SqlDataAdapter();

                    String cQueryStr = "";
                    DataSet dset = new DataSet();






                    if (!_bAllowNegative)
                    {
                        cQueryStr = @"
                                ;with det
                                AS
                                (
                                    SELECT A.PRODUCT_CODE,A.BIN_ID,C.location_code AS DEPT_ID,SUM(A.QUANTITY) AS QUANTITY 
                                    from RPS_DET A(NOLOCK) 
                                    JOIN RPS_MST C (NOLOCK) ON C.CM_ID=A.CM_ID
                                    WHERE C.cm_id='" + MemoID + @"'
                                    GROUP BY A.PRODUCT_CODE,C.location_code,A.BIN_ID
                                )
                                
                        select a.product_code as productCode,a.quantity_in_stock as stockQuantity ,
                        b.QUANTITY as grnQuantity,'Stock is going negative.' as errMsg 
                        from pmt01106 a (NOLOCK)
                        JOIN DET b ON B.PRODUCT_CODE=a.product_code and a.DEPT_ID=b.DEPT_ID AND b.BIN_ID=a.BIN_ID
                        WHERE (A.quantity_in_stock - b.QUANTITY)<0
                        ";
                        globalMethods.SelectCmdToSql(dset, cQueryStr, "tNegativeStock");
                        if (dset.Tables["tNegativeStock"].Rows.Count > 0)
                        {
                            dtResult.Rows.Add((new String[] { Convert.ToString(dset.Tables["tNegativeStock"].Rows[0]["ERRMSG"]), "" }));
                            return BadRequest(Convert.ToString(dset.Tables["tNegativeStock"].Rows[0]["ERRMSG"]));
                        }
                    }

                    {
                        if (con.State == ConnectionState.Closed)
                            con.Open();
                        SqlTransaction sqlTran = con.BeginTransaction();
                        cmd = new SqlCommand();
                        cmd.Connection = con;
                        cmd.Transaction = sqlTran;

                        try
                        {
                            cQueryStr = @"
                                ;with det
                                AS
                                (
                                    SELECT A.PRODUCT_CODE,A.BIN_ID,C.location_code AS DEPT_ID,SUM(A.QUANTITY) AS QUANTITY 
                                    from RPS_DET A(NOLOCK) 
                                    JOIN RPS_MST C (NOLOCK) ON C.CM_ID=A.CM_ID
                                    WHERE C.cm_id='" + MemoID + @"'
                                    GROUP BY A.PRODUCT_CODE,C.location_code,A.BIN_ID
                                )
                                UPDATE a SET a.quantity_in_stock=a.quantity_in_stock+b.QUANTITY 
        						from pmt01106 a WITH (ROWLOCK)
                                JOIN DET b ON B.PRODUCT_CODE=a.product_code and a.DEPT_ID=b.DEPT_ID AND b.BIN_ID=a.BIN_ID
                                ";
                            cmd.CommandText = cQueryStr;
                            cmd.CommandType = CommandType.Text;
                            cmd.ExecuteNonQuery();

                            cQueryStr = @"UPDATE A SET CANCELLED=1,LAST_UPDATE=GETDATE(),quantity_last_update =GETDATE(),A.HO_SYNCH_LAST_UPDATE=''
                            from RPS_MST A WITH (ROWLOCK) 
                            WHERE cm_id='" + MemoID + "'";

                            cmd.CommandText = cQueryStr;
                            cmd.CommandType = CommandType.Text;
                            cmd.ExecuteNonQuery();

                            sqlTran.Commit();
                            dtResult.Rows.Add((new String[] { "", MemoID }));
                            return "";
                        }
                        catch (Exception ex)
                        {
                            sqlTran.Rollback();
                            dtResult.Rows.Add((new String[] { "Error! Record Not Cancelled SQL Error : " + ex.Message.ToString(), "" }));
                            return BadRequest("Error! Record Not Cancelled SQL Error : " + ex.Message.ToString());
                        }
                        finally
                        {
                            con.Close();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                dtResult.Rows.Add((new String[] { ex.Message.ToString(), "" }));
                return BadRequest(ex.Message.ToString());
            }
        }

        public String Validation_Before_Edit(String cConStr, String MemoID)
        {
            DataSet dset = new DataSet();
            String cErr = "";
            try
            {
                APIBaseClass clsCommon = new APIBaseClass(cConStr);

                String cQueryStr = @"SELECT TOP 1 A.REF_CM_ID FROM RPS_MST A 
			        WHERE A.CM_ID='" + MemoID + "' AND A.CANCELLED=0 AND ISNULL(A.ref_cm_id,'')<>''";



                if (!String.IsNullOrEmpty(Convert.ToString(clsCommon.ExecuteScalar(cQueryStr))))
                {
                    cErr = @"This packslip has been settled in retail sales.........can not Edit/Cancel";
                }
                if (String.IsNullOrEmpty(cErr))
                {
                    cQueryStr = @"SELECT TOP 1 A.CM_ID FROM RPS_MST A 
			                WHERE A.CM_ID='" + MemoID + "' AND A.CANCELLED=1 ";

                    if (!String.IsNullOrEmpty(Convert.ToString(clsCommon.ExecuteScalar(cQueryStr))))
                    {
                        cErr = @"This packslip is cancelled...can not Edit/Cancel";
                    }
                }

            }
            catch (Exception ex)
            {
                cErr = "(" + nameof(Validation_Before_Edit) + @")Error!  : " + ex.Message.ToString();
            }
            finally
            {
            }

            return cErr;
        }

        public void GetDiscountPer(Decimal dBasicDisc, Decimal dManualDisc, Decimal dCardDisc, Decimal dMrp, ref Decimal dNetDis)
        {
            try
            {
                //String cExpr = "";
                //Decimal dDiscPer = 0;

                if (dMrp > 0)
                {
                    Decimal nbasic, nCard, nMrpvalue;
                    nbasic = (dBasicDisc + dManualDisc);
                    nCard = dCardDisc;
                    nMrpvalue = dMrp;
                    dNetDis = (100 - (((nMrpvalue - (nMrpvalue * nbasic / 100)) - ((nMrpvalue - (nMrpvalue * nbasic / 100)) * nCard / 100)) / nMrpvalue) * 100);
                    //cExpr = "declare @nbasic numeric(10,2),@nCard numeric(10,2),@nMrpvalue numeric(10,2)\n" +
                    //        "select @nbasic=" + (dBasicDisc + dManualDisc) + ",@nCard=" + dCardDisc + ",@nMrpvalue=" + dMrp + " \n" +
                    //        "select (100-(((@nMrpvalue-(@nMrpvalue*ISNULL(@nbasic,0)/100))-((@nMrpvalue-(@nMrpvalue*ISNULL(@nbasic,0)/100)) \n" +
                    //        "*ISNULL(@nCard,0)/100))/@nMrpvalue)*100) as NetDis ";

                    ////dNetDis = clsCommon.ConvertDecimal(AppSLS.dmethod.SelectCmdTOSql_Scalar(cExpr, GlobalCls.AppMain.dmethod.cConStr));
                    //String cStrQuery = cExpr;
                    //Wow._QS._QueryStr = cStrQuery;
                    //Wow._QS._TableAlias = "tDataScalar";
                    //Wow._QS._ExecuteNonQuery = false;
                    //DataTable dtQuery = Wow.ExecuteQuery();
                    //if (AppSLS.dset.Tables.Contains("tDataScalar"))
                    //    AppSLS.dset.Tables.Remove("tDataScalar");
                    //if (LoadLocalDataTable(dtQuery, AppSLS.dset, Wow._QS._TableAlias))
                    //{
                    //    dNetDis = clsCommon.ConvertDecimal(AppSLS.dset.Tables["tDataScalar"].Rows[0]["NetDis"]);
                    //}

                }
            }
            catch (Exception ex)
            {


            }
        }

        String GetNewRowID(String LoggedLocation)
        {
            String cRetVal = LoggedLocation + DateTime.Now.ToString("yyyyMMddHHmmss") + Guid.NewGuid().ToString().Substring(0, 15);

            return cRetVal;

        }
        Boolean ChangeDBNull(DataTable dt, String cSqlTableName)
        {
            Boolean bRetVal = false;
            try
            {

                foreach (DataColumn dcol in dt.Columns)
                {
                    String dcolTypename = dcol.DataType.ToString();
                    if (dcolTypename.ToUpper().Contains("INT"))
                        dcolTypename = "System.Int";
                    if (dcolTypename.ToUpper().Contains("BYTE"))
                        dcolTypename = "System.Byte";
                    switch (dcolTypename)
                    {
                        case "System.Byte":
                        case "System.Boolean":
                        case "System.Int":
                        case "System.Decimal":
                        case "System.Double":
                            dt.Select("").ToList<DataRow>().ForEach(r => r[dcol.ColumnName] = (r.IsNull(dcol.ColumnName) ? 0 : r[dcol.ColumnName]));
                            break;
                        case "System.DateTime":
                            dt.Select("").ToList<DataRow>().ForEach(r => r[dcol.ColumnName] = (r.IsNull(dcol.ColumnName) ? (new DateTime(1900, 1, 1)) : DateTime.Compare((new APIBaseClass()).ConvertDateTime(r[dcol.ColumnName]), (new DateTime(1900, 1, 1))) < 0 ? (new DateTime(1900, 1, 1)) : r[dcol.ColumnName]));
                            break;
                        default:
                            dt.Select("").ToList<DataRow>().ForEach(r => r[dcol.ColumnName] = (r.IsNull(dcol.ColumnName) ? "" : r[dcol.ColumnName]));
                            break;
                    }

                }
                bRetVal = true;
            }
            catch (Exception ex)
            {
                bRetVal = false;
            }
            return bRetVal;
        }
        String ColTypeValue(DataColumn dcol, object dataValue)
        {
            String cStrColType = "";

            String cColTypeName = dcol.DataType.Name.ToString().ToUpper();
            if (cColTypeName.Substring(0, 3) == "INT")
            {
                cStrColType = (new APIBaseClass()).ConvertInt(dataValue).ToString();// "NUMERIC(20,0)";
            }
            else if (cColTypeName == "DECIMAL")
            {
                cStrColType = (new APIBaseClass()).ConvertDecimal(dataValue).ToString();// "NUMERIC(20,3)";
            }
            else if (cColTypeName == "BOOLEAN")
            {
                cStrColType = (new APIBaseClass()).ConvertBool(dataValue) ? "1" : "0";// SqlDbType.Bit.ToString();
            }
            else if (cColTypeName == "DATETIME")
            {
                cStrColType = "'" + (new APIBaseClass()).ConvertDateTime(Convert.ToString(dataValue)).ToString("yyyy-MM-dd") + "'";// SqlDbType.DateTime.ToString();
            }
            //else if (cColTypeName == "BYTE[]")
            //{
            //    if (dcol.ColumnName.ToUpper().Trim() == "TS")
            //        cStrColType = SqlDbType.Timestamp.ToString();
            //    else
            //        cStrColType = SqlDbType.Image.ToString();
            //}
            else if (cColTypeName == "BYTE")
            {
                cStrColType = Convert.ToString(dataValue);// "NUMERIC(10,0)";
            }
            else
            {
                cStrColType = "'" + Convert.ToString(dataValue) + "'";// SqlDbType.VarChar + "(5000)";
            }
            return cStrColType;
        }
    }
}
