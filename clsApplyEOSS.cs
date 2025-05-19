using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;

namespace WOWIntegration
{
    public class clsApplyEOSS
    {
        Boolean _bAllowNegative = false;
        String LoggedLocation, LoggedUserCode, LoggedUserAlias, LoggedBin, cUserForBINs;
        Boolean bApplyFlatschemesOnly;
        public bool BApplyFlatschemesOnly { get => bApplyFlatschemesOnly; set => bApplyFlatschemesOnly = value; }

        public string _cUserForBINs { get => cUserForBINs; set => cUserForBINs = value; }
        public Boolean _AddMode = false;
        DataTable dtRedeemCoupon, dtAPP_MST, dtAPP_DET, dtEditBackUp;
        public Boolean bRoundOff_Total = true, bRoundOff_Item = true, BSaleSetUp = false;
        public String cRoundOff_Total_At = "1", cRoundOff_Item_At = "1", cSpidNew = "";
        internal Decimal dexchange_tolerance_discount_diff_pct = 0;
        String cPARA_NAME_FOR_DISCOUNT = "";
        Int32 _DISCOUNT_PICKMODE_SLR = 0;
        internal string cCmdRowId = "";
        Boolean bProcessReturnItems = true, bALLOW_NEG_STOCK = false, bAPPLY_FIX_MRP_EOSS_AND_BILLPRINT=false;
        Boolean lShown = false;
        public DataTable dtApplicableBarcodes, dtACTIVE_SCHEMES_CLONE, dtACTIVE_SCHEMES1_CLONE, dtACTIVE_SCHEMES2_CLONE, dtACTIVE_SCHEMES3_CLONE, dtACTIVE_SCHEMES_BARCODE_CLONE;
        String _CM_ID;
        DateTime _EOSS_SERVER_DATETIME = new DateTime();
        Decimal nHAPPYHOURS_MAXDISCOUNT = 0;
        public String _AppPath = "";
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
        public clsApplyEOSS(Boolean bAllowNegative, String _LoggedLocation, String _LoggedUserCode, String _LoggedUserAlias, String _LoggedBin)
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

        public Boolean InitializeApplySchemes(String cConStr,DataRow cmdRow, DataSet dset, String cMstTableName, String cDetTableName, DataTable _dtEditBackUp, DataTable dtredeem, DataTable tLocationSettingSLS, DataTable tUserSettingSLS, Decimal Dexchange_tolerance_discount_diff_pct, String PARA_NAME_FOR_DISCOUNT,Boolean APPLY_FIX_MRP_EOSS_AND_BILLPRINT, out String cErrorMsg)
        {
            cErrorMsg = "";
            Boolean bRetval = true;
            dtRedeemCoupon = new DataTable();
            dtAPP_DET = new DataTable();
            dtAPP_MST = new DataTable();
            dtRedeemCoupon = dtredeem;
            dtEditBackUp = _dtEditBackUp;

            dtAPP_MST = dset.Tables[cMstTableName];
            dtAPP_DET = dset.Tables[cDetTableName];
            dtApplicableBarcodes = dset.Tables[cDetTableName].Clone();
            dtACTIVE_SCHEMES_CLONE = dset.Tables["tACTIVE_SCHEMES"].Clone();
            dtACTIVE_SCHEMES1_CLONE = dset.Tables["tACTIVE_SCHEMES1"].Clone();
            dtACTIVE_SCHEMES2_CLONE = dset.Tables["tACTIVE_SCHEMES2"].Clone();
            dtACTIVE_SCHEMES3_CLONE = dset.Tables["tACTIVE_SCHEMES3"].Clone();
            dtACTIVE_SCHEMES_BARCODE_CLONE = dset.Tables["tACTIVE_SCHEMES4"].Clone();
            BSaleSetUp = (dset.Tables["tACTIVE_SCHEMES"].Rows.Count > 0);
            dexchange_tolerance_discount_diff_pct = Dexchange_tolerance_discount_diff_pct;
            cPARA_NAME_FOR_DISCOUNT = PARA_NAME_FOR_DISCOUNT;
            _DISCOUNT_PICKMODE_SLR = (new APIBaseClass()).ConvertInt(tLocationSettingSLS.Rows[0]["DISCOUNT_PICKMODE_SLR"]);
            nHAPPYHOURS_MAXDISCOUNT = (new APIBaseClass()).ConvertDecimal(tLocationSettingSLS.Rows[0]["HAPPYHOURS_MAXDISCOUNT"]);
            bAPPLY_FIX_MRP_EOSS_AND_BILLPRINT = APPLY_FIX_MRP_EOSS_AND_BILLPRINT;
            if (BSaleSetUp)
            {
                bRetval = ApplySchemes(cConStr, BApplyFlatschemesOnly, cmdRow, dset, out cErrorMsg);
            }
            return bRetval;
        }

        public Boolean ApplySchemes(String cConStr, Boolean bApplyFlatschemesOnly, DataRow cCmdRow, DataSet dset, out String cErrMsg)
        {
            cErrMsg = "";
            BApplyFlatschemesOnly = bApplyFlatschemesOnly;
            APIBaseClass clsCommon = new APIBaseClass();
            Boolean lretval = false;
            DataTable dt, dt1;
            StringBuilder sbProcessingTime = new StringBuilder();
            sbProcessingTime.AppendLine("Start ApplySchemes :" + DateTime.Now.ToString());

            try
            {

                if (BSaleSetUp)
                {

                    //DataMethod.SelectCmdToSql(ref AppSLS.dset, "SELECT * FROM SLS_CMM01106_UPLOAD (nolock)  WHERE 1=2", "tSLS_CMM01106_UPLOAD", false);
                    //DataMethod.SelectCmdToSql(ref AppSLS.dset, "SELECT * FROM SLS_CMD01106_UPLOAD (nolock) WHERE 1=2", "tSLS_CMD01106_UPLOAD", false);

                    dt = new DataTable();

                    dt = dtAPP_MST.Clone();
                    dt.TableName = "tAPP_MST_TEMP";

                    if (_AddMode)
                    {
                        dtAPP_MST.Rows[0]["cm_time"] = DateTime.Now;
                        //dtAPP_MST.Rows[0]["fin_year"] = AppSLS.GC_C + AppSLS.GFIN_YEAR;
                    }
                    dt.Rows.Add(dtAPP_MST.Rows[0].ItemArray);
                    //dtAPP_MST.Rows[0]["mrp_exchange_bill"] = (chkExchangeAtMRP_CANCEL.Checked ? true : false);

                    //AddRecordInUploadTable(dtAPP_MST, dt);
                    //AppSLS.dset.Tables.Add(dt);
                    //DataMethod.SelectCmdToSql(ref AppSLS.dset, "SELECT * FROM cmd_usermrp_log (nolock) WHERE 1=2", "tCMD_USERMRP_LOG", false);


                    dt1 = new DataTable();
                    dt1 = dtAPP_DET.Clone();
                    String cCmdRowID = "";
                    if (!Equals(cCmdRow, null))
                    {
                        cCmdRowID = Convert.ToString(cCmdRow["row_id"]);
                        DataRow drNew = dt1.NewRow();
                        foreach (DataColumn dcol in cCmdRow.Table.Columns)
                        {
                            if (dt1.Columns.Contains(dcol.ColumnName))
                                drNew[dcol.ColumnName] = cCmdRow[dcol.ColumnName];
                        }
                        drNew["cm_id"] = dtAPP_MST.Rows[0]["cm_id"].ToString();
                        //drNew["bin_id"] = (Convert.ToString(drNew["bin_id"]).Trim() == "" ? AppSLS.GBIN_ID : drNew["bin_id"]);
                        drNew["sp_id"] = cSpidNew;

                        dt1.Rows.Add(drNew);
                    }
                    else
                    {
                        foreach (DataRow drow in dtAPP_DET.Rows)
                        {
                            if (clsCommon.ConvertBool(drow["barcodebased_flatdisc_applied"]) && !clsCommon.ConvertBool(drow["happy_hours_applied"])) continue;
                            //if (drow.RowState != DataRowState.Deleted)
                            if (!String.IsNullOrEmpty(drow["PRODUCT_CODE"].ToString().Trim()))
                            {
                                if (_AddMode)
                                {
                                    drow.BeginEdit();
                                    drow["old_mrp"] = drow["MRP"];
                                    drow.EndEdit();
                                }
                                else if (!_AddMode)
                                {
                                    drow.BeginEdit();
                                    if (!clsCommon.ConvertBool(dtAPP_MST.Rows[0]["patchup_run"]))
                                        drow["old_mrp"] = drow["MRP"];

                                    if (Convert.ToDouble(drow["old_mrp"]) == Convert.ToDouble(drow["MRP"]))
                                    {
                                        drow["realize_sale"] = drow["rfnet"];
                                    }

                                    drow["product_code"] = (Convert.ToString(drow["org_product_code"]).Contains("@") ? Convert.ToString(drow["org_product_code"]) : Convert.ToString(drow["product_code"]));

                                    drow.EndEdit();
                                }
                                //*********************
                                drow["cm_id"] = Convert.ToString(dtAPP_MST.Rows[0]["cm_id"]);
                                //drow["bin_id"] = (Convert.ToString(drow["bin_id"]).Trim() == "" ? AppSLS.GBIN_ID : drow["bin_id"]);

                                drow.EndEdit();
                                DataRow drNew = dt1.NewRow();
                                foreach (DataColumn dcol in dtAPP_DET.Columns)
                                {
                                    if (dt1.Columns.Contains(dcol.ColumnName))
                                        drNew[dcol.ColumnName] = drow[dcol.ColumnName];
                                }
                                drNew["sp_id"] = cSpidNew;

                                dt1.Rows.Add(drNew);
                            }
                        }
                    }
                    Boolean bImplementSaleSetup = true;

                    if (clsCommon.ConvertBool(dtAPP_MST.Rows[0]["dp_changed"]) || clsCommon.ConvertBool(dtAPP_MST.Rows[0]["manual_discount"]))/*Rohit 21-04-2023 : As pr discussion in last Evening Meeting : Given Function will call in effective Salesetup*/
                    {
                        if (clsCommon.ConvertInt(dtAPP_MST.Rows[0]["bill_level_dtm_method"]) == 2)
                        {
                            dt1.Select().ToList<DataRow>().ForEach(r =>
                            {
                                r["basic_discount_percentage"] = 0;
                                r["basic_discount_amount"] = 0;
                                r["manual_dp"] = 0;
                                r["manual_discount"] = 0;
                                r["scheme_name"] = "";
                                r["slsdet_row_id"] = "";
                                r["discount_amount"] = clsCommon.ConvertDecimal(r["basic_discount_amount"]) + clsCommon.ConvertDecimal(r["manual_discount_amount"]);
                                r["discount_percentage"] = clsCommon.ConvertDecimal(r["basic_discount_percentage"]) + clsCommon.ConvertDecimal(r["manual_discount_percentage"]);
                            });

                            bImplementSaleSetup = false;
                            lretval = true;
                        }
                    }
                    if (bImplementSaleSetup)
                    {
                        //if (bAPPLY_FIX_MRP_EOSS_AND_BILLPRINT)
                        //{
                        //    //dt1.Select("(ISNULL(FIX_MRP_Applicable,0)=1 OR FIX_MRP_Applicable=True) AND ISNULL(FIX_MRP,0)>0").ToList<DataRow>().ForEach(r => r["MRP"] = r["FIX_MRP"]);
                        //    dt1.Select("(ISNULL(FIX_MRP_Applicable,0)=1 OR FIX_MRP_Applicable=True)").ToList<DataRow>().ForEach(r => r["MRP"] = r["FIX_MRP"]);
                        //}
                        sbProcessingTime.AppendLine("Start ImplementSaleSetup :" + DateTime.Now.ToString());
                        lretval = ImplementSaleSetup(cConStr, dset, dt, dt1, cCmdRowID, out cErrMsg);
                        sbProcessingTime.AppendLine("End ImplementSaleSetup :" + DateTime.Now.ToString());
                    }

                    if (lretval)
                    {
                        if (!String.IsNullOrEmpty(cCmdRowID))
                        {
                            foreach (DataRow drowScheme in dt1.Rows)
                            {
                                //DataRow[] drowDet = dtAPP_DET.Select("ROW_ID='" + Convert.ToString(drowScheme["row_id"]) + "'");
                                //foreach (DataRow drowDet1 in drowDet)
                                {
                                    /*
                                     basic_discount_amount/        basic_discount_percentage/        scheme_name/        slset_row_id/        NET/
                                            weighted_avg_disc_amt/        weighted_avg_disc_pct/                             
                                    */
                                    //cCmdRow["basic_discount_amount"] = drowScheme["basic_discount_amount"];
                                    //cCmdRow["basic_discount_percentage"] = drowScheme["basic_discount_percentage"];
                                    //cCmdRow["discount_amount"] = drowScheme["basic_discount_amount"];
                                    //cCmdRow["discount_percentage"] = drowScheme["basic_discount_percentage"];
                                    //cCmdRow["scheme_name"] = drowScheme["scheme_name"];
                                    //cCmdRow["slsdet_row_id"] = drowScheme["slsdet_row_id"];
                                    //cCmdRow["NET"] = drowScheme["NET"];
                                    //cCmdRow["weighted_avg_disc_amt"] = drowScheme["weighted_avg_disc_amt"];
                                    //cCmdRow["weighted_avg_disc_pct"] = drowScheme["weighted_avg_disc_pct"];
                                    cCmdRow["weighted_avg_disc_amt"] = drowScheme["weighted_avg_disc_amt"];
                                    cCmdRow["weighted_avg_disc_pct"] = drowScheme["weighted_avg_disc_pct"];
                                    cCmdRow["basic_discount_amount"] = drowScheme["basic_discount_amount"];
                                    cCmdRow["basic_discount_percentage"] = drowScheme["basic_discount_percentage"];
                                    //if (bAPPLY_FIX_MRP_EOSS_AND_BILLPRINT && clsCommon.ConvertBool(cCmdRow["FIX_MRP_Applicable"]) && Math.Abs(clsCommon.ConvertDecimal(drowScheme["basic_discount_amount"])) > 0)
                                    //{
                                    //    Decimal dNet = ((clsCommon.ConvertDecimal(cCmdRow["FIX_MRP"]) * clsCommon.ConvertDecimal(cCmdRow["QUANTITY"])) - clsCommon.ConvertDecimal(drowScheme["basic_discount_amount"]));
                                    //    Decimal dDiscountAmount = ((clsCommon.ConvertDecimal(cCmdRow["MRP"]) * clsCommon.ConvertDecimal(cCmdRow["QUANTITY"])) - dNet);
                                    //    if ((clsCommon.ConvertDecimal(cCmdRow["QUANTITY"]) > 0 && dDiscountAmount < 0) || (clsCommon.ConvertDecimal(cCmdRow["QUANTITY"]) < 0 && dDiscountAmount > 0))
                                    //    {

                                    //        if (String.IsNullOrEmpty(cErrMsg))
                                    //            cErrMsg = "Apply Scheme : Discount going Negative \n" + Convert.ToString(cCmdRow["product_code"]) + " : Discount Amount : " + dDiscountAmount.ToString();
                                    //        else
                                    //            cErrMsg = cErrMsg + "\n"+Convert.ToString(cCmdRow["product_code"]) + " : Discount Amount : " + dDiscountAmount.ToString();

                                    //        dDiscountAmount = 0;
                                    //    }
                                    //    cCmdRow["basic_discount_amount"] = clsCommon.ConvertDecimal(cCmdRow["QUANTITY"]) > 0 && dDiscountAmount < 0 ? 0 : dDiscountAmount;
                                    //    cCmdRow["basic_discount_percentage"] = Math.Round(Math.Abs((clsCommon.ConvertDecimal(cCmdRow["basic_discount_amount"]) * 100) / (clsCommon.ConvertDecimal(cCmdRow["MRP"]) * clsCommon.ConvertDecimal(cCmdRow["QUANTITY"]))),2);
                                    //}

                                    cCmdRow["discount_amount"] = clsCommon.ConvertDecimal(cCmdRow["basic_discount_amount"]) + clsCommon.ConvertDecimal(cCmdRow["manual_discount_amount"]) + clsCommon.ConvertDecimal(cCmdRow["card_discount_amount"]);
                                    cCmdRow["discount_percentage"] = Math.Round(Math.Abs((clsCommon.ConvertDecimal(cCmdRow["discount_amount"]) / (clsCommon.ConvertDecimal(cCmdRow["quantity"]) * clsCommon.ConvertDecimal(cCmdRow["MRP"]))) * 100), 2);// clsCommon.ConvertDecimal(drowScheme["basic_discount_percentage"]) + clsCommon.ConvertDecimal(drowScheme["manual_discount_percentage"]);
                                    cCmdRow["scheme_name"] = drowScheme["scheme_name"];
                                    cCmdRow["slsdet_row_id"] = drowScheme["slsdet_row_id"];
                                    Decimal nNet = (clsCommon.ConvertDecimal(cCmdRow["MRP"]) * clsCommon.ConvertDecimal(cCmdRow["QUANTITY"]));
                                    if (cRoundOff_Item_At == "1")
                                        nNet = Math.Round(nNet);
                                    cCmdRow["NET"] = nNet - clsCommon.ConvertDecimal(cCmdRow["discount_amount"]);


                                    //cCmdRow["basic_discount_amount"] = drowScheme["basic_discount_amount"];
                                    //cCmdRow["basic_discount_percentage"] = drowScheme["basic_discount_percentage"];
                                    //cCmdRow["discount_amount"] = clsCommon.ConvertDecimal(drowScheme["basic_discount_amount"]) + clsCommon.ConvertDecimal(drowScheme["manual_discount_amount"]);
                                    //cCmdRow["discount_percentage"] = clsCommon.ConvertDecimal(drowScheme["basic_discount_percentage"]) + clsCommon.ConvertDecimal(drowScheme["manual_discount_percentage"]);
                                    //cCmdRow["scheme_name"] = drowScheme["scheme_name"];
                                    //cCmdRow["slsdet_row_id"] = drowScheme["slsdet_row_id"];
                                    //Decimal nNet = (clsCommon.ConvertDecimal(cCmdRow["MRP"]) * clsCommon.ConvertDecimal(cCmdRow["QUANTITY"]));
                                    //if (cRoundOff_Item_At == "1")
                                    //    nNet = Math.Round(nNet);
                                    //cCmdRow["NET"] = nNet - clsCommon.ConvertDecimal(drowScheme["discount_amount"]);


                                }

                            }
                        }
                        else
                        {
                            sbProcessingTime.AppendLine("Start Updating in Details Cursor :" + DateTime.Now.ToString());
                            foreach (DataRow drowScheme in dt1.Rows)
                            {
                                DataRow[] drowDet = dtAPP_DET.Select("ISNULL(ROW_ID,'')='" + Convert.ToString(drowScheme["row_id"]) + "'");
                                foreach (DataRow drowDet1 in drowDet)
                                {
                                    /*
                                     basic_discount_amount/        basic_discount_percentage/        scheme_name/        slset_row_id/        NET/
                                            weighted_avg_disc_amt/        weighted_avg_disc_pct/                             
                                    */
                                    drowDet1["weighted_avg_disc_amt"] = drowScheme["weighted_avg_disc_amt"];
                                    drowDet1["weighted_avg_disc_pct"] = drowScheme["weighted_avg_disc_pct"];
                                    drowDet1["bngn_not_applied"] = drowScheme["bngn_not_applied"];
                                    drowDet1["basic_discount_amount"] = drowScheme["basic_discount_amount"];
                                    drowDet1["basic_discount_percentage"] = drowScheme["basic_discount_percentage"];
                                    //if (bAPPLY_FIX_MRP_EOSS_AND_BILLPRINT && clsCommon.ConvertBool(drowDet1["FIX_MRP_Applicable"]) && Math.Abs(clsCommon.ConvertDecimal(drowScheme["basic_discount_amount"])) > 0)
                                    //{
                                    //    Decimal dNet = ((clsCommon.ConvertDecimal(drowDet1["FIX_MRP"]) * clsCommon.ConvertDecimal(drowDet1["QUANTITY"])) - clsCommon.ConvertDecimal(drowScheme["basic_discount_amount"]));
                                    //    Decimal dDiscountAmount = ((clsCommon.ConvertDecimal(drowDet1["MRP"]) * clsCommon.ConvertDecimal(drowDet1["QUANTITY"])) - dNet);
                                    //    if ((clsCommon.ConvertDecimal(drowDet1["QUANTITY"]) > 0 && dDiscountAmount < 0) || (clsCommon.ConvertDecimal(drowDet1["QUANTITY"]) < 0 && dDiscountAmount > 0))
                                    //    {
                                    //        if (String.IsNullOrEmpty(cErrMsg))
                                    //            cErrMsg = "Apply Scheme : Discount going Negative \n" + Convert.ToString(drowDet1["product_code"]) + " : Discount Amount : " + dDiscountAmount.ToString();
                                    //        else
                                    //            cErrMsg = cErrMsg + "\n" + Convert.ToString(drowDet1["product_code"]) + " : Discount Amount : " + dDiscountAmount.ToString();

                                    //        dDiscountAmount = 0;
                                    //    }
                                    //    drowDet1["basic_discount_amount"] = clsCommon.ConvertDecimal(drowDet1["QUANTITY"]) > 0 && dDiscountAmount < 0 ? 0 : dDiscountAmount;
                                    //    drowDet1["basic_discount_percentage"] = Math.Round(Math.Abs( (clsCommon.ConvertDecimal(drowDet1["basic_discount_amount"]) * 100) / (clsCommon.ConvertDecimal(drowDet1["MRP"]) * clsCommon.ConvertDecimal(drowDet1["QUANTITY"]))),2);
                                    //}


                                    drowDet1["discount_amount"] = clsCommon.ConvertDecimal(drowDet1["basic_discount_amount"]) + clsCommon.ConvertDecimal(drowScheme["manual_discount_amount"]) + clsCommon.ConvertDecimal(drowScheme["card_discount_amount"]);
                                    drowDet1["discount_percentage"] = Math.Round(Math.Abs((clsCommon.ConvertDecimal(drowDet1["discount_amount"]) / (clsCommon.ConvertDecimal(drowDet1["quantity"]) * clsCommon.ConvertDecimal(drowDet1["MRP"]))) * 100), 2);// clsCommon.ConvertDecimal(drowScheme["basic_discount_percentage"]) + clsCommon.ConvertDecimal(drowScheme["manual_discount_percentage"]);
                                    drowDet1["scheme_name"] = drowScheme["scheme_name"];
                                    drowDet1["slsdet_row_id"] = drowScheme["slsdet_row_id"];
                                    Decimal nNet = (clsCommon.ConvertDecimal(drowDet1["MRP"]) * clsCommon.ConvertDecimal(drowDet1["QUANTITY"]));
                                    if (cRoundOff_Item_At == "1")
                                        nNet = Math.Round(nNet);
                                    drowDet1["NET"] = nNet - clsCommon.ConvertDecimal(drowDet1["discount_amount"]);

                                }
                            }
                            sbProcessingTime.AppendLine("End Updating in Details Cursor :" + DateTime.Now.ToString());
                            sbProcessingTime.AppendLine("Start Updating in Master Cursor :" + DateTime.Now.ToString());
                            if (/*String.IsNullOrEmpty(Convert.ToString(dtAPP_MST.Rows[0]["ecoupon_id"])) || */
                                !clsCommon.ConvertBool(dtAPP_MST.Rows[0]["dp_changed"]) && !clsCommon.ConvertBool(dtAPP_MST.Rows[0]["manual_discount"]))
                            {
                                foreach (DataRow drowScheme in dt.Rows)
                                {
                                    DataRow drowDet1 = dtAPP_MST.Rows[0];
                                    drowDet1["BillLevelSchemeApplied"] = drowScheme["BillLevelSchemeApplied"];
                                    drowDet1["discount_amount"] = drowScheme["discount_amount"];
                                    drowDet1["discount_percentage_calc"] = drowScheme["discount_percentage_calc"];
                                    drowDet1["discount_percentage"] = drowScheme["discount_percentage"];
                                    drowDet1["bill_level_dtm_method"] = drowScheme["bill_level_dtm_method"];
                                    //drowDet1["discount_amount"] = clsCommon.ConvertDecimal(drowScheme["basic_discount_amount"]) + clsCommon.ConvertDecimal(drowScheme["manual_discount_amount"]);
                                    //    drowDet1["discount_percentage"] = clsCommon.ConvertDecimal(drowScheme["basic_discount_percentage"]) + clsCommon.ConvertDecimal(drowScheme["manual_discount_percentage"]);
                                }
                                Decimal dSubTotal = clsCommon.ConvertDecimal(dtAPP_DET.Compute("SUM(NET)", "ISNULL(PRODUCT_CODE,'')<>''"));
                                if (clsCommon.ConvertDecimal(dtAPP_MST.Rows[0]["discount_amount"]) > 0)
                                {
                                    dtAPP_MST.Rows[0]["discount_percentage"] = Math.Round(Math.Abs((clsCommon.ConvertDecimal(dtAPP_MST.Rows[0]["discount_amount"]) * 100) / dSubTotal), 3);
                                }
                                else if (clsCommon.ConvertDecimal(dtAPP_MST.Rows[0]["discount_percentage"]) > 0)
                                {
                                    dtAPP_MST.Rows[0]["discount_amount"] = Math.Round((dSubTotal * clsCommon.ConvertDecimal(dtAPP_MST.Rows[0]["discount_percentage"]) / 100), 2);
                                }
                                dtAPP_MST.Rows[0]["discount_percentage_calc"] = dtAPP_MST.Rows[0]["discount_percentage"];
                            }
                            sbProcessingTime.AppendLine("End Updating in Master Cursor :" + DateTime.Now.ToString());
                        }

                        lretval = true;
                    }

                }
                else
                {
                    DataRow[] drowDet = dtAPP_DET.Select("product_code<>''");
                    foreach (DataRow drowDet1 in drowDet)
                    {
                        drowDet1["discount_amount"] = clsCommon.ConvertDecimal(drowDet1["basic_discount_amount"]) + clsCommon.ConvertDecimal(drowDet1["manual_discount_amount"]) + clsCommon.ConvertDecimal(drowDet1["card_discount_amount"]);
                        drowDet1["discount_percentage"] = Math.Round(Math.Abs((clsCommon.ConvertDecimal(drowDet1["discount_amount"]) / (clsCommon.ConvertDecimal(drowDet1["quantity"]) * clsCommon.ConvertDecimal(drowDet1["MRP"]))) * 100), 2);//
                        //drowDet1["discount_percentage"] = clsCommon.ConvertDecimal(drowDet1["basic_discount_percentage"]) + clsCommon.ConvertDecimal(drowDet1["manual_discount_percentage"]);
                        drowDet1["NET"] = (clsCommon.ConvertDecimal(drowDet1["MRP"]) * clsCommon.ConvertDecimal(drowDet1["QUANTITY"])) - clsCommon.ConvertDecimal(drowDet1["discount_amount"]);
                    }
                    lretval = true;
                }
            }
            catch (Exception ex)
            {
                //MessageBox.Show("ApplySchemes : " + ex.Message, this.ProductName, MessageBoxButtons.OK, MessageBoxIcon.Information);
                cErrMsg = "ApplySchemes : " + ex.Message;
                lretval = false;
            }
            finally
            {
                sbProcessingTime.AppendLine("End ApplySchemes :" + DateTime.Now.ToString());
                if (!String.IsNullOrEmpty(_AppPath))
                {
                    System.IO.File.WriteAllText(_AppPath + "\\logs\\AppySchemes.txt", sbProcessingTime.ToString());
                }
            }
            return lretval;
        }
        bool chekCamp(string cMainUrl,Boolean bSecure)
        {
            try
            {
                String cUrlAdd = "";
                String cResult = "";
                //cMainUrl = Environment.UserName;
                cUrlAdd = "http" + (bSecure ? "s" : "") + "://" + cMainUrl;
                Campaign.Campaign Ws = new Campaign.Campaign();
                Ws.Url = cUrlAdd + "/CampaignService/Campaign.asmx";
                Ws.PreAuthenticate = true;
                Ws.Credentials = System.Net.CredentialCache.DefaultCredentials;
                Ws.Timeout = 10000;

                cResult = Ws.TESTURL();
                if (cResult != "T")
                {
                    return false;
                }
                return true;

            }
            catch (Exception ex)
            {
                return false;
            }
        }
        internal Boolean INTERNET(String cAdd ,out Boolean bSecure)
        {
            bSecure = true;
            bool result = false;
            try
            {
                bSecure = true;
                result = chekCamp(cAdd, bSecure);
                if (!result)
                {
                    bSecure = false;
                    result = chekCamp(cAdd, bSecure);
                }

            }
            catch
            {
                result =false;
            }
            return result;
        }
        private String SSPLDATETIME(String cConstr)
        {
            APIBaseClass clsCommon = new APIBaseClass(cConstr);
            try
            {

                //String cConnectionIP_Campaign = "wizapp.in";
                //String cConnectionIP_Campaign_s = "wizapp.in";
                //string cConnectionIP_Campaign_T = "wizapp.in";
                //String cSSPLIP = "";
                //Boolean bSecure;
                //if (INTERNET(cConnectionIP_Campaign,out bSecure))
                //    cSSPLIP = cConnectionIP_Campaign;
                //else if (INTERNET(cConnectionIP_Campaign_s, out bSecure))
                //    cSSPLIP = cConnectionIP_Campaign_s;
                //else if (INTERNET(cConnectionIP_Campaign_T, out bSecure))
                //    cSSPLIP = cConnectionIP_Campaign_T;
                //else
                {
                    _EOSS_SERVER_DATETIME = clsCommon.ConvertDateTime(clsCommon.ExecuteScalar("SELECT GETDATE()"));
                    _EOSS_SERVER_DATETIME = new DateTime(1900, 1, 1, _EOSS_SERVER_DATETIME.Hour, _EOSS_SERVER_DATETIME.Minute, 0);
                    return "";
                }

                //String cUrlAdd = "";
                //String cResult = "";
                //cUrlAdd = "http" + (bSecure ? "s" : "") + "://" + cSSPLIP;
                //Campaign.Campaign Ws = new Campaign.Campaign();
                //Ws.Url = cUrlAdd + "/CampaignService/Campaign.asmx";
                //Ws.PreAuthenticate = true;
                //Ws.Credentials = System.Net.CredentialCache.DefaultCredentials;
                //Ws.Timeout = 30000;
                //cResult = Ws.GETDATE_STRING();

                //if (cResult.Length > 10)
                //{
                //    _EOSS_SERVER_DATETIME = clsCommon.ConvertDateTime(cResult);
                //    _EOSS_SERVER_DATETIME = new DateTime(1900, 1, 1, _EOSS_SERVER_DATETIME.Hour, _EOSS_SERVER_DATETIME.Minute, 0);
                //    return cResult;
                //}
                //else
                //{
                //    _EOSS_SERVER_DATETIME = clsCommon.ConvertDateTime(clsCommon.ExecuteScalar("SELECT GETDATE()"));
                //    _EOSS_SERVER_DATETIME = new DateTime(1900, 1, 1, _EOSS_SERVER_DATETIME.Hour, _EOSS_SERVER_DATETIME.Minute, 0);
                //    return "";
                //}

            }
            catch (Exception ex)
            {
                _EOSS_SERVER_DATETIME = clsCommon.ConvertDateTime(clsCommon.ExecuteScalar("SELECT GETDATE()"));
                _EOSS_SERVER_DATETIME = new DateTime(1900, 1, 1, _EOSS_SERVER_DATETIME.Hour, _EOSS_SERVER_DATETIME.Minute, 0);
                return "";
            }
        }
        Boolean ImplementSaleSetup(String cConStr, DataSet dset, DataTable dtMst, DataTable dtDetails, String cCMDRowID, out String cErrMsg)
        {
            cErrMsg = "";
            APIBaseClass clsCommon = new APIBaseClass(cConStr);
            Boolean lRetVal = false;
            String cSchemName = "";
            String cStrSchemeRowID = "";
            StringBuilder sbProcessingTime = new StringBuilder();
            sbProcessingTime.AppendLine("Start ImplementSaleSetup :" + DateTime.Now.ToString());
            try
            {
                DataTable dt = dtDetails.Copy();
                //DataTable dtSchemeApplicable_fixcode = dtDetails.Clone();
                DataTable tblActiveSchemes = new DataTable();
                tblActiveSchemes.Columns.Add("schemeRowId", typeof(String));
                DataTable tblBarCodes = new DataTable();
                tblBarCodes.Columns.Add("PRODUCT_CODE", typeof(String));
                tblBarCodes.Columns.Add("cmd_row_id", typeof(String));
                //dmethod.SelectCmdTOSql(tblActiveSchemes, "DECLARE @tblActiveSchemes tvpActiveBarCodeschemes SELECT * FROM @tblActiveSchemes", false);
                //dmethod.SelectCmdTOSql(tblBarCodes, "DECLARE @tblBarCodes tvpBarCodes select * from @tblBarCodes", false);

                StringBuilder sb = new StringBuilder();
                foreach (DataRow drow in dt.Rows)
                {
                    /*Rohit 21-04-2023 : As pr discussion in last Evening Meeting : SaleSetup will be implemented on positive quantity only*/
                    if ((clsCommon.ConvertDecimal(drow["QUANTITY"]) > 0 || _DISCOUNT_PICKMODE_SLR != 1) && !clsCommon.ConvertBool(drow["MANUAL_DP"]) && !clsCommon.ConvertBool(drow["MANUAL_DISCOUNT"]))
                    {
                        if (String.IsNullOrEmpty(Convert.ToString(drow["product_code"]))) continue;
                        sb.Append("'");
                        sb.Append(Convert.ToString(drow["product_code"]));
                        sb.Append("',");
                        DataRow drNew = tblBarCodes.NewRow();
                        drNew["product_code"] = Convert.ToString(drow["PRODUCT_CODE"]);
                        drNew["cmd_row_id"] = Convert.ToString(drow["row_id"]);
                        tblBarCodes.Rows.Add(drNew);
                    }
                }
                String cStrProductCode = sb.ToString().TrimEnd(',');
                if (String.IsNullOrEmpty(cStrProductCode))
                {
                    return true;
                }
                Int32 iHappyHours = 2;
                String cstrHappyHourFilter = (String.IsNullOrEmpty(cCMDRowID) ? "" : " schememode=2 and ") + " (ISNULL(happy_hours_applicable,0)=0 OR happy_hours_applicable=False) AND ";
                if (dset.Tables["tACTIVE_SCHEMES5"].Rows.Count > 0)
                {
                    sbProcessingTime.AppendLine(iHappyHours.ToString() + " Start SSPLDATETIME :" + DateTime.Now.ToString());
                    SSPLDATETIME(cConStr);
                    sbProcessingTime.AppendLine(iHappyHours.ToString() + " End SSPLDATETIME :" + DateTime.Now.ToString());
                    DataRow[] drowHappyHours = dset.Tables["tACTIVE_SCHEMES5"].Select((String.IsNullOrEmpty(cCMDRowID) ? "" : " schememode = 2 and ") + " (from_time<='" + _EOSS_SERVER_DATETIME.ToString() + "' AND to_time>='" + _EOSS_SERVER_DATETIME + "') ");
                    //DataRow[] drowHappyHours = dset.Tables["tACTIVE_SCHEMES5"].Select("from_time<='" + _EOSS_SERVER_DATETIME.ToString() + "' AND to_time>='" + _EOSS_SERVER_DATETIME + "'");
                    if (drowHappyHours.Length > 0)
                    {
                        iHappyHours = 1;
                        cstrHappyHourFilter = (String.IsNullOrEmpty(cCMDRowID) ? "" : " schememode=2 and ") + " (ISNULL(happy_hours_applicable,0)=1 OR happy_hours_applicable=True) AND ";
                        StringBuilder sbHH = new StringBuilder();
                        foreach (DataRow drow in drowHappyHours)
                        {
                            if (String.IsNullOrEmpty(Convert.ToString(drow["schemeRowId"]))) continue;
                            sbHH.Append("'");
                            sbHH.Append(Convert.ToString(drow["schemeRowId"]));
                            sbHH.Append("',");


                        }
                        String cStrSchemeRowIdHH = sbHH.ToString().TrimEnd(',');
                        cstrHappyHourFilter = (String.IsNullOrEmpty(cCMDRowID) ? "" : " schememode=2 and ") + " ((ISNULL(happy_hours_applicable,0)=1 OR happy_hours_applicable=True) AND schemeRowId IN (" + cStrSchemeRowIdHH + ")) AND ";
                    }
                }
                for (; iHappyHours <= 2; iHappyHours++)
                {
                    if (iHappyHours == 2)
                        cstrHappyHourFilter = (String.IsNullOrEmpty(cCMDRowID) ? "" : " schememode=2 and ") + " (ISNULL(happy_hours_applicable,0)=0 OR happy_hours_applicable=False) AND ";
                    //dmethod.SelectCmdTOSql(ref dset, "SELECT * FROM SKU_NAMES WHERE PRODUCT_CODE IN (" + cStrProductCode + ")", "tSKUNAMES", false, true);
                    dtACTIVE_SCHEMES_CLONE = dset.Tables["tACTIVE_SCHEMES"].Clone();
                    dtACTIVE_SCHEMES1_CLONE = dset.Tables["tACTIVE_SCHEMES1"].Clone();
                    dtACTIVE_SCHEMES2_CLONE = dset.Tables["tACTIVE_SCHEMES2"].Clone();
                    dtACTIVE_SCHEMES3_CLONE = dset.Tables["tACTIVE_SCHEMES3"].Clone();
                    dtACTIVE_SCHEMES_BARCODE_CLONE = dset.Tables["tACTIVE_SCHEMES4"].Clone(); //dset.Tables["tACTIVE_SCHEMES_BARCODE"].Clone();
                    Int32 nSchemeMode = 1, nBuyBc = 1, nGetBc = 1;

                    foreach (DataRow drow in dset.Tables["tACTIVE_SCHEMES"].Select(cstrHappyHourFilter + " (ISNULL(buyFilterCriteria,'')<>'' OR ISNULL(getFilterCriteria,'')<>'') ", "titleProcessingOrder desc"))
                    {
                        cSchemName = Convert.ToString(drow["schemeName"]);
                        sbProcessingTime.AppendLine(iHappyHours.ToString() + " Scheme :[" + cSchemName + "]" + DateTime.Now.ToString());
                        //if (cSchemName.ToUpper().Contains("KIDS"))
                        //    MessageBox.Show(cSchemName);
                        String cStrbuyFilterRestrictionCriteria = Convert.ToString(drow["buyFilterRestrictionCriteria"]);
                        String cStrbuyFilterCriteria_exclusion = Convert.ToString(drow["buyFilterCriteria_exclusion"]);
                        String cStrBuyFilter = Convert.ToString(drow["buyFilterCriteria"]);// + ((String.IsNullOrEmpty(cStrbuyFilterCriteria_exclusion)) ? "" : " AND NOT (" + cStrbuyFilterCriteria_exclusion + ")");
                        String cStrGetFilter = Convert.ToString(drow["getFilterCriteria"]);

                        if (String.IsNullOrEmpty(cStrBuyFilter))
                            cStrBuyFilter = ((String.IsNullOrEmpty(cStrbuyFilterCriteria_exclusion)) ? "" : " NOT (" + cStrbuyFilterCriteria_exclusion + ")");
                        else
                            cStrBuyFilter = cStrBuyFilter + ((String.IsNullOrEmpty(cStrbuyFilterCriteria_exclusion)) ? "" : " AND NOT (" + cStrbuyFilterCriteria_exclusion + ")");

                        //if (String.IsNullOrEmpty(cStrBuyFilter))
                        //    cStrBuyFilter = ((String.IsNullOrEmpty(cStrbuyFilterRestrictionCriteria)) ? "" : " (" + cStrbuyFilterRestrictionCriteria + ")");
                        //else
                        //    cStrBuyFilter = cStrBuyFilter + ((String.IsNullOrEmpty(cStrbuyFilterRestrictionCriteria)) ? "" : " AND (" + cStrbuyFilterRestrictionCriteria + ")");

                        if (String.IsNullOrEmpty(cStrGetFilter))
                            cStrGetFilter = ((String.IsNullOrEmpty(cStrbuyFilterCriteria_exclusion)) ? "" : " NOT (" + cStrbuyFilterCriteria_exclusion + ")");
                        else
                            cStrGetFilter = cStrGetFilter + ((String.IsNullOrEmpty(cStrbuyFilterCriteria_exclusion)) ? "" : " AND NOT (" + cStrbuyFilterCriteria_exclusion + ")");

                        if (String.IsNullOrEmpty(cStrBuyFilter)) cStrBuyFilter = "1=2";
                        if (String.IsNullOrEmpty(cStrGetFilter)) cStrGetFilter = "1=2";
                        if (String.IsNullOrEmpty(cStrbuyFilterRestrictionCriteria)) cStrbuyFilterRestrictionCriteria = "1=1";

                        DataRow[] dr = dt.Select("1=2");
                        //cStrBuyFilter = @"(( 
                        // (PRODUCT_CODE LIKE '%8907983625348%' OR PRODUCT_CODE LIKE '%8907983675954%' OR PRODUCT_CODE LIKE '%8907983657820%' OR PRODUCT_CODE LIKE '%8907983682761%' OR PRODUCT_CODE LIKE '%8907983658865%' OR PRODUCT_CODE LIKE '%8907983658872%' OR PRODUCT_CODE LIKE '%8907983688848%' OR PRODUCT_CODE LIKE '%8907696864386%' OR PRODUCT_CODE LIKE '%8907983454924%' OR PRODUCT_CODE LIKE '%8907983811123%' OR PRODUCT_CODE LIKE '%8905152412409%' OR PRODUCT_CODE LIKE '%8907983811956%' OR PRODUCT_CODE LIKE '%8907983817064%' OR PRODUCT_CODE LIKE '%8907696875603%' OR PRODUCT_CODE LIKE '%8907827372322%' OR PRODUCT_CODE LIKE '%8907827372346%' OR PRODUCT_CODE LIKE '%8907827372353%' OR PRODUCT_CODE LIKE '%8907696877201%' OR PRODUCT_CODE LIKE '%8907696877355%' OR PRODUCT_CODE LIKE '%8907696878079%' OR PRODUCT_CODE LIKE '%8907696878987%' OR PRODUCT_CODE LIKE '%8907983383422%' OR PRODUCT_CODE LIKE '%8907983839523%' OR PRODUCT_CODE LIKE '%8905152653901%' OR PRODUCT_CODE LIKE '%8905152282125%' OR PRODUCT_CODE LIKE '%8907983533629%' OR PRODUCT_CODE LIKE '%8907983789538%' OR PRODUCT_CODE LIKE '%8907827283949%' OR PRODUCT_CODE LIKE '%8905152416872%' OR PRODUCT_CODE LIKE '%8905152416896%' OR PRODUCT_CODE LIKE '%8905152529763%' OR PRODUCT_CODE LIKE '%8905152529787%' OR PRODUCT_CODE LIKE '%8905152529794%' OR PRODUCT_CODE LIKE '%8905772046886%' OR PRODUCT_CODE LIKE '%8905772046893%' OR PRODUCT_CODE LIKE '%8905772046909%' OR PRODUCT_CODE LIKE '%8905772046916%' OR PRODUCT_CODE LIKE '%8905772047098%' OR PRODUCT_CODE LIKE '%8905772047104%' OR PRODUCT_CODE LIKE '%8905772047111%' OR PRODUCT_CODE LIKE '%8905772047210%' OR PRODUCT_CODE LIKE '%8905772047432%' OR PRODUCT_CODE LIKE '%8905772047449%' OR PRODUCT_CODE LIKE '%8905772047456%' OR PRODUCT_CODE LIKE '%8905772047463%' OR PRODUCT_CODE LIKE '%8905772047487%' OR PRODUCT_CODE LIKE '%8905772047494%' OR PRODUCT_CODE LIKE '%8905772047500%' OR PRODUCT_CODE LIKE '%8905772047517%' OR PRODUCT_CODE LIKE '%8905772047531%' OR PRODUCT_CODE LIKE '%8905772047548%' OR PRODUCT_CODE LIKE '%8905772047555%' OR PRODUCT_CODE LIKE '%8905772047586%' OR PRODUCT_CODE LIKE '%8905772047593%' OR PRODUCT_CODE LIKE '%8905772047609%' OR PRODUCT_CODE LIKE '%8905772047616%' OR PRODUCT_CODE LIKE '%8905772047630%' OR PRODUCT_CODE LIKE '%8905772047647%' OR PRODUCT_CODE LIKE '%8905772047654%' OR PRODUCT_CODE LIKE '%8905772047661%' OR PRODUCT_CODE LIKE '%8905772047685%' OR PRODUCT_CODE LIKE '%8905772047692%' OR PRODUCT_CODE LIKE '%8905772047708%' OR PRODUCT_CODE LIKE '%8905772047715%' OR PRODUCT_CODE LIKE '%8905772047746%' OR PRODUCT_CODE LIKE '%8905772047753%' OR PRODUCT_CODE LIKE '%8905772047784%' OR PRODUCT_CODE LIKE '%8905772047791%' OR PRODUCT_CODE LIKE '%8905772047807%' OR PRODUCT_CODE LIKE '%8905772047814%' OR PRODUCT_CODE LIKE '%8905772047838%' OR PRODUCT_CODE LIKE '%8905772048002%' OR PRODUCT_CODE LIKE '%8905772048019%' OR PRODUCT_CODE LIKE '%8905772048033%' OR PRODUCT_CODE LIKE '%8905772048040%' OR PRODUCT_CODE LIKE '%8905772048064%' OR PRODUCT_CODE LIKE '%8905772048071%' OR PRODUCT_CODE LIKE '%8905772048095%' OR PRODUCT_CODE LIKE '%8905772048101%' OR PRODUCT_CODE LIKE '%8905772048118%' OR PRODUCT_CODE LIKE '%8905772354141%' OR PRODUCT_CODE LIKE '%8905772354165%' OR PRODUCT_CODE LIKE '%8905772354172%' OR PRODUCT_CODE LIKE '%8905772354189%' OR PRODUCT_CODE LIKE '%8905772354431%' OR PRODUCT_CODE LIKE '%8905772354448%' OR PRODUCT_CODE LIKE '%8905772354455%' OR PRODUCT_CODE LIKE '%8905772354462%' OR PRODUCT_CODE LIKE '%8905772354622%' OR PRODUCT_CODE LIKE '%8905772354639%' OR PRODUCT_CODE LIKE '%8905772354646%' OR PRODUCT_CODE LIKE '%8905772354653%' OR PRODUCT_CODE LIKE '%8905772354660%' OR PRODUCT_CODE LIKE '%8905772354752%' OR PRODUCT_CODE LIKE '%8905772354783%' OR PRODUCT_CODE LIKE '%8905772355490%' OR PRODUCT_CODE LIKE '%8905772355506%' OR PRODUCT_CODE LIKE '%8905772355513%' OR PRODUCT_CODE LIKE '%8905772355520%' OR PRODUCT_CODE LIKE '%8905772355780%' OR PRODUCT_CODE LIKE '%8905772355797%' OR PRODUCT_CODE LIKE '%8905772355803%' OR PRODUCT_CODE LIKE '%8905772355810%' OR PRODUCT_CODE LIKE '%8905772355827%' OR PRODUCT_CODE LIKE '%8905772085724%' OR PRODUCT_CODE LIKE '%8905772085731%' OR PRODUCT_CODE LIKE '%8905772085748%' OR PRODUCT_CODE LIKE '%8905772085755%' OR PRODUCT_CODE LIKE '%8905772085762%' OR PRODUCT_CODE LIKE '%8905772086028%' OR PRODUCT_CODE LIKE '%8905772086035%' OR PRODUCT_CODE LIKE '%8905772086042%' OR PRODUCT_CODE LIKE '%8905772086059%' OR PRODUCT_CODE LIKE '%8905772034623%' OR PRODUCT_CODE LIKE '%8905772034630%' OR PRODUCT_CODE LIKE '%8905772034647%' OR PRODUCT_CODE LIKE '%8905772034654%' OR PRODUCT_CODE LIKE '%8905772034692%' OR PRODUCT_CODE LIKE '%8905514960227%' OR PRODUCT_CODE LIKE '%8905514960234%' OR PRODUCT_CODE LIKE '%8905514960302%' OR PRODUCT_CODE LIKE '%8905514960319%' OR PRODUCT_CODE LIKE '%8905772230841%' OR PRODUCT_CODE LIKE '%8905772230858%' OR PRODUCT_CODE LIKE '%8905772230865%' OR PRODUCT_CODE LIKE '%8905514960548%' OR PRODUCT_CODE LIKE '%8905514960555%' OR PRODUCT_CODE LIKE '%8905772034760%' OR PRODUCT_CODE LIKE '%8905772034777%' OR PRODUCT_CODE LIKE '%8905514960845%' OR PRODUCT_CODE LIKE '%8905514960852%' OR PRODUCT_CODE LIKE '%8905514960869%' OR PRODUCT_CODE LIKE '%8905514960876%' OR PRODUCT_CODE LIKE '%8905514961101%' OR PRODUCT_CODE LIKE '%8905514961118%' OR PRODUCT_CODE LIKE '%8905514989556%' OR PRODUCT_CODE LIKE '%8905514989563%' OR PRODUCT_CODE LIKE '%8905514989570%' OR PRODUCT_CODE LIKE '%8905514961477%' OR PRODUCT_CODE LIKE '%8905514961484%' OR PRODUCT_CODE LIKE '%8905514961491%' OR PRODUCT_CODE LIKE '%8905514961507%' OR PRODUCT_CODE LIKE '%8905514961552%' OR PRODUCT_CODE LIKE '%8905514961569%' OR PRODUCT_CODE LIKE '%8905514961576%' OR PRODUCT_CODE LIKE '%8905514961583%' OR PRODUCT_CODE LIKE '%8905514962443%' OR PRODUCT_CODE LIKE '%8905514962450%' OR PRODUCT_CODE LIKE '%8905514962467%' OR PRODUCT_CODE LIKE '%8905514962559%' OR PRODUCT_CODE LIKE '%8905514963556%' OR PRODUCT_CODE LIKE '%8905514963563%' OR PRODUCT_CODE LIKE '%8905514963570%' OR PRODUCT_CODE LIKE '%8905514963587%' OR PRODUCT_CODE LIKE '%8905514963594%' OR PRODUCT_CODE LIKE '%8905514964355%' OR PRODUCT_CODE LIKE '%8905514964362%' OR PRODUCT_CODE LIKE '%8905514964386%' OR PRODUCT_CODE LIKE '%8905772035736%' OR PRODUCT_CODE LIKE '%8905772035750%' OR PRODUCT_CODE LIKE '%8905514333250%' OR PRODUCT_CODE LIKE '%8905514333267%' OR PRODUCT_CODE LIKE '%8905514333274%' OR PRODUCT_CODE LIKE '%8905514333281%' OR PRODUCT_CODE LIKE '%8905514333298%' OR PRODUCT_CODE LIKE '%8905514308593%' OR PRODUCT_CODE LIKE '%8905514355504%' OR PRODUCT_CODE LIKE '%8905514355511%' OR PRODUCT_CODE LIKE '%8905514355528%' OR PRODUCT_CODE LIKE '%8905514404547%' OR PRODUCT_CODE LIKE '%8905514404554%' OR PRODUCT_CODE LIKE '%8905514404561%' OR PRODUCT_CODE LIKE '%8905514404578%' OR PRODUCT_CODE LIKE '%8905514404585%' OR PRODUCT_CODE LIKE '%8905514357522%' OR PRODUCT_CODE LIKE '%8905514357539%' OR PRODUCT_CODE LIKE '%8905514335865%' OR PRODUCT_CODE LIKE '%8905514335872%' OR PRODUCT_CODE LIKE '%8905514337906%' OR PRODUCT_CODE LIKE '%8905514119670%' OR PRODUCT_CODE LIKE '%8905514119687%' OR PRODUCT_CODE LIKE '%8905514119694%' OR PRODUCT_CODE LIKE '%8905514119809%' OR PRODUCT_CODE LIKE '%8905514405551%' OR PRODUCT_CODE LIKE '%8905514120157%' OR PRODUCT_CODE LIKE '%8905514358109%' OR PRODUCT_CODE LIKE '%8905514358185%' OR PRODUCT_CODE LIKE '%8905514378510%' OR PRODUCT_CODE LIKE '%8905514418551%' OR PRODUCT_CODE LIKE '%8905514406237%' OR PRODUCT_CODE LIKE '%8905514406244%' OR PRODUCT_CODE LIKE '%8905287839614%' OR PRODUCT_CODE LIKE '%8905287839652%' OR PRODUCT_CODE LIKE '%8905514379128%' OR PRODUCT_CODE LIKE '%8905514358925%' OR PRODUCT_CODE LIKE '%8905514121239%' OR PRODUCT_CODE LIKE '%8905514121246%' OR PRODUCT_CODE LIKE '%8905514379579%' OR PRODUCT_CODE LIKE '%8905514379593%' OR PRODUCT_CODE LIKE '%8905514379609%' OR PRODUCT_CODE LIKE '%8905772114882%' OR PRODUCT_CODE LIKE '%8905772114912%' OR PRODUCT_CODE LIKE '%8905772115001%' OR PRODUCT_CODE LIKE '%8905772115018%' OR PRODUCT_CODE LIKE '%8905772115025%' OR PRODUCT_CODE LIKE '%8905772115230%' OR PRODUCT_CODE LIKE '%8905772115247%' OR PRODUCT_CODE LIKE '%8905772115254%' OR PRODUCT_CODE LIKE '%8905772153461%' OR PRODUCT_CODE LIKE '%8905514927022%' OR PRODUCT_CODE LIKE '%8905514917450%' OR PRODUCT_CODE LIKE '%8905514534893%' OR PRODUCT_CODE LIKE '%8905772094610%' OR PRODUCT_CODE LIKE '%8905772094627%' OR PRODUCT_CODE LIKE '%8905772094634%' OR PRODUCT_CODE LIKE '%8905514535180%' OR PRODUCT_CODE LIKE '%8905514535197%' OR PRODUCT_CODE LIKE '%8905772094801%' OR PRODUCT_CODE LIKE '%8905772094818%' OR PRODUCT_CODE LIKE '%8905514021133%' OR PRODUCT_CODE LIKE '%8905514021140%' OR PRODUCT_CODE LIKE '%8905514021157%' OR PRODUCT_CODE LIKE '%8905514110493%' OR PRODUCT_CODE LIKE '%8905514240688%' OR PRODUCT_CODE LIKE '%8905514588070%' OR PRODUCT_CODE LIKE '%8905514253565%' OR PRODUCT_CODE LIKE '%8905772121989%' OR PRODUCT_CODE LIKE '%8905514673035%' OR PRODUCT_CODE LIKE '%8905514673028%' OR PRODUCT_CODE LIKE '%8905514564159%' OR PRODUCT_CODE LIKE '%8905514104874%' OR PRODUCT_CODE LIKE '%8905514104881%' OR PRODUCT_CODE LIKE '%8905514127910%' OR PRODUCT_CODE LIKE '%8905772267960%' OR PRODUCT_CODE LIKE '%8905772267977%' OR PRODUCT_CODE LIKE '%8905772267991%' OR PRODUCT_CODE LIKE '%8905772268004%' OR PRODUCT_CODE LIKE '%8905514280325%' OR PRODUCT_CODE LIKE '%8905514280332%' OR PRODUCT_CODE LIKE '%8905514280349%' OR PRODUCT_CODE LIKE '%8905514958293%' OR PRODUCT_CODE LIKE '%8905514958316%' OR PRODUCT_CODE LIKE '%8905772038751%' OR PRODUCT_CODE LIKE '%8905772038768%' OR PRODUCT_CODE LIKE '%8905772146524%' OR PRODUCT_CODE LIKE '%8905772146531%' OR PRODUCT_CODE LIKE '%8905772076340%' OR PRODUCT_CODE LIKE '%8905772076364%' OR PRODUCT_CODE LIKE '%8905772151689%' OR PRODUCT_CODE LIKE '%8905514958361%' OR PRODUCT_CODE LIKE '%8905514958378%' OR PRODUCT_CODE LIKE '%8905514958385%' OR PRODUCT_CODE LIKE '%8905772187039%' OR PRODUCT_CODE LIKE '%8905772187060%' OR PRODUCT_CODE LIKE '%8905772095853%' OR PRODUCT_CODE LIKE '%8905772095860%' OR PRODUCT_CODE LIKE '%8905772095877%' OR PRODUCT_CODE LIKE '%8905772151740%' OR PRODUCT_CODE LIKE '%8905514208732%' OR PRODUCT_CODE LIKE '%8905514859699%' OR PRODUCT_CODE LIKE '%8905514566344%' OR PRODUCT_CODE LIKE '%8905772096126%' OR PRODUCT_CODE LIKE '%8905772154260%' OR PRODUCT_CODE LIKE '%8905772264426%' OR PRODUCT_CODE LIKE '%8905772264464%' OR PRODUCT_CODE LIKE '%8905772264471%' OR PRODUCT_CODE LIKE '%8905772096225%' OR PRODUCT_CODE LIKE '%8905772151795%' OR PRODUCT_CODE LIKE '%8905514331102%' OR PRODUCT_CODE LIKE '%8905514128917%' OR PRODUCT_CODE LIKE '%8905514208978%' OR PRODUCT_CODE LIKE '%8905514935706%' OR PRODUCT_CODE LIKE '%8905772096966%' OR PRODUCT_CODE LIKE '%8905514209081%' OR PRODUCT_CODE LIKE '%8905514209098%' OR PRODUCT_CODE LIKE '%8905514241050%' OR PRODUCT_CODE LIKE '%8905772097307%' OR PRODUCT_CODE LIKE '%8905772097314%' OR PRODUCT_CODE LIKE '%8905772097321%' OR PRODUCT_CODE LIKE '%8905772097499%' OR PRODUCT_CODE LIKE '%8905772097482%' OR PRODUCT_CODE LIKE '%8905514974385%' OR PRODUCT_CODE LIKE '%8905514974392%' OR PRODUCT_CODE LIKE '%8905772006873%' OR PRODUCT_CODE LIKE '%8905514282404%' OR PRODUCT_CODE LIKE '%8905514282435%' OR PRODUCT_CODE LIKE '%8905514568928%' OR PRODUCT_CODE LIKE '%8905287973370%' OR PRODUCT_CODE LIKE '%8905287973394%' OR PRODUCT_CODE LIKE '%8905287973400%' OR PRODUCT_CODE LIKE '%8905772363990%' OR PRODUCT_CODE LIKE '%8905772364003%' OR PRODUCT_CODE LIKE '%8905772364027%' OR PRODUCT_CODE LIKE '%8905772141956%' OR PRODUCT_CODE LIKE '%8905772142038%' OR PRODUCT_CODE LIKE '%8905772142045%' OR PRODUCT_CODE LIKE '%8905772142052%' OR PRODUCT_CODE LIKE '%8905772286817%' OR PRODUCT_CODE LIKE '%8905772286824%' OR PRODUCT_CODE LIKE '%8905772364522%' OR PRODUCT_CODE LIKE '%8905772364539%' OR PRODUCT_CODE LIKE '%8905287623237%' OR PRODUCT_CODE LIKE '%8905772364638%' OR PRODUCT_CODE LIKE '%8905772364645%' OR PRODUCT_CODE LIKE '%8905287847916%' OR PRODUCT_CODE LIKE '%8905287162767%' OR PRODUCT_CODE LIKE '%8905287902967%' OR PRODUCT_CODE LIKE '%8905772365093%' OR PRODUCT_CODE LIKE '%8905772365116%' OR PRODUCT_CODE LIKE '%8905772365154%' OR PRODUCT_CODE LIKE '%8905287945568%' OR PRODUCT_CODE LIKE '%8905772016407%' OR PRODUCT_CODE LIKE '%8905772016414%' OR PRODUCT_CODE LIKE '%8905772016421%' OR PRODUCT_CODE LIKE '%8905514700861%' OR PRODUCT_CODE LIKE '%8905514700878%' OR PRODUCT_CODE LIKE '%8905514700885%' OR PRODUCT_CODE LIKE '%8905514860046%' OR PRODUCT_CODE LIKE '%8905514860053%' OR PRODUCT_CODE LIKE '%8905514738086%' OR PRODUCT_CODE LIKE '%8905514729763%' OR PRODUCT_CODE LIKE '%8905514738192%' OR PRODUCT_CODE LIKE '%8905514738215%' OR PRODUCT_CODE LIKE '%8905772006422%' OR PRODUCT_CODE LIKE '%8905772006439%' OR PRODUCT_CODE LIKE '%8905772006446%' OR PRODUCT_CODE LIKE '%8905772006453%' OR PRODUCT_CODE LIKE '%8905514451152%' OR PRODUCT_CODE LIKE '%8905514575483%' OR PRODUCT_CODE LIKE '%8905772083768%' OR PRODUCT_CODE LIKE '%8905772083911%' OR PRODUCT_CODE LIKE '%8905514536972%' OR PRODUCT_CODE LIKE '%8905514536989%' OR PRODUCT_CODE LIKE '%8905514536996%' OR PRODUCT_CODE LIKE '%8905514537009%' OR PRODUCT_CODE LIKE '%8905514750149%' OR PRODUCT_CODE LIKE '%8905514750156%' OR PRODUCT_CODE LIKE '%8905514750163%' OR PRODUCT_CODE LIKE '%8905514750132%' OR PRODUCT_CODE LIKE '%8905514750170%' OR PRODUCT_CODE LIKE '%8905514537115%' OR PRODUCT_CODE LIKE '%8905514537122%' OR PRODUCT_CODE LIKE '%8905514537542%' OR PRODUCT_CODE LIKE '%8905514796345%' OR PRODUCT_CODE LIKE '%8905514108100%' OR PRODUCT_CODE LIKE '%8905514108117%' OR PRODUCT_CODE LIKE '%8905514750255%' OR PRODUCT_CODE LIKE '%8905514750262%' OR PRODUCT_CODE LIKE '%8905772153478%' OR PRODUCT_CODE LIKE '%8905772153492%' OR PRODUCT_CODE LIKE '%8905772153508%' OR PRODUCT_CODE LIKE '%8905514355658%' OR PRODUCT_CODE LIKE '%8905514359809%' OR PRODUCT_CODE LIKE '%8905514359816%' OR PRODUCT_CODE LIKE '%8905514355733%' OR PRODUCT_CODE LIKE '%8905514355757%' OR PRODUCT_CODE LIKE '%8905514355825%' OR PRODUCT_CODE LIKE '%8905514338781%' OR PRODUCT_CODE LIKE '%8905514338798%' OR PRODUCT_CODE LIKE '%8905514338804%' OR PRODUCT_CODE LIKE '%8905514338811%' OR PRODUCT_CODE LIKE '%8905287843703%' OR PRODUCT_CODE LIKE '%8905514360942%' OR PRODUCT_CODE LIKE '%8905287844311%' OR PRODUCT_CODE LIKE '%8905287844342%' OR PRODUCT_CODE LIKE '%8905514361376%' OR PRODUCT_CODE LIKE '%8905514411651%' OR PRODUCT_CODE LIKE '%8905772115797%' OR PRODUCT_CODE LIKE '%8905772115803%' OR PRODUCT_CODE LIKE '%8905772115865%' OR PRODUCT_CODE LIKE '%8905772115872%' OR PRODUCT_CODE LIKE '%8905772115896%' OR PRODUCT_CODE LIKE '%8905772115902%' OR PRODUCT_CODE LIKE '%8905772115926%' OR PRODUCT_CODE LIKE '%8905287846292%' OR PRODUCT_CODE LIKE '%8905772116145%' OR PRODUCT_CODE LIKE '%8905772116152%' OR PRODUCT_CODE LIKE '%8905772116169%' OR PRODUCT_CODE LIKE '%8905772154611%' OR PRODUCT_CODE LIKE '%8905514852331%' OR PRODUCT_CODE LIKE '%8905514852348%' OR PRODUCT_CODE LIKE '%8905514852355%' OR PRODUCT_CODE LIKE '%8905514852324%' OR PRODUCT_CODE LIKE '%8905514816838%' OR PRODUCT_CODE LIKE '%8905514816845%' OR PRODUCT_CODE LIKE '%8905514816852%' OR PRODUCT_CODE LIKE '%8905514756035%' OR PRODUCT_CODE LIKE '%8905514756066%' OR PRODUCT_CODE LIKE '%8905287884720%' OR PRODUCT_CODE LIKE '%8905287884744%' OR PRODUCT_CODE LIKE '%8905287884850%' OR PRODUCT_CODE LIKE '%8905287884867%' OR PRODUCT_CODE LIKE '%8905514816920%' OR PRODUCT_CODE LIKE '%8905514816937%' OR PRODUCT_CODE LIKE '%8905514816944%' OR PRODUCT_CODE LIKE '%8905514695105%' OR PRODUCT_CODE LIKE '%8905514695112%' OR PRODUCT_CODE LIKE '%8905514695129%' OR PRODUCT_CODE LIKE '%8905514695136%' OR PRODUCT_CODE LIKE '%8905287885239%' OR PRODUCT_CODE LIKE '%8905287885246%' OR PRODUCT_CODE LIKE '%8905287885253%' OR PRODUCT_CODE LIKE '%8905287885260%' OR PRODUCT_CODE LIKE '%8905514853819%' OR PRODUCT_CODE LIKE '%8905514853826%' OR PRODUCT_CODE LIKE '%8905514695310%' OR PRODUCT_CODE LIKE '%8905514695327%' OR PRODUCT_CODE LIKE '%8905514853871%' OR PRODUCT_CODE LIKE '%8905514853888%' OR PRODUCT_CODE LIKE '%8905514853895%' OR PRODUCT_CODE LIKE '%8905514695471%' OR PRODUCT_CODE LIKE '%8905514695488%' OR PRODUCT_CODE LIKE '%8905514695495%' OR PRODUCT_CODE LIKE '%8905514695693%' OR PRODUCT_CODE LIKE '%8905514695709%' OR PRODUCT_CODE LIKE '%8905514695723%' OR PRODUCT_CODE LIKE '%8905514756448%' OR PRODUCT_CODE LIKE '%8905514756530%' OR PRODUCT_CODE LIKE '%8905514756547%' OR PRODUCT_CODE LIKE '%8905772196642%' OR PRODUCT_CODE LIKE '%8905772196659%' OR PRODUCT_CODE LIKE '%8905287912324%' OR PRODUCT_CODE LIKE '%8905287912331%' OR PRODUCT_CODE LIKE '%8905287912348%' OR PRODUCT_CODE LIKE '%8905287912355%' OR PRODUCT_CODE LIKE '%8905772197014%' OR PRODUCT_CODE LIKE '%8905772197038%' OR PRODUCT_CODE LIKE '%8905772197670%' OR PRODUCT_CODE LIKE '%8905772197694%' OR PRODUCT_CODE LIKE '%8905772117838%' OR PRODUCT_CODE LIKE '%8905772117845%' OR PRODUCT_CODE LIKE '%8905772118248%' OR PRODUCT_CODE LIKE '%8905772118255%' OR PRODUCT_CODE LIKE '%8905772118538%' OR PRODUCT_CODE LIKE '%8905772118552%' OR PRODUCT_CODE LIKE '%8905514600994%' OR PRODUCT_CODE LIKE '%8905514601014%' OR PRODUCT_CODE LIKE '%8905514601090%' OR PRODUCT_CODE LIKE '%8905514601106%' OR PRODUCT_CODE LIKE '%8905514601113%' OR PRODUCT_CODE LIKE '%8905514601120%' OR PRODUCT_CODE LIKE '%8905514133355%' OR PRODUCT_CODE LIKE '%8905772177344%' OR PRODUCT_CODE LIKE '%8905514133621%' OR PRODUCT_CODE LIKE '%8905514601663%' OR PRODUCT_CODE LIKE '%8905287798638%' OR PRODUCT_CODE LIKE '%8905287798645%' OR PRODUCT_CODE LIKE '%8905287798652%' OR PRODUCT_CODE LIKE '%8905514313405%' OR PRODUCT_CODE LIKE '%8905514133201%' OR PRODUCT_CODE LIKE '%8905514597225%' OR PRODUCT_CODE LIKE '%8905514597232%' OR PRODUCT_CODE LIKE '%8905514597249%' OR PRODUCT_CODE LIKE '%8905514235943%' OR PRODUCT_CODE LIKE '%8905514235950%' OR PRODUCT_CODE LIKE '%8905514235967%' OR PRODUCT_CODE LIKE '%8905514774213%' OR PRODUCT_CODE LIKE '%8905514774220%' OR PRODUCT_CODE LIKE '%8905514617428%' OR PRODUCT_CODE LIKE '%8905514617435%' OR PRODUCT_CODE LIKE '%8905514681504%' OR PRODUCT_CODE LIKE '%8905514681511%' OR PRODUCT_CODE LIKE '%8905152360649%' OR PRODUCT_CODE LIKE '%8905152360656%' OR PRODUCT_CODE LIKE '%8905152360663%' OR PRODUCT_CODE LIKE '%8905152360670%' OR PRODUCT_CODE LIKE '%8905514602035%' OR PRODUCT_CODE LIKE '%8905514602042%' OR PRODUCT_CODE LIKE '%8905514293394%' OR PRODUCT_CODE LIKE '%8905514793054%' OR PRODUCT_CODE LIKE '%8905514921273%' OR PRODUCT_CODE LIKE '%8905772047845%' OR PRODUCT_CODE LIKE '%8905772047852%' OR PRODUCT_CODE LIKE '%8905772047869%' OR PRODUCT_CODE LIKE '%8905772047999%' OR PRODUCT_CODE LIKE '%8905514793061%' OR PRODUCT_CODE LIKE '%8905514793047%' OR PRODUCT_CODE LIKE '%8905514862989%' OR PRODUCT_CODE LIKE '%8905514862996%' OR PRODUCT_CODE LIKE '%8905514863009%' OR PRODUCT_CODE LIKE '%8905514252308%' OR PRODUCT_CODE LIKE '%8905514252315%' OR PRODUCT_CODE LIKE '%8905514252322%' OR PRODUCT_CODE LIKE '%8905514252339%' OR PRODUCT_CODE LIKE '%8905287512777%' OR PRODUCT_CODE LIKE '%8905514489278%' OR PRODUCT_CODE LIKE '%8905287992043%' OR PRODUCT_CODE LIKE '%8905287992036%' OR PRODUCT_CODE LIKE '%8905287992050%' OR PRODUCT_CODE LIKE '%8905514321981%' OR PRODUCT_CODE LIKE '%8905514863979%' OR PRODUCT_CODE LIKE '%8905514863986%' OR PRODUCT_CODE LIKE '%8905514863993%' OR PRODUCT_CODE LIKE '%8905514864006%' OR PRODUCT_CODE LIKE '%8905514236209%' OR PRODUCT_CODE LIKE '%8905514236216%' OR PRODUCT_CODE LIKE '%8905514236445%' OR PRODUCT_CODE LIKE '%8905514236452%' OR PRODUCT_CODE LIKE '%8905514236469%' OR PRODUCT_CODE LIKE '%8905514774718%' OR PRODUCT_CODE LIKE '%8905514774725%' OR PRODUCT_CODE LIKE '%8905514774732%' OR PRODUCT_CODE LIKE '%8905514504513%' OR PRODUCT_CODE LIKE '%8905514775111%' OR PRODUCT_CODE LIKE '%8905514775128%' OR PRODUCT_CODE LIKE '%8905514775135%' OR PRODUCT_CODE LIKE '%8905772337649%' OR PRODUCT_CODE LIKE '%8905772337656%' OR PRODUCT_CODE LIKE '%8905772337663%' OR PRODUCT_CODE LIKE '%8905772337717%' OR PRODUCT_CODE LIKE '%8905772337724%' OR PRODUCT_CODE LIKE '%8905772337731%' OR PRODUCT_CODE LIKE '%8905772337922%' OR PRODUCT_CODE LIKE '%8905772337939%' OR PRODUCT_CODE LIKE '%8905772337946%' OR PRODUCT_CODE LIKE '%8905514604688%' OR PRODUCT_CODE LIKE '%8905514604695%' OR PRODUCT_CODE LIKE '%8905514604701%' OR PRODUCT_CODE LIKE '%8905772338257%' OR PRODUCT_CODE LIKE '%8905514604763%' OR PRODUCT_CODE LIKE '%8905514604770%' OR PRODUCT_CODE LIKE '%8905514537962%' OR PRODUCT_CODE LIKE '%8905514537979%' OR PRODUCT_CODE LIKE '%8905514813943%' OR PRODUCT_CODE LIKE '%8905514813967%' OR PRODUCT_CODE LIKE '%8905514813974%' OR PRODUCT_CODE LIKE '%8905514504629%' OR PRODUCT_CODE LIKE '%8905514775678%' OR PRODUCT_CODE LIKE '%8905514775685%' OR PRODUCT_CODE LIKE '%8905514504650%' OR PRODUCT_CODE LIKE '%8905514775807%' OR PRODUCT_CODE LIKE '%8905514775814%' OR PRODUCT_CODE LIKE '%8905514504674%' OR PRODUCT_CODE LIKE '%8905514775906%' OR PRODUCT_CODE LIKE '%8905514775913%' OR PRODUCT_CODE LIKE '%8905514775920%' OR PRODUCT_CODE LIKE '%8905514504711%' OR PRODUCT_CODE LIKE '%8905514537986%' OR PRODUCT_CODE LIKE '%8905514537993%' OR PRODUCT_CODE LIKE '%8905514538006%' OR PRODUCT_CODE LIKE '%8905514504728%' OR PRODUCT_CODE LIKE '%8905514538037%' OR PRODUCT_CODE LIKE '%8905514538051%' OR PRODUCT_CODE LIKE '%8905514237541%' OR PRODUCT_CODE LIKE '%8905514237558%' OR PRODUCT_CODE LIKE '%8905514237565%' OR PRODUCT_CODE LIKE '%8905514237985%' OR PRODUCT_CODE LIKE '%8905514237992%' OR PRODUCT_CODE LIKE '%8905514238166%' OR PRODUCT_CODE LIKE '%8905514787619%' OR PRODUCT_CODE LIKE '%8905772179737%' OR PRODUCT_CODE LIKE '%8905772032452%' OR PRODUCT_CODE LIKE '%8905772032469%' OR PRODUCT_CODE LIKE '%8905772032476%' OR PRODUCT_CODE LIKE '%8905772032483%' OR PRODUCT_CODE LIKE '%8905772032490%' OR PRODUCT_CODE LIKE '%8905514520766%' OR PRODUCT_CODE LIKE '%8905514263700%' OR PRODUCT_CODE LIKE '%8905514263717%' OR PRODUCT_CODE LIKE '%8905514494784%' OR PRODUCT_CODE LIKE '%8905514494791%' OR PRODUCT_CODE LIKE '%8905514264141%' OR PRODUCT_CODE LIKE '%8905514256740%' OR PRODUCT_CODE LIKE '%8905514825953%' OR PRODUCT_CODE LIKE '%8905514834436%' OR PRODUCT_CODE LIKE '%8905514834443%' OR PRODUCT_CODE LIKE '%8905514834450%' OR PRODUCT_CODE LIKE '%8905514558554%' OR PRODUCT_CODE LIKE '%8905514558561%' OR PRODUCT_CODE LIKE '%8905514403649%' OR PRODUCT_CODE LIKE '%8905514839189%' OR PRODUCT_CODE LIKE '%8905514839196%' OR PRODUCT_CODE LIKE '%8905772183994%' OR PRODUCT_CODE LIKE '%8905772184007%' OR PRODUCT_CODE LIKE '%8905772184014%' OR PRODUCT_CODE LIKE '%8905772050203%' OR PRODUCT_CODE LIKE '%8905772050210%' OR PRODUCT_CODE LIKE '%8905514852287%' OR PRODUCT_CODE LIKE '%8905514858081%' OR PRODUCT_CODE LIKE '%8905514858098%' OR PRODUCT_CODE LIKE '%8905514840383%' OR PRODUCT_CODE LIKE '%8905514817811%' OR PRODUCT_CODE LIKE '%8905514817828%' OR PRODUCT_CODE LIKE '%8905514835563%' OR PRODUCT_CODE LIKE '%8905514835570%' OR PRODUCT_CODE LIKE '%8905514835556%' OR PRODUCT_CODE LIKE '%8905514844046%' OR PRODUCT_CODE LIKE '%8905514844053%' OR PRODUCT_CODE LIKE '%8905514844060%' OR PRODUCT_CODE LIKE '%8905514844077%' OR PRODUCT_CODE LIKE '%8905514817880%' OR PRODUCT_CODE LIKE '%8905514819297%' OR PRODUCT_CODE LIKE '%8905514819303%' OR PRODUCT_CODE LIKE '%8905514819310%' OR PRODUCT_CODE LIKE '%8905514839653%' OR PRODUCT_CODE LIKE '%8905514839660%' OR PRODUCT_CODE LIKE '%8905514839677%' OR PRODUCT_CODE LIKE '%8905514844176%' OR PRODUCT_CODE LIKE '%8905514844183%' OR PRODUCT_CODE LIKE '%8905514757520%' OR PRODUCT_CODE LIKE '%8905514868813%' OR PRODUCT_CODE LIKE '%8905514860169%' OR PRODUCT_CODE LIKE '%8905514860176%' OR PRODUCT_CODE LIKE '%8905514860183%' OR PRODUCT_CODE LIKE '%8905514860145%' OR PRODUCT_CODE LIKE '%8905514860220%' OR PRODUCT_CODE LIKE '%8905514860237%' OR PRODUCT_CODE LIKE '%8905514819433%' OR PRODUCT_CODE LIKE '%8905514819419%' OR PRODUCT_CODE LIKE '%8905514818849%' OR PRODUCT_CODE LIKE '%8905514917368%' OR PRODUCT_CODE LIKE '%8905514926988%' OR PRODUCT_CODE LIKE '%8905514926995%' OR PRODUCT_CODE LIKE '%8905514927008%' OR PRODUCT_CODE LIKE '%8905514927015%' OR PRODUCT_CODE LIKE '%8905514917375%' OR PRODUCT_CODE LIKE '%8905514917382%' OR PRODUCT_CODE LIKE '%8905514917399%' OR PRODUCT_CODE LIKE '%8905514917405%' OR PRODUCT_CODE LIKE '%8905514921280%' OR PRODUCT_CODE LIKE '%8905514917474%' OR PRODUCT_CODE LIKE '%8905514917481%' OR PRODUCT_CODE LIKE '%8905514917498%' OR PRODUCT_CODE LIKE '%8905514917504%' OR PRODUCT_CODE LIKE '%8905772079952%' OR PRODUCT_CODE LIKE '%8905772079969%' OR PRODUCT_CODE LIKE '%8905772082273%' OR PRODUCT_CODE LIKE '%8905772082297%' OR PRODUCT_CODE LIKE '%8905772160551%' OR PRODUCT_CODE LIKE '%8905514528960%' OR PRODUCT_CODE LIKE '%8905514528991%' OR PRODUCT_CODE LIKE '%8905514921457%' OR PRODUCT_CODE LIKE '%8905514921464%' OR PRODUCT_CODE LIKE '%8905514529028%' OR PRODUCT_CODE LIKE '%8905514529011%' OR PRODUCT_CODE LIKE '%8905514484228%' OR PRODUCT_CODE LIKE '%8905514506777%' OR PRODUCT_CODE LIKE '%8905514506784%' OR PRODUCT_CODE LIKE '%8905514506791%' OR PRODUCT_CODE LIKE '%8905514506845%' OR PRODUCT_CODE LIKE '%8905514506821%' OR PRODUCT_CODE LIKE '%8905514506838%' OR PRODUCT_CODE LIKE '%8905772114110%' OR PRODUCT_CODE LIKE '%8905772114073%' OR PRODUCT_CODE LIKE '%8905514529226%' OR PRODUCT_CODE LIKE '%8905514529233%' OR PRODUCT_CODE LIKE '%8905514844329%' OR PRODUCT_CODE LIKE '%8905514529288%' OR PRODUCT_CODE LIKE '%8905514354071%' OR PRODUCT_CODE LIKE '%8905514756820%' OR PRODUCT_CODE LIKE '%8905514755380%' OR PRODUCT_CODE LIKE '%8905772180917%' OR PRODUCT_CODE LIKE '%8905772180924%' OR PRODUCT_CODE LIKE '%8905772180931%' OR PRODUCT_CODE LIKE '%8905514520896%' OR PRODUCT_CODE LIKE '%8905514520902%' OR PRODUCT_CODE LIKE '%8905514354347%' OR PRODUCT_CODE LIKE '%8905514529547%' OR PRODUCT_CODE LIKE '%8905514484464%' OR PRODUCT_CODE LIKE '%8905514521213%' OR PRODUCT_CODE LIKE '%8905514521237%' OR PRODUCT_CODE LIKE '%8905514529745%' OR PRODUCT_CODE LIKE '%8905772212977%' OR PRODUCT_CODE LIKE '%8905772212984%' OR PRODUCT_CODE LIKE '%8905772212991%' OR PRODUCT_CODE LIKE '%8905772222150%' OR PRODUCT_CODE LIKE '%8905772222167%' OR PRODUCT_CODE LIKE '%8905772222174%' OR PRODUCT_CODE LIKE '%8905772222198%' OR PRODUCT_CODE LIKE '%8905772208420%' OR PRODUCT_CODE LIKE '%8905772208437%' OR PRODUCT_CODE LIKE '%8905772092395%' OR PRODUCT_CODE LIKE '%8905772092401%' OR PRODUCT_CODE LIKE '%8905772092418%' OR PRODUCT_CODE LIKE '%8905772092456%' OR PRODUCT_CODE LIKE '%8905772092463%' OR PRODUCT_CODE LIKE '%8905772092470%' OR PRODUCT_CODE LIKE '%8905772178174%' OR PRODUCT_CODE LIKE '%8905514020563%' OR PRODUCT_CODE LIKE '%8905514872001%' OR PRODUCT_CODE LIKE '%8905514872025%' OR PRODUCT_CODE LIKE '%8905514872032%' OR PRODUCT_CODE LIKE '%8905514994918%' OR PRODUCT_CODE LIKE '%8905514530369%' OR PRODUCT_CODE LIKE '%8905514530390%' OR PRODUCT_CODE LIKE '%8905514530987%' OR PRODUCT_CODE LIKE '%8905772016612%' OR PRODUCT_CODE LIKE '%8905772016636%' OR PRODUCT_CODE LIKE '%8905772016643%' OR PRODUCT_CODE LIKE '%8905514531038%' OR PRODUCT_CODE LIKE '%8905772143103%' OR PRODUCT_CODE LIKE '%8905772143110%' OR PRODUCT_CODE LIKE '%8905514998572%' OR PRODUCT_CODE LIKE '%8905514998589%' OR PRODUCT_CODE LIKE '%8905514998596%' OR PRODUCT_CODE LIKE '%8905514742700%' OR PRODUCT_CODE LIKE '%8905514934075%' OR PRODUCT_CODE LIKE '%8905514934082%' OR PRODUCT_CODE LIKE '%8905772092944%' OR PRODUCT_CODE LIKE '%8905772139373%' OR PRODUCT_CODE LIKE '%8905772139380%' OR PRODUCT_CODE LIKE '%8905772139397%' OR PRODUCT_CODE LIKE '%8905772141307%' OR PRODUCT_CODE LIKE '%8905514957807%' OR PRODUCT_CODE LIKE '%8905514532110%' OR PRODUCT_CODE LIKE '%8905514532127%' OR PRODUCT_CODE LIKE '%8905772302722%' OR PRODUCT_CODE LIKE '%8905772302739%' OR PRODUCT_CODE LIKE '%8905772302746%' OR PRODUCT_CODE LIKE '%8905772302753%' OR PRODUCT_CODE LIKE '%8905514919843%' OR PRODUCT_CODE LIKE '%8905514919850%' OR PRODUCT_CODE LIKE '%8905514919867%' OR PRODUCT_CODE LIKE '%8905514505565%' OR PRODUCT_CODE LIKE '%8905514532608%' OR PRODUCT_CODE LIKE '%8905514532622%' OR PRODUCT_CODE LIKE '%8905514104348%' OR PRODUCT_CODE LIKE '%8905772265157%' OR PRODUCT_CODE LIKE '%8905514194011%' OR PRODUCT_CODE LIKE '%8905514999081%' OR PRODUCT_CODE LIKE '%8905514999098%' OR PRODUCT_CODE LIKE '%8905514999111%' OR PRODUCT_CODE LIKE '%8905514872162%' OR PRODUCT_CODE LIKE '%8905772093972%' OR PRODUCT_CODE LIKE '%8905772093989%' OR PRODUCT_CODE LIKE '%8905514450902%' OR PRODUCT_CODE LIKE '%8905514450889%' OR PRODUCT_CODE LIKE '%8905514450896%' OR PRODUCT_CODE LIKE '%8905514872261%' OR PRODUCT_CODE LIKE '%8905772141505%' OR PRODUCT_CODE LIKE '%8905772141512%' OR PRODUCT_CODE LIKE '%8905772141529%' OR PRODUCT_CODE LIKE '%8905772141536%' OR PRODUCT_CODE LIKE '%8905772141543%' OR PRODUCT_CODE LIKE '%8905514700779%' OR PRODUCT_CODE LIKE '%8905514700762%' OR PRODUCT_CODE LIKE '%8905514534107%' OR PRODUCT_CODE LIKE '%8905514999272%' OR PRODUCT_CODE LIKE '%8905772094382%' OR PRODUCT_CODE LIKE '%8905772094399%' OR PRODUCT_CODE LIKE '%8905514743752%' OR PRODUCT_CODE LIKE '%8905514743769%' OR PRODUCT_CODE LIKE '%8905514743776%' OR PRODUCT_CODE LIKE '%8905514743783%' OR PRODUCT_CODE LIKE '%8905514856391%' OR PRODUCT_CODE LIKE '%8905514856407%' OR PRODUCT_CODE LIKE '%8905514856421%' OR PRODUCT_CODE LIKE '%8905772094559%' OR PRODUCT_CODE LIKE '%8905772094573%' OR PRODUCT_CODE LIKE '%8905514534879%' OR PRODUCT_CODE LIKE '%8905514537825%' OR PRODUCT_CODE LIKE '%8905514241456%' OR PRODUCT_CODE LIKE '%8905514241487%' OR PRODUCT_CODE LIKE '%8905772058544%' OR PRODUCT_CODE LIKE '%8905772058551%' OR PRODUCT_CODE LIKE '%8905772058575%' OR PRODUCT_CODE LIKE '%8905772058582%' OR PRODUCT_CODE LIKE '%8905514588766%' OR PRODUCT_CODE LIKE '%8905514760520%' OR PRODUCT_CODE LIKE '%8905514760537%' OR PRODUCT_CODE LIKE '%8905514569598%' OR PRODUCT_CODE LIKE '%8905514569611%' OR PRODUCT_CODE LIKE '%8905514506326%' OR PRODUCT_CODE LIKE '%8905287505298%' OR PRODUCT_CODE LIKE '%8905287505304%' OR PRODUCT_CODE LIKE '%8905514869834%' OR PRODUCT_CODE LIKE '%8905514869841%' OR PRODUCT_CODE LIKE '%8905514869858%' OR PRODUCT_CODE LIKE '%8905514869902%' OR PRODUCT_CODE LIKE '%8905514869919%' OR PRODUCT_CODE LIKE '%8905514869926%' OR PRODUCT_CODE LIKE '%8905514776477%' OR PRODUCT_CODE LIKE '%8905514776484%' OR PRODUCT_CODE LIKE '%8905514618852%' OR PRODUCT_CODE LIKE '%8905514618845%' OR PRODUCT_CODE LIKE '%8905514618869%' OR PRODUCT_CODE LIKE '%8905514712062%' OR PRODUCT_CODE LIKE '%8905514958729%' OR PRODUCT_CODE LIKE '%8905772022002%' OR PRODUCT_CODE LIKE '%8905772022019%' OR PRODUCT_CODE LIKE '%8905514958835%' OR PRODUCT_CODE LIKE '%8905514958842%' OR PRODUCT_CODE LIKE '%8905514386867%' OR PRODUCT_CODE LIKE '%8905514386881%' OR PRODUCT_CODE LIKE '%8905772009072%' OR PRODUCT_CODE LIKE '%8905772019224%' OR PRODUCT_CODE LIKE '%8905772019231%' OR PRODUCT_CODE LIKE '%8905772019248%' OR PRODUCT_CODE LIKE '%8905772019521%' OR PRODUCT_CODE LIKE '%8905772019538%' OR PRODUCT_CODE LIKE '%8905772019545%' OR PRODUCT_CODE LIKE '%8905772019644%' OR PRODUCT_CODE LIKE '%8905772019651%' OR PRODUCT_CODE LIKE '%8905772019668%' OR PRODUCT_CODE LIKE '%8905772019767%' OR PRODUCT_CODE LIKE '%8905772019774%' OR PRODUCT_CODE LIKE '%8905772019781%' OR PRODUCT_CODE LIKE '%8905772019798%' OR PRODUCT_CODE LIKE '%8905772020602%' OR PRODUCT_CODE LIKE '%8905772020619%' OR PRODUCT_CODE LIKE '%8905514137650%' OR PRODUCT_CODE LIKE '%8905514137674%' OR PRODUCT_CODE LIKE '%8905514137681%' OR PRODUCT_CODE LIKE '%8905514137711%' OR PRODUCT_CODE LIKE '%8905514137728%' OR PRODUCT_CODE LIKE '%8905514137889%' OR PRODUCT_CODE LIKE '%8905514137957%' OR PRODUCT_CODE LIKE '%8905514138060%' OR PRODUCT_CODE LIKE '%8905514138138%' OR PRODUCT_CODE LIKE '%8905514138169%' OR PRODUCT_CODE LIKE '%8905514138220%' OR PRODUCT_CODE LIKE '%8905514138244%' OR PRODUCT_CODE LIKE '%8905514446394%' OR PRODUCT_CODE LIKE '%8905514446424%' OR PRODUCT_CODE LIKE '%8905514911007%' OR PRODUCT_CODE LIKE '%8905514911014%' OR PRODUCT_CODE LIKE '%8905514911021%' OR PRODUCT_CODE LIKE '%8905514911038%' OR PRODUCT_CODE LIKE '%8905514911069%' OR PRODUCT_CODE LIKE '%8905514911076%' OR PRODUCT_CODE LIKE '%8905514911083%' OR PRODUCT_CODE LIKE '%8905514466156%' OR PRODUCT_CODE LIKE '%8905514911137%' OR PRODUCT_CODE LIKE '%8905514911144%' OR PRODUCT_CODE LIKE '%8905514911182%' OR PRODUCT_CODE LIKE '%8905514911199%' OR PRODUCT_CODE LIKE '%8905514911205%' OR PRODUCT_CODE LIKE '%8905514911212%' OR PRODUCT_CODE LIKE '%8905514911175%' OR PRODUCT_CODE LIKE '%8905514911243%' OR PRODUCT_CODE LIKE '%8905514911274%' OR PRODUCT_CODE LIKE '%8905514911588%' OR PRODUCT_CODE LIKE '%8905514911595%' OR PRODUCT_CODE LIKE '%8905514911601%' OR PRODUCT_CODE LIKE '%8905514911618%' OR PRODUCT_CODE LIKE '%8905514911571%' OR PRODUCT_CODE LIKE '%8905514446455%' OR PRODUCT_CODE LIKE '%8905514446479%' OR PRODUCT_CODE LIKE '%8905514911939%' OR PRODUCT_CODE LIKE '%8905514911946%' OR PRODUCT_CODE LIKE '%8905514911953%' OR PRODUCT_CODE LIKE '%8905514911960%' OR PRODUCT_CODE LIKE '%8905514911991%' OR PRODUCT_CODE LIKE '%8905514912004%' OR PRODUCT_CODE LIKE '%8905514912011%' OR PRODUCT_CODE LIKE '%8905514912028%' OR PRODUCT_CODE LIKE '%8905514912295%' OR PRODUCT_CODE LIKE '%8905514912301%' OR PRODUCT_CODE LIKE '%8905514912349%' OR PRODUCT_CODE LIKE '%8905514912356%' OR PRODUCT_CODE LIKE '%8905514912363%' OR PRODUCT_CODE LIKE '%8905514912370%' OR PRODUCT_CODE LIKE '%8905514912332%' OR PRODUCT_CODE LIKE '%8905514912509%' OR PRODUCT_CODE LIKE '%8905514912530%' OR PRODUCT_CODE LIKE '%8905514912516%' OR PRODUCT_CODE LIKE '%8905514912523%' OR PRODUCT_CODE LIKE '%8905514912622%' OR PRODUCT_CODE LIKE '%8905514912639%' OR PRODUCT_CODE LIKE '%8905514912646%' OR PRODUCT_CODE LIKE '%8905514912653%' OR PRODUCT_CODE LIKE '%8905514912790%' OR PRODUCT_CODE LIKE '%8905514912813%' OR PRODUCT_CODE LIKE '%8905514912851%' OR PRODUCT_CODE LIKE '%8905514912868%' OR PRODUCT_CODE LIKE '%8905514912875%' OR PRODUCT_CODE LIKE '%8905514912882%' OR PRODUCT_CODE LIKE '%8905514912905%' OR PRODUCT_CODE LIKE '%8905514912912%' OR PRODUCT_CODE LIKE '%8905514912943%' OR PRODUCT_CODE LIKE '%8905514912974%' OR PRODUCT_CODE LIKE '%8905514912981%' OR PRODUCT_CODE LIKE '%8905514913155%' OR PRODUCT_CODE LIKE '%8905514913162%' OR PRODUCT_CODE LIKE '%8905514913179%' OR PRODUCT_CODE LIKE '%8905514913186%' OR PRODUCT_CODE LIKE '%8905514913278%' OR PRODUCT_CODE LIKE '%8905514913285%' OR PRODUCT_CODE LIKE '%8905514913292%' OR PRODUCT_CODE LIKE '%8905514913308%' OR PRODUCT_CODE LIKE '%8905514913407%' OR PRODUCT_CODE LIKE '%8905514913414%' OR PRODUCT_CODE LIKE '%8905514913421%' OR PRODUCT_CODE LIKE '%8905514469935%' OR PRODUCT_CODE LIKE '%8905514469942%' OR PRODUCT_CODE LIKE '%8905514913568%' OR PRODUCT_CODE LIKE '%8905514913582%' OR PRODUCT_CODE LIKE '%8905514913599%' OR PRODUCT_CODE LIKE '%8905514471495%' OR PRODUCT_CODE LIKE '%8905514471501%' OR PRODUCT_CODE LIKE '%8905514471518%' OR PRODUCT_CODE LIKE '%8905514471525%' OR PRODUCT_CODE LIKE '%8905772020916%' OR PRODUCT_CODE LIKE '%8905772020893%' OR PRODUCT_CODE LIKE '%8905772020909%' OR PRODUCT_CODE LIKE '%8905514981505%' OR PRODUCT_CODE LIKE '%8905514471730%' OR PRODUCT_CODE LIKE '%8905514471747%' OR PRODUCT_CODE LIKE '%8905514471754%' OR PRODUCT_CODE LIKE '%8905772020961%' OR PRODUCT_CODE LIKE '%8905772020978%' OR PRODUCT_CODE LIKE '%8905514981734%' OR PRODUCT_CODE LIKE '%8905514981741%' OR PRODUCT_CODE LIKE '%8905514981758%' OR PRODUCT_CODE LIKE '%8905514981765%' OR PRODUCT_CODE LIKE '%8905772021012%' OR PRODUCT_CODE LIKE '%8905772021029%' OR PRODUCT_CODE LIKE '%8905772021036%' OR PRODUCT_CODE LIKE '%8905772021043%' OR PRODUCT_CODE LIKE '%8905772021074%' OR PRODUCT_CODE LIKE '%8905772021081%' OR PRODUCT_CODE LIKE '%8905772021098%' OR PRODUCT_CODE LIKE '%8905772021104%' OR PRODUCT_CODE LIKE '%8905514981925%' OR PRODUCT_CODE LIKE '%8905287822029%' OR PRODUCT_CODE LIKE '%8905287822036%' OR PRODUCT_CODE LIKE '%8905287822043%' OR PRODUCT_CODE LIKE '%8905287822050%' OR PRODUCT_CODE LIKE '%8905287822074%' OR PRODUCT_CODE LIKE '%8905287822081%' OR PRODUCT_CODE LIKE '%8905287822104%' OR PRODUCT_CODE LIKE '%8905287822111%' OR PRODUCT_CODE LIKE '%8905287934975%' OR PRODUCT_CODE LIKE '%8905287934982%' OR PRODUCT_CODE LIKE '%8905287934999%' OR PRODUCT_CODE LIKE '%8905287935019%' OR PRODUCT_CODE LIKE '%8905287935156%' OR PRODUCT_CODE LIKE '%8905287935163%' OR PRODUCT_CODE LIKE '%8905287935170%' OR PRODUCT_CODE LIKE '%8905287935187%' OR PRODUCT_CODE LIKE '%8905287935194%' OR PRODUCT_CODE LIKE '%8905287935392%' OR PRODUCT_CODE LIKE '%8905287935408%' OR PRODUCT_CODE LIKE '%8905287935415%' OR PRODUCT_CODE LIKE '%8905287822135%' OR PRODUCT_CODE LIKE '%8905287822142%' OR PRODUCT_CODE LIKE '%8905287822159%' OR PRODUCT_CODE LIKE '%8905287822166%' OR PRODUCT_CODE LIKE '%8905287822173%' OR PRODUCT_CODE LIKE '%8905514583280%' OR PRODUCT_CODE LIKE '%8905514583303%' OR PRODUCT_CODE LIKE '%8905514583341%' OR PRODUCT_CODE LIKE '%8905287936290%' OR PRODUCT_CODE LIKE '%8905287936306%' OR PRODUCT_CODE LIKE '%8905287936313%' OR PRODUCT_CODE LIKE '%8905287936320%' OR PRODUCT_CODE LIKE '%8905287936337%' OR PRODUCT_CODE LIKE '%8905287936474%' OR PRODUCT_CODE LIKE '%8905287936481%' OR PRODUCT_CODE LIKE '%8905287936504%' OR PRODUCT_CODE LIKE '%8905287936511%' OR PRODUCT_CODE LIKE '%8905287936498%' OR PRODUCT_CODE LIKE '%8905514472843%' OR PRODUCT_CODE LIKE '%8905514472850%' OR PRODUCT_CODE LIKE '%8905514472867%' OR PRODUCT_CODE LIKE '%8905514472874%' OR PRODUCT_CODE LIKE '%8905772485456%' OR PRODUCT_CODE LIKE '%8905772485463%' OR PRODUCT_CODE LIKE '%8905772485470%' OR PRODUCT_CODE LIKE '%8905772487252%' OR PRODUCT_CODE LIKE '%8905772487269%' OR PRODUCT_CODE LIKE '%8905514948843%' OR PRODUCT_CODE LIKE '%8905514948850%' OR PRODUCT_CODE LIKE '%8905514948911%' OR PRODUCT_CODE LIKE '%8905514949031%' OR PRODUCT_CODE LIKE '%8905514949154%' OR PRODUCT_CODE LIKE '%8905514949277%' OR PRODUCT_CODE LIKE '%8905514949284%' OR PRODUCT_CODE LIKE '%8905514583327%' OR PRODUCT_CODE LIKE '%8905514802930%' OR PRODUCT_CODE LIKE '%8905514802947%' OR PRODUCT_CODE LIKE '%8905514802954%' OR PRODUCT_CODE LIKE '%8905514802961%' OR PRODUCT_CODE LIKE '%8905514803364%' OR PRODUCT_CODE LIKE '%8905514803388%' OR PRODUCT_CODE LIKE '%8905514803593%' OR PRODUCT_CODE LIKE '%8905514803616%' OR PRODUCT_CODE LIKE '%8905514803623%' OR PRODUCT_CODE LIKE '%8905514803784%' OR PRODUCT_CODE LIKE '%8905514803791%' OR PRODUCT_CODE LIKE '%8905514803838%' OR PRODUCT_CODE LIKE '%8905514803845%' OR PRODUCT_CODE LIKE '%8905514803869%' OR PRODUCT_CODE LIKE '%8905514804255%' OR PRODUCT_CODE LIKE '%8905514804262%' OR PRODUCT_CODE LIKE '%8905514804316%' OR PRODUCT_CODE LIKE '%8905514804392%' OR PRODUCT_CODE LIKE '%8905152063250%' OR PRODUCT_CODE LIKE '%8905152073877%' OR PRODUCT_CODE LIKE '%8905514949598%' OR PRODUCT_CODE LIKE '%8905514949604%' OR PRODUCT_CODE LIKE '%8905514949628%' OR PRODUCT_CODE LIKE '%8905514282930%' OR PRODUCT_CODE LIKE '%8905287903261%' OR PRODUCT_CODE LIKE '%8905287990605%' OR PRODUCT_CODE LIKE '%8905514283135%' OR PRODUCT_CODE LIKE '%8905514283142%' OR PRODUCT_CODE LIKE '%8905772068703%' OR PRODUCT_CODE LIKE '%8905772068710%' OR PRODUCT_CODE LIKE '%8905772068727%' OR PRODUCT_CODE LIKE '%8905772068734%' OR PRODUCT_CODE LIKE '%8905514954295%' OR PRODUCT_CODE LIKE '%8905772261593%' OR PRODUCT_CODE LIKE '%8905772261623%' OR PRODUCT_CODE LIKE '%8905772265188%' OR PRODUCT_CODE LIKE '%8905514920856%' OR PRODUCT_CODE LIKE '%8905514920832%' OR PRODUCT_CODE LIKE '%8905514920849%' OR PRODUCT_CODE LIKE '%8905514903651%' OR PRODUCT_CODE LIKE '%8905772188463%' OR PRODUCT_CODE LIKE '%8905772188470%' OR PRODUCT_CODE LIKE '%8905772188487%' OR PRODUCT_CODE LIKE '%8905514845647%' OR PRODUCT_CODE LIKE '%8905514845654%' OR PRODUCT_CODE LIKE '%8905514845661%' OR PRODUCT_CODE LIKE '%8905514845678%' OR PRODUCT_CODE LIKE '%8905514992358%' OR PRODUCT_CODE LIKE '%8905772188517%' OR PRODUCT_CODE LIKE '%8905772188524%' OR PRODUCT_CODE LIKE '%8905772188531%' OR PRODUCT_CODE LIKE '%8905514919607%' OR PRODUCT_CODE LIKE '%8905514924526%' OR PRODUCT_CODE LIKE '%8905514924533%' OR PRODUCT_CODE LIKE '%8905514924540%' OR PRODUCT_CODE LIKE '%8905514924557%' OR PRODUCT_CODE LIKE '%8905287903520%' OR PRODUCT_CODE LIKE '%8905287989715%' OR PRODUCT_CODE LIKE '%8905287903575%' OR PRODUCT_CODE LIKE '%8905287995525%' OR PRODUCT_CODE LIKE '%8905514283821%' OR PRODUCT_CODE LIKE '%8905514283838%' OR PRODUCT_CODE LIKE '%8905514283845%' OR PRODUCT_CODE LIKE '%8905514283852%' OR PRODUCT_CODE LIKE '%8907983881591%' OR PRODUCT_CODE LIKE '%8907983892702%' OR PRODUCT_CODE LIKE '%8905152046260%' OR PRODUCT_CODE LIKE '%8907983928753%' OR PRODUCT_CODE LIKE '%8905152491077%' OR PRODUCT_CODE LIKE '%8905514344843%' OR PRODUCT_CODE LIKE '%8905287555309%' OR PRODUCT_CODE LIKE '%8905287783641%' OR PRODUCT_CODE LIKE '%8905514598475%' OR PRODUCT_CODE LIKE '%8905514594682%' OR PRODUCT_CODE LIKE '%8905514594699%' OR PRODUCT_CODE LIKE '%8905514594705%' OR PRODUCT_CODE LIKE '%8905514594712%' OR PRODUCT_CODE LIKE '%8905514598628%' OR PRODUCT_CODE LIKE '%8905514595054%' OR PRODUCT_CODE LIKE '%8905514595061%' OR PRODUCT_CODE LIKE '%8905514595078%' OR PRODUCT_CODE LIKE '%8905514595108%' OR PRODUCT_CODE LIKE '%8905514595115%' OR PRODUCT_CODE LIKE '%8905514595139%' OR PRODUCT_CODE LIKE '%8905514595184%' OR PRODUCT_CODE LIKE '%8905514595245%' OR PRODUCT_CODE LIKE '%8905287960479%' OR PRODUCT_CODE LIKE '%8905287960486%' OR PRODUCT_CODE LIKE '%8905287960493%' OR PRODUCT_CODE LIKE '%8905152961082%' OR PRODUCT_CODE LIKE '%8905514720753%' OR PRODUCT_CODE LIKE '%8905514720760%' OR PRODUCT_CODE LIKE '%8905514720777%' OR PRODUCT_CODE LIKE '%8905514722078%' OR PRODUCT_CODE LIKE '%8905514722085%' OR PRODUCT_CODE LIKE '%8905514722092%' OR PRODUCT_CODE LIKE '%8905514722108%' OR PRODUCT_CODE LIKE '%8905514722139%' OR PRODUCT_CODE LIKE '%8905514722146%' OR PRODUCT_CODE LIKE '%8905514722436%' OR PRODUCT_CODE LIKE '%8905514722443%' OR PRODUCT_CODE LIKE '%8905514722450%' OR PRODUCT_CODE LIKE '%8905514722498%' OR PRODUCT_CODE LIKE '%8905514722504%' OR PRODUCT_CODE LIKE '%8905514722511%' OR PRODUCT_CODE LIKE '%8905514722528%' OR PRODUCT_CODE LIKE '%8905514722856%' OR PRODUCT_CODE LIKE '%8905514722863%' OR PRODUCT_CODE LIKE '%8905514722870%' OR PRODUCT_CODE LIKE '%8905514722887%' OR PRODUCT_CODE LIKE '%8905514723150%' OR PRODUCT_CODE LIKE '%8905514723174%' OR PRODUCT_CODE LIKE '%8905514723211%' OR PRODUCT_CODE LIKE '%8905514723228%' OR PRODUCT_CODE LIKE '%8905514723235%' OR PRODUCT_CODE LIKE '%8905514723242%' OR PRODUCT_CODE LIKE '%8905514723280%' OR PRODUCT_CODE LIKE '%8905514723303%' OR PRODUCT_CODE LIKE '%8905287906095%' OR PRODUCT_CODE LIKE '%8905514723471%' OR PRODUCT_CODE LIKE '%8905514821221%' OR PRODUCT_CODE LIKE '%8905514821238%' OR PRODUCT_CODE LIKE '%8905514821245%' OR PRODUCT_CODE LIKE '%8905514821252%' OR PRODUCT_CODE LIKE '%8905514723518%' OR PRODUCT_CODE LIKE '%8905514723525%' OR PRODUCT_CODE LIKE '%8905514723532%' OR PRODUCT_CODE LIKE '%8905514017075%' OR PRODUCT_CODE LIKE '%8905514724171%' OR PRODUCT_CODE LIKE '%8905514724188%' OR PRODUCT_CODE LIKE '%8905514724195%' OR PRODUCT_CODE LIKE '%8905514014258%' OR PRODUCT_CODE LIKE '%8905514724959%' OR PRODUCT_CODE LIKE '%8905514724966%' OR PRODUCT_CODE LIKE '%8905514724973%' OR PRODUCT_CODE LIKE '%8905514724980%' OR PRODUCT_CODE LIKE '%8905514823188%' OR PRODUCT_CODE LIKE '%8905514823201%' OR PRODUCT_CODE LIKE '%8905287772904%' OR PRODUCT_CODE LIKE '%8905514014562%' OR PRODUCT_CODE LIKE '%8905514014579%' OR PRODUCT_CODE LIKE '%8905287773147%' OR PRODUCT_CODE LIKE '%8905287910368%' OR PRODUCT_CODE LIKE '%8905514726595%' OR PRODUCT_CODE LIKE '%8907983897622%' OR PRODUCT_CODE LIKE '%8905514293400%' OR PRODUCT_CODE LIKE '%8905514293417%' OR PRODUCT_CODE LIKE '%8905514602158%' OR PRODUCT_CODE LIKE '%8905514861760%' OR PRODUCT_CODE LIKE '%8905514861777%' OR PRODUCT_CODE LIKE '%8905514861784%' OR PRODUCT_CODE LIKE '%8905514861913%' OR PRODUCT_CODE LIKE '%8905514861920%' OR PRODUCT_CODE LIKE '%8905514602349%' OR PRODUCT_CODE LIKE '%8905772008013%' OR PRODUCT_CODE LIKE '%8905772008020%' OR PRODUCT_CODE LIKE '%8905772008037%' OR PRODUCT_CODE LIKE '%8905772008044%' OR PRODUCT_CODE LIKE '%8905772282468%' OR PRODUCT_CODE LIKE '%8905772282444%' OR PRODUCT_CODE LIKE '%8905514854397%' OR PRODUCT_CODE LIKE '%8905514854403%' OR PRODUCT_CODE LIKE '%8905514854410%' OR PRODUCT_CODE LIKE '%8905514854427%' OR PRODUCT_CODE LIKE '%8905514250526%' OR PRODUCT_CODE LIKE '%8905514250533%' OR PRODUCT_CODE LIKE '%8905514502847%' OR PRODUCT_CODE LIKE '%8905514502885%' OR PRODUCT_CODE LIKE '%8905514630076%' OR PRODUCT_CODE LIKE '%8905514612584%' OR PRODUCT_CODE LIKE '%8905772127875%' OR PRODUCT_CODE LIKE '%8905772127882%' OR PRODUCT_CODE LIKE '%8905514612614%' OR PRODUCT_CODE LIKE '%8905514612621%' OR PRODUCT_CODE LIKE '%8905514612638%' OR PRODUCT_CODE LIKE '%8905514612645%' OR PRODUCT_CODE LIKE '%8905514250892%' OR PRODUCT_CODE LIKE '%8905514690827%' ) 

                        // )) 
                        //AND NOT(((
                        //                     (SECTION_NAME IN ('W-FURNISHING','W-FABRIC','FURNISHING','FABRIC TUKDA','FABRIC' ))

                        //                     )))";
                        //Int32 iLike = cStrBuyFilter.LastIndexOf("LIKE");
                        String[] cstrBuySplit = cStrBuyFilter.Split(new String[] { "LIKE" }, StringSplitOptions.RemoveEmptyEntries);
                        String[] cstrGetSplit = cStrGetFilter.Split(new String[] { "LIKE" }, StringSplitOptions.RemoveEmptyEntries);
                        String[] cStrbuyFilterRestrictionCriteriaSplit = cStrbuyFilterRestrictionCriteria.Split(new String[] { "LIKE" }, StringSplitOptions.RemoveEmptyEntries);
                        //String[] cstrExclusionSplit = cStrbuyFilterCriteria_exclusion.Split(new String[] { "LIKE" }, StringSplitOptions.RemoveEmptyEntries);

                        //if (cstrExclusionSplit.Length > 300)
                        //{
                        //    DataTable tSKUNAMES_BETWEEN = new DataTable("tSKUNAMES_BETWEEN");
                        //    clsCommon.SelectCmdToSql(tSKUNAMES_BETWEEN, "SELECT PRODUCT_CODE FROM SKU_NAMES(Nolock) WHERE PRODUCT_CODE IN (" + cStrProductCode + ") AND " + cStrbuyFilterCriteria_exclusion, "tSKUNAMES_BETWEEN");
                        //    dr = tSKUNAMES_BETWEEN.Select();
                        //}
                        //else
                        //    dr = dt.Select("PRODUCT_CODE IN(" + cStrProductCode + ") AND " + cStrbuyFilterCriteria_exclusion);

                        //if (dr.Length > 0)
                        //{
                        //    if (dtACTIVE_SCHEMES_CLONE.Select("schemeRowId='" + Convert.ToString(drow["schemeRowId"]) + "'").Length == 0)
                        //        dtACTIVE_SCHEMES_CLONE.Rows.Add(drow.ItemArray);

                        //    StringBuilder sbExcl = new StringBuilder();
                        //    foreach (DataRow drFIltered in dr)
                        //        sbExcl.Append("'" + Convert.ToString(drFIltered["product_code"]) + "',");
                        //    sbExcl.ToString().TrimEnd(',');

                        //    foreach (DataRow drFIltered in dt.Select("PRODUCT_CODE IN(" + sbExcl.ToString().TrimEnd(',')+")")
                        //    {
                        //        if (dtACTIVE_SCHEMES_BARCODE_CLONE.Select("schemeRowId='" + Convert.ToString(drow["schemeRowId"]) + "' AND product_code='" + Convert.ToString(drFIltered["product_code"]) + "'").Length == 0)
                        //        {
                        //            DataRow drNew = dtACTIVE_SCHEMES_BARCODE_CLONE.NewRow();
                        //            drNew["product_code"] = Convert.ToString(drFIltered["product_code"]);
                        //            drNew["schemeRowId"] = Convert.ToString(drow["schemeRowId"]);
                        //            drNew["buyBC"] = 1;
                        //            drNew["getBC"] = 0;
                        //            drNew["schemeMode"] = clsCommon.ConvertInt(drow["schemeMode"]);
                        //            dtACTIVE_SCHEMES_BARCODE_CLONE.Rows.Add(drNew);
                        //        }
                        //        else
                        //        {
                        //            foreach (DataRow drowbuyBC in dtACTIVE_SCHEMES_BARCODE_CLONE.Select("schemeRowId='" + Convert.ToString(drow["schemeRowId"]) + "' AND product_code='" + Convert.ToString(drFIltered["product_code"]) + "'"))
                        //            {
                        //                drowbuyBC["buyBC"] = 1;
                        //            }
                        //        }
                        //    }
                        //}
                        String cQueryFilter = "";
                        if (cStrbuyFilterRestrictionCriteriaSplit.Length > 300)
                        {
                            cQueryFilter = cStrbuyFilterRestrictionCriteria.ToUpper().Replace("CHALLAN_RECEIPT_DT", "ISNULL(SX.RECEIPT_DT,'')");
                            cQueryFilter = cQueryFilter.ToUpper().Replace("PRODUCT_CODE", "A.PRODUCT_CODE");
                            cQueryFilter = cQueryFilter.ToUpper().Replace("XFER_PRICE", "A.XFER_PRICE");
                            DataTable tSKUNAMES_BETWEEN = new DataTable("tSKUNAMES_BETWEEN");
                            cQueryFilter = @"SELECT A.Product_code FROM SKU_NAMES A(Nolock) 
                            LEFT JOIN sku_xfp sx (NOLOCK) ON sx.product_code=a.product_code AND sx.dept_id='" + LoggedLocation + @"'   
                            WHERE A.PRODUCT_CODE IN (" + cStrProductCode + ") AND " + cQueryFilter;
                            //System.IO.File.WriteAllText("Y:\\WiApp3S_MA\\cStrBuyFilter.txt", cQueryFilter);
                            sbProcessingTime.AppendLine(iHappyHours.ToString() + " Scheme :[" + cSchemName + "] Start : cStrbuyFilterRestrictionCriteria : "+cQueryFilter+ " "+ DateTime.Now.ToString());
                            clsCommon.SelectCmdToSql(tSKUNAMES_BETWEEN, cQueryFilter, "tSKUNAMES_BETWEEN");
                            sbProcessingTime.AppendLine(iHappyHours.ToString() + " Scheme :[" + cSchemName + "] End : cStrbuyFilterRestrictionCriteria : " + cQueryFilter + " " + DateTime.Now.ToString());
                            dr = tSKUNAMES_BETWEEN.Select();
                        }
                        else
                            dr = dt.Select("PRODUCT_CODE IN(" + cStrProductCode + ") AND " + cStrbuyFilterRestrictionCriteria);

                        if (dr.Length == 0) continue;

                        if (cstrBuySplit.Length > 300)
                        {
                            cQueryFilter = cStrBuyFilter.ToUpper().Replace("CHALLAN_RECEIPT_DT", "ISNULL(SX.RECEIPT_DT,'')");
                            cQueryFilter = cQueryFilter.ToUpper().Replace("PRODUCT_CODE", "A.PRODUCT_CODE");
                            cQueryFilter = cQueryFilter.ToUpper().Replace("XFER_PRICE", "A.XFER_PRICE");
                            DataTable tSKUNAMES_BETWEEN = new DataTable("tSKUNAMES_BETWEEN");
                            cQueryFilter = @"SELECT A.Product_code FROM SKU_NAMES A(Nolock) 
                            LEFT JOIN sku_xfp sx (NOLOCK) ON sx.product_code=a.product_code AND sx.dept_id='" + LoggedLocation + @"'   
                            WHERE A.PRODUCT_CODE IN (" + cStrProductCode + ") AND " + cQueryFilter;
                            //System.IO.File.WriteAllText("Y:\\WiApp3S_MA\\cStrBuyFilter.txt", cQueryFilter);
                            sbProcessingTime.AppendLine(iHappyHours.ToString() + " Scheme :[" + cSchemName + "] Start :  cStrBuyFilter - "+ cQueryFilter + " " + DateTime.Now.ToString());
                            clsCommon.SelectCmdToSql(tSKUNAMES_BETWEEN, cQueryFilter, "tSKUNAMES_BETWEEN");
                            sbProcessingTime.AppendLine(iHappyHours.ToString() + " Scheme :[" + cSchemName + "] End :  cStrBuyFilter - " + cQueryFilter + " " + DateTime.Now.ToString());
                            dr = tSKUNAMES_BETWEEN.Select();
                        }
                        else
                            dr = dt.Select("PRODUCT_CODE IN(" + cStrProductCode + ") AND " + cStrBuyFilter);

                        if (dr.Length > 0)
                        {
                            if (dtACTIVE_SCHEMES_CLONE.Select("schemeRowId='" + Convert.ToString(drow["schemeRowId"]) + "'").Length == 0)
                                dtACTIVE_SCHEMES_CLONE.Rows.Add(drow.ItemArray);

                            foreach (DataRow drFIltered in dr)
                            {
                                if (dtACTIVE_SCHEMES_BARCODE_CLONE.Select("schemeRowId='" + Convert.ToString(drow["schemeRowId"]) + "' AND product_code='" + Convert.ToString(drFIltered["product_code"]) + "'").Length == 0)
                                {
                                    DataRow drNew = dtACTIVE_SCHEMES_BARCODE_CLONE.NewRow();
                                    drNew["product_code"] = Convert.ToString(drFIltered["product_code"]);
                                    drNew["schemeRowId"] = Convert.ToString(drow["schemeRowId"]);
                                    drNew["buyBC"] = 1;
                                    drNew["getBC"] = 0;
                                    drNew["schemeMode"] = clsCommon.ConvertInt(drow["schemeMode"]);
                                    dtACTIVE_SCHEMES_BARCODE_CLONE.Rows.Add(drNew);
                                }
                                else
                                {
                                    foreach (DataRow drowbuyBC in dtACTIVE_SCHEMES_BARCODE_CLONE.Select("schemeRowId='" + Convert.ToString(drow["schemeRowId"]) + "' AND product_code='" + Convert.ToString(drFIltered["product_code"]) + "'"))
                                    {
                                        drowbuyBC["buyBC"] = 1;
                                    }
                                }
                            }
                        }

                        if (cstrGetSplit.Length > 300)
                        {
                            cQueryFilter = cStrGetFilter.ToUpper().Replace("CHALLAN_RECEIPT_DT", "ISNULL(SX.RECEIPT_DT,'')");
                            cQueryFilter = cQueryFilter.ToUpper().Replace("PRODUCT_CODE", "A.PRODUCT_CODE");
                            cQueryFilter = cQueryFilter.ToUpper().Replace("XFER_PRICE", "A.XFER_PRICE");
                            DataTable tSKUNAMES_BETWEEN = new DataTable("tSKUNAMES_BETWEEN");
                            cQueryFilter = @"SELECT A.Product_code FROM SKU_NAMES A(Nolock) 
                            LEFT JOIN sku_xfp sx (NOLOCK) ON sx.product_code=a.product_code AND sx.dept_id='" + LoggedLocation + @"'   
                            WHERE A.PRODUCT_CODE IN (" + cStrProductCode + ") AND " + cQueryFilter;
                            //System.IO.File.WriteAllText("Y:\\WiApp3S_MA\\cStrGetFilter.txt", cQueryFilter);
                            sbProcessingTime.AppendLine(iHappyHours.ToString() + " Scheme :[" + cSchemName + "] Start :  cStrGetFilter - " + cQueryFilter + " " + DateTime.Now.ToString());
                            clsCommon.SelectCmdToSql(tSKUNAMES_BETWEEN, cQueryFilter, "tSKUNAMES_BETWEEN");
                            sbProcessingTime.AppendLine(iHappyHours.ToString() + " Scheme :[" + cSchemName + "] Start :  cStrGetFilter - " + cQueryFilter + " " + DateTime.Now.ToString());
                            dr = tSKUNAMES_BETWEEN.Select();
                        }
                        else
                            dr = dt.Select("PRODUCT_CODE IN(" + cStrProductCode + ") AND " + cStrGetFilter);

                        if (dr.Length > 0)
                        {
                            if (dtACTIVE_SCHEMES_CLONE.Select("schemeRowId='" + Convert.ToString(drow["schemeRowId"]) + "'").Length == 0)
                                dtACTIVE_SCHEMES_CLONE.Rows.Add(drow.ItemArray);

                            foreach (DataRow drFIltered in dr)
                            {
                                if (dtACTIVE_SCHEMES_BARCODE_CLONE.Select("schemeRowId='" + Convert.ToString(drow["schemeRowId"]) + "' AND product_code='" + Convert.ToString(drFIltered["product_code"]) + "'").Length == 0)
                                {
                                    DataRow drNew = dtACTIVE_SCHEMES_BARCODE_CLONE.NewRow();
                                    drNew["product_code"] = Convert.ToString(drFIltered["product_code"]);
                                    drNew["schemeRowId"] = Convert.ToString(drow["schemeRowId"]);
                                    drNew["getBC"] = 1;
                                    drNew["buyBC"] = 0;
                                    drNew["schemeMode"] = clsCommon.ConvertInt(drow["schemeMode"]);
                                    dtACTIVE_SCHEMES_BARCODE_CLONE.Rows.Add(drNew);
                                }
                                else
                                {
                                    foreach (DataRow drowbuyBC in dtACTIVE_SCHEMES_BARCODE_CLONE.Select("schemeRowId='" + Convert.ToString(drow["schemeRowId"]) + "' AND product_code='" + Convert.ToString(drFIltered["product_code"]) + "'"))
                                    {
                                        drowbuyBC["getBC"] = 1;
                                    }
                                }
                            }
                        }

                    }
                    foreach (DataRow drow in dset.Tables["tACTIVE_SCHEMES3"].Select(cstrHappyHourFilter + " 1=1 "))
                    {
                        cStrSchemeRowID = Convert.ToString(drow["schemeRowId"]);
                        cSchemName = Convert.ToString(dset.Tables["tACTIVE_SCHEMES"].Select(cstrHappyHourFilter + " schemeRowId ='" + cStrSchemeRowID + "'")[0]["schemeName"]);
                        nSchemeMode = clsCommon.ConvertInt(dset.Tables["tACTIVE_SCHEMES"].Select(cstrHappyHourFilter + " schemeRowId ='" + cStrSchemeRowID + "'")[0]["schememode"]);
                        nBuyBc = clsCommon.ConvertInt(dset.Tables["tACTIVE_SCHEMES"].Select(cstrHappyHourFilter + " schemeRowId ='" + cStrSchemeRowID + "'")[0]["buyFilterMode"]);
                        nGetBc = clsCommon.ConvertInt(dset.Tables["tACTIVE_SCHEMES"].Select(cstrHappyHourFilter + " schemeRowId='" + cStrSchemeRowID + "'")[0]["getFilterMode"]);
                        Boolean sub_section_name_flag = clsCommon.ConvertBool(drow["sub_section_name_flag"]);
                        Boolean section_name_flag = clsCommon.ConvertBool(drow["section_name_flag"]);
                        Boolean article_no_flag = clsCommon.ConvertBool(drow["article_no_flag"]);
                        Boolean para1_name_flag = clsCommon.ConvertBool(drow["para1_name_flag"]);
                        Boolean para2_name_flag = clsCommon.ConvertBool(drow["para2_name_flag"]);
                        Boolean para3_name_flag = clsCommon.ConvertBool(drow["para3_name_flag"]);
                        Boolean para4_name_flag = clsCommon.ConvertBool(drow["para4_name_flag"]);
                        Boolean para5_name_flag = clsCommon.ConvertBool(drow["para5_name_flag"]);
                        Boolean para6_name_flag = clsCommon.ConvertBool(drow["para6_name_flag"]);
                        StringBuilder cStrFilter = new StringBuilder();
                        if (nSchemeMode != 3)
                        {
                            DataRow[] drowPara = dset.Tables["tACTIVE_SCHEMES2"].Select(cstrHappyHourFilter + " schemeRowId='" + cStrSchemeRowID + "'");
                            if (drowPara.Length > 0)
                            {
                                foreach (DataRow drowParaFilter in drowPara)
                                {
                                    cStrFilter = new StringBuilder();
                                    if (section_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["SECTION_NAME"]))) continue;
                                        cStrFilter.Append(" AND SECTION_NAME='" + Convert.ToString(drowParaFilter["SECTION_NAME"]) + "'");
                                    }
                                    if (sub_section_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["SUB_SECTION_NAME"]))) continue;
                                        cStrFilter.Append(" AND SUB_SECTION_NAME='" + Convert.ToString(drowParaFilter["SUB_SECTION_NAME"]) + "'");
                                    }

                                    if (article_no_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["ARTICLE_NO"]))) continue;
                                        cStrFilter.Append(" AND ARTICLE_NO='" + Convert.ToString(drowParaFilter["ARTICLE_NO"]) + "'");
                                    }

                                    if (para1_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA1_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA1_NAME='" + Convert.ToString(drowParaFilter["PARA1_NAME"]) + "'");
                                    }

                                    if (para2_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA2_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA2_NAME='" + Convert.ToString(drowParaFilter["PARA2_NAME"]) + "'");
                                    }
                                    if (para3_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA3_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA3_NAME='" + Convert.ToString(drowParaFilter["PARA3_NAME"]) + "'");
                                    }
                                    if (para4_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA4_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA4_NAME='" + Convert.ToString(drowParaFilter["PARA4_NAME"]) + "'");
                                    }
                                    if (para5_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA5_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA5_NAME='" + Convert.ToString(drowParaFilter["PARA5_NAME"]) + "'");
                                    }
                                    if (para6_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA6_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA6_NAME='" + Convert.ToString(drowParaFilter["PARA6_NAME"]) + "'");
                                    }

                                    String cStrFilter1 = cStrFilter.ToString();
                                    if (String.IsNullOrEmpty(cStrFilter1)) continue;
                                    cStrFilter1 = cStrFilter1.Trim().TrimStart(new char[] { 'A', 'N', 'D' });
                                    DataRow[] dr = dt.Select("PRODUCT_CODE IN(" + cStrProductCode + ") AND  1=2");
                                    dr = dt.Select("PRODUCT_CODE IN(" + cStrProductCode + ") AND " + cStrFilter1);
                                    if (dr.Length > 0)
                                    {
                                        foreach (DataRow drowActivescheme in dset.Tables["tACTIVE_SCHEMES"].Select(cstrHappyHourFilter + " schemeRowId ='" + cStrSchemeRowID + "'"))
                                        {
                                            if (dtACTIVE_SCHEMES_CLONE.Select("schemeRowId='" + cStrSchemeRowID + "'").Length == 0)
                                                dtACTIVE_SCHEMES_CLONE.Rows.Add(drowActivescheme.ItemArray);
                                        }

                                        foreach (DataRow drFIltered in dr)
                                        {
                                            DataRow[] drowFound = dtACTIVE_SCHEMES_BARCODE_CLONE.Select("schemeRowId='" + Convert.ToString(drow["schemeRowId"]) + "' AND product_code='" + Convert.ToString(drFIltered["product_code"]) + "'");
                                            if (drowFound.Length == 0)
                                            {
                                                DataRow drNew = dtACTIVE_SCHEMES_BARCODE_CLONE.NewRow();
                                                drNew["product_code"] = Convert.ToString(drFIltered["product_code"]);
                                                drNew["schemeRowId"] = Convert.ToString(drowParaFilter["schemeRowId"]);
                                                drNew["buyBC"] = Convert.ToString(drowParaFilter["buyBC"]);
                                                drNew["getBC"] = Convert.ToString(drowParaFilter["getBC"]);
                                                drNew["flat_discountpercentage"] = Convert.ToString(drowParaFilter["flat_discountpercentage"]);
                                                drNew["flat_discountamount"] = Convert.ToString(drowParaFilter["flat_discountamount"]);
                                                drNew["flat_netprice"] = Convert.ToString(drowParaFilter["flat_netprice"]);
                                                drNew["schemeMode"] = 2;
                                                dtACTIVE_SCHEMES_BARCODE_CLONE.Rows.Add(drNew);
                                            }
                                            else
                                            {
                                                if (nBuyBc == 3)
                                                {
                                                    drowFound[0]["buyBC"] = Convert.ToString(drowParaFilter["buyBC"]);
                                                }
                                                if (nGetBc == 3)
                                                {
                                                    drowFound[0]["getBC"] = Convert.ToString(drowParaFilter["getBC"]);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        else
                        {
                            DataRow[] drowPara = dset.Tables["tACTIVE_SCHEMES2"].Select(cstrHappyHourFilter + " schemeRowId='" + cStrSchemeRowID + "' and (buyBC=1 OR buyBC=True) ");
                            if (drowPara.Length > 0)
                            {

                                foreach (DataRow drowParaFilter in drowPara)
                                {
                                    cStrFilter = new StringBuilder();
                                    if (section_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["SECTION_NAME"]))) continue;
                                        cStrFilter.Append(" AND SECTION_NAME='" + Convert.ToString(drowParaFilter["SECTION_NAME"]) + "'");
                                    }
                                    if (sub_section_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["SUB_SECTION_NAME"]))) continue;
                                        cStrFilter.Append(" AND SUB_SECTION_NAME='" + Convert.ToString(drowParaFilter["SUB_SECTION_NAME"]) + "'");
                                    }

                                    if (article_no_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["ARTICLE_NO"]))) continue;
                                        cStrFilter.Append(" AND ARTICLE_NO='" + Convert.ToString(drowParaFilter["ARTICLE_NO"]) + "'");
                                    }

                                    if (para1_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA1_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA1_NAME='" + Convert.ToString(drowParaFilter["PARA1_NAME"]) + "'");
                                    }

                                    if (para2_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA2_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA2_NAME='" + Convert.ToString(drowParaFilter["PARA2_NAME"]) + "'");
                                    }
                                    if (para3_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA3_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA3_NAME='" + Convert.ToString(drowParaFilter["PARA3_NAME"]) + "'");
                                    }
                                    if (para4_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA4_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA4_NAME='" + Convert.ToString(drowParaFilter["PARA4_NAME"]) + "'");
                                    }
                                    if (para5_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA5_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA5_NAME='" + Convert.ToString(drowParaFilter["PARA5_NAME"]) + "'");
                                    }
                                    if (para6_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA6_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA6_NAME='" + Convert.ToString(drowParaFilter["PARA6_NAME"]) + "'");
                                    }

                                    String cStrFilter1 = cStrFilter.ToString();
                                    if (String.IsNullOrEmpty(cStrFilter1)) continue;
                                    cStrFilter1 = cStrFilter1.Trim().TrimStart(new char[] { 'A', 'N', 'D' });
                                    DataRow[] dr = dt.Select("PRODUCT_CODE IN(" + cStrProductCode + ") AND  1=2");
                                    dr = dt.Select("PRODUCT_CODE IN(" + cStrProductCode + ") AND " + cStrFilter1);
                                    if (dr.Length > 0)
                                    {
                                        foreach (DataRow drowActivescheme in dset.Tables["tACTIVE_SCHEMES"].Select(cstrHappyHourFilter + " schemeRowId ='" + cStrSchemeRowID + "'"))
                                        {
                                            if (dtACTIVE_SCHEMES_CLONE.Select("schemeRowId='" + cStrSchemeRowID + "'").Length == 0)
                                                dtACTIVE_SCHEMES_CLONE.Rows.Add(drowActivescheme.ItemArray);
                                        }

                                        foreach (DataRow drFIltered in dr)
                                        {
                                            if (dtACTIVE_SCHEMES_BARCODE_CLONE.Select("schemeRowId='" + Convert.ToString(drow["schemeRowId"]) + "' AND product_code='" + Convert.ToString(drFIltered["product_code"]) + "'").Length == 0)
                                            {
                                                DataRow drNew = dtACTIVE_SCHEMES_BARCODE_CLONE.NewRow();
                                                drNew["product_code"] = Convert.ToString(drFIltered["product_code"]);
                                                drNew["schemeRowId"] = Convert.ToString(drowParaFilter["schemeRowId"]);
                                                drNew["buyBC"] = Convert.ToString(drowParaFilter["buyBC"]);
                                                drNew["getBC"] = Convert.ToString(drowParaFilter["getBC"]);
                                                drNew["flat_discountpercentage"] = Convert.ToString(drowParaFilter["flat_discountpercentage"]);
                                                drNew["flat_discountamount"] = Convert.ToString(drowParaFilter["flat_discountamount"]);
                                                drNew["flat_netprice"] = Convert.ToString(drowParaFilter["flat_netprice"]);
                                                drNew["schemeMode"] = 3;
                                                dtACTIVE_SCHEMES_BARCODE_CLONE.Rows.Add(drNew);
                                            }
                                            else
                                            {
                                                foreach (DataRow drowbuyBC in dtACTIVE_SCHEMES_BARCODE_CLONE.Select("schemeRowId='" + Convert.ToString(drow["schemeRowId"]) + "' AND product_code='" + Convert.ToString(drFIltered["product_code"]) + "'"))
                                                {
                                                    drowbuyBC["buyBC"] = 1;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            drowPara = dset.Tables["tACTIVE_SCHEMES2"].Select(cstrHappyHourFilter + " schemeRowId='" + cStrSchemeRowID + "' and (getBC=1 OR getBC=True) ");
                            if (drowPara.Length > 0)
                            {

                                foreach (DataRow drowParaFilter in drowPara)
                                {
                                    cStrFilter = new StringBuilder();
                                    if (section_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["SECTION_NAME"]))) continue;
                                        cStrFilter.Append(" AND SECTION_NAME='" + Convert.ToString(drowParaFilter["SECTION_NAME"]) + "'");
                                    }
                                    if (sub_section_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["SUB_SECTION_NAME"]))) continue;
                                        cStrFilter.Append(" AND SUB_SECTION_NAME='" + Convert.ToString(drowParaFilter["SUB_SECTION_NAME"]) + "'");
                                    }

                                    if (article_no_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["ARTICLE_NO"]))) continue;
                                        cStrFilter.Append(" AND ARTICLE_NO='" + Convert.ToString(drowParaFilter["ARTICLE_NO"]) + "'");
                                    }

                                    if (para1_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA1_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA1_NAME='" + Convert.ToString(drowParaFilter["PARA1_NAME"]) + "'");
                                    }

                                    if (para2_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA2_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA2_NAME='" + Convert.ToString(drowParaFilter["PARA2_NAME"]) + "'");
                                    }
                                    if (para3_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA3_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA3_NAME='" + Convert.ToString(drowParaFilter["PARA3_NAME"]) + "'");
                                    }
                                    if (para4_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA4_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA4_NAME='" + Convert.ToString(drowParaFilter["PARA4_NAME"]) + "'");
                                    }
                                    if (para5_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA5_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA5_NAME='" + Convert.ToString(drowParaFilter["PARA5_NAME"]) + "'");
                                    }
                                    if (para6_name_flag)
                                    {
                                        if (String.IsNullOrEmpty(Convert.ToString(drowParaFilter["PARA6_NAME"]))) continue;
                                        cStrFilter.Append(" AND PARA6_NAME='" + Convert.ToString(drowParaFilter["PARA6_NAME"]) + "'");
                                    }

                                    String cStrFilter1 = cStrFilter.ToString();
                                    if (String.IsNullOrEmpty(cStrFilter1)) continue;
                                    cStrFilter1 = cStrFilter1.Trim().TrimStart(new char[] { 'A', 'N', 'D' });
                                    DataRow[] dr = dt.Select("PRODUCT_CODE IN(" + cStrProductCode + ") AND  1=2");
                                    dr = dt.Select("PRODUCT_CODE IN(" + cStrProductCode + ") AND " + cStrFilter1);
                                    if (dr.Length > 0)
                                    {
                                        foreach (DataRow drowActivescheme in dset.Tables["tACTIVE_SCHEMES"].Select(cstrHappyHourFilter + " schemeRowId='" + cStrSchemeRowID + "'"))
                                        {
                                            if (dtACTIVE_SCHEMES_CLONE.Select("schemeRowId='" + cStrSchemeRowID + "'").Length == 0)
                                                dtACTIVE_SCHEMES_CLONE.Rows.Add(drowActivescheme.ItemArray);
                                        }

                                        foreach (DataRow drFIltered in dr)
                                        {
                                            if (dtACTIVE_SCHEMES_BARCODE_CLONE.Select("schemeRowId='" + Convert.ToString(drow["schemeRowId"]) + "' AND product_code='" + Convert.ToString(drFIltered["product_code"]) + "'").Length == 0)
                                            {
                                                DataRow drNew = dtACTIVE_SCHEMES_BARCODE_CLONE.NewRow();
                                                drNew["product_code"] = Convert.ToString(drFIltered["product_code"]);
                                                drNew["schemeRowId"] = Convert.ToString(drowParaFilter["schemeRowId"]);
                                                drNew["buyBC"] = Convert.ToString(drowParaFilter["buyBC"]);
                                                drNew["getBC"] = Convert.ToString(drowParaFilter["getBC"]);
                                                drNew["flat_discountpercentage"] = Convert.ToString(drowParaFilter["flat_discountpercentage"]);
                                                drNew["flat_discountamount"] = Convert.ToString(drowParaFilter["flat_discountamount"]);
                                                drNew["flat_netprice"] = Convert.ToString(drowParaFilter["flat_netprice"]);
                                                drNew["schemeMode"] = 3;
                                                dtACTIVE_SCHEMES_BARCODE_CLONE.Rows.Add(drNew);
                                            }
                                            else
                                            {
                                                foreach (DataRow drowbuyBC in dtACTIVE_SCHEMES_BARCODE_CLONE.Select("schemeRowId='" + Convert.ToString(drow["schemeRowId"]) + "' AND product_code='" + Convert.ToString(drFIltered["product_code"]) + "'"))
                                                {
                                                    drowbuyBC["getBC"] = 1;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    SqlConnection con = new SqlConnection(cConStr);
                    SqlCommand cmd = new SqlCommand();
                    SqlDataAdapter sda = new SqlDataAdapter();


                    sb = new StringBuilder();
                    foreach (DataRow drow in dtACTIVE_SCHEMES_CLONE.Rows)
                    {
                        if (String.IsNullOrEmpty(Convert.ToString(drow["schemeRowId"]))) continue;
                        sb.Append("'");
                        sb.Append(Convert.ToString(drow["schemeRowId"]));
                        sb.Append("',");


                    }
                    String cStrSchemeRowId = sb.ToString().TrimEnd(',');
                    if (!String.IsNullOrEmpty(cStrSchemeRowId))
                    {
                        foreach (DataRow drow in dset.Tables["tACTIVE_SCHEMES1"].Select("schemeRowId IN (" + cStrSchemeRowId + ")"))
                        {
                            dtACTIVE_SCHEMES1_CLONE.Rows.Add(drow.ItemArray);
                        }
                    }
                    if (String.IsNullOrEmpty(Convert.ToString(dtMst.Rows[0]["ecoupon_id"])) && String.IsNullOrEmpty(cCMDRowID))
                    {
                        foreach (DataRow drowActvieScheme1 in dset.Tables["tACTIVE_SCHEMES"].Select(cstrHappyHourFilter + "ISNULL(schemeApplicableLevel,0)=2"))
                        {
                            if (dtACTIVE_SCHEMES_CLONE.Select("schemeRowId ='" + Convert.ToString(drowActvieScheme1["schemeRowId"]) + "'").Length == 0)
                            {
                                foreach (DataRow drow in dset.Tables["tACTIVE_SCHEMES"].Select("schemeRowId ='" + Convert.ToString(drowActvieScheme1["schemeRowId"]) + "'"))
                                {
                                    dtACTIVE_SCHEMES_CLONE.Rows.Add(drow.ItemArray);
                                }
                            }
                            if (dtACTIVE_SCHEMES1_CLONE.Select("schemeRowId ='" + Convert.ToString(drowActvieScheme1["schemeRowId"]) + "'").Length == 0)
                            {
                                foreach (DataRow drow in dset.Tables["tACTIVE_SCHEMES1"].Select("schemeRowId ='" + Convert.ToString(drowActvieScheme1["schemeRowId"]) + "'"))
                                {
                                    dtACTIVE_SCHEMES1_CLONE.Rows.Add(drow.ItemArray);
                                }
                            }
                        }
                    }
                    foreach (DataRow drow in dset.Tables["tACTIVE_SCHEMES"].Select(cstrHappyHourFilter + " (ISNULL(schememode,0)=1 AND (ISNULL(buyFilterMode,0)=2) OR ISNULL(getFilterMode,0)=2)"))
                    {
                        tblActiveSchemes.Rows.Add(new Object[] { Convert.ToString(drow["schemeRowId"]) });
                    }

                    if (tblActiveSchemes.Rows.Count > 0)
                    {
                        sbProcessingTime.AppendLine(iHappyHours.ToString() + " Start Getting All Barcodes : " + DateTime.Now.ToString());
                        try
                        {
                            sbProcessingTime.AppendLine(iHappyHours.ToString() + " Start Open Connection : " + DateTime.Now.ToString());
                            con.Open();
                            sbProcessingTime.AppendLine(iHappyHours.ToString() + " End Open Connection : " + DateTime.Now.ToString());
                            cmd.Connection = con;

                            //cmd = new SqlCommand("SPWOW_GET_ACTIVE_SCHEME", con);
                            //cmd.CommandType = CommandType.StoredProcedure;
                            //cmd.Parameters.Clear();
                            //cmd.Parameters.AddWithValue("@nQueryId", 2);
                            //cmd.Parameters.AddWithValue("@tblActiveSchemes", tblActiveSchemes);
                            //cmd.Parameters.AddWithValue("@tblBarCodes", tblBarCodes);
                            ////cmd.Parameters.AddWithValue("@cLocId", GLOCATION);
                            ////cmd.Parameters.AddWithValue("@dXnDt", GTODAYDATE.ToString("yyyy-MM-dd"));
                            sb = new StringBuilder();
                            Int32 i = 0;
                            foreach (DataRow dr in tblActiveSchemes.Rows)
                            {
                                if (i > 0)
                                    sb.AppendLine("UNION ALL");
                                sb.AppendLine("SELECT '" + Convert.ToString(dr["schemeRowId"]) + "' AS schemeRowId");
                                if (i == 0)
                                    sb.AppendLine("INTO #tblActiveSchemes");
                                i++;
                            }
                            cmd.CommandText = sb.ToString();
                            sbProcessingTime.AppendLine(iHappyHours.ToString() + " Start Creating #tblActiveSchemes : " + DateTime.Now.ToString());
                            cmd.ExecuteNonQuery();
                            sbProcessingTime.AppendLine(iHappyHours.ToString() + " End Creating #tblActiveSchemes : " + DateTime.Now.ToString());
                            sb.AppendLine("");
                            sb = new StringBuilder();
                            i = 0;
                            foreach (DataRow dr in tblBarCodes.Rows)
                            {
                                if (i > 0)
                                    sb.AppendLine("UNION ALL");
                                sb.AppendLine("SELECT '" + Convert.ToString(dr["product_code"]) + "' AS product_code,'" + Convert.ToString(dr["cmd_row_id"]) + "' AS CMD_ROW_ID");
                                if (i == 0)
                                    sb.AppendLine("INTO #tblBarCodes");
                                i++;
                            }
                            cmd.CommandText = sb.ToString();
                            sbProcessingTime.AppendLine(iHappyHours.ToString() + " Start Creating #tblBarCodes : " + DateTime.Now.ToString());
                            cmd.ExecuteNonQuery();
                            sbProcessingTime.AppendLine(iHappyHours.ToString() + " End Creating #tblBarCodes : " + DateTime.Now.ToString());
                            sb.AppendLine("");
                            sb = new StringBuilder();
                            /*
                             SELECT CAST('' AS VARCHAR(100)) AS schemeRowId INTO #tblActiveSchemes WHERE 1=2
                                    SELECT CAST('' AS VARCHAR(100)) AS product_code,CAST('' AS VARCHAR(100)) AS cmd_row_id INTO #tblBarCodes WHERE 1=2
                            */
                            sb.AppendLine(@"
                                SELECT product_code,schemeRowId,convert(bit,0) flatdiscount,max((case when buybc=1 THEN 1 ELSE 0 END)) buybc,
                                max((case when getbc=1 THEN 1 ELSE 0 END)) getbc,0 flat_discountPercentage,0 flat_discountAmount,0 flat_netprice,0 flat_addnl_discountpercentage,
                                convert(int,0) schemeMode,0 getbcAddnl
                                FROM 
                                (
                                SELECT d.product_code,a.schemeRowId,convert(bit,1) buybc,convert(bit,0) getbc,convert(bit,0) getbcAddnl
                                from wow_SchemeSetup_slsbc_buy a (NOLOCK)
                                JOIN #tblActiveSchemes b ON a.schemeRowId=b.schemeRowId
                                JOIN wow_SchemeSetup_slabs_Det c (NOLOCK) ON c.schemeRowId=a.schemeRowId
                                JOIN wow_schemesetup_title_det t (NOLOCK) ON t.schemeRowId=a.schemeRowId
                                JOIN #tblBarCodes d ON LEFT(d.PRODUCT_CODE, ISNULL(NULLIF(CHARINDEX ('@',d.PRODUCT_CODE)-1,-1),LEN(d.PRODUCT_CODE )))=a.product_code
                                WHERE buyFilterMode=2 

                                UNION ALL
                                SELECT d.product_code,a.schemeRowId,convert(bit,0) buybc,convert(bit,(case when isnull(targetType,0)<>2 then 1 else 0 end)) getbc,
                                convert(bit,(case when isnull(targetType,0)=2 then 1 else 0 end)) getbcAddnl
                                from wow_SchemeSetup_slsbc_get a (NOLOCK)
                                JOIN #tblActiveSchemes b ON a.schemeRowId=b.schemeRowId
                                JOIN wow_SchemeSetup_slabs_Det c (NOLOCK) ON c.schemeRowId=a.schemeRowId
                                JOIN wow_schemesetup_title_det t (NOLOCK) ON t.schemeRowId=a.schemeRowId
                                JOIN #tblBarCodes d ON LEFT(d.PRODUCT_CODE, ISNULL(NULLIF(CHARINDEX ('@',d.PRODUCT_CODE)-1,-1),LEN(d.PRODUCT_CODE )))=a.product_code
                                WHERE getFilterMode=2 
                                UNION ALL
                               SELECT d.product_code,a.schemeRowId,convert(bit,1) buybc,convert(bit,0) getbc,convert(bit,0) getbcAddnl
                                from wow_SchemeSetup_slsbc_buy a (NOLOCK)
                                JOIN #tblActiveSchemes b ON a.schemeRowId=b.schemeRowId
                                JOIN wow_SchemeSetup_slabs_Det c (NOLOCK) ON c.schemeRowId=a.schemeRowId
                                JOIN wow_schemesetup_title_det t (NOLOCK) ON t.schemeRowId=a.schemeRowId
                                JOIN #tblBarCodes d ON d.PRODUCT_CODE =a.product_code
                                WHERE buyFilterMode=2 AND a.product_code LIKE '%@%'

                                UNION ALL
                                SELECT d.product_code,a.schemeRowId,convert(bit,0) buybc,convert(bit,(case when isnull(targetType,0)<>2 then 1 else 0 end)) getbc,
                                convert(bit,(case when isnull(targetType,0)=2 then 1 else 0 end)) getbcAddnl
                                from wow_SchemeSetup_slsbc_get a (NOLOCK)
                                JOIN #tblActiveSchemes b ON a.schemeRowId=b.schemeRowId
                                JOIN wow_SchemeSetup_slabs_Det c (NOLOCK) ON c.schemeRowId=a.schemeRowId
                                JOIN wow_schemesetup_title_det t (NOLOCK) ON t.schemeRowId=a.schemeRowId
                                JOIN #tblBarCodes d ON LEFT(d.PRODUCT_CODE, ISNULL(NULLIF(CHARINDEX ('@',d.PRODUCT_CODE)-1,-1),LEN(d.PRODUCT_CODE )))=a.product_code
                                WHERE getFilterMode=2  AND a.product_code LIKE '%@%'

                                ) a 
                                GROUP BY product_code,schemeRowId
                                DROP TABLE #tblActiveSchemes
                                DROP TABLE #tblBarCodes");
                            String cQueryStr = sb.ToString();
                            cmd.CommandText = cQueryStr;
                            sda = new SqlDataAdapter(cmd);
                            sbProcessingTime.AppendLine(iHappyHours.ToString() + " Start ACTIVE_SCHEMES_BARCODE : " + DateTime.Now.ToString());
                            sda.Fill(dtACTIVE_SCHEMES_BARCODE_CLONE);
                            sbProcessingTime.AppendLine(iHappyHours.ToString() + " END ACTIVE_SCHEMES_BARCODE : " + DateTime.Now.ToString());
                            foreach (DataRow drowActvieScheme1 in dtACTIVE_SCHEMES_BARCODE_CLONE.Rows)
                            {
                                if (dtACTIVE_SCHEMES_CLONE.Select("schemeRowId ='" + Convert.ToString(drowActvieScheme1["schemeRowId"]) + "'").Length == 0)
                                {
                                    foreach (DataRow drow in dset.Tables["tACTIVE_SCHEMES"].Select("schemeRowId ='" + Convert.ToString(drowActvieScheme1["schemeRowId"]) + "'"))
                                    {
                                        dtACTIVE_SCHEMES_CLONE.Rows.Add(drow.ItemArray);
                                    }
                                }

                                if (dtACTIVE_SCHEMES1_CLONE.Select("schemeRowId ='" + Convert.ToString(drowActvieScheme1["schemeRowId"]) + "'").Length == 0)
                                {
                                    foreach (DataRow drow in dset.Tables["tACTIVE_SCHEMES1"].Select("schemeRowId ='" + Convert.ToString(drowActvieScheme1["schemeRowId"]) + "'"))
                                    {
                                        dtACTIVE_SCHEMES1_CLONE.Rows.Add(drow.ItemArray);
                                    }
                                }
                            }
                        }
                        catch (Exception)
                        {

                        }
                        finally
                        {
                            sbProcessingTime.AppendLine(iHappyHours.ToString() + " End Getting All Barcodes : " + DateTime.Now.ToString());
                            if (con.State != ConnectionState.Closed)
                                con.Close();
                        }
                    }
                    clsSaleMethods clssale = new clsSaleMethods();
                    try
                    {

                        if (iHappyHours == 1)
                        {
                            foreach (DataRow drApplyHappyHours in dtDetails.Select())
                            {

                                drApplyHappyHours["basic_discount_amount"] = 0;
                                drApplyHappyHours["basic_discount_percentage"] = 0;
                                drApplyHappyHours["weighted_avg_disc_amt"] = 0;
                                drApplyHappyHours["weighted_avg_disc_pct"] = 0;
                                drApplyHappyHours["happy_hours_applied"] = false;
                                drApplyHappyHours["barcodebased_flatdisc_applied"] = false;
                                drApplyHappyHours["scheme_name"] = "";
                                drApplyHappyHours["slsdet_row_id"] = "";
                                drApplyHappyHours["discount_amount"] = clsCommon.ConvertDecimal(drApplyHappyHours["basic_discount_amount"]) + clsCommon.ConvertDecimal(drApplyHappyHours["manual_discount_amount"]) + clsCommon.ConvertDecimal(drApplyHappyHours["card_discount_amount"]);
                                drApplyHappyHours["discount_percentage"] = (clsCommon.ConvertDecimal(drApplyHappyHours["discount_amount"]) * 100) / (clsCommon.ConvertDecimal(drApplyHappyHours["MRP"]) * clsCommon.ConvertDecimal(drApplyHappyHours["quantity"]));
                                drApplyHappyHours["net"] = ((clsCommon.ConvertDecimal(drApplyHappyHours["MRP"]) * clsCommon.ConvertDecimal(drApplyHappyHours["quantity"])) - clsCommon.ConvertDecimal(drApplyHappyHours["discount_amount"]));

                            }
                        }
                        if (dtACTIVE_SCHEMES_BARCODE_CLONE.Rows.Count > 0)
                            foreach (DataRow dataRow in dtACTIVE_SCHEMES_BARCODE_CLONE.DefaultView.ToTable(true, new string[] { "schemeRowId" }).Rows)
                            {
                                dtACTIVE_SCHEMES_BARCODE_CLONE.Select("schemeRowId='" + Convert.ToString(dataRow["schemeRowId"]) + "'").ToList<DataRow>().ForEach(r => r["schemename"] = Convert.ToString(dtACTIVE_SCHEMES_CLONE.Select("schemeRowId='" + Convert.ToString(dataRow["schemeRowId"]) + "'")[0]["schemename"]));
                            }
                        foreach (DataRow drowActvieScheme1 in dset.Tables["tACTIVE_SCHEMES"].Select("ISNULL(SCHEMEAPPLICABLELEVEL,0)=2"))
                        {
                            if (dtACTIVE_SCHEMES_CLONE.Select("schemeRowId ='" + Convert.ToString(drowActvieScheme1["schemeRowId"]) + "'").Length == 0)
                            {
                                foreach (DataRow drow in dset.Tables["tACTIVE_SCHEMES"].Select("schemeRowId ='" + Convert.ToString(drowActvieScheme1["schemeRowId"]) + "'"))
                                {
                                    dtACTIVE_SCHEMES_CLONE.Rows.Add(drow.ItemArray);
                                }
                            }
                            if (dtACTIVE_SCHEMES1_CLONE.Select("schemeRowId ='" + Convert.ToString(drowActvieScheme1["schemeRowId"]) + "'").Length == 0)
                            {
                                foreach (DataRow drow in dset.Tables["tACTIVE_SCHEMES1"].Select("schemeRowId ='" + Convert.ToString(drowActvieScheme1["schemeRowId"]) + "'"))
                                {
                                    dtACTIVE_SCHEMES1_CLONE.Rows.Add(drow.ItemArray);
                                }
                            }
                        }
                        sbProcessingTime.AppendLine(iHappyHours.ToString() + " Start updateSchemeDiscounts : " + DateTime.Now.ToString());
                        clssale._AppPath = _AppPath;
                        String cRetVal = clssale.updateSchemeDiscounts(con, LoggedLocation, ref dtMst, ref dtDetails, dtACTIVE_SCHEMES_CLONE, dtACTIVE_SCHEMES1_CLONE, dtACTIVE_SCHEMES_BARCODE_CLONE, true, clsCommon.ConvertInt(cRoundOff_Item_At), bApplyFlatschemesOnly, cCMDRowID, _DISCOUNT_PICKMODE_SLR, (iHappyHours == 2), bAPPLY_FIX_MRP_EOSS_AND_BILLPRINT);
                        sbProcessingTime.AppendLine(iHappyHours.ToString() + " End updateSchemeDiscounts : " + DateTime.Now.ToString());
                        sbProcessingTime.AppendLine(iHappyHours.ToString() + " Start Processing with Discount : " + DateTime.Now.ToString());
                        dtApplicableBarcodes = dtDetails.Copy();
                        if (String.IsNullOrEmpty(cRetVal))
                        {
                            if (iHappyHours == 1)
                            {
                                if (nHAPPYHOURS_MAXDISCOUNT > 0)
                                {
                                    foreach (DataRow drApplyHappyHours in dtDetails.Select("ISNULL(basic_discount_amount,0)+ISNULL(weighted_avg_disc_amt,0)>0", "basic_discount_amount desc"))
                                    {
                                        if (nHAPPYHOURS_MAXDISCOUNT > 0)
                                        {
                                            if (clsCommon.ConvertDecimal(drApplyHappyHours["basic_discount_amount"]) > nHAPPYHOURS_MAXDISCOUNT)
                                            {
                                                drApplyHappyHours["basic_discount_amount"] = nHAPPYHOURS_MAXDISCOUNT;
                                                drApplyHappyHours["basic_discount_percentage"] = (clsCommon.ConvertDecimal(drApplyHappyHours["basic_discount_amount"]) * 100) / (clsCommon.ConvertDecimal(drApplyHappyHours["MRP"]) * clsCommon.ConvertDecimal(drApplyHappyHours["quantity"]));
                                                drApplyHappyHours["happy_hours_applied"] = true;
                                                nHAPPYHOURS_MAXDISCOUNT = 0;
                                            }
                                            else if (clsCommon.ConvertDecimal(drApplyHappyHours["basic_discount_amount"]) <= nHAPPYHOURS_MAXDISCOUNT)
                                            {
                                                nHAPPYHOURS_MAXDISCOUNT = nHAPPYHOURS_MAXDISCOUNT - clsCommon.ConvertDecimal(drApplyHappyHours["basic_discount_amount"]);
                                                drApplyHappyHours["happy_hours_applied"] = true;
                                            }
                                        }
                                        else
                                        {
                                            drApplyHappyHours["basic_discount_amount"] = 0;
                                            drApplyHappyHours["basic_discount_percentage"] = 0;
                                            drApplyHappyHours["weighted_avg_disc_amt"] = 0;
                                            drApplyHappyHours["weighted_avg_disc_pct"] = 0;
                                            drApplyHappyHours["happy_hours_applied"] = false;
                                            drApplyHappyHours["barcodebased_flatdisc_applied"] = false;
                                            drApplyHappyHours["scheme_name"] = "";
                                            drApplyHappyHours["slsdet_row_id"] = "";
                                        }
                                        drApplyHappyHours["discount_amount"] = clsCommon.ConvertDecimal(drApplyHappyHours["basic_discount_amount"]) + clsCommon.ConvertDecimal(drApplyHappyHours["manual_discount_amount"]) + clsCommon.ConvertDecimal(drApplyHappyHours["card_discount_amount"]);
                                        drApplyHappyHours["discount_percentage"] = (clsCommon.ConvertDecimal(drApplyHappyHours["discount_amount"]) * 100) / (clsCommon.ConvertDecimal(drApplyHappyHours["MRP"]) * clsCommon.ConvertDecimal(drApplyHappyHours["quantity"]));
                                        drApplyHappyHours["net"] = ((clsCommon.ConvertDecimal(drApplyHappyHours["MRP"]) * clsCommon.ConvertDecimal(drApplyHappyHours["quantity"])) - clsCommon.ConvertDecimal(drApplyHappyHours["discount_amount"]));
                                    }
                                }
                                else
                                {
                                    foreach (DataRow drApplyHappyHours in dtDetails.Select("ISNULL(basic_discount_amount,0)>0", "basic_discount_amount desc"))
                                    {
                                        drApplyHappyHours["happy_hours_applied"] = true;
                                    }
                                }
                            }
                            foreach (DataRow drowbc in dtDetails.Rows)
                            {
                                DataRow[] drowApplied = dtACTIVE_SCHEMES_BARCODE_CLONE.Select("schememode=3 and product_code='" + Convert.ToString(drowbc["product_code"]) + "' AND schemerowid='" + Convert.ToString(drowbc["slsdet_row_id"]) + "'");
                                DataRow[] drowNotApplied = dtACTIVE_SCHEMES_BARCODE_CLONE.Select("schememode=3 and product_code='" + Convert.ToString(drowbc["product_code"]) + "' AND schemerowid<>'" + Convert.ToString(drowbc["slsdet_row_id"]) + "'");
                                if (drowApplied.Length == 0 && drowNotApplied.Length > 0)
                                {
                                    //if (String.Compare(Convert.ToString(drow1[0]["scheme_id"]), Convert.ToString(drowbc["scheme_id"]),true) != 0)
                                    {
                                        drowbc["bngn_not_applied"] = 1;
                                    }
                                }
                            }
                            lRetVal = true;
                        }
                        else
                        {
                            //MessageBox.Show(cRetVal, this.ProductName, MessageBoxButtons.OK, MessageBoxIcon.Information);
                            cErrMsg = cRetVal;
                            lRetVal = false;
                        }
                        sbProcessingTime.AppendLine(iHappyHours.ToString() + " End Processing with Discount : " + DateTime.Now.ToString());
                    }
                    catch (Exception ex)
                    {
                        //MessageBox.Show("ImplementSaleSetup(updateSchemeDiscounts) : " + ex.Message, this.ProductName, MessageBoxButtons.OK, MessageBoxIcon.Information);
                        //ShowActiveSchemes();
                        cErrMsg = "ImplementSaleSetup(updateSchemeDiscounts) : " + ex.Message;
                        lRetVal = false;
                    }
                    cstrHappyHourFilter = "";
                }
                if (lRetVal)
                {
                    var JoinResult = (from p in dtDetails.AsEnumerable()
                                      join t in dset.Tables["tACTIVE_SCHEMES"].Select("apply_scheme_if_freshitem_found=1 OR apply_scheme_if_freshitem_found=True")
                                      on p.Field<String>("slsdet_row_id") equals t.Field<String>("schemeRowId")
                                      select new
                                      {
                                          apply_scheme_if_freshitem_found = t.Field<Boolean>("apply_scheme_if_freshitem_found")
                                      }).ToList();
                    DataTable dt_apply_scheme_if_freshitem_found = LINQResultToDataTable(JoinResult);
                    if (dt_apply_scheme_if_freshitem_found.Rows.Count > 0)
                    {
                        if (dtDetails.Select("ISNULL(slsdet_row_id,'')=''").Length == 0)
                        {

                            dtDetails.AsEnumerable().ToList<DataRow>().ForEach(drApplyHappyHours =>
                            {

                                drApplyHappyHours["basic_discount_amount"] = 0;
                                drApplyHappyHours["basic_discount_percentage"] = 0;
                                drApplyHappyHours["weighted_avg_disc_amt"] = 0;
                                drApplyHappyHours["weighted_avg_disc_pct"] = 0;
                                drApplyHappyHours["happy_hours_applied"] = false;
                                drApplyHappyHours["barcodebased_flatdisc_applied"] = false;
                                drApplyHappyHours["scheme_name"] = "";
                                drApplyHappyHours["slsdet_row_id"] = "";
                                drApplyHappyHours["discount_amount"] = clsCommon.ConvertDecimal(drApplyHappyHours["basic_discount_amount"]) + clsCommon.ConvertDecimal(drApplyHappyHours["manual_discount_amount"]) + clsCommon.ConvertDecimal(drApplyHappyHours["card_discount_amount"]);
                                drApplyHappyHours["discount_percentage"] = (clsCommon.ConvertDecimal(drApplyHappyHours["discount_amount"]) * 100) / (clsCommon.ConvertDecimal(drApplyHappyHours["MRP"]) * clsCommon.ConvertDecimal(drApplyHappyHours["quantity"]));
                                drApplyHappyHours["net"] = ((clsCommon.ConvertDecimal(drApplyHappyHours["MRP"]) * clsCommon.ConvertDecimal(drApplyHappyHours["quantity"])) - clsCommon.ConvertDecimal(drApplyHappyHours["discount_amount"]));

                            });
                            dtDetails.AsEnumerable().ToList<DataRow>().ForEach(drApplyHappyHours =>
                            {
                                drApplyHappyHours["discount_amount"] = clsCommon.ConvertDecimal(drApplyHappyHours["basic_discount_amount"]) + clsCommon.ConvertDecimal(drApplyHappyHours["manual_discount_amount"]) + clsCommon.ConvertDecimal(drApplyHappyHours["card_discount_amount"]);
                                drApplyHappyHours["discount_percentage"] = (clsCommon.ConvertDecimal(drApplyHappyHours["discount_amount"]) * 100) / (clsCommon.ConvertDecimal(drApplyHappyHours["MRP"]) * clsCommon.ConvertDecimal(drApplyHappyHours["quantity"]));
                                drApplyHappyHours["net"] = ((clsCommon.ConvertDecimal(drApplyHappyHours["MRP"]) * clsCommon.ConvertDecimal(drApplyHappyHours["quantity"])) - clsCommon.ConvertDecimal(drApplyHappyHours["discount_amount"]));

                            });
                        }
                    }

                    dtDetails.Select("ISNULL(weighted_avg_disc_amt,0)=0 and ISNULL(basic_discount_amount,0)<>0 and ISNULL(scheme_name,'')<>''").ToList<DataRow>().ForEach(drweighted =>
                    {
                        drweighted["weighted_avg_disc_amt"] = clsCommon.ConvertDecimal(drweighted["basic_discount_amount"]);
                        drweighted["weighted_avg_disc_pct"] = clsCommon.ConvertDecimal(drweighted["basic_discount_percentage"]);
                    });
                }
            }
            catch (Exception ex)
            {
                //MessageBox.Show("ImplementSaleSetup(" + cSchemName + ") : " + ex.Message, this.ProductName, MessageBoxButtons.OK, MessageBoxIcon.Information);
                //ShowActiveSchemes();
                cErrMsg = "ImplementSaleSetup(" + cSchemName + ") : " + ex.Message;
                lRetVal = false;
            }
            finally
            {
                sbProcessingTime.AppendLine("End ImplementSaleSetup :" + DateTime.Now.ToString());
                if (!String.IsNullOrEmpty(_AppPath))
                {
                    System.IO.File.WriteAllText(_AppPath + "\\logs\\ImplementSaleSetup.txt", sbProcessingTime.ToString());
                }
            }

            return lRetVal;
        }
        public DataTable LINQResultToDataTable<T>(IEnumerable<T> Linqlist)
        {
            DataTable dt = new DataTable();
            PropertyInfo[] columns = null;
            if (Linqlist == null) return dt;
            foreach (T Record in Linqlist)
            {
                if (columns == null)
                {
                    columns = ((Type)Record.GetType()).GetProperties();
                    foreach (PropertyInfo GetProperty in columns)
                    {
                        Type IcolType = GetProperty.PropertyType;
                        if ((IcolType.IsGenericType) && (IcolType.GetGenericTypeDefinition()
                        == typeof(Nullable<>)))
                        {
                            IcolType = IcolType.GetGenericArguments()[0];
                        }
                        dt.Columns.Add(new DataColumn(GetProperty.Name, IcolType));
                    }
                }
                DataRow dr = dt.NewRow();
                foreach (PropertyInfo p in columns)
                {
                    dr[p.Name] = p.GetValue(Record, null) == null ? DBNull.Value : p.GetValue(Record, null);
                }
                dt.Rows.Add(dr);
            }
            return dt;
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
                        String cMemoID = LoggedLocation +  cFinYear + (new String('0', (22 - (cMemoNo+ LoggedLocation + cFinYear).Length))) + cMemoNo;
                        tCM_MST.Rows[0]["CM_ID"] = cMemoID;
                        tCM_MST.Rows[0]["last_update"] = DateTime.Now;
                        tCM_MST.Rows[0]["bin_id"] = "000";
                        tCM_MST.Rows[0]["fin_year"] = "011" + cFinYear;
                        tCM_MST.Rows[0]["user_code"] = LoggedUserCode;
                        tCM_MST.Rows[0]["location_code"] = LoggedLocation;

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
                            tCM_DET.Select("").ToList<DataRow>().ForEach(r => r["LAST_UPDATE"] = DateTime.Now);

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
                                    SELECT REF_APPROVAL_MEMO_ID 
                                    from RPS_DET (NOLOCK) 
                                    WHERE cm_id='" + cMemoID + @"' AND XN_TYPE='APM'
                                    GROUP BY REF_APPROVAL_MEMO_ID
                                )
                                UPDATE a SET a.ref_cm_id='" + cMemoID + @"' 
        						from apm01106 a WITH (ROWLOCK)
                                JOIN DET b ON B.REF_APPROVAL_MEMO_ID=a.memo_id
                                ";
                        cmd.CommandText = cQueryStr;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();


                        cQueryStr = @"
                                ;with det
                                AS
                                (
                                    SELECT A.PRODUCT_CODE,A.BIN_ID,B.location_code AS DEPT_ID,SUM(A.QUANTITY) AS QUANTITY 
                                    from RPS_DET A (NOLOCK) 
                                    JOIN RPS_MST B (NOLOCK) ON B.cm_id=A.cm_id
                                    WHERE b.cm_id='" + cMemoID + @"'
                                    GROUP BY a.PRODUCT_CODE,b.location_code,a.BIN_ID
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
                                    SELECT LEFT(A.PRODUCT_CODE, ISNULL(NULLIF(CHARINDEX ('@',A.PRODUCT_CODE)-1,-1),LEN(A.PRODUCT_CODE ))) PRODUCT_CODE,A.BIN_ID,B.location_code DEPT_ID,SUM(A.QUANTITY) AS QUANTITY 
                                    from RPS_DET A (NOLOCK) 
                                    JOIN RPS_MST C (NOLOCK) ON C.CM_ID=A.CM_ID
                                    JOIN SKU_NAMES B(NOLOCK) ON B.PRODUCT_CODE=A.PRODUCT_CODE
                                    WHERE ISNULL(B.STOCK_NA,0)=0 AND  C.CM_id='" + cMemoID + @"'
                                    GROUP BY A.PRODUCT_CODE,C.LOCATION_CODE,A.BIN_ID
                                )
                                select a.product_code as productCode,a.quantity_in_stock - b.QUANTITY  as stockQuantity ,
                                b.QUANTITY as grnQuantity,'Stock is going negative.' as errMsg 
                                from pmt01106 a (NOLOCK)
                                JOIN DET b ON B.PRODUCT_CODE=LEFT(A.PRODUCT_CODE, ISNULL(NULLIF(CHARINDEX ('@',A.PRODUCT_CODE)-1,-1),LEN(A.PRODUCT_CODE ))) and a.DEPT_ID=b.DEPT_ID AND b.BIN_ID=a.BIN_ID
                                
                                ";
                        cQueryStr = @"
                                ;with det
                                AS
                                (
                                    SELECT A.PRODUCT_CODE,A.BIN_ID,C.LOCATION_CODE AS DEPT_ID,SUM(A.QUANTITY) AS QUANTITY 
                                    from RPS_DET A (NOLOCK) 
                                    JOIN RPS_MST C (NOLOCK) ON C.CM_ID=A.CM_ID
                                    JOIN SKU_NAMES B(NOLOCK) ON B.PRODUCT_CODE=A.PRODUCT_CODE
                                    WHERE ISNULL(B.STOCK_NA,0)=0 AND  C.CM_id='" + cMemoID + @"'
                                    GROUP BY A.PRODUCT_CODE,C.LOCATION_CODE,A.BIN_ID
                                )
                                select a.product_code as productCode,a.quantity_in_stock - b.QUANTITY  as stockQuantity ,
                                b.QUANTITY as grnQuantity,'Stock is going negative.' as errMsg 
                                from pmt01106 a (NOLOCK)
                                JOIN DET b ON B.PRODUCT_CODE=A.PRODUCT_CODE and a.DEPT_ID=b.DEPT_ID AND b.BIN_ID=a.BIN_ID
                                
                                ";
                        cQueryStr = @"
                                ;with det
                                AS
                                (
                                    SELECT A.PRODUCT_CODE,A.BIN_ID,C.LOCATION_CODE AS DEPT_ID,SUM(A.QUANTITY) AS QUANTITY 
                                    from RPS_DET A (NOLOCK) 
                                    JOIN RPS_MST C (NOLOCK) ON C.CM_ID=A.CM_ID
                                    JOIN SKU_NAMES B(NOLOCK) ON B.PRODUCT_CODE=A.PRODUCT_CODE
                                    WHERE ISNULL(B.STOCK_NA,0)=0 AND  C.CM_id='" + cMemoID + @"'
                                    GROUP BY A.PRODUCT_CODE,C.LOCATION_CODE,A.BIN_ID
                                )
                                select b.product_code as productCode,ISNULL(a.quantity_in_stock,0) + b.QUANTITY  as stockQuantity ,
                                b.QUANTITY as grnQuantity,'Stock is going negative.' as errMsg 
                                from DET b (NOLOCK)
                                JOIN PMT01106 a ON B.PRODUCT_CODE=A.PRODUCT_CODE and a.DEPT_ID=b.DEPT_ID AND b.BIN_ID=a.BIN_ID
                                
                                ";
                        globalMethods.SelectCmdToSql(dset, cQueryStr, "tNegativeStock");
                        DataTable dtNegativeStock = dset.Tables["tNegativeStock"].Clone();
                        if (dset.Tables["tNegativeStock"].Rows.Count > 0)
                        {
                            DataRow[] drowNegative = dset.Tables["tNegativeStock"].Select("");// "stockQuantity<0");
                            foreach (DataRow drowN in drowNegative)
                            {
                                DataRow[] drowDet = tCM_DET.Select("PRODUCT_CODE='" + Convert.ToString(drowN["PRODUCTCODE"]) + "'");
                                if (drowDet.Length == 0)
                                {
                                    Decimal nQty = 0;// globalMethods.ConvertDecimal(drowN["grnquantity"]);
                                    Decimal nStockQty = globalMethods.ConvertDecimal(drowN["stockQuantity"]);
                                    if (nQty + nStockQty < 0)
                                    {
                                        dtNegativeStock.Rows.Add(drowN.ItemArray);
                                    }
                                    //dtNegativeStock.Rows.Add(drowN.ItemArray);
                                }
                                else
                                {
                                    Decimal nQty = globalMethods.ConvertDecimal(drowDet[0]["quantity"]);
                                    Decimal nStockQty = globalMethods.ConvertDecimal(drowN["stockQuantity"]);
                                    if (nStockQty-nQty < 0)
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

                        cQueryStr = " IF EXISTS (SELECT TOP 1 a.product_code FROM rps_Det a  (NOLOCK) JOIN RPS_MST C (NOLOCK) ON C.CM_ID=A.CM_ID LEFT JOIN pmt01106 b (NOLOCK) ON a.product_code=b.product_code" +
                   "       AND c.location_code=b.dept_id AND a.bin_id=b.bin_id WHERE a.cm_id='" + cMemoID + @"' AND  b.product_code IS NULL) " +
                   "INSERT PMT01106 (PRODUCT_CODE,  QUANTITY_IN_STOCK, DEPT_ID,BIN_ID, LAST_UPDATE ) " +
                   "SELECT a.PRODUCT_CODE,0 AS QUANTITY_IN_STOCK,c.location_code DEPT_ID,A.bin_id,GETDATE() AS LAST_UPDATE " +
                   " FROM RPS_det a  (NOLOCK) " +
                   " JOIN RPS_MST C (NOLOCK) ON C.CM_ID = A.CM_ID "+
                   " LEFT JOIN pmt01106 b (NOLOCK) ON a.product_code=b.product_code AND c.location_code=b.dept_id AND a.bin_id=b.bin_id " +
                   " WHERE a.cm_id='" + cMemoID + @"' AND b.product_code IS NULL " +
                   " GROUP BY a.PRODUCT_CODE,c.location_code,A.bin_id";

                        cmd.CommandText = cQueryStr;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();

                        cQueryStr = @"
                        ;with det
                        AS
                        (
                            SELECT A.PRODUCT_CODE,A.BIN_ID,C.location_code AS DEPT_ID,SUM(A.QUANTITY) AS QUANTITY 
                            from RPS_DET A (NOLOCK) 
                            JOIN RPS_MST C (NOLOCK) ON C.CM_ID=A.CM_ID
                            JOIN SKU_NAMES B(NOLOCK) ON B.PRODUCT_CODE=A.PRODUCT_CODE
                            WHERE ISNULL(B.STOCK_NA,0)=0 AND C.CM_id='" + cMemoID + @"'
                            GROUP BY A.PRODUCT_CODE,c.location_code,a.BIN_ID
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
                        tCM_DET.Select("").ToList<DataRow>().ForEach(r => r["LAST_UPDATE"] = DateTime.Now);
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
                                    SELECT A.PRODUCT_CODE,A.BIN_ID,c.location_code AS DEPT_ID,SUM(a.QUANTITY) AS QUANTITY 
                                    from RPS_DET  A (NOLOCK) 
                                    JOIN RPS_MST C (NOLOCK) ON C.CM_ID=A.CM_ID
                                    JOIN SKU_NAMES B(NOLOCK) ON B.PRODUCT_CODE=A.PRODUCT_CODE
                                    WHERE ISNULL(B.STOCK_NA,0)=0 AND C.CM_id='" + cMemoID + @"'
                                    GROUP BY A.PRODUCT_CODE,C.location_code ,A.BIN_ID
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
                                    from RPS_DET A (NOLOCK) 
                                    JOIN RPS_MST C (NOLOCK) ON C.CM_ID=A.CM_ID
                                    WHERE A.cm_id='" + MemoID + @"'
                                    GROUP BY A.PRODUCT_CODE,C.location_code,A.BIN_ID
                                )
                                
                        select a.product_code as productCode,a.quantity_in_stock as stockQuantity ,
                        b.QUANTITY as grnQuantity,'Stock is going negative.' as errMsg 
                        from pmt01106 a (NOLOCK)
                        JOIN DET b ON B.PRODUCT_CODE=a.product_code and a.DEPT_ID=b.DEPT_ID AND b.BIN_ID=a.BIN_ID
                        WHERE (A.quantity_in_stock + b.QUANTITY)<0
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
                                    from RPS_DET A (NOLOCK) 
                                    JOIN RPS_MST C (NOLOCK) ON C.CM_ID=A.CM_ID
                                    WHERE C.cm_id='" + MemoID + @"'
                                    GROUP BY A.PRODUCT_CODE,c.location_code,A.BIN_ID
                                )
                                INSERT PMT01106(PRODUCT_CODE, QUANTITY_IN_STOCK, DEPT_ID, BIN_ID, LAST_UPDATE ) 
                                SELECT a.PRODUCT_CODE,0 AS QUANTITY_IN_STOCK,A.DEPT_ID,A.bin_id,GETDATE() AS LAST_UPDATE
                                FROM DET a 
                                LEFT JOIN pmt01106 b (NOLOCK) ON a.product_code=b.product_code AND a.dept_id=b.dept_id AND a.bin_id=b.bin_id 
                                WHERE b.product_code IS NULL 
                                ";
                            cmd.CommandText = cQueryStr;
                            cmd.CommandType = CommandType.Text;
                            cmd.ExecuteNonQuery();

                            cQueryStr = @"
                                ;with det
                                AS
                                (
                                    SELECT A.PRODUCT_CODE,A.BIN_ID,C.location_code AS DEPT_ID,SUM(A.QUANTITY) AS QUANTITY 
                                    from RPS_DET A (NOLOCK) 
                                    JOIN RPS_MST C (NOLOCK) ON C.CM_ID=A.CM_ID
                                    WHERE C.cm_id='" + MemoID + @"'
                                    GROUP BY A.PRODUCT_CODE,c.location_code,A.BIN_ID
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
