using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace WOWIntegration
{
    public class CalPOPI
    {

        public static bool cChkMD = false;
        public static double nPUR_CAL_METHOD = 1;
        public static double nFixMRPMD = 0;

        public static DateTime ConvertDateTime(object val)
        {
            string dt = Convert.ToString(val);
            DateTime dtValue = new DateTime(1900, 1, 1);

            if (string.IsNullOrEmpty(dt) == false)
                DateTime.TryParse(dt, out dtValue);

            return dtValue;
        }
        public static double ConvertDouble(object ob)
        {
            string cVal = Convert.ToString(ob);
            double nValue = 0;

            if (cVal.Length > 0)
                double.TryParse(cVal, out nValue);

            return nValue;
        }

        public static Decimal ConvertDecimal(object ob)
        {
            string cVal = Convert.ToString(ob);
            Decimal nValue = 0M;

            if (cVal.Length > 0) Decimal.TryParse(cVal, out nValue);

            return nValue;
        }

        public static bool ConvertBool(object Value)
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
        public static Int32 ConvertInt(object cVal)
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



        public static void ReCalculation_PUR(string cColName, DataRow[] rows, DataTable cTableDet, DataTable cTableMst, DataTable cTablePurConfig, DataTable cTableArticleInfo, string cMpPer, bool bTaxInc, Boolean bFC /* Forex Enabled */, Decimal nFR/* Forex Conversion Rate*/)
        {
            nFR = nFR <= 0 ? 1 : nFR;
            DataRow[] dChkMD = cTablePurConfig.Select("config_option = 'TREAT_PROFIT_PER_AS_MARK_DOWN'");
            if (dChkMD.Length > 0)
                cChkMD = ConvertBool(dChkMD[0]["value"]);
            cChkMD = (!cChkMD ? (ConvertInt(cTableMst.Rows[0]["pur_cal_method"]) == 2 ? true : false) : cChkMD);

            Boolean bTermsEnabled = (Convert.ToString(cTableMst.Rows[0]["terms"]).Trim() != "");

            if (bTermsEnabled)
                cChkMD = false;

            if (string.IsNullOrEmpty(cColName.Trim())) cColName = "GCDP_PP";
            if (Equals(rows, null)) cColName = "NULL";
            switch (cColName.Trim().ToUpper())
            {
                case "GCARTICLENO":
                case "_TAX_FORM_NAME":
                case "GCQTY":
                case "FOC_QTY":
                case "GCMPPER":
                case "GCWSPPER":
                case "GCGROSSPP":
                case "GCDP_PP":
                case "GCDA_PP":
                case "GCPURPRICE":
                case "GCMRP":
                case "GCWSP":
                    if (bFC && (String.Compare(cColName, "GCGROSSPP", true) == 0 || String.Compare(cColName, "GCDP_PP", true) == 0))
                    {
                        Decimal bDiscAmt = (ConvertDecimal(rows[0]["Forex_gross_purchase_price"]) * ConvertDecimal(rows[0]["discount_percentage"]) / 100);
                        rows[0]["Forex_DISCOUNT_AMOUNT"] = bDiscAmt;
                        rows[0]["Forex_purchase_price"] = ConvertDecimal(rows[0]["Forex_gross_purchase_price"]) - bDiscAmt;
                        rows[0]["gross_purchase_price"] = ConvertDecimal(rows[0]["Forex_gross_purchase_price"]) * nFR;
                        rows[0]["purchase_price"] = ConvertDecimal(rows[0]["Forex_purchase_price"]) * nFR;
                        rows[0]["discount_amount"] = bDiscAmt * nFR;
                    }
                    else if (bFC && String.Compare(cColName, "GCDA_PP", true) == 0)
                    {
                        Decimal bDiscAmt = (ConvertDecimal(rows[0]["Forex_gross_purchase_price"]) - ConvertDecimal(rows[0]["Forex_DISCOUNT_AMOUNT"]));
                        rows[0]["Forex_purchase_price"] = bDiscAmt;
                        rows[0]["DISCOUNT_PERCENTAGE"] = ConvertDecimal((ConvertDecimal(rows[0]["Forex_DISCOUNT_AMOUNT"]) * 100) / ConvertDecimal(rows[0]["Forex_gross_purchase_price"]));
                        rows[0]["gross_purchase_price"] = ConvertDecimal(rows[0]["Forex_gross_purchase_price"]) * nFR;
                        rows[0]["purchase_price"] = ConvertDecimal(rows[0]["Forex_purchase_price"]) * nFR;
                        rows[0]["discount_amount"] = ConvertDecimal(rows[0]["Forex_DISCOUNT_AMOUNT"]) * nFR;
                    }
                    cTableArticleInfo = null;
                    CalculatePP(rows, cColName);
                    if (!Equals(cTableMst, null) && cTableMst.Rows.Count > 0 && ConvertDouble(cTableMst.Rows[0]["PUR_CAL_METHOD"]) == 2)
                    {
                        nPUR_CAL_METHOD = ConvertDouble(cTableMst.Rows[0]["PUR_CAL_METHOD"]);
                        if (cColName.Trim().ToUpper() == "GCGROSSPP" || cColName.Trim().ToUpper() == "GCARTICLENO")
                        {
                            rows[0]["MRP"] = rows[0]["gross_purchase_price"];
                            rows[0]["manual_mrp"] = true;
                        }
                        //cChkMD = false;
                        rows[0]["manual_mrp"] = true;
                    }
                    else
                    {
                        nPUR_CAL_METHOD = 1;
                    }
                    CalculateMRP(rows, cTablePurConfig);

                    double nPurPrice = ConvertDouble(rows[0]["purchase_price"]);
                    Double nFixMDL = ConvertDouble(nFixMRPMD);

                    if (nFixMDL > 0)
                    {
                        Double nFixMrp = CalMRP(true, nPurPrice, nFixMDL);
                        rows[0]["fix_mrp"] = System.Math.Round(nFixMrp, 2);
                    }


                    //ANIL

                    Double cRspF = 0;
                    Double nFIX_MRPF = 0;

                    nFIX_MRPF = ConvertDouble(rows[0]["fix_mrp"]);
                    cRspF = ConvertDouble(rows[0]["mrp"]);

                    string cAPPLY_FIXMRP = "";
                    DataRow[] dAPPLY_FIXMRP = cTablePurConfig.Select("config_option = 'FIXMRP_FROM_MRP'");
                    if (dAPPLY_FIXMRP.Length > 0)
                        cAPPLY_FIXMRP = Convert.ToString(dAPPLY_FIXMRP[0]["value"]).Trim();

                    if (cAPPLY_FIXMRP == "1")
                    {

                        string cMRP_Rounding_Rs = "0";
                        DataRow[] dMRP_Rounding_Rs = cTablePurConfig.Select("config_option = 'MRP_ROUNDING_RS'");
                        if (dMRP_Rounding_Rs.Length > 0)
                            cMRP_Rounding_Rs = Convert.ToString(dMRP_Rounding_Rs[0]["value"]).Trim();

                        string cMRP_Rounding_Level = "1";
                        DataRow[] dMRP_Rounding_Level = cTablePurConfig.Select("config_option = 'MRP_ROUNDING_LEVEL'");
                        if (dMRP_Rounding_Level.Length > 0)
                            cMRP_Rounding_Level = Convert.ToString(dMRP_Rounding_Level[0]["value"]).Trim();

                        string cMRP_Rounding_Mode = "1";
                        DataRow[] dMRP_Rounding_Mode = cTablePurConfig.Select("config_option = 'MRP_ROUNDING_MODE'");
                        if (dMRP_Rounding_Mode.Length > 0)
                            cMRP_Rounding_Mode = Convert.ToString(dMRP_Rounding_Mode[0]["value"]).Trim();



                        string cFIXMARGIN = "0";
                        DataRow[] DFIXMARGIN = cTablePurConfig.Select("config_option = 'FIXMRP_MARGIN_PER'");
                        if (DFIXMARGIN.Length > 0)
                            cFIXMARGIN = Convert.ToString(DFIXMARGIN[0]["value"]).Trim();

                        if (ConvertDouble(cFIXMARGIN) > 0)
                        {

                            Double dFIXMG = ConvertDouble(cFIXMARGIN);
                            Double dNewFixMrp = 0;
                            dNewFixMrp = cRspF + cRspF * (dFIXMG / 100.0);

                            RoundingMRP(ref dNewFixMrp, cMRP_Rounding_Mode, cMRP_Rounding_Rs, cMRP_Rounding_Level);

                            rows[0]["fix_mrp"] = dNewFixMrp;


                        }
                    }
                    //ANIL




                    if (cColName == "GCDP_PP" || cColName == "GCDA_PP") cColName = "GCGROSSPP";
                    if (cColName == "GCMPPER") CalculateMRPByMpPer(rows);
                    else if (cColName != "GCWSPPER" && cColName != "GCMPPER" && cColName != "GCGROSSPP") CalculateMpPer(rows, cTableArticleInfo, cMpPer, cColName);

                    if (cColName == "GCWSPPER") CalculateWSPByWSPPer(rows);
                    else if (cColName != "GCWSPPER" && cColName != "GCMPPER" && cColName != "GCGROSSPP") CalculateWSPPer(rows, cTableArticleInfo, cColName);

                    if (cColName == "GCMRP") CalculateMrpWsp(rows, "MRP", cTablePurConfig, 0);
                    if (cColName == "GCWSP") CalculateMrpWsp(rows, "WSP", cTablePurConfig, 0);
                    if (cColName == "GCGROSSPP") CalculateMrpWsp(rows, "GCGROSSPP", cTablePurConfig, ConvertDouble(cMpPer));
                    if (cColName == "GCMPPER") CalculateMrpWsp(rows, "GCMPPER", cTablePurConfig, ConvertDouble(cMpPer));
                    if (cColName == "GCWSPPER") CalculateMrpWsp(rows, "GCWSPPER", cTablePurConfig, ConvertDouble(cMpPer));


                    if (cColName != "GCMRP" && cColName != "GCWSP" && cColName != "GCWSPPER" && cColName != "GCMPPER" && cColName != "GCGROSSPP")
                        CalculateMrpWsp(rows, "", cTablePurConfig, ConvertDouble(cMpPer));

                    //rows[0]["md_percentage"] = Math.Round(((ConvertDouble(rows[0]["fix_mrp"]) - ConvertDouble(rows[0]["purchase_price"])) * 100 / (ConvertDouble(rows[0]["fix_mrp"]) <= 0 ? 1 : ConvertDouble(rows[0]["fix_mrp"]))) < 0 ? 0 : ((ConvertDouble(rows[0]["fix_mrp"]) - ConvertDouble(rows[0]["purchase_price"])) * 100 / (ConvertDouble(rows[0]["fix_mrp"]) <= 0 ? 1 : ConvertDouble(rows[0]["fix_mrp"]))), 2);
                    //rows[0]["wd_percentage"] = Math.Round(((ConvertDouble(rows[0]["wholesale_price"]) - ConvertDouble(rows[0]["purchase_price"])) * 100 / (ConvertDouble(rows[0]["wholesale_price"]) <= 0 ? 1 : ConvertDouble(rows[0]["wholesale_price"]))) < 0 ? 0 : ((ConvertDouble(rows[0]["wholesale_price"]) - ConvertDouble(rows[0]["purchase_price"])) * 100 / (ConvertDouble(rows[0]["wholesale_price"]) <= 0 ? 1 : ConvertDouble(rows[0]["wholesale_price"]))), 2);

                    Boolean bMp = (ConvertBool(rows[0]["manual_mrp"]));

                    Double cRsp = 0;
                    Double dPP = 0;
                    Double dMD = 0;
                    Double cwsp = 0;
                    Double dWD = 0;

                    cRsp = ConvertDouble(rows[0]["mrp"]);
                    cwsp = ConvertDouble(rows[0]["wholesale_price"]);
                    dPP = ConvertDouble(rows[0]["purchase_price"]);
                    // dPP = dPP + ConvertDouble(rows[0]["material_cost"]);
                    if (cRsp > 0 && bMp)
                    {
                        dMD = (cRsp - dPP) * 100 / (cRsp);
                        rows[0]["md_percentage"] = Math.Round(dMD, 3);
                    }

                    if (cwsp > 0 && cChkMD == false)
                    {
                        dWD = (cwsp - dPP) * 100 / (cwsp);
                        rows[0]["wd_percentage"] = Math.Round(dWD, 3);
                    }


                    if (ConvertInt(cTableMst.Rows[0]["pur_cal_method"]) == 2) rows[0]["mp_percentage"] = CalMRPPer(cChkMD, ConvertDouble(rows[0]["purchase_price"]), ConvertDouble(rows[0]["mrp"]));
                    GetAmount(rows, bFC);
                    if (rows.Length > 0)
                        ReCalculateTaxRow(cTableDet, cTableMst, bTaxInc, Convert.ToString(rows[0]["row_id"]));
                    GetAmountValues(cTableDet, cTableMst, bTaxInc);
                    break;
                case "NULL":

                    cTableArticleInfo = null;
                    cColName = "GCDP_PP";
                    //foreach (DataRow drow in cTableDet.Rows)
                    //{
                    //    if (drow.RowState == DataRowState.Deleted || drow.RowState == DataRowState.Detached) continue;
                    //    rows = cTableDet.Select("row_id = '" + Convert.ToString(drow["row_id"]) + "'");
                    //    CalculatePP(rows, cColName);
                    //    CalculateMRP(rows, cTablePurConfig);
                    //    CalculateMpPer(rows, cTableArticleInfo, cMpPer, cColName);
                    //    CalculateWSPPer(rows, cTableArticleInfo, cColName);
                    //    CalculateMrpWsp(rows, "", cTablePurConfig, ConvertDouble(cMpPer));
                    //    rows[0]["md_percentage"] = ((ConvertDouble(rows[0]["fix_mrp"]) - ConvertDouble(rows[0]["purchase_price"])) * 100 / (ConvertDouble(rows[0]["fix_mrp"]) <= 0 ? 1 : ConvertDouble(rows[0]["fix_mrp"]))) < 0 ? 0 : ((ConvertDouble(rows[0]["fix_mrp"]) - ConvertDouble(rows[0]["purchase_price"])) * 100 / (ConvertDouble(rows[0]["fix_mrp"]) <= 0 ? 1 : ConvertDouble(rows[0]["fix_mrp"])));
                    //    rows[0]["wd_percentage"] = ((ConvertDouble(rows[0]["wholesale_price"]) - ConvertDouble(rows[0]["purchase_price"])) * 100 / (ConvertDouble(rows[0]["wholesale_price"]) <= 0 ? 1 : ConvertDouble(rows[0]["wholesale_price"]))) < 0 ? 0 : ((ConvertDouble(rows[0]["wholesale_price"]) - ConvertDouble(rows[0]["purchase_price"])) * 100 / (ConvertDouble(rows[0]["wholesale_price"]) <= 0 ? 1 : ConvertDouble(rows[0]["wholesale_price"])));
                    //    GetAmount(rows);
                    //    if (rows.Length > 0)
                    //        ReCalculateTaxRow(cTableDet, cTableMst, bTaxInc, Convert.ToString(rows[0]["row_id"]));
                    //}
                    ResetAllValues_PUR(cTableDet, cTableMst, bTaxInc);
                    //GetAmountValues(cTableDet, cTableMst, bTaxInc);

                    break;
            }


        }
        public static void ResetAllValues_PUR(DataTable cTableDet, DataTable cTableMst, bool bTaxInc)
        {
            ReCalculateTax(cTableDet, cTableMst, bTaxInc);
            GetAmountValues(cTableDet, cTableMst, bTaxInc);
        }

        public static Double CalMRP(bool chkMD, double pp, double mp)
        {
            Double rMRP = 0;
            try
            {
                if (pp > 0)
                {
                    if (chkMD) rMRP = pp / (1 - (mp / 100));
                    else rMRP = pp + (pp * mp / 100);
                }
            }
            catch { }
            return rMRP;
        }

        public static Double CalMRPPer(bool chkMD, double pp, double rMRP)
        {
            Double mp = 0;
            try
            {
                if (pp > 0)
                {
                    if (chkMD) mp = (1 - (pp / rMRP)) * 100;
                    else mp = ((rMRP - pp) * 100) / pp;
                }
            }
            catch { }
            return mp;
        }


        public static void CalculatePP(DataRow[] rows, String cColname)
        {
            try
            {
                string cArticleNo = Convert.ToString(rows[0]["article_no"]);
                int nCodingScheme = Convert.ToInt32(rows[0]["coding_scheme"]);

                if (string.IsNullOrEmpty(cArticleNo) == false)
                {
                    double nPurPrice_FC = ConvertDouble(rows[0]["forex_purchase_price"]);
                    double nGP_FC = ConvertDouble(rows[0]["forex_gross_purchase_price"]);

                    double nPurPrice = ConvertDouble(rows[0]["purchase_price"]);
                    double nGP = ConvertDouble(rows[0]["gross_purchase_price"]);
                    double nDiscPer = (nGP == 0 ? 0 : ConvertDouble(rows[0]["discount_percentage"]));
                    double nInvQty = ConvertDouble(rows[0]["invoice_quantity"]);
                    double nDiscAmt = ConvertDouble(rows[0]["discount_amount"]);
                    bool bDisRowMode = ConvertBool(rows[0]["manual_discount"]);

                    double nDiscAmtNew = 0, nPPNew = 0;
                    switch (cColname.Trim().ToUpper())
                    {
                        case "GCITEMCODE":
                        case "GCARTICLENO":
                        case "GCGROSSPP":
                        case "GCQTY":
                            if (nGP > 0)
                            {
                                if (!bDisRowMode)
                                {
                                    nDiscAmtNew = Math.Round(ConvertDouble((nGP * nDiscPer) / 100), 2);
                                    nPPNew = nGP - nDiscAmtNew;
                                    rows[0]["discount_amount"] = nDiscAmtNew * nInvQty;
                                    rows[0]["discount_percentage"] = Math.Round(nDiscPer, 2);
                                    rows[0]["purchase_price"] = nPPNew;
                                }
                                else
                                {
                                    nPPNew = ConvertDouble(nGP - (nDiscAmt / nInvQty));
                                    nDiscAmtNew = ((nDiscAmt / nInvQty) * 100) / nGP;
                                    rows[0]["discount_Percentage"] = Math.Round(nDiscAmtNew, 2);
                                    rows[0]["purchase_price"] = nPPNew;
                                }
                            }
                            break;
                        case "GCDP_PP":
                            if (nGP > 0)
                            {
                                nDiscAmtNew = Math.Round(ConvertDouble((nGP * nDiscPer) / 100), 2);
                                nPPNew = nGP - nDiscAmtNew;
                                rows[0]["discount_amount"] = nDiscAmtNew * nInvQty;
                                rows[0]["purchase_price"] = nPPNew;
                            }
                            break;
                        case "GCDA_PP":
                            if (nGP > 0)
                            {
                                nPPNew = ConvertDouble(nGP - (nDiscAmt / nInvQty));
                                nDiscAmtNew = ((nDiscAmt / nInvQty) * 100) / nGP;
                                rows[0]["discount_Percentage"] = Math.Round(nDiscAmtNew, 3);
                                rows[0]["purchase_price"] = nPPNew;
                            }
                            break;
                        case "GCPURPRICE":
                            nDiscAmtNew = ConvertDouble(nGP - nPurPrice);
                            if (nGP > 0)
                            {
                                nPPNew = (nDiscAmtNew * 100) / nGP;
                                rows[0]["discount_amount"] = nDiscAmtNew * nInvQty;
                                rows[0]["discount_percentage"] = Math.Round(nPPNew, 3);
                            }
                            break;

                        default:
                            break;
                    }

                    rows[0].EndEdit();

                }
            }
            catch { }
        }
        public static void CalculateMRP(DataRow[] rows, DataTable cTablePurConfig=null)
        {
            try
            {
                string cArticleCode = Convert.ToString(rows[0]["article_code"]);
                string cProductCode = Convert.ToString(rows[0]["product_code"]);
                bool bGenEanCodes = ConvertBool(rows[0]["gen_ean_codes"]);
                bool bMrpRowMode = ConvertBool(rows[0]["manual_mrp"]);
                bool bWspRowMode = ConvertBool(rows[0]["manual_wsp"]);
                if (string.IsNullOrEmpty(cArticleCode) == false && (string.IsNullOrEmpty(cProductCode) || bGenEanCodes))
                {
                    double nPurPrice = ConvertDouble(rows[0]["purchase_price"]);
                    double nMRP = ConvertDouble(rows[0]["mrp"]);
                    double nMPPer = ConvertDouble(rows[0]["mp_percentage"]);
                    double nWSP = ConvertDouble(rows[0]["wholesale_price"]);
                    double nWSPPer = ConvertDouble(rows[0]["wsp_percentage"]);
                    double nMRPNew = 0, nWSPNew = 0;

                    //double nMPPer = ConvertDouble(rows[0]["mp_percentage"]);
                    double nMDPer = ConvertDouble(rows[0]["md_percentage"]);
                    double nWDPer = ConvertDouble(rows[0]["WD_percentage"]);
                    //double nWSP = ConvertDouble(rows[0]["wholesale_price"]);
                    //double nWSPPer = ConvertDouble(rows[0]["wsp_percentage"]);
                    //double nMRPNew = 0, nWSPNew = 0;




                    Double nFixMDL = ConvertDouble(nFixMRPMD);
                    if (nFixMDL > 0)
                    {

                        Double nFixMrp = CalMRP(true, nPurPrice, nFixMDL);
                        rows[0]["fix_mrp"] = System.Math.Round(nFixMrp, 2);
                    }




                    if (nMRP == 0 || !bMrpRowMode)
                    {
                        rows[0].BeginEdit();

                        //if (nMDPer <= 0)
                        rows[0]["mrp"] = CalMRP(false, nPurPrice, nMPPer);
                        //else
                        //{
                        //    rows[0]["mrp"] = CalMRP(true, nPurPrice, nMDPer);
                        //    nMRP = ConvertDouble(rows[0]["mrp"]);
                        //}

                        rows[0].EndEdit();
                    }

                    if (nWSP == 0 || !bWspRowMode)
                    {
                        rows[0].BeginEdit();

                        //if (nWDPer <= 0)
                        rows[0]["wholesale_price"] = CalMRP(false, nPurPrice, nWSPPer);
                        //else
                        //{
                        //    rows[0]["wholesale_price"] = CalMRP(true, nPurPrice, nWDPer);
                        //    nWSP = ConvertDouble(rows[0]["wholesale_price"]);
                        //}

                        rows[0].EndEdit();
                    }




                    //if (nMaterialCost > 0 && bMrpRowMode == false && ConvertBool(rows[0]["manual_mpp"]) == false)
                    //{

                    //    rows[0].BeginEdit();

                    //    if (nMDPer <= 0)
                    //        rows[0]["mrp"] = CalMRP(false, nPurPrice, 0);
                    //    else
                    //    {
                    //        rows[0]["mrp"] = CalMRP(true, nPurPrice, 0);
                    //        nMRP = ConvertDouble(rows[0]["mrp"]);
                    //    }

                    //    rows[0].EndEdit();
                    //}



                    ////bool cChkMD = false;
                    ////DataRow[] dChkMD = cTablePurConfig.Select("config_option = 'TREAT_PROFIT_PER_AS_MARK_DOWN'");
                    ////if (dChkMD.Length > 0)
                    ////    cChkMD = ConvertBool(dChkMD[0]["value"]);


                    ////if (cRecalculateMRP == "1")
                    ////{
                    ////    if (bMrpRowMode && nMRP > 0) nMRPNew = nMRP;
                    ////    else nMRPNew = nPurPrice + (nPurPrice * nMPPer / 100);

                    ////    if (bWspRowMode && nWSP > 0) nWSPNew = nWSP;
                    ////    else nWSPNew = nPurPrice + (nPurPrice * nWSPPer / 100);

                    ////    rows[0].BeginEdit();
                    ////    rows[0]["mrp"] = nMRPNew;
                    ////    rows[0]["wholesale_price"] = nWSPNew;
                    ////    rows[0].EndEdit();
                    ////}
                    ////else
                    ////{
                    //if (nMRP == 0)
                    //{
                    //    //nMRPNew = nPurPrice + (nPurPrice * nMPPer / 100);
                    //    rows[0].BeginEdit();
                    //    //rows[0]["mrp"] = nMRPNew;
                    //    rows[0]["mrp"] = CalMRP(cChkMD, nPurPrice, nMPPer);
                    //    rows[0].EndEdit();
                    //}
                    //if (nWSP == 0)
                    //{
                    //    //nMRPNew = nPurPrice + (nPurPrice * nWSPPer / 100);
                    //    rows[0].BeginEdit();
                    //    //rows[0]["wholesale_price"] = nMRPNew;
                    //    rows[0]["wholesale_price"] = CalMRP(cChkMD, nPurPrice, nWSPPer);
                    //    rows[0].EndEdit();
                    //}


                    ////}
                }
            }
            catch { }
        }
        public static void CalculateMrpWsp(DataRow[] drow, String cEditMode, DataTable cTablePurConfig, Double cMpPer)
        {
            try
            {
                string cCodingScheme = Convert.ToString(drow[0]["coding_scheme"]);
                string cProductCode = Convert.ToString(drow[0]["product_code"]);
                bool bpara1set = ConvertBool(drow[0]["para1_set"]);
                bool bpara2set = ConvertBool(drow[0]["para2_set"]);
                bool bMrpRowMode = ConvertBool(drow[0]["manual_Mrp"]);
                bool bWspRowMode = ConvertBool(drow[0]["manual_wsp"]);
                bool bGenEanCodes = ConvertBool(drow[0]["gen_ean_codes"]);

                string cWSP_MRP_Mode = "";
                DataRow[] dWSP_MRP_Mode = cTablePurConfig.Select("config_option = 'MRP_WSP_MODE'");
                if (dWSP_MRP_Mode.Length > 0)
                    cWSP_MRP_Mode = Convert.ToString(dWSP_MRP_Mode[0]["value"]).Trim();

                string cWSP_MRP_MARGINPER = "0";
                DataRow[] dWSP_MRP_MARGINPER = cTablePurConfig.Select("config_option = 'MRP_WSP_MARGIN_PER'");
                if (dWSP_MRP_MARGINPER.Length > 0)
                    cWSP_MRP_MARGINPER = Convert.ToString(dWSP_MRP_MARGINPER[0]["value"]).Trim();

                string cAPPLY_MRP_WSP_MARGIN_SETTING = "";
                DataRow[] dAPPLY_MRP_WSP_MARGIN_SETTING = cTablePurConfig.Select("config_option = 'APPLY_MRP_WSP_MARGIN_SETTING'");
                if (dAPPLY_MRP_WSP_MARGIN_SETTING.Length > 0)
                    cAPPLY_MRP_WSP_MARGIN_SETTING = Convert.ToString(dAPPLY_MRP_WSP_MARGIN_SETTING[0]["value"]).Trim();

                bool bAPPLY_MRP_WSP_MARGIN_SETTING = false;
                if (cAPPLY_MRP_WSP_MARGIN_SETTING == "1") bAPPLY_MRP_WSP_MARGIN_SETTING = true;
                else bAPPLY_MRP_WSP_MARGIN_SETTING = false;


                string cMRP_Rounding_Mode = "1";
                DataRow[] dMRP_Rounding_Mode = cTablePurConfig.Select("config_option = 'MRP_ROUNDING_MODE'");
                if (dMRP_Rounding_Mode.Length > 0)
                    cMRP_Rounding_Mode = Convert.ToString(dMRP_Rounding_Mode[0]["value"]).Trim();

                string cMRP_Rounding_Rs = "0";
                DataRow[] dMRP_Rounding_Rs = cTablePurConfig.Select("config_option = 'MRP_ROUNDING_RS'");
                if (dMRP_Rounding_Rs.Length > 0)
                    cMRP_Rounding_Rs = Convert.ToString(dMRP_Rounding_Rs[0]["value"]).Trim();


                string cWSP_Rounding_Mode = "1";
                DataRow[] dWSP_Rounding_Mode = cTablePurConfig.Select("config_option = 'WSP_ROUNDING_MODE'");
                if (dWSP_Rounding_Mode.Length > 0)
                    cWSP_Rounding_Mode = Convert.ToString(dWSP_Rounding_Mode[0]["value"]).Trim();

                string cWSP_Rounding_Rs = "1";
                DataRow[] dWSP_Rounding_Rs = cTablePurConfig.Select("config_option = 'WSP_ROUNDING_RS'");
                if (dWSP_Rounding_Rs.Length > 0)
                    cWSP_Rounding_Rs = Convert.ToString(dWSP_Rounding_Rs[0]["value"]).Trim();

                string cMRP_Rounding_Level = "1";
                DataRow[] dMRP_Rounding_Level = cTablePurConfig.Select("config_option = 'MRP_ROUNDING_LEVEL'");
                if (dMRP_Rounding_Level.Length > 0)
                    cMRP_Rounding_Level = Convert.ToString(dMRP_Rounding_Level[0]["value"]).Trim();

                string cWSP_Rounding_Level = "1";
                DataRow[] dWSP_Rounding_Level = cTablePurConfig.Select("config_option = 'WSP_ROUNDING_LEVEL'");
                if (dWSP_Rounding_Level.Length > 0)
                    cWSP_Rounding_Level = Convert.ToString(dWSP_Rounding_Level[0]["value"]).Trim();

                double nPer = ConvertDouble(String.IsNullOrEmpty(cWSP_MRP_MARGINPER) ? "0" : cWSP_MRP_MARGINPER);
                drow[0].BeginEdit();

                if (string.IsNullOrEmpty(cProductCode) || bGenEanCodes)
                {
                    if (bAPPLY_MRP_WSP_MARGIN_SETTING && nPUR_CAL_METHOD != 2)
                    {
                        if (!bMrpRowMode && cWSP_MRP_Mode == "1" && (cEditMode == "GCGROSSPP" || cEditMode == "" || cEditMode == "WSP" || cEditMode == "GCWSPPER"))
                        {
                            double nValueWSP1 = ConvertDouble(drow[0]["wholesale_price"]);
                            if (!bWspRowMode) RoundingMRP(ref nValueWSP1, cWSP_Rounding_Mode, cWSP_Rounding_Rs, cWSP_Rounding_Level);
                            drow[0]["wholesale_price"] = nValueWSP1;

                            double nMrp = ConvertDouble(drow[0]["mrp"]);
                            double nWsp = ConvertDouble(drow[0]["wholesale_price"]);
                            double nValue = 0;
                            nValue = CalMRP(false, nWsp, nPer);
                            drow[0]["mrp"] = nValue;

                            double nPurPrice = ConvertDouble(drow[0]["purchase_price"]);
                            nMrp = ConvertDouble(drow[0]["mrp"]);
                            double nMpPer = 0;
                            nMpPer = CalMRPPer(false, nPurPrice, nMrp);
                            if (nPurPrice > 0)
                                drow[0]["mp_percentage"] = Math.Round(nMpPer, 3);

                            //nPurPrice = ConvertDouble(drow[0]["purchase_price"]);
                            //nMrp = ConvertDouble(drow[0]["wholesale_price"]);
                            //nMpPer = 0;
                            //nMpPer = CalMRPPer(false, nPurPrice, nMrp);
                            //drow[0]["wsp_percentage"] = Math.Round(nMpPer, 2);
                        }
                        else
                        {
                            if (!bMrpRowMode && (cEditMode == "GCGROSSPP" || cEditMode == "GCMPPER") && string.IsNullOrEmpty(Convert.ToString(drow[0]["product_code"]).Trim()))
                            {
                                double nMpPer = ConvertDouble(drow[0]["mp_percentage"]);
                                double nPurPrice = ConvertDouble(drow[0]["purchase_price"]);
                                double nValue = 0;
                                nValue = CalMRP(false, nPurPrice, nMpPer);
                                drow[0]["mrp"] = nValue;
                            }
                            else
                            {
                                double nPurPrice = ConvertDouble(drow[0]["purchase_price"]);
                                double nMrp = ConvertDouble(drow[0]["mrp"]);
                                double nMpPer = 0;
                                nMpPer = CalMRPPer(false, nPurPrice, nMrp); //((nMrp - nPurPrice) * 100) / nPurPrice;
                                if (nPurPrice > 0)
                                    drow[0]["mp_percentage"] = Math.Round(nMpPer, 3);
                            }


                        }



                        if (!bWspRowMode && cWSP_MRP_Mode == "2" && (cEditMode == "GCGROSSPP" || cEditMode == "" || cEditMode == "MRP" || cEditMode == "GCMPPER"))
                        {
                            double nValueMRP1 = ConvertDouble(drow[0]["mrp"]);
                            if (!bMrpRowMode) RoundingMRP(ref nValueMRP1, cMRP_Rounding_Mode, cMRP_Rounding_Rs, cMRP_Rounding_Level);
                            drow[0]["mrp"] = nValueMRP1;

                            double nMrp = ConvertDouble(drow[0]["mrp"]);
                            double nWsp = ConvertDouble(drow[0]["wholesale_price"]);
                            double nValue = 0;
                            nValue = CalMRP(false, nMrp, (-1) * nPer);
                            drow[0]["wholesale_price"] = nValue;

                            double nPurPrice = ConvertDouble(drow[0]["purchase_price"]);
                            nMrp = ConvertDouble(drow[0]["wholesale_price"]);
                            double nMpPer = 0;
                            nMpPer = CalMRPPer(false, nPurPrice, nMrp);
                            if (nPurPrice > 0)
                                drow[0]["wsp_percentage"] = Math.Round(nMpPer, 2);

                            //nPurPrice = ConvertDouble(drow[0]["purchase_price"]);
                            //nMrp = ConvertDouble(drow[0]["mrp"]);
                            //nMpPer = 0;
                            //nMpPer = CalMRPPer(false, nPurPrice, nMrp);
                            //if (nPurPrice > 0)
                            //    drow[0]["mp_percentage"] = Math.Round(nMpPer, 2);
                        }
                        else
                        {
                            if (!bWspRowMode && (cEditMode == "GCGROSSPP" || cEditMode == "GCWSPPER") && string.IsNullOrEmpty(Convert.ToString(drow[0]["product_code"]).Trim()))
                            {
                                double nPurPrice = ConvertDouble(drow[0]["purchase_price"]);
                                double nWspPer = ConvertDouble(drow[0]["wsp_percentage"]);
                                double nValue = 0;
                                nValue = CalMRP(false, nPurPrice, nWspPer);
                                drow[0]["wholesale_price"] = nValue;
                            }
                            else
                            {
                                double nPurPrice = ConvertDouble(drow[0]["purchase_price"]);
                                double nWsp = ConvertDouble(drow[0]["wholesale_price"]);
                                double nWspPer = 0;
                                nWspPer = CalMRPPer(false, nPurPrice, nWsp);
                                if (nPurPrice > 0)
                                    drow[0]["wsp_percentage"] = Math.Round(nWspPer, 2);
                            }
                        }

                    }
                    else
                    {
                        if (cEditMode == "GCWSPPER") goto AT_WSP;
                        if (!bMrpRowMode && (cEditMode == "GCGROSSPP" || cEditMode == "GCMPPER") && string.IsNullOrEmpty(Convert.ToString(drow[0]["product_code"]).Trim()))
                        {
                            //double nMpPer = ConvertDouble(drow[0]["mp_percentage"]);
                            //double nPurPrice = ConvertDouble(drow[0]["purchase_price"]);
                            //double nValue = 0;
                            //nValue = CalMRP(cChkMD, nPurPrice, nMpPer);
                            //drow[0]["mrp"] = nValue;


                            double nMpPer = ConvertDouble(drow[0]["mp_percentage"]);
                            double nMdPer = ConvertDouble(drow[0]["md_percentage"]);
                            //bool bManualmpp = ConvertBool(drow[0]["manual_mpp"]);
                            double nPurPrice = ConvertDouble(drow[0]["purchase_price"]);
                            //nPurPrice = nPurPrice + ConvertDouble(drow[0]["material_cost"]);
                            double nValue = 0;

                            //Anil Change
                            //nValue = CalMRP(cChkMD, nPurPrice, nMpPer);
                            // drow[0]["mrp"] = nValue;

                            //if (nMdPer <= 0)// || bManualmpp)
                            //{
                            nValue = CalMRP(cChkMD, nPurPrice, nMpPer);
                            drow[0]["mrp"] = nValue;
                            //}
                            //else
                            //{
                            //    nValue = CalMRP(true, nPurPrice, nMdPer);
                            //    drow[0]["mrp"] = nValue;
                            //}
                        }
                        else
                        {
                            double nPurPrice = ConvertDouble(drow[0]["purchase_price"]);
                            double nMrp = ConvertDouble(drow[0]["mrp"]);
                            double nMpPer = 0;
                            nMpPer = CalMRPPer(cChkMD, nPurPrice, nMrp);
                            if (nPurPrice > 0)
                                drow[0]["mp_percentage"] = Math.Round(nMpPer, 3);
                        }
                        if (cEditMode == "GCMPPER") goto AT_LAST;

                        AT_WSP:
                        if (!bWspRowMode && (cEditMode == "GCGROSSPP" || cEditMode == "GCWSPPER") && string.IsNullOrEmpty(Convert.ToString(drow[0]["product_code"]).Trim()))
                        {
                            double nPurPrice = ConvertDouble(drow[0]["purchase_price"]);
                            double nWspPer = ConvertDouble(drow[0]["wsp_percentage"]);
                            double nValue = 0;
                            nValue = CalMRP(cChkMD, nPurPrice, nWspPer);
                            drow[0]["wholesale_price"] = nValue;
                        }
                        else
                        {
                            double nPurPrice = ConvertDouble(drow[0]["purchase_price"]);
                            double nWsp = ConvertDouble(drow[0]["wholesale_price"]);
                            double nWspPer = 0;
                            nWspPer = CalMRPPer(cChkMD, nPurPrice, nWsp);
                            if (nPurPrice > 0)
                                drow[0]["wsp_percentage"] = Math.Round(nWspPer, 2);
                        }
                    }

                }
                else
                {
                    double nPurPrice = ConvertDouble(drow[0]["purchase_price"]);

                    double nMrp = ConvertDouble(drow[0]["mrp"]);
                    double nMpPer = 0;
                    nMpPer = CalMRPPer(cChkMD, nPurPrice, nMrp);
                    if (nPurPrice > 0)
                        drow[0]["mp_percentage"] = Math.Round(nMpPer, 3);

                    double nWsp = ConvertDouble(drow[0]["wholesale_price"]);
                    double nWspPer = 0;
                    nWspPer = CalMRPPer(cChkMD, nPurPrice, nWsp);
                    if (nPurPrice > 0)
                        drow[0]["wsp_percentage"] = Math.Round(nWspPer, 2);

                }
            AT_LAST:
                double nValueMRP = ConvertDouble(drow[0]["mrp"]);

                double nValueFixMRP = ConvertDouble(drow[0]["fix_mrp"]);

                double nValueWSP = ConvertDouble(drow[0]["wholesale_price"]);
                if (!bMrpRowMode) RoundingMRP(ref nValueMRP, cMRP_Rounding_Mode, cMRP_Rounding_Rs, cMRP_Rounding_Level);

                if (!bMrpRowMode) RoundingMRP(ref nValueFixMRP, cMRP_Rounding_Mode, cMRP_Rounding_Rs, cMRP_Rounding_Level);

                if (!bWspRowMode) RoundingMRP(ref nValueWSP, cWSP_Rounding_Mode, cWSP_Rounding_Rs, cWSP_Rounding_Level);
                drow[0]["mrp"] = nValueMRP;
                drow[0]["fix_mrp"] = nValueFixMRP;
                drow[0]["wholesale_price"] = nValueWSP;

                double nMdPer_1 = ConvertDouble(drow[0]["md_percentage"]);
                double nWdPer_1 = ConvertDouble(drow[0]["Wd_percentage"]);
                double nPurPrice_1 = ConvertDouble(drow[0]["purchase_price"]);
                //  nPurPrice_1 = nPurPrice_1 +ConvertDouble(drow[0]["material_cost"]);

                if (nMdPer_1 > 0)//&& bManualmpp_1 == false)
                    drow[0]["mp_percentage"] = CalMRPPer(false, nPurPrice_1, nValueMRP);

                if (nWdPer_1 > 0)//&& bManualwspp_1 == false)
                    drow[0]["wsp_percentage"] = CalMRPPer(false, nPurPrice_1, nValueWSP);


                drow[0].EndEdit();
            }
            catch { }
        }
        public static void CalculateMRPByMpPer(DataRow[] rows)
        {
            try
            {
                string cArticleNo = Convert.ToString(rows[0]["article_no"]);
                string cProductCode = Convert.ToString(rows[0]["product_code"]);
                int nCodingScheme = Convert.ToInt32(rows[0]["coding_scheme"]);
                bool bGenEanCodes = ConvertBool(rows[0]["gen_ean_codes"]);
                bool bMrpRowMode = ConvertBool(rows[0]["manual_Mrp"]);
                bool bWspRowMode = ConvertBool(rows[0]["manual_wsp"]);

                cProductCode = "";

                if (string.IsNullOrEmpty(cArticleNo) == false && (string.IsNullOrEmpty(cProductCode) || bGenEanCodes))

                {

                    double nPurPrice = ConvertDouble(rows[0]["purchase_price"]);
                    double nMPPer = ConvertDouble(rows[0]["mp_percentage"]);
                    double nMRPNew = 0;

                    if (nPurPrice > 0)
                    {
                        //nMRPNew = nPurPrice + (nPurPrice * nMPPer / 100);

                        rows[0].BeginEdit();
                        rows[0]["mrp"] = CalMRP(cChkMD, nPurPrice, nMPPer); //nMRPNew;
                        rows[0].EndEdit();
                    }
                    else
                    {
                        rows[0].BeginEdit();
                        rows[0]["mrp"] = 0;
                        rows[0].EndEdit();
                    }

                }
            }
            catch { }
        }
        public static void CalculateMpPer(DataRow[] rows, DataTable cTableArticleInfo, string cMpPer, string cColName)
        {
            try
            {
                double nPurPrice = ConvertDouble(rows[0]["purchase_price"]);
                double nMrp = ConvertDouble(rows[0]["mrp"]);
                double nMpPer = 0;
                bool bMrpRowMode = ConvertBool(rows[0]["manual_Mrp"]);
                bool bWspRowMode = ConvertBool(rows[0]["manual_wsp"]);
                rows[0].BeginEdit();
                if ((nPurPrice == 0 || nMrp == 0) && !Equals(cTableArticleInfo, null))
                {
                    if (cTableArticleInfo.Rows.Count > 0)
                    {
                        nMpPer = (ConvertDouble(cTableArticleInfo.Rows[0]["mp_percentage"]) == 0 ? ConvertDouble(cMpPer) : ConvertDouble(cTableArticleInfo.Rows[0]["mp_percentage"]));
                        rows[0]["mp_percentage"] = Math.Round(nMpPer, 3);
                    }
                }
                else if (nPurPrice > 0)
                {
                    if (cColName == "GCGROSSPP" && !bMrpRowMode)
                    {

                    }
                    else
                    {
                        nMpPer = CalMRPPer(cChkMD, nPurPrice, nMrp); //((nMrp - nPurPrice) * 100) / nPurPrice;
                        if (nMpPer < 0) nMpPer = 0;
                        rows[0]["mp_percentage"] = Math.Round(nMpPer, 3);
                    }
                }
                rows[0].EndEdit();
            }
            catch { }
        }


        public static void CalculateMdPer(DataRow[] rows, DataTable cTableArticleInfo, string cMpPer, string cColName)
        {
            try
            {

                Double cRsp = 0;
                Double dPP = 0;
                Double dMD = 0;

                cRsp = ConvertDouble(rows[0]["mrp"]);
                dPP = ConvertDouble(rows[0]["purchase_price"]);


                rows[0].BeginEdit();

                if (cRsp > 0 && dPP > 0)
                {
                    dMD = (cRsp - dPP) * 100 / (cRsp);
                    rows[0]["md_percentage"] = Math.Round(dMD, 3);
                }

                rows[0].EndEdit();
            }
            catch { }
        }



        public static void CalculateWSPByWSPPer(DataRow[] rows)
        {
            try
            {
                string cProductCode = Convert.ToString(rows[0]["product_code"]);
                string cArticleNo = Convert.ToString(rows[0]["article_no"]);
                int nCodingScheme = Convert.ToInt32(rows[0]["coding_scheme"]);
                Boolean bGenEanCodes = ConvertBool(rows[0]["gen_ean_codes"]);
                bool bMrpRowMode = ConvertBool(rows[0]["manual_Mrp"]);
                bool bWspRowMode = ConvertBool(rows[0]["manual_wsp"]);
                cProductCode = "";

                if (string.IsNullOrEmpty(cArticleNo) == false && (string.IsNullOrEmpty(cProductCode) || bGenEanCodes))
                {
                    double nPurPrice = ConvertDouble(rows[0]["purchase_price"]);
                    double nMPPer = ConvertDouble(rows[0]["wsp_percentage"]);
                    double nMRPNew = 0;

                    if (nPurPrice > 0)
                    {
                        //nMRPNew = nPurPrice + (nPurPrice * nMPPer / 100);
                        rows[0].BeginEdit();
                        rows[0]["wholesale_price"] = CalMRP(cChkMD, nPurPrice, nMPPer);// nMRPNew;
                        rows[0].EndEdit();
                    }
                    else
                    {
                        rows[0].BeginEdit();
                        rows[0]["wholesale_price"] = 0;
                        rows[0].EndEdit();
                    }
                }
            }
            catch { }
        }
        public static void CalculateWSPPer(DataRow[] rows, DataTable cTableArticleInfo, string cColName)
        {
            try
            {
                double nPurPrice = ConvertDouble(rows[0]["purchase_price"]);
                double nMrp = ConvertDouble(rows[0]["wholesale_price"]);
                double nMpPer = 0;
                bool bMrpRowMode = ConvertBool(rows[0]["manual_Mrp"]);
                bool bWspRowMode = ConvertBool(rows[0]["manual_wsp"]);
                rows[0].BeginEdit();

                if ((nPurPrice == 0 || nMrp == 0) && !Equals(cTableArticleInfo, null))
                {
                    if (cTableArticleInfo.Rows.Count > 0)
                    {
                        nMpPer = ConvertDouble(cTableArticleInfo.Rows[0]["wsp_percentage"]);
                        rows[0]["wsp_percentage"] = Math.Round(nMpPer, 2);
                    }
                }
                else if (nPurPrice > 0)
                {
                    if (cColName == "GCGROSSPP" && !bWspRowMode)
                    {

                    }
                    else
                    {
                        nMpPer = CalMRPPer(cChkMD, nPurPrice, nMrp); //((nMrp - nPurPrice) * 100) / nPurPrice;
                        if (nMpPer < 0) nMpPer = 0;
                        rows[0]["wsp_percentage"] = Math.Round(nMpPer, 2);
                    }
                }

                rows[0].EndEdit();
            }
            catch { }
        }
        public static void GetAmount(DataRow[] rows, Boolean bFC /* Forex Enabled */)
        {
            try
            {
                double nQty = ConvertDouble(rows[0]["invoice_quantity"]);
                double nFQty = ConvertDouble(rows[0]["scheme_quantity"]);
                double nTotQty = nQty + nFQty;
                double nPurPrice = ConvertDouble(rows[0]["purchase_price"]);
                if (bFC)
                    nPurPrice = ConvertDouble(rows[0]["forex_purchase_price"]);
                double nAmount = nQty * nPurPrice;

                rows[0].BeginEdit();
                rows[0]["quantity"] = nTotQty;
                rows[0]["amount"] = nAmount;
                rows[0].EndEdit();
            }
            catch { }
        }
        //public static void RoundingMRP(ref double nMRPNew, string cMRP_Rounding_Mode, string cMRP_Rounding_Rs, string Rounding_Level)
        //{
        //    try
        //    {
        //        double nRoundingRs = ConvertDouble(cMRP_Rounding_Rs);

        //        if (nRoundingRs > 0 && nMRPNew > nRoundingRs)
        //        {
        //            if (cMRP_Rounding_Mode == "1")
        //            {
        //                double nDivResult = nMRPNew / nRoundingRs;
        //                double nModeResult = nMRPNew % nRoundingRs;
        //                if (Rounding_Level == "2")
        //                {
        //                    //nMRPNew = Math.Floor(nDivResult) * nRoundingRs;
        //                    if (nModeResult > 0 && nModeResult < nRoundingRs)
        //                    {
        //                        //if (nModeResult > nRoundingRs / 2) nMRPNew = nMRPNew + (nRoundingRs - nModeResult);
        //                        //else nMRPNew = nMRPNew - nModeResult;
        //                        //Anil

        //                        nMRPNew = Math.Round(nMRPNew, 0);

        //                    }
        //                    else nMRPNew = nMRPNew - nModeResult;
        //                }
        //                else
        //                {
        //                    nMRPNew = Math.Ceiling(nDivResult) * nRoundingRs;
        //                }

        //            }
        //            else if (cMRP_Rounding_Mode == "2")
        //            {
        //                double nUpValue = Math.Ceiling(nMRPNew);
        //                double nMultiple = Math.Pow(10, cMRP_Rounding_Rs.Length);
        //                double nRightSide = nUpValue % nMultiple;
        //                double nSubResult = (nRoundingRs - nRightSide);
        //                nMRPNew = nUpValue + nSubResult;
        //                if (nMRPNew < nUpValue) nMRPNew = nMRPNew + nMultiple;
        //            }
        //        }
        //    }
        //    catch { }
        //}
        public static void RoundingMRPold(ref double nMRPNew, string cMRP_Rounding_Mode, string cMRP_Rounding_Rs, string Rounding_Level)
        {
            try
            {
                double nRoundingRs = ConvertDouble(cMRP_Rounding_Rs);

                if (nRoundingRs > 0 && nMRPNew > nRoundingRs)
                {
                    if (cMRP_Rounding_Mode == "1")
                    {
                        double nDivResult = nMRPNew / nRoundingRs;
                        double nModeResult = nMRPNew % nRoundingRs;
                        if (Rounding_Level == "2")
                        {
                            //nMRPNew = Math.Floor(nDivResult) * nRoundingRs;
                            if (nModeResult > 0 && nModeResult < nRoundingRs)
                            {
                                //if (nModeResult > nRoundingRs / 2) nMRPNew = nMRPNew + (nRoundingRs - nModeResult);
                                //else nMRPNew = nMRPNew - nModeResult;

                                if (nModeResult == 0.5)
                                {
                                    nMRPNew = Math.Floor(nMRPNew);
                                }

                                else if (nModeResult >= nRoundingRs / 2)
                                {
                                    nMRPNew = nMRPNew + (nRoundingRs - nModeResult);
                                }
                                else
                                {
                                    //nMRPNew = Math.Round(nMRPNew, 0);
                                    nMRPNew = nMRPNew - nModeResult;
                                }

                            }
                            else
                            {
                                nMRPNew = nMRPNew - nModeResult;
                            }
                        }
                        else
                        {
                            nMRPNew = Math.Ceiling(nDivResult) * nRoundingRs;
                        }

                    }
                    else if (cMRP_Rounding_Mode == "2")
                    {
                        double nUpValue = Math.Ceiling(nMRPNew);
                        double nMultiple = Math.Pow(10, cMRP_Rounding_Rs.Length);
                        double nRightSide = nUpValue % nMultiple;
                        double nSubResult = (nRoundingRs - nRightSide);
                        if (Rounding_Level == "2")
                        {
                            double nUpValue1 = nUpValue - nMultiple;//1035-100=935
                            nUpValue1 += nSubResult;
                            double nRemaim_lower = nUpValue - nUpValue1;
                            if (nRemaim_lower < nSubResult)
                                nMRPNew = nUpValue1;// - Math.Abs(nSubResult);
                            else
                                nMRPNew = nUpValue + nSubResult;
                        }
                        else
                        {
                            nMRPNew = nUpValue + (nSubResult < 0 ? nMultiple + nSubResult : nSubResult);
                        }
                        //nMRPNew = nUpValue + nSubResult;
                        ////if(Math.Abs(nSubResult)>=(ConvertDouble(cMRP_Rounding_Rs)/2))
                        ////    nMRPNew = nMRPNew + nMultiple;
                        //////if (nMRPNew < nUpValue) 
                    }
                }
            }
            catch { }
        }

        public static void RoundingMRP(ref double nMRPNew, string cMRP_Rounding_Mode, string cMRP_Rounding_Rs, string Rounding_Level)
        {
            try
            {
                double nRoundingRs = ConvertDouble(cMRP_Rounding_Rs);

                if (nRoundingRs > 0 && nMRPNew > nRoundingRs)
                {
                    if (cMRP_Rounding_Mode == "1")
                    {
                        double nDivResult = nMRPNew / nRoundingRs;
                        double nModeResult = nMRPNew % nRoundingRs;
                        if (Rounding_Level != "1")
                        {
                            //nMRPNew = Math.Floor(nDivResult) * nRoundingRs;
                            if (nModeResult > 0 && nModeResult < nRoundingRs)
                            {
                                //if (nModeResult > nRoundingRs / 2) nMRPNew = nMRPNew + (nRoundingRs - nModeResult);
                                //else nMRPNew = nMRPNew - nModeResult;

                                if (nModeResult == 0.5)
                                {
                                    //nMRPNew = Math.Ceiling(nMRPNew);
                                    nMRPNew = Math.Floor(nMRPNew);
                                }

                                else if (nModeResult >= nRoundingRs / 2)
                                {
                                    nMRPNew = nMRPNew + (nRoundingRs - nModeResult);
                                }
                                else
                                {
                                    //nMRPNew = Math.Round(nMRPNew, 0);
                                    nMRPNew = nMRPNew - nModeResult;
                                }

                            }
                            else
                            {
                                nMRPNew = nMRPNew - nModeResult;
                            }
                        }
                        else
                        {
                            nMRPNew = Math.Ceiling(nDivResult) * nRoundingRs;
                        }

                    }
                    else if (cMRP_Rounding_Mode == "2")
                    {
                        double nUpValue = Math.Ceiling(nMRPNew);
                        double nMultiple = Math.Pow(10, cMRP_Rounding_Rs.Length);
                        double nRightSide = nUpValue % nMultiple;
                        double nSubResult = (nRoundingRs - nRightSide);
                        if (Rounding_Level != "1")
                        {
                            double nUpValue1 = nUpValue - nMultiple;//1035-100=935
                            nUpValue1 += nSubResult;
                            double nRemaim_lower = nUpValue - nUpValue1;
                            if (nRemaim_lower < nSubResult)
                                nMRPNew = nUpValue1;// - Math.Abs(nSubResult);
                            else
                                nMRPNew = nUpValue + nSubResult;
                        }
                        else
                        {
                            nMRPNew = nUpValue + (nSubResult < 0 ? nMultiple + nSubResult : nSubResult);
                        }
                        //nMRPNew = nUpValue + nSubResult;
                        ////if(Math.Abs(nSubResult)>=(ConvertDouble(cMRP_Rounding_Rs)/2))
                        ////    nMRPNew = nMRPNew + nMultiple;
                        //////if (nMRPNew < nUpValue) 
                    }
                }
            }
            catch { }
        }




        public static void ReCalculateTax(DataTable cTableDet, DataTable cTableMst, bool bTaxInc)
        {
            try
            {


                return;




                //DataRow drow_Mst;
                //if (cTableMst.Rows.Count > 0)
                //{
                //    drow_Mst = cTableMst.Rows[0];

                //    Double nDiscPer = 0;
                //    Double nAmt = 0;

                //    DataTable dtCopy = cTableDet.Copy();

                //    DataView dvDataTable = new DataView(dtCopy);
                //    String[] Str = { "article_code", "invoice_Quantity", "purchase_price", "form_id" };
                //    DataTable Dt_RECALTAX = new DataTable();
                //    Dt_RECALTAX = dvDataTable.ToTable("TEMP_RECALTAX", true, Str);

                //    foreach (DataRow recal in Dt_RECALTAX.Rows)
                //    {

                //        foreach (DataRow dr in dtCopy.Select("article_code = '" + Convert.ToString(recal["article_code"]) + "' and invoice_Quantity = " + ConvertDouble(recal["invoice_Quantity"]) + " and purchase_price = " + ConvertDouble(recal["purchase_price"]) + " and form_id = '" + Convert.ToString(recal["form_id"]) + "'", ""))
                //        {
                //            if (dr.RowState == DataRowState.Deleted)
                //                continue;
                //            DataRow[] drow123 = cTableDet.Select("row_id='" + Convert.ToString(dr["row_id"]) + "'");
                //            if (drow123.Length > 0)
                //            {
                //                Int32 iInd = cTableDet.Rows.IndexOf(drow123[0]);
                //                nAmt = 0; nDiscPer = 0;
                //                nDiscPer = ConvertDouble(cTableMst.Rows[0]["discount_percentage"]);
                //                nAmt = ConvertDouble(dr["invoice_Quantity"]) * ConvertDouble(dr["purchase_price"]);

                //                Double nAmtForTax = nAmt - (nAmt * (nDiscPer / 100));
                //                Double nTaxPer = ConvertDouble(dr["tax_percentage"]);
                //                Double nTax = 0;
                //                double GExcisePer = 0;
                //                double nExcisePer = 0;
                //                try
                //                {
                //                    if (!Equals(cTableMst, null) && cTableMst.Rows.Count > 0)
                //                    {
                //                        double nExciseAmt = ConvertDouble(drow_Mst["excise_duty_amount"]);
                //                        double nNatAmt = 0;
                //                        if (!Equals(cTableDet, null) && cTableDet.Rows.Count > 0)
                //                        {
                //                            nNatAmt = ConvertDouble(cTableDet.Compute("SUM(AMOUNT)", "tax_percentage > 0"));
                //                        }
                //                        nExcisePer = nNatAmt == 0 ? 0 : System.Math.Round((nExciseAmt / nNatAmt) * 100, 3);
                //                    }
                //                }
                //                catch { }
                //                GExcisePer = nExcisePer;
                //                if (bTaxInc)
                //                {
                //                    nTax = Math.Round(Double.Parse((nTaxPer != 0 ? (nAmtForTax - ((nAmtForTax * 100) / (100 + nTaxPer))).ToString() : Decimal.Zero.ToString())), 2);
                //                }
                //                else
                //                {
                //                    //nTax = Math.Round(Double.Parse((nTaxPer != 0 ? (((nAmtForTax * (nTaxPer / 100)))).ToString() : Decimal.Zero.ToString())), 2);
                //                    nTax = Math.Round(nAmt * (1 - (nDiscPer / 100)) * (1 + (GExcisePer / 100)) * (nTaxPer / 100), 2);
                //                }

                //                dr.BeginEdit();
                //                dr["amount"] = nAmt.ToString();
                //                dr["tax_amount"] = nTax.ToString("#####0.00");
                //                cTableDet.Rows[iInd]["amount"] = nAmt.ToString("#########0.00");
                //                cTableDet.Rows[iInd]["tax_amount"] = nTax.ToString("#########0.00");
                //                dr.EndEdit();

                //                DataTable dt = cTableDet;
                //                dt.Select("article_code = '" + Convert.ToString(recal["article_code"]) + "' and invoice_Quantity = " + ConvertDouble(recal["invoice_Quantity"]) + " and purchase_price = " + ConvertDouble(recal["purchase_price"]) + " and form_id = '" + Convert.ToString(recal["form_id"]) + "'").ToList<DataRow>().ForEach(r => r["amount"] = nAmt.ToString("#########0.00"));
                //                dt.Select("article_code = '" + Convert.ToString(recal["article_code"]) + "' and invoice_Quantity = " + ConvertDouble(recal["invoice_Quantity"]) + " and purchase_price = " + ConvertDouble(recal["purchase_price"]) + " and form_id = '" + Convert.ToString(recal["form_id"]) + "'").ToList<DataRow>().ForEach(r => r["tax_amount"] = nTax.ToString("#########0.00"));

                //            }
                //            break;
                //        }
                //    }


                //}
            }
            catch { }
        }

        public static void ReCalculateTaxRow(DataTable cTableDet, DataTable cTableMst, bool bTaxInc, String cROW_ID)
        {
            try
            {

                return;

                //DataRow drow_Mst;
                //if (cTableMst.Rows.Count > 0)
                //{
                //    drow_Mst = cTableMst.Rows[0];

                //    Double nDiscPer = 0;
                //    Double nAmt = 0;

                //    DataTable dtCopy = cTableDet.Copy();
                //    foreach (DataRow dr in dtCopy.Select("row_id = '" + cROW_ID + "'"))
                //    {
                //        if (dr.RowState == DataRowState.Deleted)
                //            continue;
                //        DataRow[] drow123 = cTableDet.Select("row_id='" + Convert.ToString(dr["row_id"]) + "'");
                //        if (drow123.Length > 0)
                //        {
                //            Int32 iInd = cTableDet.Rows.IndexOf(drow123[0]);
                //            nAmt = 0; nDiscPer = 0;
                //            nDiscPer = ConvertDouble(cTableMst.Rows[0]["discount_percentage"]);
                //            nAmt = ConvertDouble(dr["invoice_Quantity"]) * ConvertDouble(dr["purchase_price"]);

                //            Double nAmtForTax = nAmt - (nAmt * (nDiscPer / 100));
                //            Double nTaxPer = ConvertDouble(dr["tax_percentage"]);
                //            Double nTax = 0;
                //            double GExcisePer = 0;
                //            double nExcisePer = 0;
                //            try
                //            {
                //                if (!Equals(cTableMst, null) && cTableMst.Rows.Count > 0)
                //                {
                //                    double nExciseAmt = ConvertDouble(drow_Mst["excise_duty_amount"]);
                //                    double nNatAmt = 0;
                //                    if (!Equals(cTableDet, null) && cTableDet.Rows.Count > 0)
                //                    {
                //                        nNatAmt = ConvertDouble(cTableDet.Compute("SUM(AMOUNT)", "tax_percentage > 0"));
                //                    }
                //                    nExcisePer = nNatAmt == 0 ? 0 : System.Math.Round((nExciseAmt / nNatAmt) * 100, 3);
                //                }
                //            }
                //            catch { }
                //            GExcisePer = nExcisePer;
                //            if (bTaxInc)
                //            {
                //                nTax = Math.Round(Double.Parse((nTaxPer != 0 ? (nAmtForTax - ((nAmtForTax * 100) / (100 + nTaxPer))).ToString() : Decimal.Zero.ToString())), 2);
                //            }
                //            else
                //            {
                //                //nTax = Math.Round(Double.Parse((nTaxPer != 0 ? (((nAmtForTax * (nTaxPer / 100)))).ToString() : Decimal.Zero.ToString())), 2);
                //                nTax = Math.Round(nAmt * (1 - (nDiscPer / 100)) * (1 + (GExcisePer / 100)) * (nTaxPer / 100), 2);
                //            }

                //            dr.BeginEdit();
                //            dr["amount"] = nAmt.ToString();
                //            dr["tax_amount"] = nTax.ToString("#####0.00");
                //            cTableDet.Rows[iInd]["amount"] = nAmt.ToString("#########0.00");
                //            cTableDet.Rows[iInd]["tax_amount"] = nTax.ToString("#########0.00");
                //            dr.EndEdit();
                //        }
                //    }


                //}
            }
            catch { }
        }
        public static void GetAmountValues(DataTable cTableDet, DataTable cTableMst, bool bTaxInc)
        {
            if (cTableDet.Rows.Count == 0) return;
            try
            {
                double nSubTot = ConvertDouble(cTableDet.Compute("sum(amount)", ""));
                double nTotalQty = ConvertDouble(cTableDet.Compute("sum(quantity)", "article_no <> ''"));
                double nTotalTAx = ConvertDouble(cTableDet.Compute("sum(tax_amount)", "article_no <> ''"));
                cTableMst.Rows[0].BeginEdit();
                cTableMst.Rows[0]["subtotal"] = nSubTot.ToString("##,##,##,##0.00");
                cTableMst.Rows[0]["total_qty"] = nTotalQty.ToString("##,##,##,##0.00");
                cTableMst.Rows[0]["tax_amount"] = nTotalTAx.ToString("##,##,##,##0.00");
                cTableMst.Rows[0].EndEdit();
                bool bManualDis = ConvertBool(cTableMst.Rows[0]["manual_discount"]);
                bool bManualRound = ConvertBool(cTableMst.Rows[0]["manual_roundoff"]);
                // bool bManualBrokerAmt = ConvertBool(cTableMst.Rows[0]["manual_broker_comm"]);
                //  double nPartyAmt = ConvertDouble(cTableMst.Rows[0]["party_inv_amount"]);
                double nSubTotal = ConvertDouble(cTableMst.Rows[0]["subtotal"]);
                double nExcise = ConvertDouble(cTableMst.Rows[0]["excise_duty_amount"]);
                double nDisAmt = ConvertDouble(cTableMst.Rows[0]["discount_amount"]);
                double nDisPer = ConvertDouble(cTableMst.Rows[0]["discount_Percentage"]);
                double nTaxAmt = ConvertDouble(cTableMst.Rows[0]["tax_amount"]);
                double nFreight = ConvertDouble(cTableMst.Rows[0]["freight"]);
                double nOtherCh = ConvertDouble(cTableMst.Rows[0]["other_charges"]);
                double nRoundOff = ConvertDouble(cTableMst.Rows[0]["round_off"]);
                //double nBrokerComPer = Convert.ToDouble(cTableMst.Rows[0]["broker_comm_percentage"]);
                //double nBrokerAmt = Convert.ToDouble(cTableMst.Rows[0]["broker_comm_amount"]);
                if (!bManualDis)
                {
                    nDisAmt = Math.Round((nSubTotal * nDisPer) / 100, 2);
                }
                else
                {
                    nDisPer = Math.Round((nDisAmt * 100) / nSubTotal, 2);
                }
                if (bTaxInc) nTaxAmt = 0;

                Double nTemp = 0;
                if (!bManualRound)
                {
                    double nTotalAmt = (nSubTotal - nDisAmt) + nExcise + nTaxAmt + nFreight + nOtherCh;
                    nTemp = Math.Round(nTotalAmt, 0);
                    nRoundOff = nTemp - nTotalAmt;
                    nRoundOff = Math.Round(nRoundOff, 2);
                }
                else
                {
                    double nTotalAmt = (nSubTotal - nDisAmt) + nExcise + nTaxAmt + nFreight + nOtherCh + nRoundOff;
                    nTemp = nTotalAmt;
                }

                //if (!bManualBrokerAmt)
                //{
                //    nBrokerAmt = (nTemp * nBrokerComPer) / 100;
                //    cTableMst.Rows[0]["broker_comm_amount"] = nBrokerAmt.ToString("####,##,##,##0.00");
                //}
                //else
                //{
                //    nBrokerComPer = (nBrokerAmt * 100) / (nTemp == 0 ? 1 : nTemp);
                //    cTableMst.Rows[0]["broker_comm_percentage"] = nBrokerComPer.ToString("####,##,##,##0.00");
                //}

                //double nDiffAmt = nPartyAmt - nTemp;
                cTableMst.Rows[0].BeginEdit();
                cTableMst.Rows[0]["total_amount"] = nTemp.ToString("##,##,##,##0.00");
                cTableMst.Rows[0]["discount_amount"] = nDisAmt;
                cTableMst.Rows[0]["discount_Percentage"] = nDisPer;
                cTableMst.Rows[0]["round_off"] = nRoundOff;
                //cTableMst.Rows[0]["difference_amount"] = nDiffAmt.ToString("##,##,##,##0.00");
                cTableMst.Rows[0].EndEdit();
            }
            catch { }
        }
        //public static void ReCalculateTax(DataTable cTableDet, DataTable cTableMst, bool bTaxInc, string cStri)
        //{
        //    try
        //    {
        //        return;


        //        DataRow drow_Mst;
        //        if (cTableMst.Rows.Count > 0)
        //        {
        //            drow_Mst = cTableMst.Rows[0];

        //            Double nDiscPer = 0;
        //            Double nAmt = 0;

        //            DataTable dtCopy = cTableDet.Copy();
        //            foreach (DataRow dr in dtCopy.Rows)
        //            {
        //                if (dr.RowState == DataRowState.Deleted)
        //                    continue;
        //                DataRow[] drow123 = cTableDet.Select("row_id='" + Convert.ToString(dr["row_id"]) + "'");
        //                if (drow123.Length > 0)
        //                {
        //                    Int32 iInd = cTableDet.Rows.IndexOf(drow123[0]);
        //                    nAmt = 0; nDiscPer = 0;
        //                    nDiscPer = ConvertDouble(cTableMst.Rows[0]["discount_percentage"]);
        //                    nAmt = ConvertDouble(dr["invoice_Quantity"]) * ConvertDouble(dr["purchase_price"]);

        //                    Double nAmtForTax = nAmt - (nAmt * (nDiscPer / 100));
        //                    Double nTaxPer = ConvertDouble(dr["tax_percentage"]);
        //                    Double nTax = 0;
        //                    double GExcisePer = 0;
        //                    double nExcisePer = 0;
        //                    try
        //                    {
        //                        if (!Equals(cTableMst, null) && cTableMst.Rows.Count > 0)
        //                        {
        //                            double nExciseAmt = ConvertDouble(drow_Mst["excise_duty_amount"]);
        //                            double nNatAmt = 0;
        //                            if (!Equals(cTableDet, null) && cTableDet.Rows.Count > 0)
        //                            {
        //                                nNatAmt = ConvertDouble(cTableDet.Compute("SUM(AMOUNT)", "tax_percentage > 0"));
        //                            }
        //                            nExcisePer = nNatAmt == 0 ? 0 : System.Math.Round((nExciseAmt / nNatAmt) * 100, 3);
        //                        }
        //                    }
        //                    catch { }
        //                    GExcisePer = nExcisePer;
        //                    if (bTaxInc)
        //                    {
        //                        nTax = Math.Round(Double.Parse((nTaxPer != 0 ? (nAmtForTax - ((nAmtForTax * 100) / (100 + nTaxPer))).ToString() : Decimal.Zero.ToString())), 2);
        //                    }
        //                    else
        //                    {
        //                        //nTax = Math.Round(Double.Parse((nTaxPer != 0 ? (((nAmtForTax * (nTaxPer / 100)))).ToString() : Decimal.Zero.ToString())), 2);
        //                        nTax = Math.Round(nAmt * (1 - (nDiscPer / 100)) * (1 + (GExcisePer / 100)) * (nTaxPer / 100), 2);
        //                    }

        //                    dr.BeginEdit();
        //                    dr["amount"] = nAmt.ToString();
        //                    dr["tax_amount"] = nTax.ToString("#####0.00");
        //                    cTableDet.Rows[iInd]["amount"] = nAmt.ToString("#########0.00");
        //                    cTableDet.Rows[iInd]["tax_amount"] = nTax.ToString("#########0.00");
        //                    dr.EndEdit();
        //                }
        //            }


        //        }
        //    }
        //    catch { }
        //}
    }
}
