using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;

namespace  WOWIntegration
{
    
    public class clsSaleMethods
    {
        public bool EossReturnItemsProcessing { get; set; }
        internal String _AppPath = "";
        private string applyPowerPricingInc(ref DataTable dtCmdSchemes, DataTable dtSlabs, DataRow drSchemeDet, DataTable dtSkuNames)
        {

            decimal nSchemeBuyValue, nSchemeToRange, nSchemeGetValue;
            Boolean bSchemeApplied = false;
            int nBuyType, nGetType;
            decimal nSetQty = 0;
            string retMsg = "";
            commonMethods globalMethods = new commonMethods();

            try
            {
                string cSchemeName = drSchemeDet["schemeName"].ToString();
                string cSchemeRowId = drSchemeDet["schemeRowId"].ToString();

                decimal nQty = 0;
                // We need to do this so that we can process the scheme on the items ordered on pending scheme qty/amount desc
                foreach (DataRow dr in dtCmdSchemes.Rows)
                {
                    nQty = Math.Abs(globalMethods.ConvertDecimal(dr["quantity"])) - Math.Abs(globalMethods.ConvertDecimal(dr["scheme_applied_qty"]));
                    dr["pending_scheme_apply_qty"] = nQty;
                    dr["pending_scheme_apply_amount"] = (globalMethods.ConvertDecimal(dr["mrp"]) * Math.Abs(globalMethods.ConvertDecimal(dr["quantity"]))) - globalMethods.ConvertDecimal(dr["scheme_applied_amount"]);
                }

                dtCmdSchemes.AcceptChanges();


                DataRow[] drSkuNamesBuy = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "' and buybc=1", "");

                Decimal BuyItemsTotal = 0, GetItemsTotal = 0, nNetValue, nSchemeAppliedQty = 0, nLoopValue = 0, nAddValue = 0,
                nMrp, nDiscountFigure, nDiscountAmount, nDiscountPercentage, nSchemeAppliedAmount = 0;


                string cProductCode;

                int nDiscMethod;

                DataTable dtFilteredCmdBuy = dtCmdSchemes.Clone();

                if (drSkuNamesBuy.Length == 0)
                    goto lblEnd;


                string filterTableData;

                DataTable dtSkuNamesBuy = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "' and buybc=1", "").CopyToDataTable();

                filterTableData = " pending_scheme_apply_qty>0 ";

                string cMessage = globalMethods.JoinDataTables(dtCmdSchemes, dtSkuNamesBuy, ref dtFilteredCmdBuy, filterTableData,
                (row1, row2) =>
                row1.Field<String>("product_code") == row2.Field<String>("product_code"));

                if (!String.IsNullOrEmpty(cMessage))
                    return cMessage;

                if (dtFilteredCmdBuy.Rows.Count == 0)
                    goto lblEnd;

                decimal nLoop = 0, nBaseQty = 0, nTotQty = 0;
                nLoopValue = 0;

                bool bAPplyWtdDiscount = false;

                DataTable dtFilteredCmdBuyOrdered = new DataTable();

                string cOrderColumn = "mrp";

                dtFilteredCmdBuy.DefaultView.Sort = cOrderColumn + " DESC ";

                dtFilteredCmdBuyOrdered = dtFilteredCmdBuy.DefaultView.ToTable();

                dtFilteredCmdBuyOrdered.Columns.Add("WtdDiscountBaseValue", typeof(decimal));

                DataTable dtFilteredSlab = dtSlabs.Select("schemeRowId='" + cSchemeRowId + "'", "").CopyToDataTable();

                foreach (DataRow drDetail in dtFilteredCmdBuyOrdered.Rows)
                {
                    nBaseQty = globalMethods.ConvertDecimal(drDetail["pending_scheme_apply_qty"]);

                    nTotQty = nTotQty + nBaseQty;

                    DataRow[] drSearch = dtFilteredSlab.Select(nTotQty.ToString() + ">=buyFromRange and " + nTotQty.ToString() + "<=buyToRange", "");

                    if (drSearch.Length == 0)
                        break;

                    DataRow drSlabs = drSearch[0];

                    string cErr = "";

                    cErr = applyDiscountasPerMethod(drDetail, cSchemeRowId, globalMethods.ConvertDecimal(drSlabs["discountPercentage"]),
                    globalMethods.ConvertDecimal(drSlabs["discountAmount"]), globalMethods.ConvertDecimal(drSlabs["netPrice"]));

                    if (!string.IsNullOrEmpty(cErr))
                        return cErr;


                    drDetail["scheme_applied_qty"] = globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]) + nBaseQty;
                    drDetail["scheme_applied_amount"] = globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]) +
                        (globalMethods.ConvertDecimal(drDetail["mrp"]) * nBaseQty);


                    drDetail["pending_scheme_apply_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["quantity"])) - globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]);
                    drDetail["pending_scheme_apply_amount"] = Math.Abs((globalMethods.ConvertDecimal(drDetail["quantity"]) * globalMethods.ConvertDecimal(drDetail["mrp"])) -
                        globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]));


                    if (!drDetail["scheme_name"].ToString().Contains(cSchemeName))
                    {
                        drDetail["scheme_name"] = drDetail["scheme_name"] + (string.IsNullOrEmpty(drDetail["scheme_name"].ToString()) ? "" : ",") +
                        drSchemeDet["schemeName"].ToString();
                    }

                    drDetail["slsdet_row_id"] = cSchemeRowId;

                }

                synchCmdSchemes(dtFilteredCmdBuyOrdered, ref dtCmdSchemes);

                // Commit changes in cmd for scheme applied  for current set
                dtCmdSchemes.AcceptChanges();

            }

            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);
                retMsg = "Error in Applying Power pricing (inc) at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();

            }

        lblEnd:

            return retMsg;

        }
        private string ReCalNetOnRange(ref DataTable dtCmd, DataTable tConfig)
        {
            commonMethods globalMethods = new commonMethods();
            String cLineNo = "ReCalNetOnRange 10";
            bool dataFound = false;
            try
            {


                DataRow[] drExcl = dtCmd.Select("tax_method=2", "");

                if (drExcl.Length > 0)
                {
                    cLineNo = "ReCalNetOnRange 20";
                    decimal nExclRangeFrom = globalMethods.ConvertDecimal(tConfig.Rows[0]["FROM_NET_RANGE_EXCLUSIVE"]);
                    decimal nExclRangeTo = globalMethods.ConvertDecimal(tConfig.Rows[0]["TO_NET_RANGE_EXCLUSIVE"]);
                    decimal nExclRangeNet = globalMethods.ConvertDecimal(tConfig.Rows[0]["RANGE_EXCLUSIVE_CONVERT_NET"]);

                    if (nExclRangeNet > 0)
                    {
                        DataTable dtCmdExcl = dtCmd.Select("tax_method=2 AND (net-cmm_discount_amount) BETWEEN " + nExclRangeFrom.ToString() + " AND " + nExclRangeTo.ToString(), "").CopyToDataTable();


                        if (dtCmdExcl.Rows.Count > 0)
                        {
                            dtCmdExcl.AsEnumerable().ToList().ForEach(r =>
                            {
                                r["BASIC_DISCOUNT_AMOUNT"] = globalMethods.ConvertDecimal(r["BASIC_DISCOUNT_AMOUNT"]) + (globalMethods.ConvertDecimal(r["net"]) - globalMethods.ConvertDecimal(r["CMM_DISCOUNT_AMOUNT"])
                                - (nExclRangeNet * (globalMethods.ConvertDecimal(r["quantity"]) > 0 ? 1 : -1)));
                                r["BASIC_DISCOUNT_PERCENTAGE"] = Math.Abs(Math.Round(globalMethods.ConvertDecimal(r["BASIC_DISCOUNT_AMOUNT"]) / (globalMethods.ConvertDecimal(r["mrp"])
                                                                  * globalMethods.ConvertDecimal(r["quantity"]) * 100), 3));

                                r["DISCOUNT_AMOUNT"] = globalMethods.ConvertDecimal(r["BASIC_DISCOUNT_AMOUNT"]) + globalMethods.ConvertDecimal(r["CARD_DISCOUNT_AMOUNT"]);
                                r["DISCOUNT_PERCENTAGE"] = Math.Abs(Math.Round((globalMethods.ConvertDecimal(r["DISCOUNT_AMOUNT"]) / (globalMethods.ConvertDecimal(r["mrp"]) * globalMethods.ConvertDecimal(r["quantity"]))) * 100, 3));

                                r["net"] = (globalMethods.ConvertDecimal(r["mrp"]) * globalMethods.ConvertDecimal(r["quantity"])) - globalMethods.ConvertDecimal(r["DISCOUNT_AMOUNT"]);

                            });

                            dataFound = true;
                        }
                    }

                }

                DataRow[] drIncl = dtCmd.Select("isnull(tax_method,0) in (0,1)", "");

                if (drIncl.Length > 0)
                {
                    cLineNo = "ReCalNetOnRange 30";
                    decimal nInclRangeFrom = globalMethods.ConvertDecimal(tConfig.Rows[0]["FROM_NET_RANGE_INCLUSIVE"]);
                    decimal nInclRangeTo = globalMethods.ConvertDecimal(tConfig.Rows[0]["TO_NET_RANGE_INCLUSIVE"]);
                    decimal nInclRangeNet = globalMethods.ConvertDecimal(tConfig.Rows[0]["RANGE_INCLUSIVE_CONVERT_NET"]);

                    if (nInclRangeNet > 0)
                    {
                        DataTable dtCmdIncl = dtCmd.Clone();
                        DataRow[] drCmdIncl = dtCmd.Select("isnull(tax_method,0) in (0,1) AND (net-cmm_discount_amount)>=" + nInclRangeFrom.ToString() + " AND (net-cmm_discount_amount)<=" + nInclRangeTo.ToString(), "");
                        if (drCmdIncl.Length > 0)
                            dtCmdIncl = drCmdIncl.CopyToDataTable();
                        if (dtCmdIncl.Rows.Count > 0)
                        {
                            cLineNo = "ReCalNetOnRange 40";
                            dtCmdIncl.AsEnumerable().ToList().ForEach(r =>
                            {
                                r["BASIC_DISCOUNT_AMOUNT"] = globalMethods.ConvertDecimal(r["BASIC_DISCOUNT_AMOUNT"]) + (globalMethods.ConvertDecimal(r["net"]) - globalMethods.ConvertDecimal(r["CMM_DISCOUNT_AMOUNT"])
                                - (nInclRangeNet * (globalMethods.ConvertDecimal(r["quantity"]) > 0 ? 1 : -1)));
                                r["BASIC_DISCOUNT_PERCENTAGE"] = Math.Abs(Math.Round(globalMethods.ConvertDecimal(r["BASIC_DISCOUNT_AMOUNT"]) / (globalMethods.ConvertDecimal(r["mrp"]) * globalMethods.ConvertDecimal(r["quantity"]) * 100), 3));

                                r["DISCOUNT_AMOUNT"] = globalMethods.ConvertDecimal(r["BASIC_DISCOUNT_AMOUNT"]) + globalMethods.ConvertDecimal(r["CARD_DISCOUNT_AMOUNT"]);
                                r["DISCOUNT_PERCENTAGE"] = Math.Round((globalMethods.ConvertDecimal(r["DISCOUNT_AMOUNT"]) / (globalMethods.ConvertDecimal(r["mrp"]) * globalMethods.ConvertDecimal(r["quantity"]))) * 100, 3);

                                r["net"] = (globalMethods.ConvertDecimal(r["mrp"]) * globalMethods.ConvertDecimal(r["quantity"])) - globalMethods.ConvertDecimal(r["DISCOUNT_AMOUNT"]);

                            });

                            dataFound = true;
                        }
                    }

                }
                cLineNo = "";
            }
            catch (Exception ex)
            {
                cLineNo = cLineNo + ex.Message;
            }
            return cLineNo;
        }

        private string ReCalNetOnRange_WSL(ref DataTable dtCmd, DataTable tConfig)
        {
            commonMethods globalMethods = new commonMethods();

            bool dataFound = false;

            DataRow[] drExcl = dtCmd.Select("tax_method=2", "");

            if (drExcl.Length > 0)
            {
                decimal nExclRangeFrom = globalMethods.ConvertDecimal(tConfig.Rows[0]["FROM_NET_RANGE_EXCLUSIVE"]);
                decimal nExclRangeTo = globalMethods.ConvertDecimal(tConfig.Rows[0]["TO_NET_RANGE_EXCLUSIVE"]);
                decimal nExclRangeNet = globalMethods.ConvertDecimal(tConfig.Rows[0]["RANGE_EXCLUSIVE_CONVERT_NET"]);

                if (nExclRangeNet > 0)
                {
                    DataTable dtCmdExcl = dtCmd.Select("tax_method=2 AND (net-cmm_discount_amount) BETWEEN " + nExclRangeFrom.ToString() + " AND " + nExclRangeTo.ToString(), "").CopyToDataTable();


                    if (dtCmdExcl.Rows.Count > 0)
                    {
                        dtCmdExcl.AsEnumerable().ToList().ForEach(r =>
                        {
                            r["BASIC_DISCOUNT_AMOUNT"] = globalMethods.ConvertDecimal(r["BASIC_DISCOUNT_AMOUNT"]) + (globalMethods.ConvertDecimal(r["net"]) - globalMethods.ConvertDecimal(r["CMM_DISCOUNT_AMOUNT"])
                            - (nExclRangeNet * (globalMethods.ConvertDecimal(r["quantity"]) > 0 ? 1 : -1)));
                            r["BASIC_DISCOUNT_PERCENTAGE"] = Math.Abs(Math.Round(globalMethods.ConvertDecimal(r["BASIC_DISCOUNT_AMOUNT"]) / (globalMethods.ConvertDecimal(r["mrp"])
                                                              * globalMethods.ConvertDecimal(r["quantity"]) * 100), 3));

                            r["DISCOUNT_AMOUNT"] = globalMethods.ConvertDecimal(r["BASIC_DISCOUNT_AMOUNT"]) + globalMethods.ConvertDecimal(r["CARD_DISCOUNT_AMOUNT"]);
                            r["DISCOUNT_PERCENTAGE"] = Math.Abs(Math.Round((globalMethods.ConvertDecimal(r["DISCOUNT_AMOUNT"]) / (globalMethods.ConvertDecimal(r["mrp"]) * globalMethods.ConvertDecimal(r["quantity"]))) * 100, 3));

                            r["net"] = (globalMethods.ConvertDecimal(r["mrp"]) * globalMethods.ConvertDecimal(r["quantity"])) - globalMethods.ConvertDecimal(r["DISCOUNT_AMOUNT"]);

                        });

                        dataFound = true;
                    }
                }

            }

            DataRow[] drIncl = dtCmd.Select("isnull(tax_method,0) in (0,1)", "");

            if (drIncl.Length > 0)
            {
                decimal nInclRangeFrom = globalMethods.ConvertDecimal(tConfig.Rows[0]["FROM_NET_RANGE_INCLUSIVE"]);
                decimal nInclRangeTo = globalMethods.ConvertDecimal(tConfig.Rows[0]["TO_NET_RANGE_INCLUSIVE"]);
                decimal nInclRangeNet = globalMethods.ConvertDecimal(tConfig.Rows[0]["RANGE_INCLUSIVE_CONVERT_NET"]);

                if (nInclRangeNet > 0)
                {
                    DataTable dtCmdIncl = dtCmd.Select("isnull(tax_method,0) in (0,1) AND (net-cmm_discount_amount)>=" + nInclRangeFrom.ToString() + " AND (net-cmm_discount_amount)<=" + nInclRangeTo.ToString(), "").CopyToDataTable();

                    if (dtCmdIncl.Rows.Count > 0)
                    {
                        dtCmdIncl.AsEnumerable().ToList().ForEach(r =>
                        {
                            r["BASIC_DISCOUNT_AMOUNT"] = globalMethods.ConvertDecimal(r["BASIC_DISCOUNT_AMOUNT"]) + (globalMethods.ConvertDecimal(r["net"]) - globalMethods.ConvertDecimal(r["CMM_DISCOUNT_AMOUNT"])
                            - (nInclRangeNet * (globalMethods.ConvertDecimal(r["quantity"]) > 0 ? 1 : -1)));
                            r["BASIC_DISCOUNT_PERCENTAGE"] = Math.Abs(Math.Round(globalMethods.ConvertDecimal(r["BASIC_DISCOUNT_AMOUNT"]) / (globalMethods.ConvertDecimal(r["mrp"]) * globalMethods.ConvertDecimal(r["quantity"]) * 100), 3));

                            r["DISCOUNT_AMOUNT"] = globalMethods.ConvertDecimal(r["BASIC_DISCOUNT_AMOUNT"]) + globalMethods.ConvertDecimal(r["CARD_DISCOUNT_AMOUNT"]);
                            r["DISCOUNT_PERCENTAGE"] = Math.Round((globalMethods.ConvertDecimal(r["DISCOUNT_AMOUNT"]) / (globalMethods.ConvertDecimal(r["mrp"]) * globalMethods.ConvertDecimal(r["quantity"]))) * 100, 3);

                            r["net"] = (globalMethods.ConvertDecimal(r["mrp"]) * globalMethods.ConvertDecimal(r["quantity"])) - globalMethods.ConvertDecimal(r["DISCOUNT_AMOUNT"]);

                        });

                        dataFound = true;
                    }
                }

            }

            return "";
        }

        private string ProcessEossRoundOff(ref DataTable dtCmdSchemes, int nRoundOffConfig)
        {
            commonMethods globalMethods = new commonMethods();
            if (nRoundOffConfig == 1)
            {
                foreach (DataRow dr in dtCmdSchemes.Rows)
                {
                    if (!String.IsNullOrEmpty(dr["scheme_name"].ToString()))
                    {
                        dr["basic_discount_amount"] = Math.Round(globalMethods.ConvertDecimal(dr["basic_discount_amount"]), MidpointRounding.AwayFromZero);
                        //dr["net"] = Math.Round((globalMethods.ConvertDecimal(dr["quantity"]) * globalMethods.ConvertDecimal(dr["mrp"])) - globalMethods.ConvertDecimal(dr["basic_discount_amount"]), 2);
                        dr["net"] = Math.Round((globalMethods.ConvertDecimal(dr["quantity"]) * globalMethods.ConvertDecimal(dr["mrp"]))) - globalMethods.ConvertDecimal(dr["basic_discount_amount"]);
                    }

                }
            }

            return "";
        }


        private string updateColsonJoinTable(DataTable dtSource, ref DataTable dtTarget, string cSourceJoinColumn, string cTargetJoinColumn,
            string cSourceRefColumn, string cTargetUpdateColumn)
        {
            commonMethods globalMethods = new commonMethods();

            dtTarget.AsEnumerable().Join
            (
                dtSource.AsEnumerable(),
                lMaster => lMaster[cSourceJoinColumn], lChild => lChild[cTargetJoinColumn],
                (lMaster, lChild) => new { lMaster, lChild }
                ).ToList().ForEach
            (
            o =>
            {
                o.lMaster.SetField(cTargetUpdateColumn, globalMethods.ConvertDecimal(o.lChild[cSourceRefColumn]));
            }
            );

            return "";
        }
        public string CalcGstOC_WSL(ref DataTable dtCmm, DataTable dtCmd, Boolean bRegisteredDealer, string cCurStateCode, string cPartyStateCode, DataTable tConfig)
        {
            //@CCURSTATE_CODE,@CLOC_GSTN_NO,@CPARTY_GSTN_NO,@BREGISTERED_DELEER,@BCESS_APPLICABLE,@CFC_CODE,@CALL_XN_IGST,
            //@CALWAYS_PICK_GST_MODE_IN_RETAIL

            string cMessage = "";
            commonMethods globalMethods = new commonMethods();

            decimal nOtherCharges = globalMethods.ConvertDecimal(dtCmm.Rows[0]["other_charges"]);

            if (nOtherCharges == 0 || !bRegisteredDealer)
            {
                dtCmm.Rows[0]["OTHER_CHARGES_GST_PERCENTAGE"] = 0;
                dtCmm.Rows[0]["OTHER_CHARGES_IGST_AMOUNT"] = 0;
                dtCmm.Rows[0]["OTHER_CHARGES_CGST_AMOUNT"] = 0;
                dtCmm.Rows[0]["OTHER_CHARGES_SGST_AMOUNT"] = 0;
                dtCmm.Rows[0]["OTHER_CHARGES_TAXABLE_VALUE"] = dtCmm.Rows[0]["other_charges"];
                dtCmm.Rows[0]["OTHER_CHARGES_HSN_CODE"] = "0000000000";

                return "";
            }

            string cOcHsnCode =Convert.ToString(dtCmm.Rows[0]["OTHER_CHARGES_HSN_CODE"]);
            decimal nOcGstPct = globalMethods.ConvertDecimal(dtCmm.Rows[0]["OTHER_CHARGES_Gst_Percentage"]);

            if (String.IsNullOrEmpty(cOcHsnCode))
            {
                cMessage = "Other charges Hsn Code not found";
                return cMessage;
            }

            //dtCmm.Rows[0]["OTHER_CHARGES_HSN_CODE"] = cOcHsnCode;
            //if (nOcGstPct == 0)
            //{
            //    nOcGstPct = globalMethods.ConvertDecimal(dtCmd.Compute("max(gst_percentage)", ""));
            //}

            //dtCmm.Rows[0]["OTHER_CHARGES_GST_PERCENTAGE"] = nOcGstPct;

            int nOhTaxMethod = globalMethods.ConvertInt(dtCmm.Rows[0]["OH_TAX_METHOD"]);

            dtCmm.Rows[0]["OTHER_CHARGES_TAXABLE_VALUE"] = Math.Round(nOtherCharges - (nOtherCharges * (nOhTaxMethod == 2 ? nOcGstPct / (100 + nOcGstPct) : 0)), 2);

            decimal nOcGstAmount = Math.Round(globalMethods.ConvertDecimal(dtCmm.Rows[0]["OTHER_CHARGES_TAXABLE_VALUE"]) * nOcGstPct / 100, 2);

            if (cCurStateCode == cPartyStateCode)
            {
                dtCmm.Rows[0]["OTHER_CHARGES_CGST_AMOUNT"] = Math.Round(nOcGstAmount / 2, 2);
                dtCmm.Rows[0]["OTHER_CHARGES_SGST_AMOUNT"] = dtCmm.Rows[0]["OTHER_CHARGES_CGST_AMOUNT"];
            }
            else
            {
                dtCmm.Rows[0]["OTHER_CHARGES_CGST_AMOUNT"] = nOcGstAmount;
            }

            return "";
        }

        public string CalcGstFreight_WSL(ref DataTable dtCmm, DataTable dtCmd, Boolean bRegisteredDealer, string cCurStateCode, string cPartyStateCode, DataTable tConfig)
        {
            //@CCURSTATE_CODE,@CLOC_GSTN_NO,@CPARTY_GSTN_NO,@BREGISTERED_DELEER,@BCESS_APPLICABLE,@CFC_CODE,@CALL_XN_IGST,
            //@CALWAYS_PICK_GST_MODE_IN_RETAIL

            string cMessage = "";
            commonMethods globalMethods = new commonMethods();

            decimal nOtherCharges = globalMethods.ConvertDecimal(dtCmm.Rows[0]["freight"]);

            if (nOtherCharges == 0 || !bRegisteredDealer)
            {
                dtCmm.Rows[0]["FREIGHT_GST_PERCENTAGE"] = 0;
                dtCmm.Rows[0]["FREIGHT_IGST_AMOUNT"] = 0;
                dtCmm.Rows[0]["FREIGHT_CGST_AMOUNT"] = 0;
                dtCmm.Rows[0]["FREIGHT_SGST_AMOUNT"] = 0;
                dtCmm.Rows[0]["FREIGHT_TAXABLE_VALUE"] = dtCmm.Rows[0]["FREIGHT"];
                dtCmm.Rows[0]["FREIGHT_HSN_CODE"] = "0000000000";

                return "";
            }

            string cOcHsnCode = Convert.ToString(dtCmm.Rows[0]["freight_hsn_code"]);
            decimal nOcGstPct = globalMethods.ConvertDecimal(dtCmm.Rows[0]["freight_gst_percentage"]);

            if (String.IsNullOrEmpty(cOcHsnCode))
            {
                cMessage = "Freight charges Hsn Code not found";
                return cMessage;
            }

            //if (nOcGstPct == 0)
            //{
            //    nOcGstPct = globalMethods.ConvertDecimal(dtCmd.Compute("max(gst_percentage)", ""));
            //}

            //dtCmm.Rows[0]["freight_GST_PERCENTAGE"] = nOcGstPct;

            int nOhTaxMethod = 2;// globalMethods.ConvertInt(dtCmm.Rows[0]["OH_TAX_METHOD"]);

            dtCmm.Rows[0]["FREIGHT_TAXABLE_VALUE"] = Math.Round(nOtherCharges - (nOtherCharges * (nOhTaxMethod == 2 ? nOcGstPct / (100 + nOcGstPct) : 0)), 2);

            decimal nOcGstAmount = Math.Round(globalMethods.ConvertDecimal(dtCmm.Rows[0]["FREIGHT_TAXABLE_VALUE"]) * nOcGstPct / 100, 2);

            if (cCurStateCode == cPartyStateCode)
            {
                dtCmm.Rows[0]["FREIGHT_CGST_AMOUNT"] = Math.Round(nOcGstAmount / 2, 2);
                dtCmm.Rows[0]["FREIGHT_SGST_AMOUNT"] = dtCmm.Rows[0]["FREIGHT_CGST_AMOUNT"];
            }
            else
            {
                dtCmm.Rows[0]["FREIGHT_CGST_AMOUNT"] = nOcGstAmount;
            }

            return "";
        }
        public string CalcGstOC(ref DataTable dtCmm, DataTable dtCmd, Boolean bRegisteredDealer, string cCurStateCode, string cPartyStateCode, DataTable tConfig)
        {
            //@CCURSTATE_CODE,@CLOC_GSTN_NO,@CPARTY_GSTN_NO,@BREGISTERED_DELEER,@BCESS_APPLICABLE,@CFC_CODE,@CALL_XN_IGST,
            //@CALWAYS_PICK_GST_MODE_IN_RETAIL

            string cMessage = "";
            commonMethods globalMethods = new commonMethods();

            decimal nOtherCharges = globalMethods.ConvertDecimal(dtCmm.Rows[0]["atd_charges"]);

            if (nOtherCharges == 0 || !bRegisteredDealer)
            {
                dtCmm.Rows[0]["OTHER_CHARGES_GST_PERCENTAGE"] = 0;
                dtCmm.Rows[0]["OTHER_CHARGES_IGST_AMOUNT"] = 0;
                dtCmm.Rows[0]["OTHER_CHARGES_CGST_AMOUNT"] = 0;
                dtCmm.Rows[0]["OTHER_CHARGES_SGST_AMOUNT"] = 0;
                dtCmm.Rows[0]["OTHER_CHARGES_TAXABLE_VALUE"] = dtCmm.Rows[0]["atd_charges"];
                dtCmm.Rows[0]["OTHER_CHARGES_HSN_CODE"] = "0000000000";

                return "";
            }

            string cOcHsnCode = tConfig.Rows[0]["OTHER_CHARGES_HSN_CODE"].ToString();
            decimal nOcGstPct = globalMethods.ConvertDecimal(tConfig.Rows[0]["OTHER_CHARGES_Gst_Percentage"]);

            if (String.IsNullOrEmpty(cOcHsnCode))
            {
                cMessage = "Other charges Hsn Code not found";
                return cMessage;
            }

            dtCmm.Rows[0]["OTHER_CHARGES_HSN_CODE"] = cOcHsnCode;
            if (nOcGstPct == 0)
            {
                nOcGstPct = globalMethods.ConvertDecimal(dtCmd.Compute("max(gst_percentage)", ""));
            }

            dtCmm.Rows[0]["OTHER_CHARGES_GST_PERCENTAGE"] = nOcGstPct;

            int nOhTaxMethod = globalMethods.ConvertInt(dtCmm.Rows[0]["OH_TAX_METHOD"]);

            dtCmm.Rows[0]["OTHER_CHARGES_TAXABLE_VALUE"] = Math.Round(nOtherCharges - (nOtherCharges * (nOhTaxMethod == 2 ? nOcGstPct / (100 + nOcGstPct) : 0)), 2);

            decimal nOcGstAmount = Math.Round(globalMethods.ConvertDecimal(dtCmm.Rows[0]["OTHER_CHARGES_TAXABLE_VALUE"]) * nOcGstPct / 100, 2);

            if (cCurStateCode == cPartyStateCode)
            {
                dtCmm.Rows[0]["OTHER_CHARGES_CGST_AMOUNT"] = Math.Round(nOcGstAmount / 2, 2);
                dtCmm.Rows[0]["OTHER_CHARGES_SGST_AMOUNT"] = dtCmm.Rows[0]["OTHER_CHARGES_CGST_AMOUNT"];
            }
            else
            {
                dtCmm.Rows[0]["OTHER_CHARGES_CGST_AMOUNT"] = nOcGstAmount;
            }

            return "";
        }

        private string CalcCmmDiscountAmount(ref DataTable dtCmd, decimal nCmmDiscount)
        {

            //Firstly we need to reset this column so that if user makes old discount to zero now
            dtCmd.AsEnumerable().ToList().ForEach(r =>
            {
                r["cmm_discount_amount"] = 0;
            });

            if (nCmmDiscount == 0)
                return "";

            decimal nSubtotal, nSlsSubtotal, nSlrSubtotal;

            commonMethods globalMethods = new commonMethods();

            nSubtotal = globalMethods.ConvertDecimal(dtCmd.Compute("SUM(net)", ""));
            nSlsSubtotal = globalMethods.ConvertDecimal(dtCmd.Compute("SUM(net)", "quantity>0"));
            nSlrSubtotal = globalMethods.ConvertDecimal(dtCmd.Compute("SUM(net)", "quantity<0"));

            if (nSubtotal > 0)
            {

                dtCmd.AsEnumerable().ToList().ForEach(r =>
                {
                    if (globalMethods.ConvertDecimal(r["quantity"]) > 0)
                        r["cmm_discount_amount"] = Math.Round((globalMethods.ConvertDecimal(r["NET"]) * nCmmDiscount) / nSlsSubtotal, 2);

                });
            }
            else
            {
                dtCmd.AsEnumerable().ToList().ForEach(r =>
                {
                    if (globalMethods.ConvertDecimal(r["quantity"]) < 0)
                        r["cmm_discount_amount"] = Math.Round((globalMethods.ConvertDecimal(r["NET"]) * nCmmDiscount) / nSlrSubtotal, 2);

                });
            }

            return "";
        }

        public string CalcGst(SqlConnection conn, ref DataTable dtCmm, ref DataTable dtCmd, DataTable tConfig, DataTable dtHsnMst, DataTable dtHsnDet,
                              DataTable dtGstCalc)
        {

            string cMessage = "";
            commonMethods globalMethods = new commonMethods();

            Int32 nRegisteredDealer = Convert.ToInt32(tConfig.Rows[0]["REGISTERED_DEALER"]);

            Boolean bRegisteredDealer;
            bRegisteredDealer = (nRegisteredDealer == 1 ? true : false);

            string cApplyIgstForcly = tConfig.Rows[0]["ALL_XN_IGST"].ToString();
            string cPartyStateCode = tConfig.Rows[0]["party_state_code"].ToString();
            string cCurStateCode = tConfig.Rows[0]["CURSTATE_CODE"].ToString();
            string cPartyGstNo = tConfig.Rows[0]["PARTY_GST_NO"].ToString();
            string cLocGstNo = tConfig.Rows[0]["LOC_GST_NO"].ToString();
            string cAlwaysPickGstModeFromHsnMst = tConfig.Rows[0]["ALWAYS_PICK_GST_MODE_IN_RETAIL_SALE_FROM_HSN_MASTER"].ToString();
            Boolean bCessApplicable = globalMethods.ConvertBool(tConfig.Rows[0]["CESS_APPLICABLE"]);
            Boolean bExportGstApplicable = globalMethods.ConvertBool(tConfig.Rows[0]["custdym_export_gst_percentage_Applicable"]);
            decimal nExportGstPct = globalMethods.ConvertDecimal(tConfig.Rows[0]["custdym_export_gst_percentage"]);


            dtGstCalc.Rows.Clear();

            string cReupdateNetForRangeExclusive = tConfig.Rows[0]["REUPDATE_NET_FOR_RANGE_EXCLUSIVE"].ToString();
            string cReupdateNetForRangeInclusive = tConfig.Rows[0]["REUPDATE_NET_FOR_RANGE_INCLUSIVE"].ToString();

            CalcCmmDiscountAmount(ref dtCmd, globalMethods.ConvertDecimal(dtCmm.Rows[0]["discount_amount"]));

            if ((cReupdateNetForRangeExclusive == "1" || cReupdateNetForRangeInclusive == "1") && globalMethods.ConvertDecimal(dtCmm.Rows[0]["discount_amount"]) == 0)
            {
                ReCalNetOnRange(ref dtCmd, tConfig);
            }

            foreach (DataRow dr in dtCmd.Rows)
            {
                if (String.IsNullOrEmpty(dr["hsn_code"].ToString()) || dr["hsn_code"].ToString() == "0000000000")
                {
                    cMessage = " Hsn Code can't be blank for Item code :" + dr["product_code"].ToString();
                    return cMessage;
                }

                DataRow drGst = dtGstCalc.NewRow();
                drGst["row_id"] = dr["row_id"];
                drGst["hsn_code"] = dr["hsn_code"];
                drGst["mrp"] = dr["mrp"];
                drGst["quantity"] = dr["quantity"];
                drGst["net_value"] = globalMethods.ConvertDecimal(dr["net"]) - globalMethods.ConvertDecimal(dr["cmm_discount_amount"]);
                drGst["cgst_amount"] = 0;
                drGst["sgst_amount"] = 0;
                drGst["igst_amount"] = 0;
                drGst["cess_amount"] = 0;
                drGst["gst_cess_amount"] = 0;
                drGst["gst_percentage"] = 0;
                drGst["tax_method"] = (globalMethods.ConvertInt(dr["tax_method"]) == 0 ? 1 : dr["tax_method"]);
                dtGstCalc.Rows.Add(drGst);
            }




            if (cApplyIgstForcly == "1")
            {
                bRegisteredDealer = true;
                cCurStateCode = "NA1";
                cPartyStateCode = "NA2";
                cPartyGstNo = "NA1";
                cLocGstNo = "NA2";
            }

            if (bRegisteredDealer)
            {

                if (cApplyIgstForcly != "1")
                {
                    if (Enumerable.Contains(new string[] { "", "0000000" }, cPartyStateCode))
                        return "INVALID STATE CODE";
                }



                DataTable dtHsnMstFiltered = dtHsnMst.Clone();
                DataTable dtHsnDetFiltered = dtHsnDet.Clone();


                cMessage = globalMethods.JoinDataTables(dtHsnMst, dtCmd, ref dtHsnMstFiltered, "1=1",
                (row1, row2) =>
                row1.Field<String>("hsn_code") == row2.Field<String>("hsn_code"));

                if (!String.IsNullOrEmpty(cMessage))
                    return cMessage;

                dtHsnMstFiltered.AsEnumerable().ToList().ForEach(r => {
                    r["RETAILSALE_TAX_METHOD"] = (globalMethods.ConvertInt(r["RETAILSALE_TAX_METHOD"]) == 2 ? 1 : 2);
                });

                cMessage = globalMethods.JoinDataTables(dtHsnDet, dtCmd, ref dtHsnDetFiltered, "1=1",
                (row1, row2) =>
                row1.Field<String>("hsn_code") == row2.Field<String>("hsn_code"));

                if (!String.IsNullOrEmpty(cMessage))
                    return cMessage;


                updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "GST_CAL_BASIS", "GST_CAL_BASIS");
                updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "RATE_CUTOFF_TAX_PERCENTAGE", "RATE_CUTOFF_TAX_PERCENTAGE");
                updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "TAX_PERCENTAGE", "TAX_PERCENTAGE");
                updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "gst_cess_percentage", "gst_cess_percentage");
                updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "rate_cutoff", "rate_cutoff");

                foreach (DataRow dr in dtGstCalc.Rows)
                {
                    dr["net_value"] = (globalMethods.ConvertInt(dr["GST_CAL_BASIS"]) == 2 ? Convert.ToInt32(dr["mrp"]) * Convert.ToInt32(dr["quantity"]) : dr["net_value"]);
                }

                if (cAlwaysPickGstModeFromHsnMst == "1")
                {
                    updateColsonJoinTable(dtHsnMstFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "RETAILSALE_TAX_METHOD", "tax_method");
                }

                if (bCessApplicable && cCurStateCode == cPartyStateCode)
                {
                    dtGstCalc.AsEnumerable().ToList().ForEach(r => r["cess_percentage"] = tConfig.Rows[0]["cess_percentage"]);
                }


                if (cPartyGstNo != cLocGstNo)
                {
                    dtGstCalc.AsEnumerable().ToList().ForEach(r => r["NET_VALUE_WOTAX"] = (globalMethods.ConvertInt(r["tax_method"]) != 2 ?
                       Math.Round(globalMethods.ConvertDecimal(r["net_value"]) - (globalMethods.ConvertDecimal(r["net_value"]) *
                       (globalMethods.ConvertDecimal(r["RATE_CUTOFF_TAX_PERCENTAGE"]) + globalMethods.ConvertDecimal(r["cess_percentage"]) +
                        globalMethods.ConvertDecimal(r["gst_cess_percentage"])) / (100 + globalMethods.ConvertDecimal(r["RATE_CUTOFF_TAX_PERCENTAGE"]) + globalMethods.ConvertDecimal(r["cess_percentage"]) +
                        globalMethods.ConvertDecimal(r["gst_cess_percentage"]))), 2) :
                        Math.Round(globalMethods.ConvertDecimal(r["net_value"]), 2)));

                    if (bExportGstApplicable)
                    {
                        dtGstCalc.AsEnumerable().ToList().ForEach(r => r["GST_PERCENTAGE"] = nExportGstPct);
                    }
                    else
                    {
                        dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                        {
                            if ((globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) < (globalMethods.ConvertDecimal(r["NET_VALUE_WOTAX"]) / (globalMethods.ConvertDecimal(r["quantity"]))))
                                 || globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) == 0)
                                r["GST_PERCENTAGE"] = globalMethods.ConvertDecimal(r["tax_percentage"]);
                            else
                                r["GST_PERCENTAGE"] = globalMethods.ConvertDecimal(r["RATE_CUTOFF_TAX_PERCENTAGE"]);
                        });
                    }

                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        if ((globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) < (globalMethods.ConvertDecimal(r["NET_VALUE_WOTAX"]) / Math.Abs(globalMethods.ConvertDecimal(r["quantity"]))))
                                || globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) == 0)
                            r["Gst_Cess_Percentage"] = globalMethods.ConvertDecimal(r["Gst_Cess_Percentage"]);
                        else
                            r["Gst_Cess_Percentage"] = globalMethods.ConvertDecimal(r["Rate_CutOff_Gst_Cess_Percentage"]);

                        r["igst_amount"] = 0; r["cgst_amount"] = 0;
                    });
                }

                if (cPartyGstNo == cLocGstNo)
                {
                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        r["gst_percentage"] = 0;
                        r["gst_cess_percentage"] = 0;
                        r["NET_VALUE_WOTAX"] = r["NET_VALUE"];
                    });
                }


                dtGstCalc.AsEnumerable().ToList().ForEach(r => r["XN_VALUE_WITHOUT_GST"] = (globalMethods.ConvertInt(r["tax_method"]) == 1 ?
                                      Math.Round(globalMethods.ConvertDecimal(r["net_value"]) - (globalMethods.ConvertDecimal(r["net_value"]) *
                                      (globalMethods.ConvertDecimal(r["gst_percentage"]) + globalMethods.ConvertDecimal(r["cess_percentage"]) +
                                       globalMethods.ConvertDecimal(r["gst_cess_percentage"])) / (100 +
                                       globalMethods.ConvertDecimal(r["gst_percentage"]) +
                                       globalMethods.ConvertDecimal(r["cess_percentage"]) +
                                       globalMethods.ConvertDecimal(r["gst_cess_percentage"]))), 2) :
                                       Math.Round(globalMethods.ConvertDecimal(r["net_value"]), 2)));

                if (bCessApplicable)
                {
                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        r["cess_amount"] = globalMethods.ConvertDecimal(r["xn_value_without_gst"]) * globalMethods.ConvertDecimal(r["cess_percentage"]) / 100;
                    });

                }

                dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                {
                    r["Gst_Cess_Amount"] = Math.Round(globalMethods.ConvertDecimal(r["xn_value_without_gst"]) * globalMethods.ConvertDecimal(r["gst_CESS_PERCENTAGE"]) / 100, 2);
                });

                if (cCurStateCode != cPartyStateCode)
                {
                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        r["igst_amount"] = Math.Round(globalMethods.ConvertDecimal(r["XN_VALUE_WITHOUT_GST"]) * globalMethods.ConvertDecimal(r["gst_percentage"]) / 100, 2);
                    });
                }
                else
                {
                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        r["cgst_amount"] = Math.Round(globalMethods.ConvertDecimal(r["XN_VALUE_WITHOUT_GST"]) * globalMethods.ConvertDecimal(r["gst_percentage"]) / 100 / 2, 2);
                        r["sgst_amount"] = r["cgst_amount"];
                    });
                }

                dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                {
                    r["XN_VALUE_WITH_GST"] = globalMethods.ConvertDecimal(r["XN_VALUE_WITHOUT_GST"]) + globalMethods.ConvertDecimal(r["igst_amount"]) +
                    globalMethods.ConvertDecimal(r["cgst_amount"]) + globalMethods.ConvertDecimal(r["sgst_amount"]);
                });

                synchCmdGst(dtGstCalc, ref dtCmd);

            }


            if (!bRegisteredDealer)
            {
                dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                {
                    r["hsn_code"] = (String.IsNullOrEmpty(r["hsn_code"].ToString()) ? "0000000000" : r["hsn_code"]);
                    r["gst_percentage"] = 0;
                    r["Gst_Cess_Percentage"] = 0;
                    r["IGST_AMOUNT"] = 0;
                    r["CGST_AMOUNT"] = 0;
                    r["SGST_AMOUNT"] = 0;
                    r["NET_VALUE_WOTAX"] = r["NET_VALUE"];
                    r["XN_VALUE_WITHOUT_GST"] = r["NET_VALUE"];
                    r["XN_VALUE_WITH_GST"] = r["NET_VALUE"];
                });

                synchCmdGst(dtGstCalc, ref dtCmd);
            }


            decimal nSubtotal, nAtdCharges;

            nAtdCharges = globalMethods.ConvertDecimal(dtCmm.Rows[0]["atd_charges"]);
            nSubtotal = globalMethods.ConvertDecimal(dtCmm.Rows[0]["subtotal"]);

            dtCmd.AsEnumerable().ToList().ForEach(r =>
            {
                r["rfnet"] = globalMethods.ConvertDecimal(r["net"]) - globalMethods.ConvertDecimal(r["cmm_discount_amount"]) + (globalMethods.ConvertInt(r["tax_method"]) == 2 ?
                globalMethods.ConvertDecimal(r["igst_amount"]) + globalMethods.ConvertDecimal(r["cgst_amount"]) + globalMethods.ConvertDecimal(r["sgst_amount"]) +
                globalMethods.ConvertDecimal(r["gst_cess_amount"]) : 0);

                if (nAtdCharges == 0)
                    r["rfnet_with_other_charges"] = r["rfnet"];
                else
                    r["rfnet_with_other_charges"] = globalMethods.ConvertDecimal(r["rfnet"]) + ((nAtdCharges / nSubtotal) * globalMethods.ConvertDecimal(r["net"]));
            });



            cMessage = CalcGstOC(ref dtCmm, dtCmd, bRegisteredDealer, cCurStateCode, cPartyStateCode, tConfig);

            return cMessage;
        }

        public string CalcGst_PackSize(SqlConnection conn, ref DataTable dtCmm, ref DataTable dtCmd, DataTable tConfig, 
            DataTable dtHsnMst, DataTable dtHsnDet, DataTable dtGstCalc)
        {

            string cMessage = "";
            String cLineNo = "1";
            commonMethods globalMethods = new commonMethods();
            try
            {


                Int32 nRegisteredDealer = Convert.ToInt32(tConfig.Rows[0]["REGISTERED_DEALER"]);

                Boolean bRegisteredDealer;
                bRegisteredDealer = (nRegisteredDealer == 1 ? true : false);

                string cApplyIgstForcly = tConfig.Rows[0]["ALL_XN_IGST"].ToString();
                string cPartyStateCode = tConfig.Rows[0]["party_state_code"].ToString();
                string cCurStateCode = tConfig.Rows[0]["CURSTATE_CODE"].ToString();
                string cPartyGstNo = tConfig.Rows[0]["PARTY_GST_NO"].ToString();
                string cLocGstNo = tConfig.Rows[0]["LOC_GST_NO"].ToString();
                string cAlwaysPickGstModeFromHsnMst = tConfig.Rows[0]["ALWAYS_PICK_GST_MODE_IN_RETAIL_SALE_FROM_HSN_MASTER"].ToString();
                Boolean bCessApplicable = globalMethods.ConvertBool(tConfig.Rows[0]["CESS_APPLICABLE"]);
                Boolean bExportGstApplicable = globalMethods.ConvertBool(tConfig.Rows[0]["custdym_export_gst_percentage_Applicable"]);
                decimal nExportGstPct = globalMethods.ConvertDecimal(tConfig.Rows[0]["custdym_export_gst_percentage"]);
                cLineNo = "5";

                dtGstCalc.Rows.Clear();

                string cReupdateNetForRangeExclusive = tConfig.Rows[0]["REUPDATE_NET_FOR_RANGE_EXCLUSIVE"].ToString();
                string cReupdateNetForRangeInclusive = tConfig.Rows[0]["REUPDATE_NET_FOR_RANGE_INCLUSIVE"].ToString();

                CalcCmmDiscountAmount(ref dtCmd, globalMethods.ConvertDecimal(dtCmm.Rows[0]["discount_amount"]));
                cLineNo = "10";
                if ((cReupdateNetForRangeExclusive == "1" || cReupdateNetForRangeInclusive == "1") && globalMethods.ConvertDecimal(dtCmm.Rows[0]["discount_amount"]) == 0)
                {
                    cLineNo = "10.1";
                    String cErrorStr = ReCalNetOnRange(ref dtCmd, tConfig);
                    if (!String.IsNullOrEmpty(cErrorStr))
                    {
                        cLineNo = cLineNo + "" + cErrorStr;
                        return cLineNo;
                    }
                }
                if (!dtGstCalc.Columns.Contains("ARTICLE_PACK_SIZE"))
                {
                    dtGstCalc.Columns.Add("ARTICLE_PACK_SIZE", typeof(decimal));
                }
                foreach (DataRow dr in dtCmd.Rows)
                {
                    if (String.IsNullOrEmpty(dr["hsn_code"].ToString()) || dr["hsn_code"].ToString() == "0000000000")
                    {
                        cMessage = " Hsn Code can't be blank for Item code :" + dr["product_code"].ToString();
                        return cMessage;
                    }
                    Decimal ARTICLE_PACK_SIZE = globalMethods.ConvertDecimal(dr["ARTICLE_PACK_SIZE"]);
                    ARTICLE_PACK_SIZE = (ARTICLE_PACK_SIZE <= 0 ? 1m : ARTICLE_PACK_SIZE);
                    dr["ARTICLE_PACK_SIZE"] = ARTICLE_PACK_SIZE;
                    DataRow drGst = dtGstCalc.NewRow();
                    drGst["row_id"] = dr["row_id"];
                    drGst["hsn_code"] = dr["hsn_code"];
                    drGst["mrp"] = dr["mrp"];
                    drGst["quantity"] = dr["quantity"];
                    drGst["net_value"] = (!bRegisteredDealer ? globalMethods.ConvertDecimal(dr["net"])- globalMethods.ConvertDecimal(dr["cmm_discount_amount"]) : Math.Round(globalMethods.ConvertDecimal(dr["net"]) / globalMethods.ConvertDecimal(dr["ARTICLE_PACK_SIZE"]), 2) - (globalMethods.ConvertDecimal(dr["cmm_discount_amount"]) / globalMethods.ConvertDecimal(dr["ARTICLE_PACK_SIZE"])));
                    drGst["cgst_amount"] = 0;
                    drGst["sgst_amount"] = 0;
                    drGst["igst_amount"] = 0;
                    drGst["cess_amount"] = 0;
                    drGst["gst_cess_amount"] = 0;
                    drGst["gst_percentage"] = 0;
                    drGst["tax_method"] = (globalMethods.ConvertInt(dr["tax_method"]) == 0 ? 1 : dr["tax_method"]);
                    drGst["ARTICLE_PACK_SIZE"] = ARTICLE_PACK_SIZE;
                    dtGstCalc.Rows.Add(drGst);
                }

                cLineNo = "15";


                if (cApplyIgstForcly == "1")
                {
                    bRegisteredDealer = true;
                    cCurStateCode = "NA1";
                    cPartyStateCode = "NA2";
                    cPartyGstNo = "NA1";
                    cLocGstNo = "NA2";
                }

                if (bRegisteredDealer)
                {

                    if (cApplyIgstForcly != "1")
                    {
                        if (Enumerable.Contains(new string[] { "", "0000000" }, cPartyStateCode))
                            return "INVALID STATE CODE";
                    }



                    DataTable dtHsnMstFiltered = dtHsnMst.Clone();
                    DataTable dtHsnDetFiltered = dtHsnDet.Clone();

                    cLineNo = "20";
                    cMessage = globalMethods.JoinDataTables(dtHsnMst, dtCmd, ref dtHsnMstFiltered, "1=1",
                (row1, row2) =>
                row1.Field<String>("hsn_code") == row2.Field<String>("hsn_code"));

                    if (!String.IsNullOrEmpty(cMessage))
                        return cMessage;

                    //dtHsnMstFiltered.AsEnumerable().ToList().ForEach(r => {
                    //    r["RETAILSALE_TAX_METHOD"] = (globalMethods.ConvertInt(r["RETAILSALE_TAX_METHOD"]) == 2 ? 1 : 2);
                    //});
                    cLineNo = "25";
                    cMessage = globalMethods.JoinDataTables(dtHsnDet, dtCmd, ref dtHsnDetFiltered, "1=1",
                (row1, row2) =>
                row1.Field<String>("hsn_code") == row2.Field<String>("hsn_code"));

                    if (!String.IsNullOrEmpty(cMessage))
                        return cMessage;

                    cLineNo = "30";
                    updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "GST_CAL_BASIS", "GST_CAL_BASIS");
                    cLineNo = "35";
                    updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "RATE_CUTOFF_TAX_PERCENTAGE", "RATE_CUTOFF_TAX_PERCENTAGE");
                    cLineNo = "40";
                    updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "TAX_PERCENTAGE", "TAX_PERCENTAGE");
                    cLineNo = "45";
                    updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "gst_cess_percentage", "gst_cess_percentage");
                    cLineNo = "50";
                    updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "rate_cutoff", "rate_cutoff");

                    foreach (DataRow dr in dtGstCalc.Rows)
                    {
                        dr["mrp"] = (globalMethods.ConvertInt(dr["GST_CAL_BASIS"]) == 2 ?
                            globalMethods.ConvertDecimal(dr["mrp"]) * globalMethods.ConvertDecimal(dr["quantity"]) : dr["net_value"]);
                    }

                    if (cAlwaysPickGstModeFromHsnMst == "1")
                    {
                        cLineNo = "60";
                        updateColsonJoinTable(dtHsnMstFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "RETAILSALE_TAX_METHOD", "tax_method");
                    }

                    if (bCessApplicable && cCurStateCode == cPartyStateCode)
                    {
                        cLineNo = "70";
                        dtGstCalc.AsEnumerable().ToList().ForEach(r => r["cess_percentage"] = tConfig.Rows[0]["cess_percentage"]);
                    }


                    if (cPartyGstNo != cLocGstNo)
                    {
                        cLineNo = "80";
                        dtGstCalc.AsEnumerable().ToList().ForEach(r => r["NET_VALUE_WOTAX"] = (globalMethods.ConvertInt(r["tax_method"]) != 2 ?
                       Math.Round(globalMethods.ConvertDecimal(r["mrp"]) - (globalMethods.ConvertDecimal(r["mrp"]) *
                       (globalMethods.ConvertDecimal(r["RATE_CUTOFF_TAX_PERCENTAGE"]) + globalMethods.ConvertDecimal(r["cess_percentage"]) +
                        globalMethods.ConvertDecimal(r["gst_cess_percentage"])) / (100 + globalMethods.ConvertDecimal(r["RATE_CUTOFF_TAX_PERCENTAGE"]) + globalMethods.ConvertDecimal(r["cess_percentage"]) +
                        globalMethods.ConvertDecimal(r["gst_cess_percentage"]))), 2) :
                        Math.Round(globalMethods.ConvertDecimal(r["net_value"]), 2)));

                        if (bExportGstApplicable)
                        {
                            cLineNo = "90";
                            dtGstCalc.AsEnumerable().ToList().ForEach(r => r["GST_PERCENTAGE"] = nExportGstPct);
                        }
                        else
                        {
                            cLineNo = "100";
                            dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                        {
                            if ((globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) < (globalMethods.ConvertDecimal(r["NET_VALUE_WOTAX"]) / (globalMethods.ConvertDecimal(r["quantity"]))))
                                 || globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) == 0)
                                r["GST_PERCENTAGE"] = globalMethods.ConvertDecimal(r["tax_percentage"]);
                            else
                                r["GST_PERCENTAGE"] = globalMethods.ConvertDecimal(r["RATE_CUTOFF_TAX_PERCENTAGE"]);
                        });
                        }
                        cLineNo = "110";
                        dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        if ((globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) < (globalMethods.ConvertDecimal(r["NET_VALUE_WOTAX"]) / Math.Abs(globalMethods.ConvertDecimal(r["quantity"]))))
                                || globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) == 0)
                            r["Gst_Cess_Percentage"] = globalMethods.ConvertDecimal(r["Gst_Cess_Percentage"]);
                        else
                            r["Gst_Cess_Percentage"] = globalMethods.ConvertDecimal(r["Rate_CutOff_Gst_Cess_Percentage"]);

                        r["igst_amount"] = 0; r["cgst_amount"] = 0;
                    });
                    }

                    if (cPartyGstNo == cLocGstNo)
                    {
                        cLineNo = "120";
                        dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        r["gst_percentage"] = 0;
                        r["gst_cess_percentage"] = 0;
                        r["NET_VALUE_WOTAX"] = r["NET_VALUE"];
                    });
                    }

                    cLineNo = "130";
                    dtGstCalc.AsEnumerable().ToList().ForEach(r => r["XN_VALUE_WITHOUT_GST"] = (globalMethods.ConvertInt(r["tax_method"]) == 1 ?
                                      Math.Round((globalMethods.ConvertDecimal(r["net_value"]) * globalMethods.ConvertDecimal(r["ARTICLE_PACK_SIZE"])) - ((globalMethods.ConvertDecimal(r["net_value"]) * globalMethods.ConvertDecimal(r["ARTICLE_PACK_SIZE"])) *
                                      (globalMethods.ConvertDecimal(r["gst_percentage"]) + globalMethods.ConvertDecimal(r["cess_percentage"]) +
                                       globalMethods.ConvertDecimal(r["gst_cess_percentage"])) / (100 +
                                       globalMethods.ConvertDecimal(r["gst_percentage"]) +
                                       globalMethods.ConvertDecimal(r["cess_percentage"]) +
                                       globalMethods.ConvertDecimal(r["gst_cess_percentage"]))), 2) :
                                       Math.Round(globalMethods.ConvertDecimal(r["net_value"]) * globalMethods.ConvertDecimal(r["ARTICLE_PACK_SIZE"]), 2)));

                    if (bCessApplicable)
                    {
                        cLineNo = "140";
                        dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        r["cess_amount"] = globalMethods.ConvertDecimal(r["xn_value_without_gst"]) * globalMethods.ConvertDecimal(r["cess_percentage"]) / 100;
                    });

                    }
                    cLineNo = "150";
                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                {
                    r["Gst_Cess_Amount"] = Math.Round(globalMethods.ConvertDecimal(r["xn_value_without_gst"]) * globalMethods.ConvertDecimal(r["gst_CESS_PERCENTAGE"]) / 100, 2);
                });

                    if (cCurStateCode != cPartyStateCode)
                    {
                        cLineNo = "160";
                        dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        r["igst_amount"] = Math.Round(globalMethods.ConvertDecimal(r["XN_VALUE_WITHOUT_GST"]) * globalMethods.ConvertDecimal(r["gst_percentage"]) / 100, 2);
                    });
                    }
                    else
                    {
                        cLineNo = "170";
                        dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        r["cgst_amount"] = Math.Round(globalMethods.ConvertDecimal(r["XN_VALUE_WITHOUT_GST"]) * globalMethods.ConvertDecimal(r["gst_percentage"]) / 100 / 2, 2);
                        r["sgst_amount"] = r["cgst_amount"];
                    });
                    }
                    cLineNo = "180";
                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                {
                    r["XN_VALUE_WITH_GST"] = globalMethods.ConvertDecimal(r["XN_VALUE_WITHOUT_GST"]) + globalMethods.ConvertDecimal(r["igst_amount"]) +
                    globalMethods.ConvertDecimal(r["cgst_amount"]) + globalMethods.ConvertDecimal(r["sgst_amount"]);
                });
                    cLineNo = "190";
                    synchCmdGst(dtGstCalc, ref dtCmd);

                }


                if (!bRegisteredDealer)
                {
                    cLineNo = "200";
                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                {
                    r["hsn_code"] = (String.IsNullOrEmpty(r["hsn_code"].ToString()) ? "0000000000" : r["hsn_code"]);
                    r["gst_percentage"] = 0;
                    r["Gst_Cess_Percentage"] = 0;
                    r["IGST_AMOUNT"] = 0;
                    r["CGST_AMOUNT"] = 0;
                    r["SGST_AMOUNT"] = 0;
                    r["NET_VALUE_WOTAX"] = r["NET_VALUE"]; 
                    r["XN_VALUE_WITHOUT_GST"] = r["NET_VALUE"];
                    r["XN_VALUE_WITH_GST"] = r["NET_VALUE"];
                });
                    cLineNo = "201";
                    synchCmdGst(dtGstCalc, ref dtCmd);
                }


                decimal nSubtotal, nAtdCharges;

                nAtdCharges = globalMethods.ConvertDecimal(dtCmm.Rows[0]["atd_charges"]);
                nSubtotal = globalMethods.ConvertDecimal(dtCmm.Rows[0]["subtotal"]);
                if (dtCmm.Columns.Contains("SUBTOTAL_R"))
                {
                    nSubtotal = nSubtotal + globalMethods.ConvertDecimal(dtCmm.Rows[0]["subtotal_R"]);
                }
                cLineNo = "210";
                dtCmd.AsEnumerable().ToList().ForEach(r =>
                {
                    r["rfnet"] = globalMethods.ConvertDecimal(r["net"]) - globalMethods.ConvertDecimal(r["cmm_discount_amount"]) + (globalMethods.ConvertInt(r["tax_method"]) == 2 ?
                    globalMethods.ConvertDecimal(r["igst_amount"]) + globalMethods.ConvertDecimal(r["cgst_amount"]) + globalMethods.ConvertDecimal(r["sgst_amount"]) +
                    globalMethods.ConvertDecimal(r["gst_cess_amount"]) : 0);

                    if (nAtdCharges == 0)
                        r["rfnet_with_other_charges"] = r["rfnet"];
                    else
                        r["rfnet_with_other_charges"] = globalMethods.ConvertDecimal(r["rfnet"]) + ((nAtdCharges / nSubtotal) * globalMethods.ConvertDecimal(r["net"]));
                });


                cLineNo = "220";
                cMessage = CalcGstOC(ref dtCmm, dtCmd, bRegisteredDealer, cCurStateCode, cPartyStateCode, tConfig);
            }
            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);
                cMessage = "Line No: " + errLineNo.ToString() + " : " + cLineNo + " : " + ex.Message;
            }
            return cMessage;
        }

        public string CalcGst_RBO_PackSize(SqlConnection conn, ref DataTable dtCmm, ref DataTable dtCmd, DataTable tConfig, DataTable dtHsnMst, DataTable dtHsnDet,
                              DataTable dtGstCalc)
        {

            string cMessage = "";
            commonMethods globalMethods = new commonMethods();

            Int32 nRegisteredDealer = Convert.ToInt32(tConfig.Rows[0]["REGISTERED_DEALER"]);

            Boolean bRegisteredDealer;
            bRegisteredDealer = (nRegisteredDealer == 1 ? true : false);

            string cApplyIgstForcly = tConfig.Rows[0]["ALL_XN_IGST"].ToString();
            string cPartyStateCode = tConfig.Rows[0]["party_state_code"].ToString();
            string cCurStateCode = tConfig.Rows[0]["CURSTATE_CODE"].ToString();
            string cPartyGstNo = tConfig.Rows[0]["PARTY_GST_NO"].ToString();
            string cLocGstNo = tConfig.Rows[0]["LOC_GST_NO"].ToString();
            //string cAlwaysPickGstModeFromHsnMst = tConfig.Rows[0]["ALWAYS_PICK_GST_MODE_IN_RETAIL_SALE_FROM_HSN_MASTER"].ToString();
            Boolean bCessApplicable = globalMethods.ConvertBool(tConfig.Rows[0]["CESS_APPLICABLE"]);
            Boolean bExportGstApplicable = globalMethods.ConvertBool(tConfig.Rows[0]["custdym_export_gst_percentage_Applicable"]);
            decimal nExportGstPct = globalMethods.ConvertDecimal(tConfig.Rows[0]["custdym_export_gst_percentage"]);


            dtGstCalc.Rows.Clear();

            string cReupdateNetForRangeExclusive = tConfig.Rows[0]["REUPDATE_NET_FOR_RANGE_EXCLUSIVE"].ToString();
            string cReupdateNetForRangeInclusive = tConfig.Rows[0]["REUPDATE_NET_FOR_RANGE_INCLUSIVE"].ToString();

            //CalcCmmDiscountAmount(ref dtCmd, globalMethods.ConvertDecimal(dtCmm.Rows[0]["discount_amount"]));

            if ((cReupdateNetForRangeExclusive == "1" || cReupdateNetForRangeInclusive == "1") && globalMethods.ConvertDecimal(dtCmm.Rows[0]["discount_amount"]) == 0)
            {
                ReCalNetOnRange_WSL(ref dtCmd, tConfig);
            }
            if (!dtGstCalc.Columns.Contains("ARTICLE_PACK_SIZE"))
            {
                dtGstCalc.Columns.Add("ARTICLE_PACK_SIZE", typeof(decimal));
            }
            foreach (DataRow dr in dtCmd.Rows)
            {
                if (String.IsNullOrEmpty(dr["hsn_code"].ToString()) || dr["hsn_code"].ToString() == "0000000000")
                {
                    cMessage = " Hsn Code can't be blank for Item code :" + dr["product_code"].ToString();
                    return cMessage;
                }

                DataRow drGst = dtGstCalc.NewRow();
                drGst["row_id"] = dr["row_id"];
                drGst["hsn_code"] = dr["hsn_code"];
                drGst["mrp"] = dr["mrp"];
                drGst["quantity"] = dr["quantity"];
                drGst["net_value"] = (!bRegisteredDealer ? globalMethods.ConvertDecimal(dr["net_rate"])- globalMethods.ConvertDecimal(dr["inmdiscountamount"]) : Math.Round((globalMethods.ConvertDecimal(dr["net_rate"]) * globalMethods.ConvertDecimal(dr["quantity"])) / globalMethods.ConvertDecimal(dr["ARTICLE_PACK_SIZE"]), 2) - (globalMethods.ConvertDecimal(dr["inmdiscountamount"]) / globalMethods.ConvertDecimal(dr["ARTICLE_PACK_SIZE"])));
                drGst["cgst_amount"] = 0;
                drGst["sgst_amount"] = 0;
                drGst["igst_amount"] = 0;
                drGst["cess_amount"] = 0;
                drGst["gst_cess_amount"] = 0;
                drGst["gst_percentage"] = 0;
                drGst["tax_method"] = (globalMethods.ConvertInt(dr["tax_method"]) == 0 ? 2 : dr["tax_method"]);
                drGst["ARTICLE_PACK_SIZE"] = globalMethods.ConvertDecimal(dr["ARTICLE_PACK_SIZE"]);
                dtGstCalc.Rows.Add(drGst);
            }




            if (cApplyIgstForcly == "1")
            {
                bRegisteredDealer = true;
                cCurStateCode = "NA1";
                cPartyStateCode = "NA2";
                cPartyGstNo = "NA1";
                cLocGstNo = "NA2";
            }

            if (bRegisteredDealer)
            {

                if (cApplyIgstForcly != "1")
                {
                    if (Enumerable.Contains(new string[] { "", "0000000" }, cPartyStateCode))
                        return "INVALID STATE CODE";
                }



                DataTable dtHsnMstFiltered = dtHsnMst.Clone();
                DataTable dtHsnDetFiltered = dtHsnDet.Clone();


                cMessage = globalMethods.JoinDataTables(dtHsnMst, dtCmd, ref dtHsnMstFiltered, "1=1",
                (row1, row2) =>
                row1.Field<String>("hsn_code") == row2.Field<String>("hsn_code"));

                if (!String.IsNullOrEmpty(cMessage))
                    return cMessage;

                dtHsnMstFiltered.AsEnumerable().ToList().ForEach(r => {
                    r["RETAILSALE_TAX_METHOD"] = (globalMethods.ConvertInt(r["RETAILSALE_TAX_METHOD"]) == 2 ? 1 : 2);
                });

                cMessage = globalMethods.JoinDataTables(dtHsnDet, dtCmd, ref dtHsnDetFiltered, "1=1",
                (row1, row2) =>
                row1.Field<String>("hsn_code") == row2.Field<String>("hsn_code"));

                if (!String.IsNullOrEmpty(cMessage))
                    return cMessage;


                updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "GST_CAL_BASIS", "GST_CAL_BASIS");
                updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "RATE_CUTOFF_TAX_PERCENTAGE", "RATE_CUTOFF_TAX_PERCENTAGE");
                updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "TAX_PERCENTAGE", "TAX_PERCENTAGE");
                updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "gst_cess_percentage", "gst_cess_percentage");
                updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "rate_cutoff", "rate_cutoff");

                foreach (DataRow dr in dtGstCalc.Rows)
                {
                    dr["net_value"] = (globalMethods.ConvertInt(dr["GST_CAL_BASIS"]) == 2 ? Convert.ToInt32(dr["rate"]) * Convert.ToInt32(dr["quantity"]) : dr["net_value"]);
                }

                //if (cAlwaysPickGstModeFromHsnMst == "1")
                //{
                //    updateColsonJoinTable(dtHsnMstFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "RETAILSALE_TAX_METHOD", "tax_method");
                //}

                //if (bCessApplicable && cCurStateCode == cPartyStateCode)
                //{
                //    dtGstCalc.AsEnumerable().ToList().ForEach(r => r["cess_percentage"] = tConfig.Rows[0]["cess_percentage"]);
                //}


                if (cPartyGstNo != cLocGstNo)
                {
                    dtGstCalc.AsEnumerable().ToList().ForEach(r => r["NET_VALUE_WOTAX"] = (globalMethods.ConvertInt(r["tax_method"]) != 2 ?
                       Math.Round(globalMethods.ConvertDecimal(r["net_value"]) - (globalMethods.ConvertDecimal(r["net_value"]) *
                       (globalMethods.ConvertDecimal(r["RATE_CUTOFF_TAX_PERCENTAGE"]) + globalMethods.ConvertDecimal(r["cess_percentage"]) +
                        globalMethods.ConvertDecimal(r["gst_cess_percentage"])) / (100 + globalMethods.ConvertDecimal(r["RATE_CUTOFF_TAX_PERCENTAGE"]) + globalMethods.ConvertDecimal(r["cess_percentage"]) +
                        globalMethods.ConvertDecimal(r["gst_cess_percentage"]))), 2) :
                        Math.Round(globalMethods.ConvertDecimal(r["net_value"]), 2)));

                    if (bExportGstApplicable)
                    {
                        dtGstCalc.AsEnumerable().ToList().ForEach(r => r["GST_PERCENTAGE"] = nExportGstPct);
                    }
                    else
                    {
                        dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                        {
                            if ((globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) < (globalMethods.ConvertDecimal(r["NET_VALUE_WOTAX"]) / (globalMethods.ConvertDecimal(r["quantity"]))))
                                 || globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) == 0)
                                r["GST_PERCENTAGE"] = globalMethods.ConvertDecimal(r["tax_percentage"]);
                            else
                                r["GST_PERCENTAGE"] = globalMethods.ConvertDecimal(r["RATE_CUTOFF_TAX_PERCENTAGE"]);
                        });
                    }

                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        if ((globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) < (globalMethods.ConvertDecimal(r["NET_VALUE_WOTAX"]) / Math.Abs(globalMethods.ConvertDecimal(r["quantity"]))))
                                || globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) == 0)
                            r["Gst_Cess_Percentage"] = globalMethods.ConvertDecimal(r["Gst_Cess_Percentage"]);
                        else
                            r["Gst_Cess_Percentage"] = globalMethods.ConvertDecimal(r["Rate_CutOff_Gst_Cess_Percentage"]);

                        r["igst_amount"] = 0; r["cgst_amount"] = 0;
                    });
                }

                if (cPartyGstNo == cLocGstNo)
                {
                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        r["gst_percentage"] = 0;
                        r["gst_cess_percentage"] = 0;
                        r["NET_VALUE_WOTAX"] = r["NET_VALUE"];
                    });
                }


                dtGstCalc.AsEnumerable().ToList().ForEach(r => r["XN_VALUE_WITHOUT_GST"] = (globalMethods.ConvertInt(r["tax_method"]) == 1 ?
                                      Math.Round((globalMethods.ConvertDecimal(r["net_value"]) * globalMethods.ConvertDecimal(r["ARTICLE_PACK_SIZE"])) - ((globalMethods.ConvertDecimal(r["net_value"]) * globalMethods.ConvertDecimal(r["ARTICLE_PACK_SIZE"])) *
                                      (globalMethods.ConvertDecimal(r["gst_percentage"]) + globalMethods.ConvertDecimal(r["cess_percentage"]) +
                                       globalMethods.ConvertDecimal(r["gst_cess_percentage"])) / (100 +
                                       globalMethods.ConvertDecimal(r["gst_percentage"]) +
                                       globalMethods.ConvertDecimal(r["cess_percentage"]) +
                                       globalMethods.ConvertDecimal(r["gst_cess_percentage"]))), 2) :
                                       Math.Round(globalMethods.ConvertDecimal(r["net_value"]) * globalMethods.ConvertDecimal(r["ARTICLE_PACK_SIZE"]), 2)));

                //if (bCessApplicable)
                //{
                //    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                //    {
                //        r["cess_amount"] = globalMethods.ConvertDecimal(r["xn_value_without_gst"]) * globalMethods.ConvertDecimal(r["cess_percentage"]) / 100;
                //    });

                //}

                dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                {
                    r["Gst_Cess_Amount"] = Math.Round(globalMethods.ConvertDecimal(r["xn_value_without_gst"]) * globalMethods.ConvertDecimal(r["gst_CESS_PERCENTAGE"]) / 100, 2);
                });

                if (cCurStateCode != cPartyStateCode)
                {
                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        r["igst_amount"] = Math.Round(globalMethods.ConvertDecimal(r["XN_VALUE_WITHOUT_GST"]) * globalMethods.ConvertDecimal(r["gst_percentage"]) / 100, 2);
                    });
                }
                else
                {
                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        r["cgst_amount"] = Math.Round(globalMethods.ConvertDecimal(r["XN_VALUE_WITHOUT_GST"]) * globalMethods.ConvertDecimal(r["gst_percentage"]) / 100 / 2, 2);
                        r["sgst_amount"] = r["cgst_amount"];
                    });
                }

                dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                {
                    r["XN_VALUE_WITH_GST"] = globalMethods.ConvertDecimal(r["XN_VALUE_WITHOUT_GST"]) + globalMethods.ConvertDecimal(r["igst_amount"]) +
                    globalMethods.ConvertDecimal(r["cgst_amount"]) + globalMethods.ConvertDecimal(r["sgst_amount"]);
                });

                synchCmdGst(dtGstCalc, ref dtCmd);

            }


            if (!bRegisteredDealer)
            {
                dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                {
                    r["hsn_code"] = (String.IsNullOrEmpty(r["hsn_code"].ToString()) ? "0000000000" : r["hsn_code"]);
                    r["gst_percentage"] = 0;
                    r["Gst_Cess_Percentage"] = 0;
                    r["IGST_AMOUNT"] = 0;
                    r["CGST_AMOUNT"] = 0;
                    r["SGST_AMOUNT"] = 0;
                    r["NET_VALUE_WOTAX"] = r["NET_VALUE"];
                    r["XN_VALUE_WITHOUT_GST"] = r["NET_VALUE"];
                    r["XN_VALUE_WITH_GST"] = r["NET_VALUE"];
                });

                synchCmdGst(dtGstCalc, ref dtCmd);
            }


            //decimal nSubtotal, nAtdCharges;

            //nAtdCharges = globalMethods.ConvertDecimal(dtCmm.Rows[0]["other_charges"]);
            //nSubtotal = globalMethods.ConvertDecimal(dtCmm.Rows[0]["subtotal"]);

            //dtCmd.AsEnumerable().ToList().ForEach(r =>
            //{
            //    r["rfnet"] = globalMethods.ConvertDecimal(r["net"]) - globalMethods.ConvertDecimal(r["cmm_discount_amount"]) + (globalMethods.ConvertInt(r["tax_method"]) == 2 ?
            //    globalMethods.ConvertDecimal(r["igst_amount"]) + globalMethods.ConvertDecimal(r["cgst_amount"]) + globalMethods.ConvertDecimal(r["sgst_amount"]) +
            //    globalMethods.ConvertDecimal(r["gst_cess_amount"]) : 0);

            //    if (nAtdCharges == 0)
            //        r["rfnet_with_other_charges"] = r["rfnet"];
            //    else
            //        r["rfnet_with_other_charges"] = globalMethods.ConvertDecimal(r["rfnet"]) + ((nAtdCharges / nSubtotal) * globalMethods.ConvertDecimal(r["net"]));
            //});

            cMessage = CalcGstOC_WSL(ref dtCmm, dtCmd, bRegisteredDealer, cCurStateCode, cPartyStateCode, tConfig);

            if (String.IsNullOrEmpty(cMessage))
            {
                cMessage = CalcGstFreight_WSL(ref dtCmm, dtCmd, bRegisteredDealer, cCurStateCode, cPartyStateCode, tConfig);
            }
            return cMessage;
        }

        public string CalcGst_WSL_PackSize(SqlConnection conn, ref DataTable dtCmm, ref DataTable dtCmd, DataTable tConfig, DataTable dtHsnMst, DataTable dtHsnDet,
                              DataTable dtGstCalc)
        {

            string cMessage = "";
            commonMethods globalMethods = new commonMethods();

            Int32 nRegisteredDealer = Convert.ToInt32(tConfig.Rows[0]["REGISTERED_DEALER"]);

            Boolean bRegisteredDealer;
            bRegisteredDealer = (nRegisteredDealer == 1 ? true : false);

            string cApplyIgstForcly = tConfig.Rows[0]["ALL_XN_IGST"].ToString();
            string cPartyStateCode = tConfig.Rows[0]["party_state_code"].ToString();
            string cCurStateCode = tConfig.Rows[0]["CURSTATE_CODE"].ToString();
            string cPartyGstNo = tConfig.Rows[0]["PARTY_GST_NO"].ToString();
            string cLocGstNo = tConfig.Rows[0]["LOC_GST_NO"].ToString();
            //string cAlwaysPickGstModeFromHsnMst = tConfig.Rows[0]["ALWAYS_PICK_GST_MODE_IN_RETAIL_SALE_FROM_HSN_MASTER"].ToString();
            Boolean bCessApplicable = globalMethods.ConvertBool(tConfig.Rows[0]["CESS_APPLICABLE"]);
            Boolean bExportGstApplicable = globalMethods.ConvertBool(tConfig.Rows[0]["custdym_export_gst_percentage_Applicable"]);
            decimal nExportGstPct = globalMethods.ConvertDecimal(tConfig.Rows[0]["custdym_export_gst_percentage"]);


            dtGstCalc.Rows.Clear();

            string cReupdateNetForRangeExclusive = tConfig.Rows[0]["REUPDATE_NET_FOR_RANGE_EXCLUSIVE"].ToString();
            string cReupdateNetForRangeInclusive = tConfig.Rows[0]["REUPDATE_NET_FOR_RANGE_INCLUSIVE"].ToString();

            //CalcCmmDiscountAmount(ref dtCmd, globalMethods.ConvertDecimal(dtCmm.Rows[0]["discount_amount"]));

            if ((cReupdateNetForRangeExclusive == "1" || cReupdateNetForRangeInclusive == "1") && globalMethods.ConvertDecimal(dtCmm.Rows[0]["discount_amount"]) == 0)
            {
                ReCalNetOnRange_WSL(ref dtCmd, tConfig);
            }
            if (!dtGstCalc.Columns.Contains("ARTICLE_PACK_SIZE"))
            {
                dtGstCalc.Columns.Add("ARTICLE_PACK_SIZE", typeof(decimal));
            }
            foreach (DataRow dr in dtCmd.Rows)
            {
                if (String.IsNullOrEmpty(dr["hsn_code"].ToString()) || dr["hsn_code"].ToString() == "0000000000")
                {
                    cMessage = " Hsn Code can't be blank for Item code :" + dr["product_code"].ToString();
                    return cMessage;
                }

                DataRow drGst = dtGstCalc.NewRow();
                drGst["row_id"] = dr["row_id"];
                drGst["hsn_code"] = dr["hsn_code"];
                drGst["mrp"] = dr["mrp"];
                drGst["quantity"] = dr["quantity"];
                drGst["net_value"] = (!bRegisteredDealer ? globalMethods.ConvertDecimal(dr["net_rate"])- globalMethods.ConvertDecimal(dr["inmdiscountamount"]) : Math.Round((globalMethods.ConvertDecimal(dr["net_rate"]) * globalMethods.ConvertDecimal(dr["quantity"])) / globalMethods.ConvertDecimal(dr["ARTICLE_PACK_SIZE"]), 2) - (globalMethods.ConvertDecimal(dr["inmdiscountamount"]) / globalMethods.ConvertDecimal(dr["ARTICLE_PACK_SIZE"])));
                drGst["cgst_amount"] = 0;
                drGst["sgst_amount"] = 0;
                drGst["igst_amount"] = 0;
                drGst["cess_amount"] = 0;
                drGst["gst_cess_amount"] = 0;
                drGst["gst_percentage"] = 0;
                drGst["tax_method"] = (globalMethods.ConvertInt(dr["tax_method"]) == 0 ? 2 : dr["tax_method"]);
                drGst["ARTICLE_PACK_SIZE"] = globalMethods.ConvertDecimal(dr["ARTICLE_PACK_SIZE"]);
                dtGstCalc.Rows.Add(drGst);
            }




            if (cApplyIgstForcly == "1")
            {
                bRegisteredDealer = true;
                cCurStateCode = "NA1";
                cPartyStateCode = "NA2";
                cPartyGstNo = "NA1";
                cLocGstNo = "NA2";
            }

            if (bRegisteredDealer)
            {

                if (cApplyIgstForcly != "1")
                {
                    if (Enumerable.Contains(new string[] { "", "0000000" }, cPartyStateCode))
                        return "INVALID STATE CODE";
                }



                DataTable dtHsnMstFiltered = dtHsnMst.Clone();
                DataTable dtHsnDetFiltered = dtHsnDet.Clone();


                cMessage = globalMethods.JoinDataTables(dtHsnMst, dtCmd, ref dtHsnMstFiltered, "1=1",
                (row1, row2) =>
                row1.Field<String>("hsn_code") == row2.Field<String>("hsn_code"));

                if (!String.IsNullOrEmpty(cMessage))
                    return cMessage;

                dtHsnMstFiltered.AsEnumerable().ToList().ForEach(r => {
                    r["RETAILSALE_TAX_METHOD"] = (globalMethods.ConvertInt(r["RETAILSALE_TAX_METHOD"]) == 2 ? 1 : 2);
                });

                cMessage = globalMethods.JoinDataTables(dtHsnDet, dtCmd, ref dtHsnDetFiltered, "1=1",
                (row1, row2) =>
                row1.Field<String>("hsn_code") == row2.Field<String>("hsn_code"));

                if (!String.IsNullOrEmpty(cMessage))
                    return cMessage;


                updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "GST_CAL_BASIS", "GST_CAL_BASIS");
                updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "RATE_CUTOFF_TAX_PERCENTAGE", "RATE_CUTOFF_TAX_PERCENTAGE");
                updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "TAX_PERCENTAGE", "TAX_PERCENTAGE");
                updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "gst_cess_percentage", "gst_cess_percentage");
                updateColsonJoinTable(dtHsnDetFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "rate_cutoff", "rate_cutoff");

                foreach (DataRow dr in dtGstCalc.Rows)
                {
                    dr["net_value"] = (globalMethods.ConvertInt(dr["GST_CAL_BASIS"]) == 2 ? Convert.ToInt32(dr["rate"]) * Convert.ToInt32(dr["quantity"]) : dr["net_value"]);
                }

                //if (cAlwaysPickGstModeFromHsnMst == "1")
                //{
                //    updateColsonJoinTable(dtHsnMstFiltered, ref dtGstCalc, "hsn_code", "hsn_code", "RETAILSALE_TAX_METHOD", "tax_method");
                //}

                //if (bCessApplicable && cCurStateCode == cPartyStateCode)
                //{
                //    dtGstCalc.AsEnumerable().ToList().ForEach(r => r["cess_percentage"] = tConfig.Rows[0]["cess_percentage"]);
                //}


                if (cPartyGstNo != cLocGstNo)
                {
                    dtGstCalc.AsEnumerable().ToList().ForEach(r => r["NET_VALUE_WOTAX"] = (globalMethods.ConvertInt(r["tax_method"]) != 2 ?
                       Math.Round(globalMethods.ConvertDecimal(r["net_value"]) - (globalMethods.ConvertDecimal(r["net_value"]) *
                       (globalMethods.ConvertDecimal(r["RATE_CUTOFF_TAX_PERCENTAGE"]) + globalMethods.ConvertDecimal(r["cess_percentage"]) +
                        globalMethods.ConvertDecimal(r["gst_cess_percentage"])) / (100 + globalMethods.ConvertDecimal(r["RATE_CUTOFF_TAX_PERCENTAGE"]) + globalMethods.ConvertDecimal(r["cess_percentage"]) +
                        globalMethods.ConvertDecimal(r["gst_cess_percentage"]))), 2) :
                        Math.Round(globalMethods.ConvertDecimal(r["net_value"]), 2)));

                    if (bExportGstApplicable)
                    {
                        dtGstCalc.AsEnumerable().ToList().ForEach(r => r["GST_PERCENTAGE"] = nExportGstPct);
                    }
                    else
                    {
                        dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                        {
                            if ((globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) < (globalMethods.ConvertDecimal(r["NET_VALUE_WOTAX"]) / (globalMethods.ConvertDecimal(r["quantity"]))))
                                 || globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) == 0)
                                r["GST_PERCENTAGE"] = globalMethods.ConvertDecimal(r["tax_percentage"]);
                            else
                                r["GST_PERCENTAGE"] = globalMethods.ConvertDecimal(r["RATE_CUTOFF_TAX_PERCENTAGE"]);
                        });
                    }

                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        if ((globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) < (globalMethods.ConvertDecimal(r["NET_VALUE_WOTAX"]) / Math.Abs(globalMethods.ConvertDecimal(r["quantity"]))))
                                || globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) == 0)
                            r["Gst_Cess_Percentage"] = globalMethods.ConvertDecimal(r["Gst_Cess_Percentage"]);
                        else
                            r["Gst_Cess_Percentage"] = globalMethods.ConvertDecimal(r["Rate_CutOff_Gst_Cess_Percentage"]);

                        r["igst_amount"] = 0; r["cgst_amount"] = 0;
                    });
                }

                if (cPartyGstNo == cLocGstNo)
                {
                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        r["gst_percentage"] = 0;
                        r["gst_cess_percentage"] = 0;
                        r["NET_VALUE_WOTAX"] = r["NET_VALUE"];
                    });
                }


                dtGstCalc.AsEnumerable().ToList().ForEach(r => r["XN_VALUE_WITHOUT_GST"] = (globalMethods.ConvertInt(r["tax_method"]) == 1 ?
                                      Math.Round((globalMethods.ConvertDecimal(r["net_value"]) * globalMethods.ConvertDecimal(r["ARTICLE_PACK_SIZE"])) - ((globalMethods.ConvertDecimal(r["net_value"]) * globalMethods.ConvertDecimal(r["ARTICLE_PACK_SIZE"])) *
                                      (globalMethods.ConvertDecimal(r["gst_percentage"]) + globalMethods.ConvertDecimal(r["cess_percentage"]) +
                                       globalMethods.ConvertDecimal(r["gst_cess_percentage"])) / (100 +
                                       globalMethods.ConvertDecimal(r["gst_percentage"]) +
                                       globalMethods.ConvertDecimal(r["cess_percentage"]) +
                                       globalMethods.ConvertDecimal(r["gst_cess_percentage"]))), 2) :
                                       Math.Round(globalMethods.ConvertDecimal(r["net_value"]) * globalMethods.ConvertDecimal(r["ARTICLE_PACK_SIZE"]), 2)));

                //if (bCessApplicable)
                //{
                //    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                //    {
                //        r["cess_amount"] = globalMethods.ConvertDecimal(r["xn_value_without_gst"]) * globalMethods.ConvertDecimal(r["cess_percentage"]) / 100;
                //    });

                //}

                dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                {
                    r["Gst_Cess_Amount"] = Math.Round(globalMethods.ConvertDecimal(r["xn_value_without_gst"]) * globalMethods.ConvertDecimal(r["gst_CESS_PERCENTAGE"]) / 100, 2);
                });

                if (cCurStateCode != cPartyStateCode)
                {
                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        r["igst_amount"] = Math.Round(globalMethods.ConvertDecimal(r["XN_VALUE_WITHOUT_GST"]) * globalMethods.ConvertDecimal(r["gst_percentage"]) / 100, 2);
                    });
                }
                else
                {
                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        r["cgst_amount"] = Math.Round(globalMethods.ConvertDecimal(r["XN_VALUE_WITHOUT_GST"]) * globalMethods.ConvertDecimal(r["gst_percentage"]) / 100 / 2, 2);
                        r["sgst_amount"] = r["cgst_amount"];
                    });
                }

                dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                {
                    r["XN_VALUE_WITH_GST"] = globalMethods.ConvertDecimal(r["XN_VALUE_WITHOUT_GST"]) + globalMethods.ConvertDecimal(r["igst_amount"]) +
                    globalMethods.ConvertDecimal(r["cgst_amount"]) + globalMethods.ConvertDecimal(r["sgst_amount"]);
                });

                synchCmdGst(dtGstCalc, ref dtCmd);

            }


            if (!bRegisteredDealer)
            {
                dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                {
                    r["hsn_code"] = (String.IsNullOrEmpty(r["hsn_code"].ToString()) ? "0000000000" : r["hsn_code"]);
                    r["gst_percentage"] = 0;
                    r["Gst_Cess_Percentage"] = 0;
                    r["IGST_AMOUNT"] = 0;
                    r["CGST_AMOUNT"] = 0;
                    r["SGST_AMOUNT"] = 0;
                    r["NET_VALUE_WOTAX"] = r["NET_VALUE"];
                    r["XN_VALUE_WITHOUT_GST"] = r["NET_VALUE"];
                    r["XN_VALUE_WITH_GST"] = r["NET_VALUE"];
                });

                synchCmdGst(dtGstCalc, ref dtCmd);
            }


            //decimal nSubtotal, nAtdCharges;

            //nAtdCharges = globalMethods.ConvertDecimal(dtCmm.Rows[0]["other_charges"]);
            //nSubtotal = globalMethods.ConvertDecimal(dtCmm.Rows[0]["subtotal"]);

            //dtCmd.AsEnumerable().ToList().ForEach(r =>
            //{
            //    r["rfnet"] = globalMethods.ConvertDecimal(r["net"]) - globalMethods.ConvertDecimal(r["cmm_discount_amount"]) + (globalMethods.ConvertInt(r["tax_method"]) == 2 ?
            //    globalMethods.ConvertDecimal(r["igst_amount"]) + globalMethods.ConvertDecimal(r["cgst_amount"]) + globalMethods.ConvertDecimal(r["sgst_amount"]) +
            //    globalMethods.ConvertDecimal(r["gst_cess_amount"]) : 0);

            //    if (nAtdCharges == 0)
            //        r["rfnet_with_other_charges"] = r["rfnet"];
            //    else
            //        r["rfnet_with_other_charges"] = globalMethods.ConvertDecimal(r["rfnet"]) + ((nAtdCharges / nSubtotal) * globalMethods.ConvertDecimal(r["net"]));
            //});

            cMessage = CalcGstOC_WSL(ref dtCmm, dtCmd, bRegisteredDealer, cCurStateCode, cPartyStateCode, tConfig);

            if (String.IsNullOrEmpty(cMessage))
            {
                cMessage = CalcGstFreight_WSL(ref dtCmm, dtCmd, bRegisteredDealer, cCurStateCode, cPartyStateCode, tConfig);
            }
            return cMessage;
        }
        private string applyBillLevelScheme(ref DataTable dtCmm, DataTable dtCmd, DataTable dtSlabs, DataRow drSchemeDet)
        {
            decimal nNetValue, nSchemeAppliedQty, nMrp, nQty, nDiscountFigure, nDiscountAmount = 0, nDiscountPercentage = 0;
            string cProductCode, filterTableData;

            string cSchemeName = drSchemeDet["schemeName"].ToString();

            commonMethods globalMethods = new commonMethods();

            // No need to apply Bill level scheme If already applied or Ecoupon is there
            if ((!String.IsNullOrEmpty(dtCmm.Rows[0]["ecoupon_id"].ToString()) && !globalMethods.ConvertBool(drSchemeDet["wizclip_based_scheme"])) || globalMethods.ConvertBool(dtCmm.Rows[0]["manual_discount"])
                || globalMethods.ConvertBool(dtCmm.Rows[0]["dp_changed"]) || globalMethods.ConvertBool(dtCmm.Rows[0]["discount_changed"]))
                return "";

            dtCmm.Rows[0]["discount_percentage"] = 0;
            dtCmm.Rows[0]["discount_amount"] = 0;

            string cSchemeRowId = drSchemeDet["schemeRowId"].ToString();

            filterTableData = " schemeRowId='" + cSchemeRowId + "'";
            DataRow drSlabs = dtSlabs.Rows[0];

            int nBuyType = Convert.ToInt32(drSchemeDet["buyType"]);


            Decimal totals = 0, maxQtySlab = 0;
            foreach (DataRow dr in dtCmd.Rows)
            {
                if (nBuyType == 1)
                    totals = totals + Convert.ToDecimal(dr["QUANTITY"]);
                else
                    totals = totals + ((globalMethods.ConvertDecimal(dr["mrp"]) * globalMethods.ConvertDecimal(dr["quantity"])) -
                                       globalMethods.ConvertDecimal(dr["basic_discount_amount"]) - globalMethods.ConvertDecimal(dr["card_discount_amount"]) -
                                       globalMethods.ConvertDecimal(dr["manual_Discount_amount"]));

            }

            maxQtySlab = Convert.ToDecimal(dtSlabs.Compute("max(buyToRange)", ""));

            if (totals > maxQtySlab)
                filterTableData = "buyToRange=" + maxQtySlab.ToString();
            else
                filterTableData = totals.ToString() + ">=buyFromRange AND " + totals.ToString() + "<=buyToRange";

            DataRow[] drSearch = dtSlabs.Select(filterTableData, "");

            DataTable dtFilteredSlab = new DataTable();


            if (drSearch.Length > 0)
            {
                dtFilteredSlab = dtSlabs.Select(filterTableData, "").CopyToDataTable();
            }

            if (dtFilteredSlab.Rows.Count == 0)
                return "";


            if (globalMethods.ConvertDecimal(dtFilteredSlab.Rows[0]["discountPercentage"]) > 0)
            {
                dtCmm.Rows[0]["discount_percentage"] = globalMethods.ConvertDecimal(dtFilteredSlab.Rows[0]["discountPercentage"]);
                dtCmm.Rows[0]["BillLevelSchemeApplied"] = true;
            }
            else if (globalMethods.ConvertDecimal(dtFilteredSlab.Rows[0]["discountAmount"]) > 0)
            {
                dtCmm.Rows[0]["discount_amount"] = dtFilteredSlab.Rows[0]["discountAmount"];
                dtCmm.Rows[0]["BillLevelSchemeApplied"] = true;
            }

            return "";

        }
        private string UpdateBNGNWtdAvgDisc(ref DataTable dtCmdSchemes, String cSlabRowId, bool donot_distribute_weighted_avg_disc_bngn)
        {

            //if (AppConfigModel.EossTestCase)
            //    donot_distribute_weighted_avg_disc_bngn = true;

            string msg = "";
            try
            {

                DataTable dtBnGnCmd = new DataTable();


                var bngnRowIds = dtCmdSchemes.Select("slabRowId='" + cSlabRowId + "'", "").AsEnumerable()
                        .Select(row => row.Field<string>("BuynGetnRowId") ?? "").Distinct()
                        .ToList();

                foreach (var bngnRowId in bngnRowIds)
                {

                    if (string.IsNullOrEmpty(bngnRowId.ToString()))
                        continue;

                    dtBnGnCmd = dtCmdSchemes.Select("slabRowId='" + cSlabRowId + "' and addnlBnGnDiscount=false and BuynGetnRowId='" + bngnRowId.ToString() + "'", "").CopyToDataTable();

                    //Have to do this looping because compute on multiplication of 2 column values does not work
                    decimal GrossSale = 0, totalDiscount = 0;
                    foreach (DataRow dr in dtBnGnCmd.Rows)
                    {
                        GrossSale = GrossSale + (Convert.ToDecimal(dr["MRP"]) * Convert.ToDecimal(dr["QUANTITY"]));
                        totalDiscount = totalDiscount + Convert.ToDecimal(dr["basic_discount_amount"]);
                    }

                    decimal NetSale = GrossSale - totalDiscount;

                    decimal nTOTWTDDISC = 0, nWtdDisc = 0, nTotSchemeDisc = 0;
                    String cBnGnRowId = "";
                    foreach (DataRow dr in dtBnGnCmd.Rows)
                    {
                        if (dr["slabRowId"].ToString() == cSlabRowId)
                        {
                            nWtdDisc = Math.Round((Convert.ToDecimal(dr["MRP"]) * Convert.ToDecimal(dr["QUANTITY"]) * ((GrossSale - NetSale) / GrossSale)), 2);
                            dr["WEIGHTED_AVG_DISC_AMT"] = nWtdDisc;
                            nTOTWTDDISC = nTOTWTDDISC + nWtdDisc;
                            nTotSchemeDisc = nTotSchemeDisc + Convert.ToDecimal(dr["basic_discount_amount"]);
                            dr["WEIGHTED_AVG_DISC_PCT"] = Math.Round((nWtdDisc / (Convert.ToDecimal(dr["mrp"]) * Convert.ToDecimal(dr["quantity"]))) * 100, 3);
                            cBnGnRowId = dr["row_id"].ToString();
                        }
                    }

                    if (nTOTWTDDISC != nTotSchemeDisc)
                    {
                        //dtCmdSchemes.Select(string.Format("[row_id] = '{0}'", cBnGnRowId))
                        dtBnGnCmd.Select("row_id='" + cBnGnRowId + "'", "")
                         .ToList<DataRow>()
                         .ForEach(r =>
                         {
                             r["WEIGHTED_AVG_DISC_AMT"] = Convert.ToDecimal(r["WEIGHTED_AVG_DISC_AMT"]) + nTotSchemeDisc - nTOTWTDDISC;
                         });
                    }


                    if (!donot_distribute_weighted_avg_disc_bngn)
                    {
                        dtBnGnCmd.AsEnumerable().ToList().ForEach(r =>
                        {
                            r["basic_discount_percentage"] = r["WEIGHTED_AVG_DISC_PCT"];
                            r["basic_discount_amount"] = r["WEIGHTED_AVG_DISC_AMT"];
                            r["net"] = Math.Round((Convert.ToDecimal(r["quantity"]) * Convert.ToDecimal(r["mrp"])) - Convert.ToDecimal(r["basic_discount_amount"]), 2);
                        });
                    }

                    synchCmdSchemes(dtBnGnCmd, ref dtCmdSchemes);
                }

            }


            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);
                msg = "Error in APplying Buy n Get n Weighted Avg discounts at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();
            }

            return msg;
        }
        //private string applyBillLevelScheme(ref DataTable dtCmm, DataTable dtCmd, DataTable dtSlabs, DataRow drSchemeDet)
        //{
        //    decimal nNetValue, nSchemeAppliedQty, nMrp, nQty, nDiscountFigure, nDiscountAmount = 0, nDiscountPercentage = 0;
        //    string cProductCode, filterTableData;

        //    commonMethods globalMethods = new commonMethods();

        //    // No need to apply Bill level scheme If already applied or Ecoupon is there
        //    //if (!String.IsNullOrEmpty(dtCmm.Rows[0]["ecoupon_id"].ToString()))
        //    //    return "";
        //    if ((!String.IsNullOrEmpty(dtCmm.Rows[0]["ecoupon_id"].ToString()) && !globalMethods.ConvertBool(drSchemeDet["wizclip_based_scheme"])) || globalMethods.ConvertBool(dtCmm.Rows[0]["manual_discount"])
        //     || globalMethods.ConvertBool(dtCmm.Rows[0]["dp_changed"]) || globalMethods.ConvertBool(dtCmm.Rows[0]["discount_changed"])) 
        //        return "";

        //    dtCmm.Rows[0]["discount_percentage"] = 0;
        //    dtCmm.Rows[0]["discount_amount"] = 0;

        //    string cSchemeName = drSchemeDet["schemeName"].ToString();
        //    string cSchemeRowId = drSchemeDet["schemeRowId"].ToString();

        //    filterTableData = " schemeRowId='" + cSchemeRowId + "'";
        //    DataRow drSlabs = dtSlabs.Rows[0];

        //    int nBuyType = Convert.ToInt32(drSchemeDet["buyType"]);


        //    Decimal totals = 0, maxQtySlab = 0;
        //    foreach (DataRow dr in dtCmd.Rows)
        //    {
        //        if (nBuyType == 1)
        //            totals = totals + globalMethods.ConvertDecimal(dr["QUANTITY"]);
        //        else
        //            totals = totals + ((globalMethods.ConvertDecimal(dr["mrp"]) * globalMethods.ConvertDecimal(dr["quantity"])) -
        //                               globalMethods.ConvertDecimal(dr["basic_discount_amount"]) - globalMethods.ConvertDecimal(dr["card_discount_amount"]) -
        //                               globalMethods.ConvertDecimal(dr["manual_Discount_amount"]));

        //    }

        //    maxQtySlab = globalMethods.ConvertDecimal(dtSlabs.Compute("max(buyToRange)", ""));

        //    if (totals > maxQtySlab)
        //        filterTableData = "buyToRange=" + maxQtySlab.ToString();
        //    else
        //        filterTableData = totals.ToString() + ">=buyFromRange AND " + totals.ToString() + "<=buyToRange";

        //    DataRow[] drSearch = dtSlabs.Select(filterTableData, "");

        //    DataTable dtFilteredSlab = new DataTable();


        //    if (drSearch.Length > 0)
        //    {
        //        dtFilteredSlab = dtSlabs.Select(filterTableData, "").CopyToDataTable();
        //    }

        //    if (dtFilteredSlab.Rows.Count == 0)
        //        return "";


        //    if (globalMethods.ConvertDecimal(dtFilteredSlab.Rows[0]["discountPercentage"]) > 0)
        //    {
        //        dtCmm.Rows[0]["discount_percentage"] = globalMethods.ConvertDecimal(dtFilteredSlab.Rows[0]["discountPercentage"]);
        //    }
        //    else if (globalMethods.ConvertDecimal(dtFilteredSlab.Rows[0]["discountAmount"]) > 0)
        //    {
        //        dtCmm.Rows[0]["discount_amount"] = dtFilteredSlab.Rows[0]["discountAmount"];

        //    }

        //    return "";

        //}

        //private string UpdateBNGNWtdAvgDisc(ref DataTable dtCmdSchemes, String cSlabRowId, bool donot_distribute_weighted_avg_disc_bngn)
        //{
        //    string msg = "";
        //    try
        //    {
        //        commonMethods globalMethods = new commonMethods();
        //        DataTable dtBnGnCmd = new DataTable();


        //        var bngnRowIds = dtCmdSchemes.AsEnumerable()
        //                .Select(row => row.Field<string>("BuynGetnRowId") ?? "").Distinct()
        //                .ToList();

        //        foreach (var bngnRowId in bngnRowIds)
        //        {

        //            if (string.IsNullOrEmpty(bngnRowId.ToString()))
        //                continue;

        //            dtBnGnCmd = dtCmdSchemes.Clone();
        //            DataRow[] drowBnGn = dtCmdSchemes.Select("slabRowId='" + cSlabRowId + "' and addnlBnGnDiscount=false and BuynGetnRowId='" + bngnRowId.ToString() + "'", "");
        //            if(drowBnGn.Length>0)
        //            dtBnGnCmd = drowBnGn.CopyToDataTable();

        //            //Have to do this looping because compute on multiplication of 2 column values does not work
        //            decimal GrossSale = 0, totalDiscount = 0;
        //            foreach (DataRow dr in dtBnGnCmd.Rows)
        //            {
        //                GrossSale = GrossSale + (globalMethods.ConvertDecimal(dr["MRP"]) * globalMethods.ConvertDecimal(dr["QUANTITY"]));
        //                totalDiscount = totalDiscount + globalMethods.ConvertDecimal(dr["basic_discount_amount"]);
        //            }

        //            decimal NetSale = GrossSale - totalDiscount;

        //            decimal nTOTWTDDISC = 0, nWtdDisc = 0, nTotSchemeDisc = 0;
        //            String cBnGnRowId = "";
        //            foreach (DataRow dr in dtBnGnCmd.Rows)
        //            {
        //                if (dr["slabRowId"].ToString() == cSlabRowId)
        //                {
        //                    nWtdDisc = Math.Round((globalMethods.ConvertDecimal(dr["MRP"]) * globalMethods.ConvertDecimal(dr["QUANTITY"]) * ((GrossSale - NetSale) / GrossSale)), 2);
        //                    dr["WEIGHTED_AVG_DISC_AMT"] = nWtdDisc;
        //                    nTOTWTDDISC = nTOTWTDDISC + nWtdDisc;
        //                    nTotSchemeDisc = nTotSchemeDisc + globalMethods.ConvertDecimal(dr["basic_discount_amount"]);
        //                    dr["WEIGHTED_AVG_DISC_PCT"] = Math.Round((nWtdDisc / (globalMethods.ConvertDecimal(dr["mrp"]) * globalMethods.ConvertDecimal(dr["quantity"]))) * 100, 3);
        //                    cBnGnRowId = dr["row_id"].ToString();
        //                }
        //            }

        //            if (nTOTWTDDISC != nTotSchemeDisc)
        //            {
        //                //dtCmdSchemes.Select(string.Format("[row_id] = '{0}'", cBnGnRowId))
        //                dtBnGnCmd.Select("row_id='" + cBnGnRowId + "'", "")
        //                 .ToList<DataRow>()
        //                 .ForEach(r =>
        //                 {
        //                     r["WEIGHTED_AVG_DISC_AMT"] = globalMethods.ConvertDecimal(r["WEIGHTED_AVG_DISC_AMT"]) + nTotSchemeDisc - nTOTWTDDISC;
        //                 });
        //            }


        //            if (!donot_distribute_weighted_avg_disc_bngn)
        //            {
        //                dtBnGnCmd.AsEnumerable().ToList().ForEach(r =>
        //                {
        //                    r["basic_discount_percentage"] = r["WEIGHTED_AVG_DISC_PCT"];
        //                    r["basic_discount_amount"] = r["WEIGHTED_AVG_DISC_AMT"];
        //                    r["net"] = Math.Round((globalMethods.ConvertDecimal(r["quantity"]) * globalMethods.ConvertDecimal(r["mrp"])) - globalMethods.ConvertDecimal(r["basic_discount_amount"]), 2);
        //                });
        //            }

        //            synchCmdSchemes(dtBnGnCmd, ref dtCmdSchemes);
        //        }

        //    }


        //    catch (Exception ex)
        //    {
        //        int errLineNo = new commonMethods().GetErrorLineNo(ex);
        //        msg = "Error in APplying Buy n Get n Weighted Avg discounts at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();
        //    }

        //    return msg;
        //}

        public DataTable ConvertToDataTable<T>(List<T> items)
        {
            DataTable dataTable = new DataTable(typeof(T).Name);
            //Get all the properties
            PropertyInfo[] Props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            String propName;
            Type cDataType;
            foreach (PropertyInfo prop in Props)
            {
                //Setting column names as Property names
                dataTable.Columns.Add(prop.Name);
                propName = prop.PropertyType.FullName.ToUpper().Trim();

                cDataType = (propName.Contains("SYSTEM.DECIMAL") ? typeof(decimal) : (propName.Contains("SYSTEM.INT") ? typeof(Int32) :
                    (propName.Contains("SYSTEM.BOOLEAN") ? typeof(Boolean) : (propName.Contains("SYSTEM.DATETIME") ? typeof(DateTime) : typeof(String)))));

                dataTable.Columns[prop.Name].DataType = cDataType;
                dataTable.Columns[prop.Name].AllowDBNull = true;
            }
            foreach (T item in items)
            {

                var values = new object[Props.Length];
                for (int i = 0; i < Props.Length; i++)
                {
                    //inserting property values to datatable rows
                    values[i] = Props[i].GetValue(item, null);
                }

                dataTable.Rows.Add(values);
            }
            //put a breakpoint here and check datatable
            return dataTable;
        }
        public DataSet GetActiveSchemesData(SqlConnection conn, String LoggedLocation)
        {
            //dynamic result = new ExpandoObject();

            String cExpr = "SpWow_GetBarcodes_SchemeInfo";
            SqlCommand cmd = new SqlCommand(cExpr, conn);
            cmd.CommandType = CommandType.StoredProcedure;

            DataSet ds = new DataSet();
            SqlDataAdapter sda = new SqlDataAdapter(cmd);

            sda.Fill(ds);

            //result.data = ds;

            //return result;
            return ds;

        }


        private string synchCmdGst(DataTable dtSource, ref DataTable dtTarget)
        {
            commonMethods globalMethods = new commonMethods();
            dtTarget.AsEnumerable().Join
            (
                dtSource.AsEnumerable(),
                lMaster => lMaster["row_id"], lChild => lChild["row_id"],
                (lMaster, lChild) => new { lMaster, lChild }
                ).ToList().ForEach
            (
            o =>
            {
                o.lMaster.SetField("GST_PERCENTAGE", o.lChild["GST_PERCENTAGE"]);
                o.lMaster.SetField("xn_value_without_gst", globalMethods.ConvertDecimal(o.lChild["xn_value_without_gst"]));
                o.lMaster.SetField("xn_value_with_gst", globalMethods.ConvertDecimal(o.lChild["xn_value_with_gst"]));
                o.lMaster.SetField("igst_amount", globalMethods.ConvertDecimal(o.lChild["igst_amount"]));
                o.lMaster.SetField("cgst_amount", globalMethods.ConvertDecimal(o.lChild["cgst_amount"]));
                o.lMaster.SetField("sgst_amount", globalMethods.ConvertDecimal(o.lChild["sgst_amount"]));
                o.lMaster.SetField("cess_amount", o.lChild["cess_amount"]);
                o.lMaster.SetField("gst_Cess_amount", o.lChild["gst_Cess_amount"]);
                o.lMaster.SetField("GST_CESS_PERCENTAGE", o.lChild["GST_CESS_PERCENTAGE"]);
                o.lMaster.SetField("tax_method", o.lChild["tax_method"]);
            }
            );
            return "";
        }

        //myValue.isNull(new MyValue())
        private string synchCmdSchemes(DataTable dtSource, ref DataTable dtTarget, Boolean bFinalTable = false)
        {
            commonMethods globalMethods = new commonMethods();
            dtTarget.AsEnumerable().Join
            (
                dtSource.AsEnumerable(),
                lMaster => lMaster["row_id"], lChild => lChild["row_id"],
                (lMaster, lChild) => new { lMaster, lChild }
                ).ToList().ForEach
            (
            o =>
            {
                o.lMaster.SetField("basic_discount_percentage", globalMethods.ConvertDecimal(o.lChild["basic_discount_percentage"]));
                o.lMaster.SetField("basic_discount_amount", globalMethods.ConvertDecimal(o.lChild["basic_discount_amount"]));
                o.lMaster.SetField("net", globalMethods.ConvertDecimal(o.lChild["net"]));
                o.lMaster.SetField("WEIGHTED_AVG_DISC_AMT", globalMethods.ConvertDecimal(o.lChild["WEIGHTED_AVG_DISC_AMT"]));
                o.lMaster.SetField("WEIGHTED_AVG_DISC_PCT", globalMethods.ConvertDecimal(o.lChild["WEIGHTED_AVG_DISC_PCT"]));
                o.lMaster.SetField("scheme_name", o.lChild["scheme_name"].ToString());
                o.lMaster.SetField("slsdet_row_id", o.lChild["slsdet_row_id"].ToString());


                if (!bFinalTable)
                {
                    o.lMaster.SetField("addnlBnGnDiscount", globalMethods.ConvertBool(o.lChild["addnlBnGnDiscount"]));
                    o.lMaster.SetField("pending_scheme_apply_qty", globalMethods.ConvertDecimal(o.lChild["pending_scheme_apply_qty"]));
                    o.lMaster.SetField("scheme_applied_qty", globalMethods.ConvertDecimal(o.lChild["scheme_applied_qty"]));
                    o.lMaster.SetField("pending_scheme_apply_amount", globalMethods.ConvertDecimal(o.lChild["pending_scheme_apply_amount"]));
                    o.lMaster.SetField("scheme_applied_amount", globalMethods.ConvertDecimal(o.lChild["scheme_applied_amount"]));
                    o.lMaster.SetField("slabRowId", o.lChild["slabRowId"].ToString());
                    o.lMaster.SetField("BuynGetnRowId", o.lChild["BuynGetnRowId"].ToString());
                }
                else 
                {
                    o.lMaster.SetField("discount_percentage", (globalMethods.ConvertDecimal(o.lChild["basic_discount_percentage"])+ globalMethods.ConvertDecimal(o.lChild["manual_discount_percentage"])));
                    o.lMaster.SetField("discount_amount", (globalMethods.ConvertDecimal(o.lChild["basic_discount_amount"])+ globalMethods.ConvertDecimal(o.lChild["manual_discount_amount"])));
                }

            }
            );
            return "";
        }


        private String GetBarcodesfromEoss(SqlConnection conn, Boolean bFlatDiscountBarCodes, ref DataTable dtFlatBarcodesDiscount, DataTable dtTvpBarCodes, DataTable dtFlatSchemes)
        {

            string retMessage = "";
            try
            {
                String cExpr = "SPWOW_GET_ACTIVE_SCHEME";
                SqlCommand cmd = new SqlCommand(cExpr, conn);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@tblBarCodes", dtTvpBarCodes);
                cmd.Parameters.AddWithValue("@bCalledForFlatDiscount", bFlatDiscountBarCodes);
                cmd.Parameters.AddWithValue("@tblActiveSchemes", dtFlatSchemes);
                cmd.Parameters.AddWithValue("@nQueryId", 2);

                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                sda.Fill(dtFlatBarcodesDiscount);
            }

            catch (Exception ex)
            {
                retMessage = ex.Message.ToString();
            }

            return retMessage;

        }
        private string applyDiscountasPerMethod(DataRow drCmd, string cSchemeRowId, decimal discountPercentage, decimal discountAmount, decimal netPrice, decimal nAppliedQty = 1)
        {
            decimal nDiscountFigure = 0, nNetValue = 0, nBaseValue, nDiscountAmount, nQty, nDiscountPercentage, nPendingSchemeAppliedAmount,
            nPendingSchemeApplyAmount, nMrp;

            int nDiscMethod = 1;


            try
            {
                commonMethods globalMethods = new commonMethods();

                nPendingSchemeApplyAmount = Convert.ToDecimal(drCmd["pending_scheme_apply_amount"]);
                nQty = Convert.ToDecimal(drCmd["quantity"]);
                nBaseValue = Math.Abs(nPendingSchemeApplyAmount) * (nQty > 0 ? 1 : -1);
                nMrp = Convert.ToDecimal(drCmd["mrp"]);

                nDiscountAmount = 0;
                if (discountPercentage > 0)
                {
                    nDiscountFigure = discountPercentage;
                    nDiscountAmount = nBaseValue * nDiscountFigure / 100;
                    nDiscMethod = 1;
                }
                else if (discountAmount > 0)
                {
                    nDiscountFigure = discountAmount;

                    nDiscountAmount = (nDiscountFigure > nPendingSchemeApplyAmount ? nPendingSchemeApplyAmount : nDiscountFigure);
                    nDiscMethod = 2;
                }
                else if (netPrice > 0)
                {
                    nDiscountFigure = netPrice;

                    nDiscountAmount = (nDiscountFigure > nMrp ? nMrp : nPendingSchemeApplyAmount - nDiscountFigure);
                    nDiscMethod = 3;
                }

                nDiscountAmount = Math.Abs(nDiscountAmount * nAppliedQty) * (nQty < 0 ? -1 : 1);

                drCmd["basic_discount_amount"] = globalMethods.ConvertDecimal(drCmd["basic_discount_amount"]) + nDiscountAmount;

                nDiscountPercentage = (nDiscMethod == 1 ? nDiscountFigure : Math.Round((globalMethods.ConvertDecimal(drCmd["basic_discount_amount"]) / (nMrp * nQty)) * 100, 3));

                if (Convert.ToDecimal(drCmd["basic_discount_amount"]) == 0)
                {
                    drCmd["basic_discount_percentage"] = Math.Abs(nDiscountPercentage);
                    drCmd["slsdet_row_id"] = cSchemeRowId;
                    drCmd["net"] = nNetValue;
                }
                else
                {
                    drCmd["basic_discount_percentage"] = Math.Abs(Math.Round((globalMethods.ConvertDecimal(drCmd["basic_discount_amount"]) / (nMrp * nQty)) * 100, 3));
                    drCmd["net"] = (nMrp * nQty) - globalMethods.ConvertDecimal(drCmd["basic_discount_amount"]);
                    drCmd["slsdet_row_id"] = cSchemeRowId;
                }


                drCmd["scheme_applied_amount"] = Convert.ToDecimal(drCmd["mrp"]) * Math.Abs(Convert.ToDecimal(drCmd["quantity"]));

            }

            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);
                return "Error in applyDiscountasPerMethod at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();

            }

            return "";
        }


        //private string applyDiscountasPerMethod(DataRow drCmd, string cSchemeRowId, decimal discountPercentage, decimal discountAmount, decimal netPrice, decimal nAppliedQty = 1)
        //{
        //    decimal nDiscountFigure = 0, nNetValue = 0, nBaseValue, nDiscountAmount, nQty, nDiscountPercentage, nPendingSchemeAppliedAmount,
        //    nPendingSchemeApplyAmount, nMrp;

        //    int nDiscMethod = 1;


        //    try
        //    {
        //        commonMethods globalMethods = new commonMethods();

        //        nPendingSchemeApplyAmount = globalMethods.ConvertDecimal(drCmd["pending_scheme_apply_amount"]);
        //        nQty = globalMethods.ConvertDecimal(drCmd["quantity"]);
        //        nBaseValue = Math.Abs(nPendingSchemeApplyAmount) * (nQty > 0 ? 1 : -1);
        //        nMrp = globalMethods.ConvertDecimal(drCmd["mrp"]);

        //        nDiscountAmount = 0;
        //        if (discountPercentage > 0)
        //        {
        //            nDiscountFigure = discountPercentage;
        //            nDiscountAmount = nBaseValue * nDiscountFigure / 100;
        //            nDiscMethod = 1;
        //        }
        //        else if (discountAmount > 0)
        //        {
        //            nDiscountFigure = discountAmount;

        //            nDiscountAmount = (nDiscountFigure > nPendingSchemeApplyAmount ? nPendingSchemeApplyAmount : nDiscountFigure);
        //            nDiscMethod = 2;
        //        }
        //        else if (netPrice > 0)
        //        {
        //            nDiscountFigure = netPrice;

        //            nDiscountAmount = (nDiscountFigure > nMrp ? nMrp : nMrp - nDiscountFigure);
        //            nDiscMethod = 3;
        //        }

        //        nDiscountAmount = Math.Abs(nDiscountAmount * nAppliedQty) * (nQty < 0 ? -1 : 1);

        //        drCmd["basic_discount_amount"] = globalMethods.ConvertDecimal(drCmd["basic_discount_amount"]) + nDiscountAmount;

        //        nDiscountPercentage = (nDiscMethod == 1 ? nDiscountFigure : Math.Round((globalMethods.ConvertDecimal(drCmd["basic_discount_amount"]) / (nMrp * nQty)) * 100,3));

        //        if (globalMethods.ConvertDecimal(drCmd["basic_discount_amount"]) == 0)
        //        {
        //            drCmd["basic_discount_percentage"] = Math.Abs(nDiscountPercentage);
        //            drCmd["slsdet_row_id"] = cSchemeRowId;
        //            drCmd["net"] = nNetValue;
        //            drCmd["scheme_applied_amount"] = globalMethods.ConvertDecimal(drCmd["scheme_applied_amount"]) + nDiscountAmount;
        //        }
        //        else
        //        {

        //            drCmd["scheme_applied_amount"] = globalMethods.ConvertDecimal(drCmd["scheme_applied_amount"]) + nDiscountAmount;
        //            drCmd["basic_discount_percentage"] = Math.Abs(Math.Round((globalMethods.ConvertDecimal(drCmd["basic_discount_amount"]) / (nMrp * nQty)) * 100, 3));
        //            drCmd["net"] = (nMrp * nQty) - globalMethods.ConvertDecimal(drCmd["basic_discount_amount"]);
        //            drCmd["slsdet_row_id"] = cSchemeRowId;
        //        }
        //    }

        //    catch (Exception ex)
        //    {
        //        int errLineNo = new commonMethods().GetErrorLineNo(ex);
        //        return "Error in applyDiscountasPerMethod at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();

        //    }

        //    return "";
        //}

        private string applyEossFlatDiscountBarcodes(ref DataTable dtCmdSchemes, DataTable dtBarCodewiseDiscounts)
        {
            decimal nNetValue, nSchemeAppliedQty, nMrp, nQty, nDiscountFigure, nDiscountAmount = 0, nDiscountPercentage = 0;
            string cProductCode, filterTableData;
            int nDiscMethod;


            commonMethods globalMethods = new commonMethods();
            DataTable dtBarCodeInfo = new DataTable();

            foreach (DataRow drDetail in dtCmdSchemes.Rows)
            {
                cProductCode = drDetail["product_code"].ToString();

                nMrp = globalMethods.ConvertDecimal(drDetail["mrp"]);
                nQty = globalMethods.ConvertDecimal(drDetail["quantity"]);
                decimal nMrpValue = Math.Round(nMrp * nQty, 2);

                nNetValue = 0;
                nDiscMethod = 1;
                nDiscountFigure = 0;


                DataRow[] drSearch = dtBarCodewiseDiscounts.Select("product_code='" + cProductCode + "'", "");

                if (drSearch.Length > 0)
                {
                    dtBarCodeInfo.Rows.Clear();

                    dtBarCodeInfo = dtBarCodewiseDiscounts.Select("product_code='" + cProductCode + "'", "").CopyToDataTable();

                    if (globalMethods.ConvertDecimal(dtBarCodeInfo.Rows[0]["flat_discountPercentage"]) > 0)
                    {
                        nDiscountFigure = globalMethods.ConvertDecimal(dtBarCodeInfo.Rows[0]["flat_discountPercentage"]);
                        nNetValue = nMrpValue - (nMrpValue * globalMethods.ConvertDecimal(dtBarCodeInfo.Rows[0]["flat_discountPercentage"]) / 100);
                        nDiscMethod = 1;
                    }
                    else if (globalMethods.ConvertDecimal(dtBarCodeInfo.Rows[0]["flat_discountAmount"]) > 0)
                    {
                        nDiscountFigure = globalMethods.ConvertDecimal(dtBarCodeInfo.Rows[0]["flat_discountAmount"]);

                        nNetValue = (nDiscountFigure > nMrpValue ? 0 : nMrpValue - nDiscountFigure);
                        nDiscMethod = 2;
                    }
                    else if (Convert.ToDouble(dtBarCodeInfo.Rows[0]["flat_netPrice"]) > 0)
                    {
                        nDiscountFigure = globalMethods.ConvertDecimal(dtBarCodeInfo.Rows[0]["flat_netPrice"]);

                        nNetValue = (nDiscountFigure > nMrpValue ? 0 : nDiscountFigure);
                        nDiscMethod = 3;
                    }

                    nDiscountAmount = (nMrpValue - nNetValue);


                    nDiscountPercentage = (nDiscMethod == 1 ? nDiscountFigure : (nDiscountAmount / (nMrpValue)) * 100);

                    drDetail["basic_discount_percentage"] = nDiscountPercentage;
                    drDetail["basic_discount_amount"] = nDiscountAmount;
                    drDetail["slsdet_row_id"] = dtBarCodeInfo.Rows[0]["schemeRowId"];
                    drDetail["net"] = nNetValue;
                    drDetail["scheme_applied_amount"] = nMrpValue;
                    drDetail["scheme_applied_qty"] = drDetail["quantity"];

                    drDetail["scheme_name"] = drDetail["scheme_name"] + dtBarCodeInfo.Rows[0]["schemeName"].ToString();
                }

            }

            return "";
        }


        private void ProcessBnGnValidity(int nMode, int nBuyType, int nGetType, ref decimal BuyItemsTotal, ref decimal GetItemsTotal, ref decimal AddnlGetItemsTotal, decimal nBaseValue,
            decimal nPendingSchemeApplyQty, decimal nPendingSchemeApplyAmount,
            ref decimal nSchemeAppliedQty, ref decimal nSchemeAppliedAmount, ref bool breakFromLoop)
        {
            decimal nAddValue = 0;


            if (nMode == 3)
            {
                nAddValue = nPendingSchemeApplyQty;
                nAddValue = ((nAddValue + AddnlGetItemsTotal) > nBaseValue ? nAddValue + AddnlGetItemsTotal - nBaseValue : nAddValue);
                AddnlGetItemsTotal = AddnlGetItemsTotal + Math.Abs(nAddValue);

                if (AddnlGetItemsTotal >= nBaseValue)
                    breakFromLoop = true;
            }
            else
            if (nMode == 2)
            {
                nAddValue = (nBuyType == 1 ? nPendingSchemeApplyQty : nPendingSchemeApplyAmount);
                nAddValue = ((nAddValue + BuyItemsTotal) > nBaseValue ? nAddValue + BuyItemsTotal - nBaseValue : nAddValue);
                BuyItemsTotal = BuyItemsTotal + Math.Abs(nAddValue);
                nSchemeAppliedQty = nSchemeAppliedQty + (nBuyType == 1 ? nAddValue : 0);
                nSchemeAppliedAmount = nSchemeAppliedAmount + (nBuyType == 2 ? nAddValue : 0);
                if (BuyItemsTotal >= nBaseValue)
                    breakFromLoop = true;
            }
            else
            {
                nAddValue = (nGetType == 1 ? nPendingSchemeApplyQty : nPendingSchemeApplyAmount);
                nAddValue = ((nAddValue + GetItemsTotal) > nBaseValue ? nAddValue + GetItemsTotal - nBaseValue : nAddValue);
                GetItemsTotal = GetItemsTotal + Math.Abs(nAddValue);
                nSchemeAppliedQty = nSchemeAppliedQty + (nGetType == 1 ? nAddValue : 0);
                nSchemeAppliedAmount = nSchemeAppliedAmount + (nGetType == 2 ? nAddValue : 0);
                if (GetItemsTotal >= nBaseValue)
                    breakFromLoop = true;
            }


        }


        private String ProcessBnGnAddnlDiscounts(string cSchemeRowId, string cSchemeName, ref DataTable dtCmdSchemes, DataRow drSlabs, DataTable dtSkuNames)
        {
            int nGetQty = Convert.ToInt32(drSlabs["addnlgetQty"]);

            DataRow[] drSkuNamesGet = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "' and getbcAddnl=1", "");

            if (drSkuNamesGet.Length == 0)
                return "";

            DataTable dtSkuNamesGet = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "' and getbcAddnl=1", "").CopyToDataTable();
            string filterTableData = " pending_scheme_apply_qty>0 or slsdet_row_id='" + cSchemeRowId + "'";

            commonMethods globalMethods = new commonMethods();


            DataTable dtcmdSchemesGet = dtCmdSchemes.Clone();

            string cMessage = globalMethods.JoinDataTables(dtCmdSchemes, dtSkuNamesGet, ref dtcmdSchemesGet, filterTableData,
            (row1, row2) =>
            row1.Field<String>("product_code") == row2.Field<String>("product_code"));

            if (!String.IsNullOrEmpty(cMessage))
                return cMessage;

            if (dtcmdSchemesGet.Rows.Count == 0)
                return "";

            int nItemsCnt = 0;
            foreach (DataRow dr in dtcmdSchemesGet.Rows)
            {
                applyDiscountasPerMethod(dr, cSchemeRowId, globalMethods.ConvertDecimal(drSlabs["addnlDiscountPercentage"]),
                            globalMethods.ConvertDecimal(drSlabs["addnlDiscountAmount"]), 0);
                nItemsCnt++;

                dr["scheme_applied_qty"] = globalMethods.ConvertDecimal(dr["scheme_applied_qty"]) + 1;
                dr["scheme_applied_amount"] = globalMethods.ConvertDecimal(dr["scheme_applied_amount"]) + (globalMethods.ConvertDecimal(dr["scheme_applied_qty"]) * globalMethods.ConvertDecimal(dr["mrp"]));

                dr["pending_scheme_apply_qty"] = Math.Abs(globalMethods.ConvertDecimal(dr["quantity"]) - globalMethods.ConvertDecimal(dr["scheme_applied_qty"]));
                dr["pending_scheme_apply_amount"] = Math.Abs((globalMethods.ConvertDecimal(dr["quantity"]) *
                    globalMethods.ConvertDecimal(dr["mrp"])) - globalMethods.ConvertDecimal(dr["scheme_applied_amount"]));

                dr["scheme_name"] = cSchemeName;
                dr["slsdet_row_id"] = cSchemeRowId;
                dr["addnlBnGnDiscount"] = true;

                if (nItemsCnt >= nGetQty)
                    break;
            }

            synchCmdSchemes(dtcmdSchemesGet, ref dtCmdSchemes);

            return "";
        }

        private string applyEossFlatDiscount(ref DataTable dtCmdSchemes, DataRow drSchemeDet, DataTable dtSkuNames, string cCmdRowId = "", Boolean bAPPLY_FIX_MRP_EOSS_AND_BILLPRINT = false)
        {
            try
            {

                decimal nNetValue, nSchemeAppliedQty, nMrp, nQty, nDiscountFigure, nDiscountAmount = 0, nDiscountPercentage = 0, nFIX_MRP = 0;  
                string cProductCode, filterTableData;
                int nDiscMethod;
                Boolean bFIX_MRP_Applicable = false;
                string cSchemeName = drSchemeDet["schemeName"].ToString();

                string cSchemeRowId = drSchemeDet["schemeRowId"].ToString();

                filterTableData = " scheme_applied_qty=0 and scheme_applied_amount=0";

                DataTable dtFilteredCmdBuy = dtCmdSchemes.Clone();
                DataTable dtSkuNamesBuy = dtSkuNames.Clone();

                DataRow[] drSkuNames = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "'", "");

                if (drSkuNames.Length > 0)
                    dtSkuNamesBuy = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "'", "").CopyToDataTable();

                commonMethods globalMethods = new commonMethods();

                string cMessage = globalMethods.JoinDataTables(dtCmdSchemes, dtSkuNamesBuy, ref dtFilteredCmdBuy, filterTableData,
                (row1, row2) =>
                row1.Field<String>("product_code") == row2.Field<String>("product_code"));

                if (!String.IsNullOrEmpty(cMessage))
                    return cMessage;

                if (dtFilteredCmdBuy.Rows.Count == 0)
                    return "";

                DataRow[] drCmd = dtFilteredCmdBuy.Select();

                foreach (DataRow drDetail in drCmd)
                {
                    bFIX_MRP_Applicable = globalMethods.ConvertBool(drDetail["FIX_MRP_Applicable"]);
                    cProductCode = drDetail["product_code"].ToString();
                    nFIX_MRP = globalMethods.ConvertDecimal(drDetail["fix_mrp"]);

                    nMrp = globalMethods.ConvertDecimal(drDetail["mrp"]);
                    if (bAPPLY_FIX_MRP_EOSS_AND_BILLPRINT && bFIX_MRP_Applicable)
                    {
                        nMrp = nFIX_MRP;
                    }

                    nQty = globalMethods.ConvertDecimal(drDetail["quantity"]);
                    decimal nMrpValue = Math.Round(nMrp * nQty, 2);

                    nSchemeAppliedQty = globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]);
                    /*
                     Rohit 16-05-2025 : Taiga #1725 REQ - DAMILANO - NEW EOSS Created by KUSHALVEER SINGH 22 Apr 2025 14:04
                     New EOSS campaign RANGE BASE, Fresh Article (0%) discount is not UPDATING . 
                     */
                    nNetValue = nMrpValue;
                    nDiscMethod = 1;
                    nDiscountFigure = 0;

                    DataRow[] drSku = dtSkuNamesBuy.Select("product_code='" + cProductCode + "'", "");

                    if (globalMethods.ConvertDecimal(drSku[0]["flat_discountPercentage"]) > 0)
                    {
                        nDiscountFigure = globalMethods.ConvertDecimal(drSku[0]["flat_discountPercentage"]);
                        nNetValue = nMrpValue - (nMrpValue * globalMethods.ConvertDecimal(drSku[0]["flat_discountPercentage"]) / 100);
                        nDiscMethod = 1;
                    }
                    else if (globalMethods.ConvertDecimal(drSku[0]["flat_discountAmount"]) > 0)
                    {
                        nDiscountFigure = globalMethods.ConvertDecimal(drSku[0]["flat_discountAmount"]);

                        nNetValue = (nDiscountFigure > Math.Abs(nMrpValue) ? 0 : (Math.Abs(nMrpValue) - nDiscountFigure) * (nMrpValue < 0 ? -1 : 1));
                        nDiscMethod = 2;
                    }
                    else if (Convert.ToDouble(drSku[0]["flat_netPrice"]) > 0)
                    {
                        nDiscountFigure = globalMethods.ConvertDecimal(drSku[0]["flat_netPrice"]);

                        nNetValue = (nDiscountFigure > Math.Abs(nMrpValue) ? 0 : nDiscountFigure * (nMrpValue < 0 ? -1 : 1));
                        nDiscMethod = 3;
                    }

                    nDiscountAmount = (nMrpValue - nNetValue);


                    nDiscountPercentage = (nDiscMethod == 1 ? nDiscountFigure : (nDiscountAmount / (nMrpValue)) * 100);

                    if (bAPPLY_FIX_MRP_EOSS_AND_BILLPRINT && bFIX_MRP_Applicable)
                    {
                        nMrp = globalMethods.ConvertDecimal(drDetail["mrp"]); 
                        nMrpValue = Math.Round(nMrp * nQty, 2);
                        nDiscountAmount = (nMrpValue - nNetValue);
                        if ((globalMethods.ConvertDecimal(drDetail["QUANTITY"]) > 0 && nDiscountAmount < 0) || (globalMethods.ConvertDecimal(drDetail["QUANTITY"]) < 0 && nDiscountAmount > 0))
                        {
                            return "Apply EOSS Flat Discount : Discount going Negative \n" + Convert.ToString(drDetail["product_code"]) + " : Discount Amount : " + nDiscountAmount.ToString();

                        }
                        nDiscountPercentage = ((nDiscountAmount / (nMrpValue)) * 100);
                    }



                    if (nSchemeAppliedQty == 0)
                    {
                        drDetail["basic_discount_percentage"] = nDiscountPercentage;
                        drDetail["basic_discount_amount"] = nDiscountAmount;
                        drDetail["slsdet_row_id"] = drSchemeDet["schemeRowId"];
                        drDetail["net"] = nNetValue;

                        //if (string.IsNullOrEmpty(cCmdRowId))
                        {
                            drDetail["scheme_applied_amount"] = globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]) + nDiscountAmount;
                            drDetail["scheme_applied_qty"] = drDetail["quantity"];
                        }
                    }
                    else
                    {
                        drDetail["basic_discount_amount"] = globalMethods.ConvertDecimal(drDetail["basic_discount_amount"]) + nDiscountAmount;

                        drDetail["basic_discount_percentage"] = Math.Round((globalMethods.ConvertDecimal(drDetail["basic_discount_amount"]) / nMrpValue) * 100, 3);
                        drDetail["net"] = (nMrp * nQty) - globalMethods.ConvertDecimal(drDetail["basic_discount_amount"]);

                        //if (string.IsNullOrEmpty(cCmdRowId))
                        {
                            drDetail["scheme_applied_amount"] = globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]) + nDiscountAmount;
                            drDetail["scheme_applied_qty"] = drDetail["quantity"];
                        }
                    }

                    drDetail["scheme_name"] = drDetail["scheme_name"] + (string.IsNullOrEmpty(drDetail["scheme_name"].ToString()) ? "" : ",") + drSchemeDet["schemeName"].ToString();

                }

                synchCmdSchemes(dtFilteredCmdBuy, ref dtCmdSchemes);
            }
            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);

                return "Error at Line#" + errLineNo.ToString() + " of UpdateSchemeDiscounts : " + ex.Message.ToString();
            }
            return "";
        }
        public string updateSchemeDiscounts(SqlConnection conn, string cLocId, ref DataTable dtCmm, ref DataTable dtCmd, DataTable tEossSchemes,
                                         DataTable tEossSlabs, DataTable tBcScheme, Boolean bCalledFromWizapp, int nItemLevelRoundOff, Boolean bApplyFlatschemesOnly = false, string cCmdRowId = "",
                                         Int32 nSlrDiscountMode = 2, bool bDonotApplyHappyHours = false, Boolean bAPPLY_FIX_MRP_EOSS_AND_BILLPRINT=false)
        {
            bool bBillLevelSchemeProcessed = false, bBillLevelSchemeFound = false;
            commonMethods globalMethods = new commonMethods();

            string retMsgFromSchemeMethod = "";
            try
            {
                int nItemsLoop = 1, nProcessLoop = 1;
                if (nSlrDiscountMode >= 2)
                    nItemsLoop = 2;

                /*AppConfigModel.*/
                EossReturnItemsProcessing = false;
            lblStart:

                if (nProcessLoop == 2)
                    /*AppConfigModel.*/
                    EossReturnItemsProcessing = true;

                string cmdFilter = "";
                if (bApplyFlatschemesOnly)
                    cmdFilter = " row_id ='" + cCmdRowId + "'";
                else
                {
                    cmdFilter = " isnull(barcodebased_flatdisc_applied,false)=false and isnull(manual_discount,false)=false and isnull(manual_dp,false)=false ";

                    if (bDonotApplyHappyHours)
                        cmdFilter = cmdFilter + " AND isnull(happy_hours_applied,false)=false ";

                    if (nProcessLoop == 2)
                        cmdFilter = cmdFilter + " and quantity<0";
                    else
                        cmdFilter = cmdFilter + " and quantity>0";
                }

                DataView view = new DataView(dtCmd, cmdFilter, "", DataViewRowState.CurrentRows);
                DataTable dtCmdSchemes = view.ToTable();

                dtCmdSchemes.Columns.Add("scheme_applied_qty", typeof(decimal));
                dtCmdSchemes.Columns["scheme_applied_qty"].DefaultValue = 0;
                dtCmdSchemes.Columns.Add("scheme_applied_amount", typeof(decimal));
                dtCmdSchemes.Columns["scheme_applied_amount"].DefaultValue = 0;

                dtCmdSchemes.Columns.Add("pending_scheme_apply_amount", typeof(decimal));
                dtCmdSchemes.Columns["pending_scheme_apply_amount"].DefaultValue = 0;
                dtCmdSchemes.Columns.Add("pending_scheme_apply_qty", typeof(decimal));
                dtCmdSchemes.Columns["pending_scheme_apply_qty"].DefaultValue = 0;
                dtCmdSchemes.Columns.Add("addnlBnGnDiscount", typeof(bool));
                dtCmdSchemes.Columns.Add("cmdRowId", typeof(string));
                dtCmdSchemes.Columns.Add("slabRowId", typeof(string));
                dtCmdSchemes.Columns.Add("BuyNGetnRowId", typeof(string));

                DateTime dXnDt = Convert.ToDateTime(dtCmm.Rows[0]["cm_dt"]);

                DataSet dsSchemeInfo = new DataSet();

                dsSchemeInfo.Tables.Add(tEossSchemes);
                dsSchemeInfo.Tables.Add(tEossSlabs);
                dsSchemeInfo.Tables.Add(tBcScheme);

                dsSchemeInfo.Tables[0].TableName = "schemeDetails";
                dsSchemeInfo.Tables[1].TableName = "slabDetails";
                dsSchemeInfo.Tables[2].TableName = "skuNames";


                if (dtCmdSchemes.Rows.Count == 0)
                {
                    nProcessLoop = nProcessLoop + 1;
                    goto lblProcessReturn;
                }

                retMsgFromSchemeMethod = NormalizeCmdForEoss(ref dtCmdSchemes);

                if (!string.IsNullOrEmpty(retMsgFromSchemeMethod))
                    return retMsgFromSchemeMethod;

                string cSchemeRowId;
                decimal buyFromRange, buyToRage;
                Boolean bIncrementalScheme = false;


                DataTable dtCopySchemes = new DataTable();

                // AS per discussion in meeting on 24-04-2023 ,Bar codes based Flat discount will be applied on scanning only

                DataTable dtSchemeTitles = new DataTable();
                string filterSchemes = "";
                string cEcouponId = dtCmm.Rows[0]["ecoupon_id"].ToString();
                Boolean bDollarCoupon = false;

                if (!String.IsNullOrEmpty(cEcouponId))
                {
                    char[] cEcouponIdArr = cEcouponId.ToCharArray();
                    if (cEcouponId.Length > 3)
                    {
                        if (cEcouponIdArr[2].ToString() == "$")
                            bDollarCoupon = true;
                    }

                }

                if (!bDollarCoupon)
                {
                    filterSchemes = " wizclip_based_scheme=false ";
                }

                if (bApplyFlatschemesOnly)
                    filterSchemes = filterSchemes + (String.IsNullOrEmpty(filterSchemes) ? "" : " AND ") + " buytype=1";

                filterSchemes = filterSchemes + (String.IsNullOrEmpty(filterSchemes) ? "" : " AND ") + "  schemeApplicableLevel=1";


                DataRow[] drBillLevelSchemeSearch = dsSchemeInfo.Tables[0].Select("schemeApplicableLevel=2", "");
                if (drBillLevelSchemeSearch.Length > 0)
                    bBillLevelSchemeFound = true;

                DataRow[] drSchemes = dsSchemeInfo.Tables[0].Select(filterSchemes, "");
                if (drSchemes.Length == 0)
                {
                    if (!bBillLevelSchemeFound)
                        return "";
                    else
                        goto lblNextStep;
                }

                dtSchemeTitles = dsSchemeInfo.Tables[0].Select(filterSchemes, "").CopyToDataTable();

                String cSchemeName;
                Int32 nSchemeMode, nBuyType, nGetType;
                DataTable dFSlabsDetails = new DataTable();

                DataTable dtSlabs = dsSchemeInfo.Tables["slabDetails"].Clone();

                bool bAdditionalSchemeFound = false;
                var matchingRecords = from row1 in dtSchemeTitles.Select("isnull(additionalScheme,0)=1 AND schemeapplicablelevel=1", "").AsEnumerable()
                                      select row1;
                if (matchingRecords.Any())
                    bAdditionalSchemeFound = true;

                int nMainLoop = 1;
                int nMainLoopCount = (nProcessLoop == 1 ? 2 : 1);

                DataTable dtApplySchemes = new DataTable();
                while (nMainLoop <= nMainLoopCount)
                {


                    if (nMainLoop == 2)
                    {
                        dtApplySchemes = dtSchemeTitles.Select("additionalScheme=1", "titleProcessingOrder desc").CopyToDataTable();
                        dtCmdSchemes.AsEnumerable().ToList().ForEach(r =>
                        {
                            r["scheme_applied_qty"] = 0;
                            r["scheme_applied_amount"] = r["basic_discount_amount"];
                            r["pending_scheme_apply_qty"] = r["quantity"];
                            r["pending_scheme_apply_amount"] = r["net"];
                        });
                    }
                    else
                    {
                        DataRow[] drMainSchemes = dtSchemeTitles.Select("isnull(additionalScheme,0)=0 and schemeApplicablelevel=1", "");
                        if (drMainSchemes.Length > 0)
                            dtApplySchemes = dtSchemeTitles.Select("isnull(additionalScheme,0)=0 and schemeApplicablelevel=1", "titleProcessingOrder desc").CopyToDataTable();
                        else
                            goto lblNextLevel;
                    }
                    StringBuilder sbProcessingTime = new StringBuilder();
                    sbProcessingTime.AppendLine("Start Implementing Schemes :" + DateTime.Now.ToString());
                    foreach (DataRow drTitles in dtApplySchemes.Rows)
                    {
                        cSchemeRowId = drTitles["schemeRowId"].ToString();
                        cSchemeName = drTitles["schemeName"].ToString();
                        nSchemeMode = Convert.ToInt32(drTitles["schemeMode"]);
                        sbProcessingTime.AppendLine("Start Implementing Schemes :[" + cSchemeName + "] RowID :[" +cSchemeRowId+"] "+ DateTime.Now.ToString());

                        if (nMainLoop == 1)
                        {
                            dtCmdSchemes.AsEnumerable().ToList().ForEach(dr =>
                            {
                                dr["scheme_applied_amount"] = globalMethods.ConvertDecimal(dr["scheme_applied_qty"]) * globalMethods.ConvertDecimal(dr["mrp"]);
                            });
                        }


                        if (nSchemeMode == 2)
                        {
                            //Call Flat discount Logic

                            retMsgFromSchemeMethod = applyEossFlatDiscount(ref dtCmdSchemes, drTitles, dsSchemeInfo.Tables["skuNames"], cCmdRowId, bAPPLY_FIX_MRP_EOSS_AND_BILLPRINT);
                            if (!string.IsNullOrEmpty(retMsgFromSchemeMethod))
                                return retMsgFromSchemeMethod;

                            goto lblNextScheme;
                        }


                        // Need to commit the datatable changes before applying the next scheme
                        // as there may be need to rollback the changes within Buy n Get n scheme in case of failure/mismatch
                        dtCmdSchemes.AcceptChanges();

                        string filterSlabs = "schemeRowId='" + cSchemeRowId + "'";

                        if (bApplyFlatschemesOnly)
                            filterSlabs = filterSlabs + " AND gettype=3";


                        DataRow[] drSearchSlabs = dsSchemeInfo.Tables["slabDetails"].Select(filterSlabs, "");

                        if (drSearchSlabs.Length > 0)
                            dtSlabs = dsSchemeInfo.Tables["slabDetails"].Select(filterSlabs, "").CopyToDataTable();

                        if (!dtSlabs.Columns.Contains("setValue"))
                            dtSlabs.Columns.Add("setValue", typeof(decimal));

                        dtSlabs.AsEnumerable().ToList().ForEach(r =>
                        {
                            r["setValue"] = globalMethods.ConvertDecimal(r["buyFromRange"]) + globalMethods.ConvertDecimal(r["getQty"]);
                        });

                        dtSlabs.DefaultView.Sort = "setValue desc";

                        DataTable dtSortedSlabs = dtSlabs.DefaultView.ToTable();

                        string filterTableData = "";
                        if (globalMethods.ConvertBool(drTitles["incrementalScheme"]) == true)
                            retMsgFromSchemeMethod = applyPowerPricingInc(ref dtCmdSchemes, dtSortedSlabs, drTitles, dsSchemeInfo.Tables["skuNames"]);
                        else
                        {
                            foreach (DataRow drSlabs in dtSortedSlabs.Rows)
                            {
                                nBuyType = Convert.ToInt32(drTitles["buyType"]);
                                nGetType = Convert.ToInt32(drSlabs["getType"]);
                                if (nBuyType == 2 && nGetType == 2 && globalMethods.ConvertDecimal(drSlabs["discountAmount"]) > 0)
                                    retMsgFromSchemeMethod = applyEossBnGnAmount(ref dtCmdSchemes, drSlabs, drTitles, dsSchemeInfo.Tables["skuNames"]);
                                else
                                    retMsgFromSchemeMethod = applyEossRangeBased(ref dtCmdSchemes, drSlabs, drTitles, dsSchemeInfo.Tables["skuNames"]);

                                if (!string.IsNullOrEmpty(retMsgFromSchemeMethod))
                                    goto lblLast;

                                filterTableData = " (quantity" + (/*AppConfigModel.*/EossReturnItemsProcessing ? "*-1)" : ")") +
                                "- scheme_applied_qty>0 or (mrp*quantity" + (/*AppConfigModel.*/EossReturnItemsProcessing ? "*-1)" : ")") + "-scheme_applied_amount>0";
                                DataRow[] drPendingCmd = dtCmdSchemes.Select(filterTableData, "");
                                if (drPendingCmd.Length <= 0)
                                    break;
                            }
                        }


                        if (!String.IsNullOrEmpty(retMsgFromSchemeMethod))
                            return retMsgFromSchemeMethod;

                        lblNextScheme:
                        dFSlabsDetails.Rows.Clear();
                        sbProcessingTime.AppendLine("End Implementing Schemes :[" + cSchemeName + "]  RowID :[" + cSchemeRowId + "] " + DateTime.Now.ToString());
                        // No need to proceed further if No pending bar code is left for Eoss applying
                        filterTableData = " (quantity" + (/*AppConfigModel.*/EossReturnItemsProcessing ? "*-1)" : ")") +
                            "- scheme_applied_qty>0 or (mrp*quantity" + (/*AppConfigModel.*/EossReturnItemsProcessing ? "*-1)" : ")") + "-scheme_applied_amount>0";
                        DataRow[] drPending = dtCmdSchemes.Select(filterTableData, "");
                        if (drPending.Length <= 0)
                            break;
                        
                    }
                    sbProcessingTime.AppendLine("End Implementing Schemes :" + DateTime.Now.ToString());

                    if (!String.IsNullOrEmpty(_AppPath))
                    {
                        System.IO.File.WriteAllText(_AppPath + "\\logs\\UpdateSchemeDiscounts.txt", sbProcessingTime.ToString());
                    }
                    // Check for applying Max discount in case of Return Items by comparing Eoss discount and Last sold discount
                    if (nProcessLoop == 2 && nSlrDiscountMode == 2 && !bApplyFlatschemesOnly)
                    {

                        decimal nLastSoldDiscPct, nEossDiscPct;
                        foreach (DataRow dr in dtCmdSchemes.Rows)
                        {
                            nEossDiscPct = (globalMethods.ConvertDecimal(dr["WEIGHTED_AVG_DISC_PCT"]) > 0 ? globalMethods.ConvertDecimal(dr["WEIGHTED_AVG_DISC_PCT"]) : globalMethods.ConvertDecimal(dr["basic_discount_percentage"]));
                            nLastSoldDiscPct = globalMethods.ConvertDecimal(dtCmd.Select("row_id='" + dr["row_id"] + "'".ToString())
                                            .CopyToDataTable().Rows[0]["last_sls_discount_percentage"]);

                            if (nEossDiscPct < nLastSoldDiscPct)
                            {
                                dr["scheme_name"] = "";
                                dr["slsdet_row_id"] = "";
                                dr["basic_discount_percentage"] = Math.Abs(nLastSoldDiscPct);
                                dr["basic_discount_amount"] = Math.Round(Convert.ToDecimal(dr["quantity"]) * Convert.ToDecimal(dr["mrp"]) * nLastSoldDiscPct / 100, 2);
                                dr["net"] = Math.Round((Convert.ToDecimal(dr["quantity"]) * Convert.ToDecimal(dr["mrp"])) - Convert.ToDecimal(dr["basic_discount_amount"]), 2);
                            }
                        }
                    }


                    retMsgFromSchemeMethod = NormalizeCmdForEoss(ref dtCmdSchemes, true);

                    if (!string.IsNullOrEmpty(retMsgFromSchemeMethod))
                        return retMsgFromSchemeMethod;

                    if (nItemLevelRoundOff > 0)
                    {
                        DataRow[] drSchemeRow = dtCmdSchemes.Select("scheme_name<>''", "");

                        if (drSchemeRow.Length > 0)
                            ProcessEossRoundOff(ref dtCmdSchemes, nItemLevelRoundOff);
                    }

                lblNextLevel:
                    nMainLoop++;

                    if (nMainLoop == 2 && !bAdditionalSchemeFound)
                        nMainLoop++;

                }


                nProcessLoop = nProcessLoop + 1;

                synchCmdSchemes(dtCmdSchemes, ref dtCmd, true);

            lblProcessReturn:
                if (nProcessLoop <= nItemsLoop)
                {
                    dsSchemeInfo.Tables.Clear();

                    goto lblStart;
                }

            lblNextStep:


                //Process Bill level scheme at last after applying all item level normal & additional schemes
                if (bBillLevelSchemeFound)
                {
                    dtSchemeTitles = dsSchemeInfo.Tables[0].Select("schemeApplicablelevel=2", "").CopyToDataTable();
                    dtSchemeTitles.DefaultView.Sort = "titleProcessingOrder desc";

                    DataTable dtSortedBillLevelSchemes = dtSchemeTitles.DefaultView.ToTable();
                    foreach (DataRow drScheme in dtSortedBillLevelSchemes.Rows)
                    {
                        cSchemeRowId = drScheme["schemeRowId"].ToString();
                        DataRow[] drSlabs = dsSchemeInfo.Tables["slabDetails"].Select("schemeRowId='" + cSchemeRowId + "'", "");
                        //Call Bill Level scheme of Buy More Pay Less Logic
                        if (drSlabs.Length > 0)
                        {
                            dFSlabsDetails = dsSchemeInfo.Tables["slabDetails"].Select("schemeRowId='" + cSchemeRowId + "'", "").CopyToDataTable();

                            retMsgFromSchemeMethod = applyBillLevelScheme(ref dtCmm, dtCmd, dFSlabsDetails, drScheme);

                            if (!string.IsNullOrEmpty(retMsgFromSchemeMethod))
                                goto lblLast;
                        }

                        if (bBillLevelSchemeProcessed)
                            break;

                    }
                }

                //dtCmd.AsEnumerable().ToList().ForEach(r =>
                //{
                //    r["WEIGHTED_AVG_DISC_AMT"] = (globalMethods.ConvertDecimal(r["basic_discount_amount"]) != 0 ? r["basic_discount_amount"] : r["WEIGHTED_AVG_DISC_AMT"]);
                //    r["WEIGHTED_AVG_DISC_PCT"] = (globalMethods.ConvertDecimal(r["basic_discount_amount"]) != 0 ? r["basic_discount_percentage"] : r["WEIGHTED_AVG_DISC_PCT"]);
                //});
                dtCmd.AsEnumerable().ToList().ForEach(r =>
                {
                    r["WEIGHTED_AVG_DISC_AMT"] = (globalMethods.ConvertDecimal(r["WEIGHTED_AVG_DISC_AMT"]) == 0 ? r["basic_discount_amount"] : r["WEIGHTED_AVG_DISC_AMT"]);
                    r["WEIGHTED_AVG_DISC_PCT"] = (globalMethods.ConvertDecimal(r["WEIGHTED_AVG_DISC_PCT"]) == 0 ? r["basic_discount_percentage"] : r["WEIGHTED_AVG_DISC_PCT"]);
                });
            }

            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);

                return "Error at Line#" + errLineNo.ToString() + " of UpdateSchemeDiscounts : " + ex.Message.ToString();
            }



        lblLast:


            return retMsgFromSchemeMethod;

        }
        //public string updateSchemeDiscounts(SqlConnection conn, string cLocId, ref DataTable dtCmm, ref DataTable dtCmd, DataTable tEossSchemes,
        //                                  DataTable tEossSlabs, DataTable tBcScheme, Boolean bCalledFromWizapp, int nItemLevelRoundOff, Boolean bApplyFlatschemesOnly = false, string cCmdRowId = "",
        //                                  Int32 nSlrDiscountMode = 2, bool bDonotApplyHappyHours = false)
        //{
        //    bool bBillLevelSchemeProcessed = false, bBillLevelSchemeFound = false;
        //    commonMethods globalMethods = new commonMethods();

        //    string retMsgFromSchemeMethod = "";
        //    try
        //    {
        //        int nItemsLoop = 1, nProcessLoop = 1;
        //        if (nSlrDiscountMode >= 2)
        //            nItemsLoop = 2;

        //        /*AppConfigModel.*/EossReturnItemsProcessing = false;
        //    lblStart:

        //        if (nProcessLoop == 2)
        //            /*AppConfigModel.*/EossReturnItemsProcessing = true;

        //        string cmdFilter = "";
        //        if (bApplyFlatschemesOnly)
        //            cmdFilter = " row_id ='" + cCmdRowId + "'";
        //        else
        //        {
        //            cmdFilter = " isnull(barcodebased_flatdisc_applied,false)=false and isnull(manual_discount,false)=false and isnull(manual_dp,false)=false ";

        //            if (bDonotApplyHappyHours)
        //                cmdFilter = cmdFilter + " AND isnull(happy_hours_applied,false)=false ";

        //            if (nProcessLoop == 2)
        //                cmdFilter = cmdFilter + " and quantity<0";
        //            else
        //                cmdFilter = cmdFilter + " and quantity>0";
        //        }

        //        DataView view = new DataView(dtCmd, cmdFilter, "", DataViewRowState.CurrentRows);
        //        DataTable dtCmdSchemes = view.ToTable();

        //        dtCmdSchemes.Columns.Add("scheme_applied_qty", typeof(decimal));
        //        dtCmdSchemes.Columns["scheme_applied_qty"].DefaultValue = 0;
        //        dtCmdSchemes.Columns.Add("scheme_applied_amount", typeof(decimal));
        //        dtCmdSchemes.Columns["scheme_applied_amount"].DefaultValue = 0;

        //        dtCmdSchemes.Columns.Add("pending_scheme_apply_amount", typeof(decimal));
        //        dtCmdSchemes.Columns["pending_scheme_apply_amount"].DefaultValue = 0;
        //        dtCmdSchemes.Columns.Add("pending_scheme_apply_qty", typeof(decimal));
        //        dtCmdSchemes.Columns["pending_scheme_apply_qty"].DefaultValue = 0;
        //        dtCmdSchemes.Columns.Add("addnlBnGnDiscount", typeof(bool));
        //        dtCmdSchemes.Columns.Add("cmdRowId", typeof(string));
        //        dtCmdSchemes.Columns.Add("slabRowId", typeof(string));
        //        dtCmdSchemes.Columns.Add("BuyNGetnRowId", typeof(string));

        //        DateTime dXnDt = Convert.ToDateTime(dtCmm.Rows[0]["cm_dt"]);

        //        DataSet dsSchemeInfo = new DataSet();

        //        dsSchemeInfo.Tables.Add(tEossSchemes);
        //        dsSchemeInfo.Tables.Add(tEossSlabs);
        //        dsSchemeInfo.Tables.Add(tBcScheme);

        //        dsSchemeInfo.Tables[0].TableName = "schemeDetails";
        //        dsSchemeInfo.Tables[1].TableName = "slabDetails";
        //        dsSchemeInfo.Tables[2].TableName = "skuNames";


        //        if (dtCmdSchemes.Rows.Count == 0)
        //            goto lblNextStep;


        //        retMsgFromSchemeMethod = NormalizeCmdForEoss(ref dtCmdSchemes);

        //        if (!string.IsNullOrEmpty(retMsgFromSchemeMethod))
        //            return retMsgFromSchemeMethod;

        //        string cSchemeRowId;
        //        decimal buyFromRange, buyToRage;
        //        Boolean bIncrementalScheme = false;


        //        DataTable dtCopySchemes = new DataTable();

        //        // AS per discussion in meeting on 24-04-2023 ,Bar codes based Flat discount will be applied on scanning only

        //        DataTable dtSchemeTitles = new DataTable();
        //        string filterSchemes = "";
        //        string cEcouponId = dtCmm.Rows[0]["ecoupon_id"].ToString();
        //        Boolean bDollarCoupon = false;

        //        if (!String.IsNullOrEmpty(cEcouponId))
        //        {
        //            char[] cEcouponIdArr = cEcouponId.ToCharArray();
        //            if (cEcouponId.Length > 3)
        //            {
        //                if (cEcouponIdArr[2].ToString() == "$")
        //                    bDollarCoupon = true;
        //            }

        //        }

        //        if (!bDollarCoupon)
        //        {
        //            filterSchemes = " wizclip_based_scheme=false ";
        //        }

        //        if (bApplyFlatschemesOnly)
        //            filterSchemes = filterSchemes + (String.IsNullOrEmpty(filterSchemes) ? "" : " AND ") + " buytype=1";

        //        filterSchemes = filterSchemes + (String.IsNullOrEmpty(filterSchemes) ? "" : " AND ") + "  schemeApplicableLevel=1";


        //        DataRow[] drBillLevelSchemeSearch = dsSchemeInfo.Tables[0].Select("schemeApplicableLevel=2", "");
        //        if (drBillLevelSchemeSearch.Length > 0)
        //            bBillLevelSchemeFound = true;

        //        DataRow[] drSchemes = dsSchemeInfo.Tables[0].Select(filterSchemes, "");
        //        if (drSchemes.Length == 0)
        //        {
        //            if (!bBillLevelSchemeFound)
        //                return "";
        //            else
        //                goto lblNextStep;
        //        }

        //        dtSchemeTitles = dsSchemeInfo.Tables[0].Select(filterSchemes, "").CopyToDataTable();

        //        String cSchemeName;
        //        Int32 nSchemeMode, nBuyType, nGetType;
        //        DataTable dFSlabsDetails = new DataTable();

        //        dtSchemeTitles.DefaultView.Sort = "titleProcessingOrder desc";

        //        DataTable dtSortedSchemes = dtSchemeTitles.DefaultView.ToTable();


        //        DataTable dtSlabs = dsSchemeInfo.Tables["slabDetails"].Clone();

        //        bool bAdditionalSchemeFound = false;
        //        var matchingRecords = from row1 in dtSortedSchemes.Select("isnull(additionalScheme,0)=1 AND schemeapplicablelevel=1", "").AsEnumerable()
        //                              select row1;
        //        if (matchingRecords.Any())
        //            bAdditionalSchemeFound = true;

        //        int nMainLoop = 1;
        //        int nMainLoopCount = (nProcessLoop == 1 ? 2 : 1);

        //        while (nMainLoop <= nMainLoopCount)
        //        {
        //            DataTable dtApplySchemes = dtSortedSchemes.Copy();
        //            if (nMainLoop == 2)
        //            {
        //                dtApplySchemes = dtApplySchemes.Select("additionalScheme=1", "").CopyToDataTable();
        //                dtCmdSchemes.AsEnumerable().ToList().ForEach(r =>
        //                {
        //                    r["scheme_applied_qty"] = 0;
        //                    r["scheme_applied_amount"] = r["basic_discount_amount"];
        //                    r["pending_scheme_apply_qty"] = r["quantity"];
        //                    r["pending_scheme_apply_amount"] = r["net"];
        //                });
        //            }
        //            else
        //            {
        //                dtApplySchemes = dtApplySchemes.Select("isnull(additionalScheme,0)=0", "").CopyToDataTable();
        //            }
        //            foreach (DataRow drTitles in dtApplySchemes.Rows)
        //            {

        //                cSchemeRowId = drTitles["schemeRowId"].ToString();
        //                cSchemeName = drTitles["schemeName"].ToString();
        //                nSchemeMode = Convert.ToInt32(drTitles["schemeMode"]);


        //                if (nSchemeMode == 2)
        //                {
        //                    //Call Flat discount Logic

        //                    retMsgFromSchemeMethod = applyEossFlatDiscount(ref dtCmdSchemes, drTitles, dsSchemeInfo.Tables["skuNames"], cCmdRowId);
        //                    if (!string.IsNullOrEmpty(retMsgFromSchemeMethod))
        //                        return retMsgFromSchemeMethod;

        //                    goto lblNextScheme;
        //                }


        //                // Need to commit the datatable changes before applying the next scheme
        //                // as there may be need to rollback the changes within Buy n Get n scheme in case of failure/mismatch
        //                dtCmdSchemes.AcceptChanges();

        //                string filterSlabs = "schemeRowId='" + cSchemeRowId + "'";

        //                if (bApplyFlatschemesOnly)
        //                    filterSlabs = filterSlabs + " AND gettype=3";


        //                DataRow[] drSearchSlabs = dsSchemeInfo.Tables["slabDetails"].Select(filterSlabs, "");

        //                if (drSearchSlabs.Length > 0)
        //                    dtSlabs = dsSchemeInfo.Tables["slabDetails"].Select(filterSlabs, "").CopyToDataTable();

        //                if (!dtSlabs.Columns.Contains("setValue"))
        //                    dtSlabs.Columns.Add("setValue", typeof(decimal));

        //                dtSlabs.AsEnumerable().ToList().ForEach(r =>
        //                {
        //                    r["setValue"] = globalMethods.ConvertDecimal(r["buyFromRange"]) + globalMethods.ConvertDecimal(r["getQty"]);
        //                });

        //                dtSlabs.DefaultView.Sort = "setValue desc";

        //                DataTable dtSortedSlabs = dtSlabs.DefaultView.ToTable();

        //                if (globalMethods.ConvertBool(drTitles["incrementalScheme"]) == true)
        //                    retMsgFromSchemeMethod = applyPowerPricingInc(ref dtCmdSchemes, dtSortedSlabs, drTitles, dsSchemeInfo.Tables["skuNames"]);
        //                else
        //                {
        //                    foreach (DataRow drSlabs in dtSortedSlabs.Rows)
        //                    {
        //                        nBuyType = Convert.ToInt32(drTitles["buyType"]);
        //                        nGetType = Convert.ToInt32(drSlabs["getType"]);
        //                        if (nBuyType == 2 && nGetType == 2 && globalMethods.ConvertDecimal(drSlabs["discountAmount"]) > 0)
        //                            retMsgFromSchemeMethod = applyEossBnGnAmount(ref dtCmdSchemes, drSlabs, drTitles, dsSchemeInfo.Tables["skuNames"]);
        //                        else
        //                            retMsgFromSchemeMethod = applyEossRangeBased(ref dtCmdSchemes, drSlabs, drTitles, dsSchemeInfo.Tables["skuNames"]);

        //                        if (!string.IsNullOrEmpty(retMsgFromSchemeMethod))
        //                            goto lblLast;
        //                    }
        //                }


        //                if (!String.IsNullOrEmpty(retMsgFromSchemeMethod))
        //                    return retMsgFromSchemeMethod;

        //                lblNextScheme:
        //                dFSlabsDetails.Rows.Clear();

        //                // No need to proceed further if No pending bar code is left for Eoss applying
        //                string filterTableData = " (quantity" + (/*AppConfigModel.*/EossReturnItemsProcessing ? "*-1)" : ")") +
        //                    "- scheme_applied_qty>0 or (mrp*quantity" + (/*AppConfigModel.*/EossReturnItemsProcessing ? "*-1)" : ")") + "-scheme_applied_amount>0";
        //                DataRow[] drPending = dtCmdSchemes.Select(filterTableData, "");
        //                if (drPending.Length <= 0)
        //                    break;
        //            }


        //            // Check for applying Max discount in case of Return Items by comparing Eoss discount and Last sold discount
        //            if (nProcessLoop == 2 && nSlrDiscountMode == 2 && !bApplyFlatschemesOnly)
        //            {

        //                decimal nLastSoldDiscPct, nEossDiscPct;
        //                foreach (DataRow dr in dtCmdSchemes.Rows)
        //                {
        //                    nEossDiscPct = (globalMethods.ConvertDecimal(dr["WEIGHTED_AVG_DISC_PCT"]) > 0 ? globalMethods.ConvertDecimal(dr["WEIGHTED_AVG_DISC_PCT"]) : globalMethods.ConvertDecimal(dr["basic_discount_percentage"]));
        //                    nLastSoldDiscPct = globalMethods.ConvertDecimal(dtCmd.Select("row_id='" + dr["row_id"] + "'".ToString())
        //                                    .CopyToDataTable().Rows[0]["last_sls_discount_percentage"]);

        //                    if (nEossDiscPct < nLastSoldDiscPct)
        //                    {
        //                        dr["scheme_name"] = "";
        //                        dr["slsdet_row_id"] = "";
        //                        dr["basic_discount_percentage"] = Math.Abs(nLastSoldDiscPct);
        //                        dr["basic_discount_amount"] = Math.Round(Convert.ToDecimal(dr["quantity"]) * Convert.ToDecimal(dr["mrp"]) * nLastSoldDiscPct / 100, 2);
        //                        dr["net"] = Math.Round((Convert.ToDecimal(dr["quantity"]) * Convert.ToDecimal(dr["mrp"])) - Convert.ToDecimal(dr["basic_discount_amount"]), 2);
        //                    }
        //                }
        //            }


        //            retMsgFromSchemeMethod = NormalizeCmdForEoss(ref dtCmdSchemes, true);

        //            if (!string.IsNullOrEmpty(retMsgFromSchemeMethod))
        //                return retMsgFromSchemeMethod;

        //            if (nItemLevelRoundOff > 0)
        //            {
        //                DataRow[] drSchemeRow = dtCmdSchemes.Select("scheme_name<>''", "");

        //                if (drSchemeRow.Length > 0)
        //                    ProcessEossRoundOff(ref dtCmdSchemes, nItemLevelRoundOff);
        //            }

        //            nMainLoop++;

        //            if (nMainLoop == 2 && !bAdditionalSchemeFound)
        //                nMainLoop++;

        //        }


        //        nProcessLoop = nProcessLoop + 1;

        //        synchCmdSchemes(dtCmdSchemes, ref dtCmd, true);

        //        if (nProcessLoop <= nItemsLoop)
        //        {
        //            dsSchemeInfo.Tables.Clear();

        //            goto lblStart;
        //        }

        //    lblNextStep:
        //        //Process Bill level scheme at last after applying all item level normal & additional schemes
        //        if (bBillLevelSchemeFound)
        //        {
        //            dtSchemeTitles = dsSchemeInfo.Tables[0].Select("schemeApplicablelevel=2", "").CopyToDataTable();
        //            dtSchemeTitles.DefaultView.Sort = "titleProcessingOrder desc";

        //            DataTable dtSortedBillLevelSchemes = dtSchemeTitles.DefaultView.ToTable();
        //            foreach (DataRow drScheme in dtSortedBillLevelSchemes.Rows)
        //            {
        //                cSchemeRowId = drScheme["schemeRowId"].ToString();
        //                DataRow[] drSlabs = dsSchemeInfo.Tables["slabDetails"].Select("schemeRowId='" + cSchemeRowId + "'", "");
        //                //Call Bill Level scheme of Buy More Pay Less Logic
        //                if (drSlabs.Length > 0)
        //                {
        //                    dFSlabsDetails = dsSchemeInfo.Tables["slabDetails"].Select("schemeRowId='" + cSchemeRowId + "'", "").CopyToDataTable();

        //                    retMsgFromSchemeMethod = applyBillLevelScheme(ref dtCmm, dtCmd, dFSlabsDetails, drScheme);

        //                    if (!string.IsNullOrEmpty(retMsgFromSchemeMethod))
        //                        goto lblLast;
        //                }

        //                if (bBillLevelSchemeProcessed)
        //                    break;

        //            }
        //        }

        //        dtCmd.AsEnumerable().ToList().ForEach(r =>
        //        {
        //            r["WEIGHTED_AVG_DISC_AMT"] = (globalMethods.ConvertDecimal(r["WEIGHTED_AVG_DISC_AMT"]) == 0 ? r["basic_discount_amount"] : r["WEIGHTED_AVG_DISC_AMT"]);
        //            r["WEIGHTED_AVG_DISC_PCT"] = (globalMethods.ConvertDecimal(r["WEIGHTED_AVG_DISC_PCT"]) == 0 ? r["basic_discount_percentage"] : r["WEIGHTED_AVG_DISC_PCT"]);
        //        });

        //    }

        //    catch (Exception ex)
        //    {
        //        int errLineNo = new commonMethods().GetErrorLineNo(ex);

        //        return "Error at Line#" + errLineNo.ToString() + " of UpdateSchemeDiscounts : " + ex.Message.ToString();
        //    }



        //lblLast:


        //    return retMsgFromSchemeMethod;

        //}

        private string applyEossRangeBased(ref DataTable dtCmdSchemes, DataRow drSlabs, DataRow drSchemeDet, DataTable dtSkuNames)
        {

            decimal nSchemeBuyValue, nSchemeToRange, nSchemeGetValue;
            Boolean bSchemeApplied = false;
            int nBuyType, nGetType;
            decimal nSetQty = 0;
            string retMsg = "";
            commonMethods globalMethods = new commonMethods();

            try
            {
                string cSchemeName = drSchemeDet["schemeName"].ToString();
                string cSchemeRowId = drSchemeDet["schemeRowId"].ToString();


                Decimal nQty;
                // We need to do this so that we can process the scheme on the items ordered on pending scheme qty/amount desc
                foreach (DataRow dr in dtCmdSchemes.Rows)
                {
                    nQty = Math.Abs(globalMethods.ConvertDecimal(dr["quantity"])) - Math.Abs(globalMethods.ConvertDecimal(dr["scheme_applied_qty"]));
                    dr["pending_scheme_apply_qty"] = nQty;
                    dr["pending_scheme_apply_amount"] = (globalMethods.ConvertDecimal(dr["mrp"]) * Math.Abs(globalMethods.ConvertDecimal(dr["quantity"]))) - globalMethods.ConvertDecimal(dr["scheme_applied_amount"]);
                }

                dtCmdSchemes.AcceptChanges();

                string cSlabRowId = drSlabs["rowId"].ToString();

                DataRow[] drSkuNamesBuy = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "' and buybc=1", "");
                DataRow[] drSkuNamesGet = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "' and getbc=1", "");

                nBuyType = Convert.ToInt32(drSchemeDet["buyType"]);
                nGetType = Convert.ToInt32(drSlabs["getType"]);

                nSchemeBuyValue = globalMethods.ConvertDecimal(drSlabs["buyFromRange"]);
                nSchemeToRange = globalMethods.ConvertDecimal(drSlabs["buyToRange"]);
                if (nGetType == 2)
                    nSchemeGetValue = globalMethods.ConvertDecimal(drSlabs["discountAmount"]);
                else
                    nSchemeGetValue = globalMethods.ConvertDecimal(drSlabs["getQty"]);


                lblReProcess:

                Decimal BuyItemsTotal = 0, GetItemsTotal = 0, nNetValue, nSchemeAppliedQty = 0, nLoopValue = 0, nAddValue = 0,
                nMrp, nDiscountFigure, nDiscountAmount, nDiscountPercentage, nSchemeAppliedAmount = 0;


                nQty = 0;
                string cProductCode;

                int nDiscMethod;

                DataTable dtFilteredCmdBuy = dtCmdSchemes.Clone();

                DataTable dtFilteredCmdGet = dtCmdSchemes.Clone();

                string cBuynGetnRowId = "";

                if (drSkuNamesBuy.Length == 0 || (drSkuNamesGet.Length == 0 && nGetType != 3))
                    goto lblUpdateWtdDisc;

                if (nGetType != 3)
                    cBuynGetnRowId = Guid.NewGuid().ToString();

                string filterTableData;

                DataTable dtSkuNamesBuy = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "' and buybc=1", "").CopyToDataTable();

                filterTableData = (nBuyType == 1 ? " pending_scheme_apply_qty>0 " : " pending_scheme_apply_amount>0 ");

                string cMessage = globalMethods.JoinDataTables(dtCmdSchemes, dtSkuNamesBuy, ref dtFilteredCmdBuy, filterTableData,
                (row1, row2) =>
                row1.Field<String>("product_code") == row2.Field<String>("product_code"));

                if (!String.IsNullOrEmpty(cMessage))
                    return cMessage;

                if (dtFilteredCmdBuy.Rows.Count == 0)
                    goto lblUpdateWtdDisc;

                decimal nLoop = 0, nBaseQtyorAmount = 0, nAppliedValue = 0;
                nLoopValue = 0;

                bool bAPplyWtdDiscount = false;

                DataTable dtFilteredCmdBuyOrdered = new DataTable();

                string cOrderColumn = (nBuyType == 1 ? "mrp" : "pending_scheme_apply_amount");

                dtFilteredCmdBuy.DefaultView.Sort = cOrderColumn + " DESC "; //  string.Format("{0} {1}", cOrderColumn, "DES"); //sort descending

                dtFilteredCmdBuyOrdered = dtFilteredCmdBuy.DefaultView.ToTable();


                if (globalMethods.ConvertDecimal(drSlabs["discountAmount"]) != 0 && nGetType == 3)
                    bAPplyWtdDiscount = true;

                dtFilteredCmdBuyOrdered.Columns.Add("WtdDiscountBaseValue", typeof(decimal));

                string cItemCode = "";
                foreach (DataRow drDetail in dtFilteredCmdBuyOrdered.Rows)
                {
                    if (nBuyType == 1)
                        nBaseQtyorAmount = globalMethods.ConvertDecimal(drDetail["pending_scheme_apply_qty"]);
                    else
                        nBaseQtyorAmount = globalMethods.ConvertDecimal(drDetail["pending_scheme_apply_amount"]);


                    cItemCode = drDetail["product_code"].ToString();

                    if (nGetType == 3)
                    {
                        string cErr = "";
                        nAppliedValue = Math.Abs(((nLoopValue + nBaseQtyorAmount) > nSchemeToRange ? (nSchemeToRange - nLoopValue) : nBaseQtyorAmount));

                        if (!bAPplyWtdDiscount)
                            cErr = applyDiscountasPerMethod(drDetail, cSchemeRowId, globalMethods.ConvertDecimal(drSlabs["discountPercentage"]),
                            globalMethods.ConvertDecimal(drSlabs["discountAmount"]), globalMethods.ConvertDecimal(drSlabs["netPrice"]));
                        else
                            drDetail["WtdDiscountBaseValue"] = (nBuyType == 1 ? nBaseQtyorAmount * Convert.ToDecimal(drDetail["mrp"]) : nBaseQtyorAmount);

                        if (!string.IsNullOrEmpty(cErr))
                            return cErr;
                    }
                    else
                    {
                        nAppliedValue = Math.Abs(((nLoopValue + nBaseQtyorAmount) > nSchemeBuyValue ? (nSchemeBuyValue - nLoopValue) : nBaseQtyorAmount));
                    }

                    if (nBuyType == 1)
                    {
                        drDetail["scheme_applied_qty"] = globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]) + nAppliedValue;
                    }
                    else
                    {
                        drDetail["scheme_applied_qty"] = Math.Abs(Convert.ToDecimal(drDetail["quantity"]));
                    }

                    if (bAPplyWtdDiscount || nGetType != 3)
                        drDetail["scheme_applied_amount"] = globalMethods.ConvertDecimal(drDetail["mrp"]) * globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]);

                    if (globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]) > Math.Abs(globalMethods.ConvertDecimal(drDetail["quantity"])))
                    {
                        retMsg = "Scheme applied quantity going more than quantity";
                        goto lblEnd;
                    }

                    decimal nAppliedSchemeAmount = globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]);
                    decimal nBaseMrpValue = globalMethods.ConvertDecimal(drDetail["mrp"]) * globalMethods.ConvertDecimal(drDetail["quantity"]);

                    if (nAppliedSchemeAmount > Math.Abs(nBaseMrpValue))
                    {
                        retMsg = "Scheme applied amount :" + nAppliedSchemeAmount.ToString() + " going more than mrp value :" + nBaseMrpValue.ToString();
                        goto lblEnd;
                    }

                    drDetail["pending_scheme_apply_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["quantity"])) - globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]);
                    drDetail["pending_scheme_apply_amount"] = (Math.Abs(globalMethods.ConvertDecimal(drDetail["quantity"]) * globalMethods.ConvertDecimal(drDetail["mrp"])) -
                        globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]));

                    drDetail["buynGetnRowId"] = cBuynGetnRowId;
                    nLoopValue = nLoopValue + nAppliedValue;


                    if (!drDetail["scheme_name"].ToString().Contains(cSchemeName))
                    {
                        drDetail["scheme_name"] = drDetail["scheme_name"] + (string.IsNullOrEmpty(drDetail["scheme_name"].ToString()) ? "" : ",") +
                        drSchemeDet["schemeName"].ToString();
                    }

                    drDetail["slsdet_row_id"] = cSchemeRowId;
                    drDetail["slabRowId"] = cSlabRowId;
                    if ((nLoopValue >= nSchemeBuyValue && nGetType != 3) || (nLoopValue >= nSchemeToRange && nGetType == 3))
                        break;
                }

                if (nLoopValue < nSchemeBuyValue)
                    goto lblUpdateWtdDisc;


                if (bAPplyWtdDiscount)
                {

                    cMessage = DistributeWtdDiscount(ref dtFilteredCmdBuyOrdered, globalMethods.ConvertDecimal(drSlabs["discountAmount"]));
                    if (!string.IsNullOrEmpty(cMessage))
                        return cMessage;
                }

                synchCmdSchemes(dtFilteredCmdBuyOrdered, ref dtCmdSchemes);

                if (nGetType == 3)
                {
                    bSchemeApplied = true;
                    goto lblUpdateWtdDisc;
                }
                DataTable dtSkuNamesGet = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "' and getbc=1", "").CopyToDataTable();

                filterTableData = (nGetType == 1 ? " pending_scheme_apply_qty>0 " : " scheme_applied_amount=0 ");

                cMessage = globalMethods.JoinDataTables(dtCmdSchemes, dtSkuNamesGet, ref dtFilteredCmdGet, filterTableData,
                (row1, row2) =>
                row1.Field<String>("product_code") == row2.Field<String>("product_code"));

                if (!String.IsNullOrEmpty(cMessage) || dtFilteredCmdGet.Rows.Count == 0)
                {
                    dtCmdSchemes.RejectChanges();

                    if (!String.IsNullOrEmpty(cMessage))
                        return cMessage;

                    goto lblUpdateWtdDisc;
                }

                //Populate dt here
                DataTable dtFilteredCmdGetOrdered = new DataTable();

                dtFilteredCmdGet.DefaultView.Sort = (nGetType == 1 ? "mrp DESC,pending_scheme_apply_qty ASC " : "pending_scheme_apply_amount"); //  string.Format("{0} {1}", cOrderColumn, "DES"); //sort descending

                dtFilteredCmdGetOrdered = dtFilteredCmdGet.DefaultView.ToTable();

                nLoopValue = 0;

                decimal nBuySchAppliedQty = 0, nBuySchAppliedAmt = 0;
                foreach (DataRow drDetail in dtFilteredCmdGetOrdered.Rows)
                {

                    nBuySchAppliedQty = globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]);
                    nBuySchAppliedAmt = globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]);

                    if (nGetType == 1)
                        nBaseQtyorAmount = globalMethods.ConvertDecimal(drDetail["pending_scheme_apply_qty"]);
                    else
                        nBaseQtyorAmount = globalMethods.ConvertDecimal(drDetail["pending_scheme_apply_amount"]);

                    if (nBaseQtyorAmount == 0)
                        continue;

                    nAppliedValue = Math.Abs(((nLoopValue + nBaseQtyorAmount) > nSchemeGetValue ? (nSchemeGetValue - nLoopValue) : nBaseQtyorAmount));

                    cSchemeRowId = drSchemeDet["schemeRowId"].ToString();

                    string cErr = applyDiscountasPerMethod(drDetail, cSchemeRowId, globalMethods.ConvertDecimal(drSlabs["discountPercentage"]),
                        globalMethods.ConvertDecimal(drSlabs["discountAmount"]), globalMethods.ConvertDecimal(drSlabs["netPrice"]),
                        (nGetType == 1 ? nAppliedValue : 1));

                    if (!string.IsNullOrEmpty(cErr))
                        return cErr;

                    if (nGetType == 1)
                        drDetail["scheme_applied_qty"] = globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]) + nAppliedValue;

                    drDetail["pending_scheme_apply_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["quantity"])) - globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]);
                    drDetail["pending_scheme_apply_amount"] = (Math.Abs(globalMethods.ConvertDecimal(drDetail["quantity"])) *
                        globalMethods.ConvertDecimal(drDetail["mrp"])) - globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]);

                    nLoopValue = nLoopValue + nAppliedValue;

                    if (!drDetail["scheme_name"].ToString().Contains(cSchemeName))
                    {
                        drDetail["scheme_name"] = drDetail["scheme_name"] + (string.IsNullOrEmpty(drDetail["scheme_name"].ToString()) ? "" : ",") +
                        drSchemeDet["schemeName"].ToString();
                    }

                    drDetail["buynGetnRowId"] = cBuynGetnRowId;
                    drDetail["slsdet_row_id"] = cSchemeRowId;
                    drDetail["slabRowId"] = cSlabRowId;
                    if (nLoopValue >= nSchemeGetValue)
                        break;

                }

                if (nLoopValue < nSchemeGetValue && nGetType == 1)
                {
                    dtCmdSchemes.RejectChanges();
                    goto lblUpdateWtdDisc;
                }


                bSchemeApplied = true;
                synchCmdSchemes(dtFilteredCmdGetOrdered, ref dtCmdSchemes);


                if (globalMethods.ConvertDecimal(drSlabs["addnlGetQty"]) > 0)
                {
                    retMsg = ProcessBnGnAddnlDiscounts(cSchemeRowId, cSchemeName, ref dtCmdSchemes, drSlabs, dtSkuNames);
                    if (!String.IsNullOrEmpty(retMsg))
                        goto lblEnd;
                }

                // Commit changes in cmd for scheme applied  for current set
                dtCmdSchemes.AcceptChanges();

                goto lblReProcess;

            lblUpdateWtdDisc:

                if (nGetType != 3)
                {
                    // If BnGn Qty based scheme is not applied by following the chronology of Buy items first and then Get Items
                    // Then we need to reverse the processing order by fetching get items first and then look for buy Items
                    if (!bSchemeApplied)// && nGetType==1 && nBuyType==1)
                    {
                        decimal nPendingValue = 0;

                        if (nBuyType == 1)
                            nPendingValue = Convert.ToDecimal(dtCmdSchemes.Compute("sum(pending_scheme_apply_qty)", ""));
                        else
                            nPendingValue = Convert.ToDecimal(dtCmdSchemes.Compute("sum(pending_scheme_apply_amount)", ""));

                        if (globalMethods.ConvertInt(drSlabs["setValue"]) <= nPendingValue)
                        {
                            string retMsgFromSchemeMethod = applyEossBnGnSpl(ref dtCmdSchemes, drSlabs, drSchemeDet, dtSkuNames);
                            if (!string.IsNullOrEmpty(retMsgFromSchemeMethod))
                                return retMsgFromSchemeMethod;
                        }
                    }

                    // Calculate  Weighted avg discounts in all items which are part of Buy n Get n schemes
                    else
                    {
                        bSchemeApplied = false;
                        if (!globalMethods.ConvertBool(drSchemeDet["additionalScheme"]))
                        {
                            retMsg = UpdateBNGNWtdAvgDisc(ref dtCmdSchemes, cSlabRowId, globalMethods.ConvertBool(drSchemeDet["donot_distribute_weighted_avg_disc_bngn"]));
                            if (!String.IsNullOrEmpty(retMsg))
                                goto lblEnd;
                        }
                    }
                }
                else if (nGetType == 3 && bSchemeApplied)
                {
                    bSchemeApplied = false;
                    goto lblReProcess;
                }
                //Slab loop ends here



            }

            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);
                retMsg = "Error in APplying Buy n Get n at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();

            }

        lblEnd:

            return retMsg;

        }
        private string NormalizeCmdForEoss(ref DataTable dtCmdSchemes, bool deNormalize = false)
        {

            try
            {
                int nRowNo = -1;

                commonMethods globalMethods = new commonMethods();

                DataRow[] drSearch;
                if (deNormalize)
                {
                    drSearch = dtCmdSchemes.Select("row_id<>cmdRowId", "");

                    if (drSearch.Length == 0)
                        return "";

                    DataTable dtCmdClubbed = dtCmdSchemes.Clone(); // view.ToTable();


                    foreach (DataRow dr in dtCmdSchemes.Rows)
                    {
                        if (dr["row_id"] != dr["cmdRowId"])
                        {
                            drSearch = dtCmdClubbed.Select("row_id='" + dr["cmdRowId"].ToString() + "'", "");
                            if (drSearch.Length > 0)
                            {
                                dtCmdClubbed.Select("row_id='" + dr["cmdRowId"].ToString() + "'", "").AsEnumerable().ToList().ForEach(drClubbed =>
                                {
                                    drClubbed["quantity"] = globalMethods.ConvertDecimal(drClubbed["quantity"]) + globalMethods.ConvertDecimal(dr["quantity"]);
                                    drClubbed["basic_discount_amount"] = globalMethods.ConvertDecimal(drClubbed["basic_discount_amount"]) +
                                    globalMethods.ConvertDecimal(dr["basic_discount_amount"]);
                                    drClubbed["WEIGHTED_AVG_DISC_AMT"] = globalMethods.ConvertDecimal(drClubbed["WEIGHTED_AVG_DISC_AMT"]) +
                                    globalMethods.ConvertDecimal(dr["WEIGHTED_AVG_DISC_AMT"]);

                                    drClubbed["DISCOUNT_AMOUNT"] = globalMethods.ConvertDecimal(drClubbed["BASIC_DISCOUNT_AMOUNT"]) +
                                    globalMethods.ConvertDecimal(drClubbed["CARD_DISCOUNT_AMOUNT"]);

                                    drClubbed["scheme_applied_qty"] = globalMethods.ConvertDecimal(drClubbed["scheme_applied_qty"]) +
                                    globalMethods.ConvertDecimal(dr["scheme_applied_qty"]);

                                    drClubbed["scheme_applied_amount"] = globalMethods.ConvertDecimal(drClubbed["scheme_applied_amount"]) +
                                    globalMethods.ConvertDecimal(dr["scheme_applied_amount"]);

                                });
                            }

                            else
                            {
                                nRowNo = nRowNo + 1;
                                dtCmdClubbed.ImportRow(dr);
                                dtCmdClubbed.Rows[nRowNo]["row_id"] = dr["cmdRowId"];
                            }

                            dr["row_id"] = "";
                        }



                    }

                    foreach (DataRow dr in dtCmdClubbed.Rows)
                    {
                        if (!string.IsNullOrEmpty(dr["scheme_name"].ToString()))
                        {
                            dr["basic_discount_percentage"] = Math.Abs(Math.Round((globalMethods.ConvertDecimal(dr["basic_discount_amount"]) / (globalMethods.ConvertDecimal(dr["quantity"]) *
                                                              globalMethods.ConvertDecimal(dr["mrp"])) * 100), 3));

                            if (globalMethods.ConvertDecimal(dr["WEIGHTED_AVG_DISC_AMT"]) != 0)
                                dr["WEIGHTED_AVG_DISC_PCT"] = Math.Round((globalMethods.ConvertDecimal(dr["WEIGHTED_AVG_DISC_AMT"]) / (globalMethods.ConvertDecimal(dr["quantity"]) *
                                                                  globalMethods.ConvertDecimal(dr["mrp"])) * 100), 3);

                            dr["DISCOUNT_PERCENTAGE"] = Math.Round((globalMethods.ConvertDecimal(dr["DISCOUNT_AMOUNT"]) / (globalMethods.ConvertDecimal(dr["mrp"]) * globalMethods.ConvertDecimal(dr["quantity"]))) * 100, 3);

                        }

                        dr["net"] = (globalMethods.ConvertDecimal(dr["mrp"]) * globalMethods.ConvertDecimal(dr["quantity"])) - globalMethods.ConvertDecimal(dr["discount_amount"]);


                        dtCmdSchemes.ImportRow(dr);
                    }

                    DataRow[] rowsToDelete = dtCmdSchemes.Select("row_id=''", "");

                    foreach (DataRow dr in rowsToDelete)
                        dtCmdSchemes.Rows.Remove(dr);


                    return "";
                }

                DataTable dtCmdSchemesNormalized = dtCmdSchemes.Clone();

                string cOrgRowId = "";
                decimal nCmdQty, nLoopQty;
                nRowNo = -1;
                // Need to do this because of Null exception error while applying sum on these columns
                // Also reset all previous auto Eoss discounts if User again saves the bill after Eoss being applied
                foreach (DataRow dr in dtCmdSchemes.Rows)
                {
                    if (globalMethods.ConvertBool(dr["manual_discount"]) || globalMethods.ConvertBool(dr["manual_dp"]))
                        continue;
                    dr["basic_discount_percentage"] = 0;
                    dr["basic_discount_amount"] = 0;
                    dr["discount_percentage"] = 0;
                    dr["discount_amount"] = 0;
                    dr["WEIGHTED_AVG_DISC_AMT"] = 0;
                    dr["WEIGHTED_AVG_DISC_PCT"] = 0;
                    dr["net"] = globalMethods.ConvertDecimal(dr["mrp"]) * globalMethods.ConvertDecimal(dr["quantity"]);
                    dr["scheme_name"] = "";
                    dr["scheme_applied_qty"] = 0;
                    dr["scheme_applied_amount"] = 0;
                    dr["pending_scheme_apply_qty"] = 0;
                    dr["pending_scheme_apply_amount"] = 0;
                    dr["addnlBnGnDiscount"] = false;
                    dr["cmdRowId"] = dr["row_id"];
                    if (globalMethods.ConvertDecimal(dr["quantity"]) > 1)
                    {
                        cOrgRowId = dr["row_id"].ToString();
                        nCmdQty = globalMethods.ConvertDecimal(dr["quantity"]);
                        nLoopQty = Math.Abs(nCmdQty);
                        while (nLoopQty > 0)
                        {
                            nRowNo = nRowNo + 1;
                            //dtCmdSchemesNormalized.Rows.Add(dr);

                            dtCmdSchemesNormalized.ImportRow(dr);

                            dtCmdSchemesNormalized.Rows[nRowNo]["quantity"] = (nLoopQty >= 1 ? 1 : nLoopQty) * (nCmdQty >= 1 ? 1 : nCmdQty);
                            dtCmdSchemesNormalized.Rows[nRowNo]["net"] = globalMethods.ConvertDecimal(dr["mrp"]) * (nCmdQty >= 1 ? 1 : nCmdQty);
                            dtCmdSchemesNormalized.Rows[nRowNo]["row_id"] = Guid.NewGuid().ToString();
                            nLoopQty = nLoopQty - 1;
                        }
                        dr["row_id"] = "";
                    }

                }


                if (dtCmdSchemesNormalized.Rows.Count > 0)
                {
                    foreach (DataRow dr in dtCmdSchemesNormalized.Rows)
                        dtCmdSchemes.ImportRow(dr);

                    DataRow[] rowsToDelete = dtCmdSchemes.Select("row_id=''", "");

                    foreach (DataRow dr in rowsToDelete)
                        dtCmdSchemes.Rows.Remove(dr);
                }
            }

            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);
                string msg = "Error in Normalizing Cmd for EOSS at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();

                return msg;
            }

            return "";
        }
        private string DistributeWtdDiscount(ref DataTable dtBuyItems, decimal nDiscountAmount)
        {
            commonMethods globalMethods = new commonMethods();
            try
            {

                decimal nPrevDiscount = globalMethods.ConvertDecimal(dtBuyItems.Select("isnull(WtdDiscountBaseValue,0)>0", "").CopyToDataTable().Compute("sum(basic_discount_amount)", ""));
                //decimal nMrpValue = globalMethods.ConvertDecimal(dtBuyItems.Select("isnull(WtdDiscountBaseValue,0)>0", "").CopyToDataTable().Compute("sum(net)", ""));
                decimal nMrpValue = 0;
                foreach (DataRow dr in dtBuyItems.Select("isnull(WtdDiscountBaseValue,0)>0", ""))
                {
                    nMrpValue += (globalMethods.ConvertDecimal(dr["MRP"]) * globalMethods.ConvertDecimal(dr["quantity"]));
                }

                dtBuyItems.AsEnumerable().ToList().ForEach(r =>
                {
                    if (globalMethods.ConvertDecimal(r["WtdDiscountBaseValue"]) > 0)
                    {
                        r["basic_discount_amount"] = globalMethods.ConvertDecimal(r["basic_discount_amount"]) + Math.Round((nDiscountAmount / nMrpValue) *
                        Convert.ToDecimal(r["WtdDiscountBaseValue"]), 2);

                        r["scheme_applied_amount"] = globalMethods.ConvertDecimal(r["mrp"]) * Math.Abs(globalMethods.ConvertDecimal(r["quantity"]));

                        r["pending_scheme_apply_amount"] = (Math.Abs((globalMethods.ConvertDecimal(r["quantity"])) * globalMethods.ConvertDecimal(r["mrp"])) -
                        globalMethods.ConvertDecimal(r["scheme_applied_amount"]));

                        r["basic_discount_percentage"] = Math.Abs(Math.Round((globalMethods.ConvertDecimal(r["basic_discount_amount"]) / (globalMethods.ConvertDecimal(r["mrp"]) *
                            globalMethods.ConvertDecimal(r["quantity"]))) * 100, 3));

                        r["net"] = ((globalMethods.ConvertDecimal(r["mrp"]) * globalMethods.ConvertDecimal(r["quantity"])) -
                        globalMethods.ConvertDecimal(r["basic_discount_amount"]));
                    }
                }
                );

                decimal nCalcDiscount = globalMethods.ConvertDecimal(dtBuyItems.Select("isnull(WtdDiscountBaseValue,0)>0", "").CopyToDataTable().Compute("sum(basic_discount_amount)", ""));
                nCalcDiscount = Math.Abs(nCalcDiscount);
                if (nCalcDiscount < (nDiscountAmount + nPrevDiscount))
                {
                    //string cRowId = dtBuyItems.Select("isnull(WtdDiscountBaseValue,0)>0", "").CopyToDataTable().Rows[0]["row_id"].ToString();
                    //dtBuyItems.Select("row_id='" + cRowId + "'", "").AsEnumerable().ToList().ForEach(r =>
                    //{
                    //    r["basic_discount_amount"] = Convert.ToDecimal(r["basic_discount_amount"]) +
                    //    (nDiscountAmount + nPrevDiscount - nCalcDiscount) * (Convert.ToDecimal(r["quantity"]) < 0 ? -1 : 1);

                    //    r["basic_discount_percentage"] = Math.Abs(Math.Round((globalMethods.ConvertDecimal(r["basic_discount_amount"]) / (globalMethods.ConvertDecimal(r["mrp"]) *
                    //        globalMethods.ConvertDecimal(r["quantity"]))) * 100, 3));
                    //});

                    //string cRowId = dtBuyItems.Select("isnull(WtdDiscountBaseValue,0)>0", "").CopyToDataTable().Rows[0]["row_id"].ToString();
                    Decimal nCalcDiscount1 = (nDiscountAmount + nPrevDiscount) - nCalcDiscount;
                    foreach (DataRow r in dtBuyItems.Select("", "net desc"))
                    {
                        if (Math.Abs(nCalcDiscount1) <= 0) break;
                        if (nCalcDiscount1 > Convert.ToDecimal(r["net"]))
                        {
                            r["basic_discount_amount"] = Convert.ToDecimal(r["basic_discount_amount"]) + Convert.ToDecimal(r["net"]);
                            nCalcDiscount1 = nCalcDiscount1 - Convert.ToDecimal(r["net"]);
                        }
                        else
                        {
                            r["basic_discount_amount"] = Convert.ToDecimal(r["basic_discount_amount"]) +
                            (nCalcDiscount1) * (Convert.ToDecimal(r["quantity"]) < 0 ? -1 : 1);
                            nCalcDiscount1 = 0;
                        }

                        r["basic_discount_percentage"] = Math.Abs(Math.Round((globalMethods.ConvertDecimal(r["basic_discount_amount"]) / (globalMethods.ConvertDecimal(r["mrp"]) *
                            globalMethods.ConvertDecimal(r["quantity"]))) * 100, 3));

                        
                    }
                }
            }

            catch (Exception ex)
            {
                int errLineNo = globalMethods.GetErrorLineNo(ex);
                string msg = "Error in DistributeWtdDiscount at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();

                return msg;

            }
            return "";
        }
        //private string DistributeWtdDiscount(ref DataTable dtBuyItems, decimal nDiscountAmount)
        //{
        //    commonMethods globalMethods = new commonMethods();
        //    try
        //    {
        //        decimal nMrpValue = globalMethods.ConvertDecimal(dtBuyItems.Select("isnull(WtdDiscountBaseValue,0)>0", "").CopyToDataTable().Compute("sum(net)", ""));

        //        dtBuyItems.AsEnumerable().ToList().ForEach(r =>
        //        {
        //            if (globalMethods.ConvertDecimal(r["WtdDiscountBaseValue"]) > 0)
        //            {
        //                r["basic_discount_amount"] = globalMethods.ConvertDecimal(r["basic_discount_amount"]) + Math.Round((nDiscountAmount / nMrpValue) *
        //                globalMethods.ConvertDecimal(r["WtdDiscountBaseValue"]), 2);
        //                r["basic_discount_percentage"] = Math.Abs(Math.Round((globalMethods.ConvertDecimal(r["basic_discount_amount"]) / (globalMethods.ConvertDecimal(r["mrp"]) *
        //                    globalMethods.ConvertDecimal(r["quantity"]))) * 100, 3));

        //                r["net"] = ((globalMethods.ConvertDecimal(r["mrp"]) * globalMethods.ConvertDecimal(r["quantity"])) -
        //                globalMethods.ConvertDecimal(r["basic_discount_amount"]));
        //            }
        //        }
        //        );

        //        decimal nCalcDiscount = globalMethods.ConvertDecimal(dtBuyItems.Select("isnull(WtdDiscountBaseValue,0)>0", "").CopyToDataTable().Compute("sum(basic_discount_amount)", ""));
        //        if (nCalcDiscount != nDiscountAmount)
        //        {
        //            string cRowId = dtBuyItems.Select("isnull(WtdDiscountBaseValue,0)>0", "").CopyToDataTable().Rows[0]["row_id"].ToString();
        //            dtBuyItems.Select("row_id='" + cRowId + "'", "").AsEnumerable().ToList().ForEach(r =>
        //            {
        //                r["basic_discount_amount"] = globalMethods.ConvertDecimal(r["basic_discount_amount"]) + nDiscountAmount - nCalcDiscount;
        //            });
        //        }
        //    }

        //    catch (Exception ex)
        //    {
        //        int errLineNo = globalMethods.GetErrorLineNo(ex);
        //        string msg = "Error in DistributeWtdDiscount at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();

        //        return msg;

        //    }
        //    return "";
        //}
        private string applyEossBnGnAmount(ref DataTable dtCmdSchemes, DataRow drSlabs, DataRow drSchemeDet, DataTable dtSkuNames)
        {

            decimal nSchemeBuyValue, nSchemeToRange, nSchemeGetValue;
            Boolean bSchemeApplied = false;
            int nBuyType, nGetType;
            decimal nSetQty = 0;
            string retMsg = "";

            try
            {
                string cSchemeName = drSchemeDet["schemeName"].ToString();
                string cSchemeRowId = drSchemeDet["schemeRowId"].ToString();

                commonMethods globalMethods = new commonMethods();

                Decimal nQty;
                Int32 nTrial = 1;


                string cSlabRowId = drSlabs["rowId"].ToString();

                DataRow[] drSkuNamesBuy = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "' and buybc=1", "");
                DataRow[] drSkuNamesGet = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "' and getbc=1", "");

                string cBuynGetnRowId = Guid.NewGuid().ToString();

                if (drSkuNamesBuy.Length == 0 || drSkuNamesGet.Length == 0)
                    goto lblEnd;

                nSchemeBuyValue = globalMethods.ConvertDecimal(drSlabs["buyFromRange"]);
                nSchemeToRange = globalMethods.ConvertDecimal(drSlabs["buyToRange"]);
                nSchemeGetValue = globalMethods.ConvertDecimal(drSlabs["discountAmount"]);


                // We need to do this so that we can process the scheme on the items ordered on pending scheme qty/amount desc
                foreach (DataRow dr in dtCmdSchemes.Rows)
                {
                    nQty = Math.Abs(globalMethods.ConvertDecimal(dr["quantity"])) - Math.Abs(globalMethods.ConvertDecimal(dr["scheme_applied_qty"]));
                    dr["pending_scheme_apply_qty"] = nQty;
                    dr["pending_scheme_apply_amount"] = (globalMethods.ConvertDecimal(dr["mrp"]) * Math.Abs(globalMethods.ConvertDecimal(dr["quantity"]))) - globalMethods.ConvertDecimal(dr["scheme_applied_amount"]);
                }


                // Need to do this to be able to rollback the cmd changes in current cycle if set is not created completely
                dtCmdSchemes.AcceptChanges();
            lblReProcess:

                Decimal BuyItemsTotal = 0, GetItemsTotal = 0, nNetValue, nSchemeAppliedQty = 0, nLoopValue = 0, nAddValue = 0,
                nMrp, nDiscountFigure, nDiscountAmount, nDiscountPercentage, nSchemeAppliedAmount = 0;

                nQty = 0;
                string cProductCode;

                int nDiscMethod;

                DataTable dtFilteredCmdBuy = dtCmdSchemes.Clone();

                DataTable dtFilteredCmdGet = dtCmdSchemes.Clone();

                string filterTableData;

                DataTable dtSkuNamesBuy = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "' and buybc=1", "").CopyToDataTable();
                DataTable dtSkuNamesGet = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "' and getbc=1", "").CopyToDataTable();

                filterTableData = " pending_scheme_apply_qty>0 AND scheme_applied_amount=0 ";

                string cMessage;
                cMessage = globalMethods.JoinDataTables(dtCmdSchemes, dtSkuNamesBuy, ref dtFilteredCmdBuy, filterTableData,
                (row1, row2) =>
                row1.Field<String>("product_code") == row2.Field<String>("product_code"));

                if (!String.IsNullOrEmpty(cMessage) || dtFilteredCmdBuy.Rows.Count == 0)
                {
                    dtCmdSchemes.RejectChanges();
                    return cMessage;
                }

                decimal nLoop = 0, nBaseQtyorAmount = 0, nAppliedValue = 0, nPendingAppliedValue = 0;


                nLoopValue = 0;

                DataRow drDetail;
                nPendingAppliedValue = nSchemeBuyValue;

                while (nPendingAppliedValue > 0)
                {
                    nLoop = nLoop + 1;
                    nBaseQtyorAmount = 0;
                    if (nLoop == 1)
                        nBaseQtyorAmount = globalMethods.ConvertDecimal(dtFilteredCmdBuy.Compute("MAX(pending_scheme_apply_amount)", "pending_scheme_apply_amount>0"));
                    else if (nTrial == 1)
                        nBaseQtyorAmount = globalMethods.ConvertDecimal(dtFilteredCmdBuy.Compute("MIN(pending_scheme_apply_amount)", "pending_scheme_apply_amount>0 AND pending_scheme_apply_amount<=" + nPendingAppliedValue.ToString()));
                    else if (nTrial == 2)
                        nBaseQtyorAmount = globalMethods.ConvertDecimal(dtFilteredCmdBuy.Compute("MAX(pending_scheme_apply_amount)", "pending_scheme_apply_amount>0 AND pending_scheme_apply_amount<=" + nPendingAppliedValue.ToString()));
                    else if (nTrial == 3)
                        nBaseQtyorAmount = globalMethods.ConvertDecimal(dtFilteredCmdBuy.Compute("MIN(pending_scheme_apply_amount)", "pending_scheme_apply_amount>0"));


                    if (nBaseQtyorAmount == 0)
                        break;

                    nAppliedValue = Math.Abs(((nLoopValue + nBaseQtyorAmount) > nSchemeToRange ? (nSchemeToRange - nLoopValue) : nBaseQtyorAmount));


                    drDetail = dtFilteredCmdBuy.Select("pending_scheme_apply_amount=" + nBaseQtyorAmount.ToString() + " AND pending_scheme_apply_amount>0", "").FirstOrDefault();

                    drDetail["scheme_applied_amount"] = globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]) + nAppliedValue;
                    drDetail["scheme_applied_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["quantity"]));

                    drDetail["pending_scheme_apply_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["quantity"])) - globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]);
                    drDetail["pending_scheme_apply_amount"] = Math.Abs((globalMethods.ConvertDecimal(drDetail["quantity"]) * globalMethods.ConvertDecimal(drDetail["mrp"])) -
                        globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]));

                    nLoopValue = nLoopValue + nAppliedValue;

                    if (!drDetail["scheme_name"].ToString().Contains(cSchemeName))
                    {
                        drDetail["scheme_name"] = drDetail["scheme_name"] + (string.IsNullOrEmpty(drDetail["scheme_name"].ToString()) ? "" : ",") +
                        drSchemeDet["schemeName"].ToString();
                    }


                    drDetail["BuynGetnRowId"] = cBuynGetnRowId;
                    drDetail["slsdet_row_id"] = cSchemeRowId;
                    drDetail["slabRowId"] = cSlabRowId;


                    nPendingAppliedValue = nPendingAppliedValue - nAppliedValue;
                    if (nLoopValue >= nSchemeToRange)
                        break;
                }

                if (nLoopValue < nSchemeBuyValue)
                {
                    dtCmdSchemes.RejectChanges();
                    goto lblUpdateWtdDisc;
                }

                synchCmdSchemes(dtFilteredCmdBuy, ref dtCmdSchemes);


                cMessage = globalMethods.JoinDataTables(dtCmdSchemes, dtSkuNamesGet, ref dtFilteredCmdGet, filterTableData,
                (row1, row2) =>
                row1.Field<String>("product_code") == row2.Field<String>("product_code"));

                if (!String.IsNullOrEmpty(cMessage))
                    return cMessage;

                if (dtFilteredCmdGet.Rows.Count == 0)
                    goto lblUpdateWtdDisc;

                nLoopValue = 0;
                nPendingAppliedValue = nSchemeGetValue;
                while (nPendingAppliedValue > 0)
                {

                    nBaseQtyorAmount = 0;
                    if (nTrial <= 2)
                    {
                        nBaseQtyorAmount = globalMethods.ConvertDecimal(dtFilteredCmdGet.Compute("MIN(pending_scheme_apply_amount)", "pending_scheme_apply_amount>=" + nPendingAppliedValue.ToString()));
                        if (nBaseQtyorAmount == 0)
                            nBaseQtyorAmount = globalMethods.ConvertDecimal(dtFilteredCmdGet.Compute("MIN(pending_scheme_apply_amount)",
                                " pending_scheme_apply_amount<" + nPendingAppliedValue.ToString()));
                    }
                    else
                    if (nTrial == 3)
                    {
                        nBaseQtyorAmount = globalMethods.ConvertDecimal(dtFilteredCmdGet.Compute("MAX(pending_scheme_apply_amount)", "pending_scheme_apply_amount>0 AND pending_scheme_apply_amount<=" + nPendingAppliedValue.ToString()));
                        if (nBaseQtyorAmount == 0)
                            nBaseQtyorAmount = globalMethods.ConvertDecimal(dtFilteredCmdGet.Compute("MAX(pending_scheme_apply_amount)",
                                " pending_scheme_apply_amount>" + nPendingAppliedValue.ToString()));
                    }

                    if (nBaseQtyorAmount == 0)
                        break;

                    nAppliedValue = Math.Abs(((nLoopValue + nBaseQtyorAmount) > nSchemeGetValue ? (nSchemeGetValue - nLoopValue) : nBaseQtyorAmount));



                    drDetail = dtFilteredCmdGet.Select("pending_scheme_apply_amount=" + nBaseQtyorAmount.ToString() + " AND pending_scheme_apply_amount>0", "").FirstOrDefault();

                    drDetail["scheme_applied_amount"] = globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]) + nAppliedValue;
                    drDetail["scheme_applied_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["quantity"]));

                    drDetail["basic_discount_amount"] = nAppliedValue;
                    drDetail["basic_discount_percentage"] = Math.Round((globalMethods.ConvertDecimal(drDetail["basic_discount_amount"]) /
                        (globalMethods.ConvertDecimal(drDetail["mrp"]) * globalMethods.ConvertDecimal(drDetail["quantity"]))) * 100, 3);

                    drDetail["net"] = (globalMethods.ConvertDecimal(drDetail["mrp"]) * globalMethods.ConvertDecimal(drDetail["quantity"])) - globalMethods.ConvertDecimal(drDetail["basic_discount_amount"]);
                    drDetail["pending_scheme_apply_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["quantity"])) - globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]);

                    drDetail["pending_scheme_apply_amount"] = Math.Abs((globalMethods.ConvertDecimal(drDetail["quantity"]) * globalMethods.ConvertDecimal(drDetail["mrp"])) -
                        globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]));

                    nLoopValue = nLoopValue + nAppliedValue;

                    if (!drDetail["scheme_name"].ToString().Contains(cSchemeName))
                    {
                        drDetail["scheme_name"] = drDetail["scheme_name"] + (string.IsNullOrEmpty(drDetail["scheme_name"].ToString()) ? "" : ",") +
                        drSchemeDet["schemeName"].ToString();
                    }

                    drDetail["slsdet_row_id"] = cSchemeRowId;
                    drDetail["slabRowId"] = cSlabRowId;
                    drDetail["BuynGetnRowId"] = cBuynGetnRowId;

                    nPendingAppliedValue = nPendingAppliedValue - nAppliedValue;
                    if (nLoopValue >= nSchemeToRange)
                        break;
                }


                synchCmdSchemes(dtFilteredCmdGet, ref dtCmdSchemes);

                bSchemeApplied = true;

                // Commit changes in cmd for scheme applied  for current set
                dtCmdSchemes.AcceptChanges();

            lblUpdateWtdDisc:
                // Calculate  Weighted avg discounts in all items which are part of Buy n Get n schemes
                if (bSchemeApplied)
                {

                    bSchemeApplied = false;
                    retMsg = UpdateBNGNWtdAvgDisc(ref dtCmdSchemes, cSlabRowId, globalMethods.ConvertBool(drSchemeDet["donot_distribute_weighted_avg_disc_bngn"]));
                    if (!String.IsNullOrEmpty(retMsg))
                        goto lblEnd;


                    if (globalMethods.ConvertDecimal(drSlabs["addnlGetQty"]) > 0)
                    {
                        retMsg = ProcessBnGnAddnlDiscounts(cSchemeRowId, cSchemeName, ref dtCmdSchemes, drSlabs, dtSkuNames);
                        if (!String.IsNullOrEmpty(retMsg))
                            goto lblEnd;
                    }

                }
                else if (nTrial < 3)
                {
                    nTrial = nTrial + 1;
                    goto lblReProcess;
                }

                //Slab loop ends here


            }

            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);
                retMsg = "Error in APplying Buy n Get n (Amount based) at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();

            }

        lblEnd:

            return retMsg;

        }
        private string applyEossBnGnSpl(ref DataTable dtCmdSchemes, DataRow drSlabs, DataRow drSchemeDet, DataTable dtSkuNames)
        {

            decimal nSchemeBuyValue, nSchemeToRange, nSchemeGetValue, nAddnlDiscPct = 0, nAddnlDiscAmt = 0;
            Boolean bSchemeApplied = false, bAddnlDiscApplicable = false;
            int nBuyType, nGetType, nSchemeAddnlGetQty = 0;
            decimal nSetQty = 0;
            string retMsg = "";

            try
            {
                string cSchemeName = drSchemeDet["schemeName"].ToString();
                string cSchemeRowId = drSchemeDet["schemeRowId"].ToString();


                nBuyType = Convert.ToInt32(drSchemeDet["buyType"]);
                nGetType = 1;

                commonMethods globalMethods = new commonMethods();

                Decimal nQty;
                // We need to do this so that we can process the scheme on the items ordered on pending scheme qty/amount desc
                foreach (DataRow dr in dtCmdSchemes.Rows)
                {
                    nQty = Math.Abs(globalMethods.ConvertDecimal(dr["quantity"])) - Math.Abs(globalMethods.ConvertDecimal(dr["scheme_applied_qty"]));
                    dr["pending_scheme_apply_qty"] = nQty;
                    dr["pending_scheme_apply_amount"] = (globalMethods.ConvertDecimal(dr["mrp"]) * Math.Abs(globalMethods.ConvertDecimal(dr["quantity"]))) - globalMethods.ConvertDecimal(dr["scheme_applied_amount"]);
                }

                // Need to do this to be able to rollback the cmd changes in current cycle if set is not created completely
                dtCmdSchemes.AcceptChanges();

                string cSlabRowId = drSlabs["rowId"].ToString();

                DataRow[] drSkuNamesBuy = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "' and buybc=1", "");
                DataRow[] drSkuNamesGet = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "' and getbc=1", "");

                nAddnlDiscPct = globalMethods.ConvertDecimal(drSlabs["addnlDiscountPercentage"]);
                nAddnlDiscAmt = globalMethods.ConvertDecimal(drSlabs["addnlDiscountAmount"]);

                bAddnlDiscApplicable = false;
                if (nAddnlDiscPct > 0 || nAddnlDiscAmt > 0)
                {
                    bAddnlDiscApplicable = true;
                }


                nSchemeBuyValue = globalMethods.ConvertDecimal(drSlabs["buyFromRange"]);
                nSchemeToRange = globalMethods.ConvertDecimal(drSlabs["buyToRange"]);
                string cBuynGetnRowId = Guid.NewGuid().ToString();
                if (nGetType == 2)
                    nSchemeGetValue = globalMethods.ConvertDecimal(drSlabs["discountAmount"]);
                else
                    nSchemeGetValue = globalMethods.ConvertDecimal(drSlabs["getQty"]);

                if (globalMethods.ConvertDecimal(drSlabs["addnlGetQty"]) > 0)
                {
                    nSchemeAddnlGetQty = globalMethods.ConvertInt(drSlabs["addnlGetQty"]);
                }

            lblReProcess:

                Decimal BuyItemsTotal = 0, GetItemsTotal = 0, AddnlGetItemsTotal = 0, nNetValue, nSchemeAppliedQty = 0, nLoopValue = 0, nAddValue = 0,
                nMrp, nDiscountFigure, nDiscountAmount, nDiscountPercentage, nSchemeAppliedAmount = 0;

                nQty = 0;
                string cProductCode;

                int nDiscMethod;

                DataTable dtFilteredCmdBuy = dtCmdSchemes.Clone();

                DataTable dtFilteredCmdGet = dtCmdSchemes.Clone();


                string filterTableData;
                if (drSkuNamesBuy.Length > 0 && drSkuNamesGet.Length > 0)
                {

                    DataTable dtSkuNamesBuy = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "' and buybc=1", "").CopyToDataTable();
                    DataTable dtSkuNamesGet = dtSkuNames.Select("schemeRowId='" + cSchemeRowId + "' and getbc=1", "").CopyToDataTable();

                    filterTableData = (nBuyType == 1 ? " pending_scheme_apply_qty>0 " : " scheme_applied_amount=0 ");

                    string cMessage = globalMethods.JoinDataTables(dtCmdSchemes, dtSkuNamesBuy, ref dtFilteredCmdBuy, filterTableData,
                    (row1, row2) =>
                    row1.Field<String>("product_code") == row2.Field<String>("product_code"));

                    if (!String.IsNullOrEmpty(cMessage))
                        return cMessage;

                    if (dtFilteredCmdBuy.Rows.Count == 0)
                    {
                        dtCmdSchemes.RejectChanges();
                        goto lblUpdateWtdDisc;
                    }

                    filterTableData = (nGetType == 1 ? " pending_scheme_apply_qty>0 " : " scheme_applied_amount=0 ");

                    cMessage = globalMethods.JoinDataTables(dtCmdSchemes, dtSkuNamesGet, ref dtFilteredCmdGet, filterTableData,
                    (row1, row2) =>
                    row1.Field<String>("product_code") == row2.Field<String>("product_code"));

                    if (!String.IsNullOrEmpty(cMessage))
                        return cMessage;

                    if (dtFilteredCmdGet.Rows.Count == 0)
                        goto lblUpdateWtdDisc;


                    decimal nLoop = 0, nBaseQtyorAmount = 0, nAppliedValue = 0;
                    nLoopValue = 0;



                    //Populate dt here
                    DataTable dtFilteredCmdGetOrdered = new DataTable();

                    dtFilteredCmdGet.DefaultView.Sort = (nGetType == 1 ? "mrp ASC,pending_scheme_apply_qty ASC " : "pending_scheme_apply_amount"); //  string.Format("{0} {1}", cOrderColumn, "DES"); //sort descending

                    dtFilteredCmdGetOrdered = dtFilteredCmdGet.DefaultView.ToTable();

                    nLoopValue = 0;

                    decimal nBuySchAppliedQty = 0, nBuySchAppliedAmt = 0;
                    foreach (DataRow drDetail in dtFilteredCmdGetOrdered.Rows)
                    {

                        nBuySchAppliedQty = globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]);
                        nBuySchAppliedAmt = globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]);

                        if (nGetType == 1)
                            nBaseQtyorAmount = globalMethods.ConvertDecimal(drDetail["pending_scheme_apply_qty"]);
                        else
                            nBaseQtyorAmount = globalMethods.ConvertDecimal(drDetail["pending_scheme_apply_amount"]);

                        if (nBaseQtyorAmount == 0)
                            continue;


                        nAppliedValue = Math.Abs(((nLoopValue + nBaseQtyorAmount) > nSchemeGetValue ? (nSchemeGetValue - nLoopValue) : nBaseQtyorAmount));

                        cSchemeRowId = drSchemeDet["schemeRowId"].ToString();

                        string cErr = applyDiscountasPerMethod(drDetail, cSchemeRowId, globalMethods.ConvertDecimal(drSlabs["discountPercentage"]),
                            globalMethods.ConvertDecimal(drSlabs["discountAmount"]), globalMethods.ConvertDecimal(drSlabs["netPrice"]),
                            (nGetType == 1 ? nAppliedValue : 1));

                        if (!string.IsNullOrEmpty(cErr))
                            return cErr;

                        if (nGetType == 1)
                            drDetail["scheme_applied_qty"] = globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]) + nAppliedValue;

                        drDetail["pending_scheme_apply_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["quantity"]) - globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]));
                        drDetail["pending_scheme_apply_amount"] = Math.Abs((globalMethods.ConvertDecimal(drDetail["quantity"]) *
                            globalMethods.ConvertDecimal(drDetail["mrp"])) - globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]));

                        nLoopValue = nLoopValue + nAppliedValue;

                        if (!drDetail["scheme_name"].ToString().Contains(cSchemeName))
                        {
                            drDetail["scheme_name"] = drDetail["scheme_name"] + (string.IsNullOrEmpty(drDetail["scheme_name"].ToString()) ? "" : ",") +
                            drSchemeDet["schemeName"].ToString();
                        }
                        drDetail["BuynGetnRowId"] = cBuynGetnRowId;
                        drDetail["slsdet_row_id"] = cSchemeRowId;
                        drDetail["slabRowId"] = cSlabRowId;
                        if (nLoopValue >= nSchemeGetValue)
                            break;

                    }

                    if (nLoopValue < nSchemeGetValue)
                    {
                        dtCmdSchemes.RejectChanges();
                        goto lblUpdateWtdDisc;
                    }


                    synchCmdSchemes(dtFilteredCmdGetOrdered, ref dtCmdSchemes);


                    // We need to refresh the Get Cmd ursor due to common bar codes between Buy and Get filter
                    dtFilteredCmdBuy.Rows.Clear();
                    cMessage = globalMethods.JoinDataTables(dtCmdSchemes, dtSkuNamesBuy, ref dtFilteredCmdBuy, filterTableData,
                    (row1, row2) =>
                    row1.Field<String>("product_code") == row2.Field<String>("product_code"));

                    if (!String.IsNullOrEmpty(cMessage) || dtFilteredCmdBuy.Rows.Count == 0)
                    {
                        dtCmdSchemes.RejectChanges();
                        return cMessage;
                    }


                    nLoopValue = 0;

                    DataTable dtFilteredCmdBuyOrdered = new DataTable();

                    string cOrderColumn = "pending_scheme_apply_amount";

                    dtFilteredCmdBuy.DefaultView.Sort = cOrderColumn + " DESC"; //  string.Format("{0} {1}", cOrderColumn, "DES"); //sort descending

                    dtFilteredCmdBuyOrdered = dtFilteredCmdBuy.DefaultView.ToTable();


                    foreach (DataRow drDetail in dtFilteredCmdBuyOrdered.Rows)
                    {
                        if (nBuyType == 1)
                            nBaseQtyorAmount = globalMethods.ConvertDecimal(drDetail["pending_scheme_apply_qty"]);
                        else
                            nBaseQtyorAmount = globalMethods.ConvertDecimal(drDetail["pending_scheme_apply_amount"]);

                        nAppliedValue = Math.Abs(((nLoopValue + nBaseQtyorAmount) > nSchemeToRange ? (nSchemeToRange - nLoopValue) : nBaseQtyorAmount));

                        if (nBuyType == 1)
                        {
                            drDetail["scheme_applied_qty"] = globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]) + nAppliedValue;
                            drDetail["scheme_applied_amount"] = globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]) +
                                (globalMethods.ConvertDecimal(drDetail["mrp"]) * nAppliedValue);

                        }
                        else
                        {
                            drDetail["scheme_applied_amount"] = globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]) + nAppliedValue;
                            drDetail["scheme_applied_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["quantity"]));
                        }


                        drDetail["pending_scheme_apply_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["quantity"])) - globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]);
                        drDetail["pending_scheme_apply_amount"] = Math.Abs((globalMethods.ConvertDecimal(drDetail["quantity"]) * globalMethods.ConvertDecimal(drDetail["mrp"])) -
                            globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]));

                        nLoopValue = nLoopValue + nAppliedValue;


                        if (!drDetail["scheme_name"].ToString().Contains(cSchemeName))
                        {
                            drDetail["scheme_name"] = drDetail["scheme_name"] + (string.IsNullOrEmpty(drDetail["scheme_name"].ToString()) ? "" : ",") +
                            drSchemeDet["schemeName"].ToString();
                        }
                        drDetail["BuynGetnRowId"] = cBuynGetnRowId;
                        drDetail["slsdet_row_id"] = cSchemeRowId;
                        drDetail["slabRowId"] = cSlabRowId;
                        if (nLoopValue >= nSchemeToRange)
                            break;
                    }

                    if (nLoopValue < nSchemeBuyValue)
                    {
                        dtCmdSchemes.RejectChanges();
                        goto lblUpdateWtdDisc;
                    }


                    bSchemeApplied = true;
                    synchCmdSchemes(dtFilteredCmdBuyOrdered, ref dtCmdSchemes);


                    if (globalMethods.ConvertDecimal(drSlabs["addnlGetQty"]) > 0)
                    {
                        retMsg = ProcessBnGnAddnlDiscounts(cSchemeRowId, cSchemeName, ref dtCmdSchemes, drSlabs, dtSkuNames);
                        if (!String.IsNullOrEmpty(retMsg))
                            goto lblEnd;
                    }

                    // Commit changes in cmd for scheme applied  for current set
                    dtCmdSchemes.AcceptChanges();

                    goto lblReProcess;
                }

            lblUpdateWtdDisc:
                // Calculate  Weighted avg discounts in all items which are part of Buy n Get n schemes
                if (bSchemeApplied)
                {

                    bSchemeApplied = false;
                    retMsg = UpdateBNGNWtdAvgDisc(ref dtCmdSchemes, cSlabRowId, globalMethods.ConvertBool(drSchemeDet["donot_distribute_weighted_avg_disc_bngn"]));
                    if (!String.IsNullOrEmpty(retMsg))
                        goto lblEnd;

                }

                //Slab loop ends here

            }

            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);
                retMsg = "Error in APplying Buy n Get n (Valuse based) at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();

            }

        lblEnd:

            return retMsg;

        }
    }

    public class commonMethods
    {
        public DataTable ValidateBarCodes(SqlCommand cmd, DataTable dtSource, string cLocId, ref string cError, ref Boolean bStRecoBarCodesFound)
        {

            DataTable tErrBc = new DataTable();

            try
            {
                string cExpr = "";
                cExpr = "select * from #tBarCodesXns";
                cmd.CommandText = cExpr;
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                DataTable dt = new DataTable();
                sda.Fill(dt);



                cExpr = "SELECT TOP 1 A.MEMO_ID FROM STMH01106 A (NOLOCK) " +
               "JOIN PMT01106 B (NOLOCK) ON A.REP_ID=B.REP_ID AND B.BIN_ID=A.RECON_BIN_ID " +
               "JOIN #tBarCodesXns C ON C.PRODUCT_CODE=B.PRODUCT_CODE AND C.BIN_ID=B.BIN_ID " +
               "WHERE B.DEPT_ID = '" + cLocId + "'";


                cmd.CommandText = cExpr;
                var cMemoId = cmd.ExecuteScalar();

                if (cMemoId != null)
                {
                    cExpr = "INSERT INTO #tBarCodesStReco" +
                            "SELECT DISTINCT C.PRODUCT_CODE" +
                            "FROM STMH01106 A (NOLOCK) JOIN PMT01106 B (NOLOCK) ON A.REP_ID=B.REP_ID AND B.BIN_ID=A.RECON_BIN_ID " +
                            "JOIN #tBarCodesXns C ON C.PRODUCT_CODE=B.PRODUCT_CODE AND C.BIN_ID=B.BIN_ID WHERE B.DEPT_ID = '" + cLocId + "'";

                    bStRecoBarCodesFound = true;

                    cmd.CommandText = cExpr;
                    sda = new SqlDataAdapter(cmd);
                    sda.Fill(tErrBc);

                }

                cExpr = "SELECT TOP 1 a.product_code FROM #tBarCodesXns a LEFT JOIN sku b (NOLOCK) ON a.product_code=b.product_code" +
                    "  WHERE b.product_code IS NULL";

                cmd.CommandText = cExpr;
                var cPcNotFound = cmd.ExecuteScalar();

                if (cPcNotFound != null)
                {
                    cExpr = "SELECT  a.product_code FROM #tBarCodesXns a LEFT JOIN sku b (NOLOCK) ON a.product_code=b.product_code " +
                            " WHERE b.product_code IS NULL";
                    cmd.CommandText = cExpr;
                    sda = new SqlDataAdapter(cmd);
                    sda.Fill(tErrBc);
                }
            }

            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);
                cError = "Error in ValidateBarCodes at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();
                goto lblLast;
            }

        lblLast:
            return tErrBc;
        }
        public string GetNextKey(SqlCommand cmd, string cKeysTable, string cTableName, string cColName, int nWidth, string cPrefix, Boolean bLeadingZeros, string cFinYear,
                                          ref string cNextKeyVal)

        {

            try
            {
                string cExpr = " SELECT LASTKEYVAL FROM [" + cKeysTable + "] WHERE TABLENAME= '" + cTableName + "'  AND COLUMNNAME='" + cColName + "' AND PREFIX='" +
                    cPrefix + "'  AND FINYEAR='" + cFinYear + "'";

                cmd.CommandText = cExpr;
                DataTable dt = new DataTable();

                SqlDataAdapter sda = new SqlDataAdapter(cmd);

                int nQueryResult;
                sda.Fill(dt);
                if (dt.Rows.Count == 0)
                {
                    cExpr = "INSERT [" + cKeysTable + "] ( TABLENAME, COLUMNNAME, PREFIX, FINYEAR, LASTKEYVAL )   VALUES ( '" + cTableName + "', '" + cColName + "','" +
                        cPrefix + "','" + cFinYear + "','')";
                    cmd.CommandText = cExpr;
                    nQueryResult = cmd.ExecuteNonQuery();

                    if (nQueryResult == 0)
                    {
                        return "Error inserting data into Keys table";
                    }
                }


                cExpr = "SELECT TOP 1  LEN( LASTKEYVAL ) tempWidth,lastkeyval  FROM [" + cKeysTable + "]  WHERE TABLENAME = '" + cTableName + "' AND COLUMNNAME  = '" + cColName +
                        "' AND PREFIX  = '" + cPrefix + "'  AND FINYEAR = '" + cFinYear + "'";

                DataTable dtKeys = new DataTable();
                cmd.CommandText = cExpr;
                sda = new SqlDataAdapter(cmd);
                sda.Fill(dtKeys);

                string cLastKeyVal = dtKeys.Rows[0]["lastkeyval"].ToString();
                int nTempWidth = (bLeadingZeros || string.IsNullOrEmpty(cLastKeyVal) ? nWidth : cLastKeyVal.Trim().Length);

                int nValLen = nTempWidth - cPrefix.Trim().Length;
                if (nValLen < 0)
                    nValLen = 0;

                int nTemplastkeyval = 0;// new commonMethods().ConvertInt(cLastKeyVal.Substring(cPrefix.Length));
                if (cLastKeyVal.Length >= cPrefix.Length)
                    nTemplastkeyval = new commonMethods().ConvertInt(cLastKeyVal.Substring(cPrefix.Length));
                int nLastkeyval = (nTemplastkeyval + 1);

                string cZeros = "";
                if (bLeadingZeros)
                    cZeros = new String('0', nValLen - nLastkeyval.ToString().Trim().Length);

                cNextKeyVal = cPrefix.Trim() + cZeros.Trim() + (nLastkeyval.ToString().Trim());


                if (!string.IsNullOrEmpty(cNextKeyVal))
                {

                    cExpr = $" UPDATE [" + cKeysTable + "] SET LASTKEYVAL = '" + cNextKeyVal + "' WHERE TABLENAME = '" + cTableName +
                        "' AND COLUMNNAME = '" + cColName + "' AND PREFIX = '" + cPrefix + "' AND FINYEAR = '" + cFinYear + "'";

                    cmd.CommandText = cExpr;
                    nQueryResult = cmd.ExecuteNonQuery();

                    if (nQueryResult == 0)
                    {
                        return "Error Updating Keys table";
                    }

                }

            }
            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);
                return "Error in method GEtnextKey at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();
            }

            return "";
        }

        public string GetFinYear(DateTime dDate, ref string cFinYear)
        {
            try
            {
                cFinYear = "011";
                string cYear = dDate.Year.ToString();
                string cNextYear = (dDate.Year + 1).ToString();
                cFinYear = cFinYear + (new List<int>() { 1, 2, 3 }.Contains(dDate.Month) ? cYear.Substring(cYear.Length - 2) : cNextYear.Substring(cNextYear.Length - 2));

            }

            catch (Exception ex)
            {
                return ex.Message.ToString();
            }

            return "";
        }

        public DataTable NormalizeBatchCodes(SqlCommand cmd, DataTable dtSource, string cLocId, ref string cErr, string cBinIdColName = null)
        {
            DataTable dtRet = dtSource.Clone();
            try
            {

                commonMethods globalMethods = new commonMethods();
                string cExpr = "";
                cExpr = "select * from #tBarCodesXns";
                cmd.CommandText = cExpr;
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                DataTable dt = new DataTable();
                sda.Fill(dt);

                cmd.CommandText = "SELECT newid() row_id,b.product_code, a.product_code batch_code,quantity_in_stock,sn.mrp,a.bin_id FROM pmt01106 a (NOLOCK) " +
                    " JOIN sku_names sn (NOLOCK) ON sn.product_code=a.product_code " +
                    " JOIN #tBarCodesXns b ON b.PRODUCT_CODE=LEFT(sn.PRODUCT_CODE, ISNULL(NULLIF(CHARINDEX ('@',sn.PRODUCT_CODE)-1,-1),LEN(sn.PRODUCT_CODE ))) " +
                    " AND a.bin_id=b.bin_id AND a.dept_id=b.dept_id AND b.mrp=sn.mrp WHERE quantity_in_stock>0 AND" +
                    " sn_barcode_coding_scheme=1 AND b.xn_qty>0";

                DataTable tPmtBatches = new DataTable();
                sda = new SqlDataAdapter(cmd);
                sda.Fill(tPmtBatches);

                decimal nQty = 0, nMrp = 0, nQtyUpdated = 0;
                string cBinId = "", cProductCode = "", cBatchCode = "", cRowId = "";
                DataTable tSearch = new DataTable();

                if (string.IsNullOrEmpty(cBinIdColName))
                    cBinIdColName = "bin_id";

                foreach (DataRow dr in dtSource.Rows)
                {


                    if (Convert.ToInt16(dr["coding_Scheme"]) != 1 || dr["product_code"].ToString().Contains("@"))
                    {
                        DataRow drNew = dtRet.NewRow();
                        drNew.ItemArray = dr.ItemArray;
                        dtRet.Rows.Add(drNew);
                        continue;
                    }


                    nQty = new commonMethods().ConvertDecimal(dr["quantity"]);
                    nMrp = new commonMethods().ConvertDecimal(dr["mrp"]);
                    cProductCode = dr["product_code"].ToString();
                    cBinId = dr[cBinIdColName].ToString();

                    string cFilter = "product_code='" + cProductCode + "' AND mrp=" + nMrp.ToString() + " AND bin_id='" + cBinId + "' AND quantity_in_stock>0";
                    while (nQty > 0)
                    {


                        DataRow[] drSearch = tPmtBatches.Select(cFilter, "");

                        if (drSearch.Length > 0)
                        {
                            tSearch = tPmtBatches.Select(cFilter, "").CopyToDataTable();
                            cRowId = tSearch.Rows[0]["row_id"].ToString();
                            nQtyUpdated = (nQty >= globalMethods.ConvertDecimal(tSearch.Rows[0]["quantity_in_stock"]) ? globalMethods.ConvertDecimal(tSearch.Rows[0]["quantity_in_stock"]) : nQty);
                            dr["quantity"] = nQtyUpdated;
                            dr["product_code"] = tSearch.Rows[0]["batch_code"];

                            tPmtBatches.Select("row_id='" + cRowId + "'", "").AsEnumerable().ToList().ForEach(r =>
                            {
                                r["quantity_in_stock"] = globalMethods.ConvertDecimal(r["quantity_in_stock"]) - nQtyUpdated;
                            });

                            nQty = nQty - nQtyUpdated;

                            dr["row_id"] = cLocId + Guid.NewGuid().ToString();

                            DataRow drNew = dtRet.NewRow();
                            drNew.ItemArray = dr.ItemArray;
                            dtRet.Rows.Add(drNew);
                        }
                        else
                        {
                            cErr = "Stock not available for Item code :" + cProductCode;
                            goto lblLast;
                        }
                    }
                }

            }

            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);
                cErr = "Error in NormalizeBatchCodes at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();
            }

        lblLast:
            return dtRet;
        }

        public DataTable UpdatePmt(SqlCommand cmd, DataTable tSourceData, bool bRevertFlag, ref string cErr)
        {

            string cExpr = "";
            cExpr = "select * from #tBarCodesXns";
            cmd.CommandText = cExpr;
            SqlDataAdapter sda = new SqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            sda.Fill(dt);

            DataTable tRet = new DataTable();

            try
            {

                cExpr = " IF EXISTS (SELECT TOP 1 a.product_code FROM #tBarCodesXns a LEFT JOIN pmt01106 b (NOLOCK) ON a.product_code=b.product_code" +
                    "       AND a.dept_id=b.dept_id AND a.bin_id=b.bin_id WHERE b.product_code IS NULL) " +
                    "INSERT PMT01106 (PRODUCT_CODE,  QUANTITY_IN_STOCK, DEPT_ID,BIN_ID, LAST_UPDATE ) " +
                    "SELECT a.PRODUCT_CODE,0 AS QUANTITY_IN_STOCK,A.DEPT_ID,A.bin_id,GETDATE() AS LAST_UPDATE " +
                    " FROM #tBarCodesXns a LEFT JOIN pmt01106 b (NOLOCK) ON a.product_code=b.product_code AND a.dept_id=b.dept_id AND a.bin_id=b.bin_id " +
                    " WHERE b.product_code IS NULL " +
                    " GROUP BY a.PRODUCT_CODE,A.DEPT_ID,A.bin_id";

                cmd.CommandText = cExpr;
                cmd.ExecuteNonQuery();

                cExpr = "UPDATE A SET QUANTITY_IN_STOCK=QUANTITY_IN_STOCK-XN_QTY FROM PMT01106 A WITH (ROWLOCK)" +
                    " JOIN (SELECT  product_code,dept_id,bin_id,sum(xn_qty) xn_qty from  #tBarCodesXns" +
                    "  WHERE isnull(newEntry,0)=" + (bRevertFlag ? "0" : "1") + " GROUP BY product_code,dept_id,bin_id) B " +
                    "  ON A.product_code=B.product_code AND A.DEPT_ID=B.DEPT_ID AND A.BIN_ID=B.BIN_ID";


                cmd.CommandText = cExpr;
                cmd.ExecuteNonQuery();

                if (!bRevertFlag)
                {
                    cExpr = "SELECT a.product_code productCode,c.bin_name rackNo,d.bin_name zoneName,a.quantity [Scanned Qty],isnull(b.quantity_in_stock,a.quantity*-1) [Stock Qty] FROM  " +
                        "  ( SELECT dept_id,product_code,bin_id,SUM(xn_qty) quantity FROM #tBarCodesXns GROUP BY dept_id,product_code,bin_id) a" +
                        " LEFT JOIN pmt01106 b (NOLOCK) ON a.dept_id=b.dept_id AND a.product_Code=b.product_code AND a.bin_id=b.bin_id " +
                        " JOIN bin c (NOLOCK) ON a.bin_id=c.bin_id JOIN bin d (NOLOCK) ON d.bin_id=c.major_bin_id " +
                        " WHERE isnull(b.quantity_in_stock,0)<0 OR b.product_code IS NULL";

                    cmd.CommandText = cExpr;
                    sda = new SqlDataAdapter(cmd);
                    sda.Fill(tRet);

                }
            }

            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);
                cErr = "Error in method UpdatePmt at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();

            }

            return tRet;
        }
        public string UpdateBarcodesInTemp(SqlCommand cmd, SqlConnection conn, string cLocId, DataTable dtSource, bool bCreateTempTable, SqlBulkCopyOptions options, SqlTransaction sqlTran,
                string cMrpColName = "", string cQtyColName = "", string cProductCodeColName = "", string cBinIdColname = "", string cLocIdColName = "")
        {

            string cErr = "";
            try
            {
                if (bCreateTempTable)
                {
                    cmd.CommandText = "CREATE TABLE #tBarCodesXns (product_code VARCHAR(50),mrp NUMERIC(10,2),row_id VARCHAR(50),dept_id VARCHAR(5),xn_qty NUMERIC(10,2)," +
                        " bin_id VARCHAR(10),newEntry BIT)";
                    cmd.ExecuteNonQuery();
                }

                if (string.IsNullOrEmpty(cProductCodeColName))
                    cProductCodeColName = "product_code";

                if (string.IsNullOrEmpty(cBinIdColname))
                    cBinIdColname = "bin_id";

                if (string.IsNullOrEmpty(cLocIdColName))
                    cLocIdColName = "dept_id";

                if (string.IsNullOrEmpty(cQtyColName))
                    cQtyColName = "quantity";

                if (string.IsNullOrEmpty(cMrpColName))
                    cMrpColName = "mrp";


                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn, options, sqlTran))
                {
                    bulkCopy.BulkCopyTimeout = 50000;
                    bulkCopy.BatchSize = 5000;
                    bulkCopy.DestinationTableName = "#tBarCodesXns";
                    bulkCopy.ColumnMappings.Add(cProductCodeColName, "product_code");
                    bulkCopy.ColumnMappings.Add(cBinIdColname, "bin_id");
                    bulkCopy.ColumnMappings.Add("newEntry", "newEntry");
                    bulkCopy.ColumnMappings.Add(cLocIdColName, "dept_id");
                    bulkCopy.ColumnMappings.Add(cQtyColName, "xn_qty");
                    bulkCopy.ColumnMappings.Add("row_id", "row_id");
                    bulkCopy.ColumnMappings.Add(cMrpColName, "mrp");
                    try
                    {
                        bulkCopy.WriteToServer(dtSource);
                    }
                    catch (Exception ex)
                    {
                        cErr = "Error! Record Not Updated in tbarCodeXns Table SQL Error : " + ex.Message.ToString();
                    }

                }

            }


            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);
                cErr = "Error in method UpdateBarcodesInTemp at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();

            }

            return cErr;
        }

        public string addBulkCopyColMappings(SqlCommand cmd, DataTable cSourceTable, string cSqlTableName, ref List<SqlBulkCopyColumnMapping> columnMappings)
        {
            try
            {

                bool columnExists;

                cmd.CommandText = "select * from " + cSqlTableName + " (NOLOCK) WHERE 1=2";
                SqlDataAdapter sda = new SqlDataAdapter(cmd);

                DataTable dtCursor = new DataTable();
                sda.Fill(dtCursor);

                foreach (DataColumn dc in dtCursor.Columns)
                {
                    columnExists = cSourceTable.Columns
                    .Cast<DataColumn>()
                    .Any(column => string.Equals(column.ColumnName, dc.ColumnName, StringComparison.OrdinalIgnoreCase));

                    if (columnExists)
                        columnMappings.Add(new SqlBulkCopyColumnMapping(dc.ColumnName, dc.ColumnName));
                }
            }

            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);
                return "Error in addBulkCopyColMappings at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();
            }

            return "";
        }

        public int GetErrorLineNo(Exception ex)
        {
            var lineNumber = 0;
            const string lineSearch = ":line ";
            var index = ex.StackTrace.LastIndexOf(lineSearch);
            if (index != -1)
            {
                var lineNumberText = ex.StackTrace.Substring(index + lineSearch.Length);
                if (int.TryParse(lineNumberText, out lineNumber))
                {
                }
            }
            return lineNumber;
        }
        public string JoinDataTables(DataTable srcTable, DataTable joinTable, ref DataTable tResult, string filterTableData, params Func<DataRow, DataRow, bool>[] joinOn)
        {

            DataTable srcFilterTable = srcTable.Clone();

            if (!String.IsNullOrEmpty(filterTableData))
            {
                DataRow[] drFilter = srcTable.Select(filterTableData, "");
                if (drFilter.Length > 0)
                    srcFilterTable = srcTable.Select(filterTableData, "").CopyToDataTable();
            }

            foreach (DataRow row1 in srcFilterTable.Rows)
            {
                var joinRows = joinTable.AsEnumerable().Where(row2 =>
                {
                    foreach (var parameter in joinOn)
                    {
                        if (!parameter(row1, row2)) return false;
                    }
                    return true;
                });
                foreach (DataRow fromRow in joinRows)
                {
                    DataRow insertRow = tResult.NewRow();
                    foreach (DataColumn col1 in srcFilterTable.Columns)
                    {
                        insertRow[col1.ColumnName] = row1[col1.ColumnName];
                    }

                    tResult.Rows.Add(insertRow);
                }
            }
            return "";
        }

        public DataTable CreateDataTable<T>(List<T> items)
        {
            DataTable dataTable = new DataTable(typeof(T).Name);
            //Get all the properties
            PropertyInfo[] Props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (PropertyInfo prop in Props)
            {
                //Setting column names as Property names
                dataTable.Columns.Add(prop.Name);
                dataTable.Columns[prop.Name].AllowDBNull = true;
            }
            foreach (T item in items)
            {

                var values = new object[Props.Length];
                for (int i = 0; i < Props.Length; i++)
                {
                    //inserting property values to datatable rows
                    values[i] = Props[i].GetValue(item, null);
                }

                dataTable.Rows.Add(values);
            }
            //put a breakpoint here and check datatable
            return dataTable;
        }

        public DataTable CreateDataTablewithNull<T>(List<T> items)
        {
            DataTable dataTable = new DataTable(typeof(T).Name);
            //Get all the properties
            PropertyInfo[] Props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (PropertyInfo prop in Props)
            {
                //Setting column names as Property names
                dataTable.Columns.Add(prop.Name);
                dataTable.Columns[prop.Name].AllowDBNull = true;
            }
            foreach (T item in items)
            {
                DataRow dr = dataTable.NewRow();
                for (int i = 0; i < Props.Length; i++)
                {
                    //inserting property values to datatable rows
                    dr[i] = Props[i].GetValue(item, null);
                }

                dataTable.Rows.Add(dr);
            }
            //put a breakpoint here and check datatable
            return dataTable;
        }
        public void AddUploadTableData(DataTable dtSourceTable, DataTable dtTargetTable, ref String cError)
        {
            try
            {

                dtTargetTable.Rows.Clear();
                foreach (DataRow dr in dtSourceTable.Rows)
                {
                    if (dr.RowState != DataRowState.Deleted && dr.RowState != DataRowState.Detached)
                    {

                        DataRow drNew = dtTargetTable.NewRow();
                        foreach (DataColumn dcol in dtSourceTable.Columns)
                        {

                            drNew[dcol.ColumnName] = dr[dcol.ColumnName];

                        }

                        dtTargetTable.Rows.Add(drNew);
                    }
                }
            }
            catch (Exception ex)
            {
                cError = ex.Message;
            }
        }


        public bool HasProperty(Object obj, string name)
        {
            Type objType = obj.GetType();

            //if (objType == typeof(ExpandoObject))
            //{
            //    return ((IDictionary<string, object>)obj).ContainsKey(name);
            //}

            return objType.GetProperty(name) != null;
        }


        public void AddDataInUploadTablewithMapping(DataTable dtMapTable, DataTable dtSourceTable, DataTable dtTargetTable, ref String cError)
        {
            try
            {

                Boolean dataFound;

                dataFound = false;

                dtTargetTable.Rows.Clear();
                foreach (DataRow dr in dtSourceTable.Rows)
                {
                    if (dr.RowState != DataRowState.Deleted && dr.RowState != DataRowState.Detached)
                    {

                        DataRow drNew = dtTargetTable.NewRow();
                        foreach (DataColumn dcol in dtSourceTable.Columns)
                        {
                            if (dcol.ColumnName.Trim().ToUpper().ToString() == "DELETED")
                            {
                                drNew["deleted"] = ConvertBool(dr[dcol.ColumnName]);
                                dataFound = true;
                                continue;
                            }

                            DataRow[] dF = dtMapTable.Select("DevColumnName = '" + dcol.ColumnName.Trim() + "'", "");
                            if (dF.Length > 0)
                            {
                                String cOrgCol = Convert.ToString(dF[0]["OrgColumnName"]);

                                if (dtTargetTable.Columns.Contains(cOrgCol))
                                {
                                    //   drNew[cOrgCol] = dr[dcol.ColumnName];
                                    Type t = dtTargetTable.Columns[cOrgCol].DataType;

                                    if (Type.GetTypeCode(t) == TypeCode.Object && t == typeof(byte[]))
                                    {
                                        drNew[cOrgCol] = (byte[])Convert.FromBase64String(dr[dcol.ColumnName].ToString());
                                        dataFound = true;
                                        continue;
                                    }

                                    switch (Type.GetTypeCode(t))
                                    {
                                        //case TypeCode.Decimal:
                                        //    drNew[cOrgCol] = ConvertDecimal(dr[dcol.ColumnName]);
                                        //    break;

                                        //case TypeCode.Boolean:
                                        //    drNew[cOrgCol] = ConvertBool(dr[dcol.ColumnName]);
                                        //    break;

                                        default:
                                            drNew[cOrgCol] = dr[dcol.ColumnName];

                                            if (!String.IsNullOrEmpty(dr[dcol.ColumnName].ToString()))
                                                dataFound = true;

                                            break;
                                    }
                                }
                            }
                        }

                        if (dataFound)
                            dtTargetTable.Rows.Add(drNew);
                    }
                }
            }
            catch (Exception ex)
            {
                cError = ex.Message;
            }
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


    }
}
