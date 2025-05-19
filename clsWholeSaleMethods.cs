using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;

namespace WOWIntegration
{
    public class clsWholeSaleMethods
    {
        public bool EossReturnItemsProcessing { get; set; }


        private string ReCalNetOnRange(ref DataTable dtCmd, DataTable tConfig)
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
                    DataTable dtCmdExcl = dtCmd.Select("tax_method=2 AND (net_rate-INMDISCOUNTAMOUNT) BETWEEN " + nExclRangeFrom.ToString() + " AND " + nExclRangeTo.ToString(), "").CopyToDataTable();


                    if (dtCmdExcl.Rows.Count > 0)
                    {
                        dtCmdExcl.AsEnumerable().ToList().ForEach(r =>
                        {
                            r["DISCOUNT_AMOUNT"] = globalMethods.ConvertDecimal(r["DISCOUNT_AMOUNT"]) + (globalMethods.ConvertDecimal(r["net_rate"]) - globalMethods.ConvertDecimal(r["INMDISCOUNTAMOUNT"])
                            - (nExclRangeNet * (globalMethods.ConvertDecimal(r["invoice_quantity"]) > 0 ? 1 : -1)));
                            r["DISCOUNT_PERCENTAGE"] = Math.Abs(Math.Round(globalMethods.ConvertDecimal(r["DISCOUNT_AMOUNT"]) / (globalMethods.ConvertDecimal(r["rate"])
                                                              * globalMethods.ConvertDecimal(r["invoice_quantity"]) * 100), 3));

                            //r["DISCOUNT_AMOUNT"] = globalMethods.ConvertDecimal(r["BASIC_DISCOUNT_AMOUNT"]) + globalMethods.ConvertDecimal(r["CARD_DISCOUNT_AMOUNT"]);
                            //r["DISCOUNT_PERCENTAGE"] = Math.Abs(Math.Round((globalMethods.ConvertDecimal(r["DISCOUNT_AMOUNT"]) / (globalMethods.ConvertDecimal(r["RATE"]) * globalMethods.ConvertDecimal(r["invoice_quantity"]))) * 100, 3));

                            //r["net_rate"] = (globalMethods.ConvertDecimal(r["rate"]) * globalMethods.ConvertDecimal(r["invoice_quantity"])) - globalMethods.ConvertDecimal(r["DISCOUNT_AMOUNT"]);
                            Decimal nNet = 0;
                            if (globalMethods.ConvertDecimal(r["discount_amount"]) > 0)
                                nNet = (globalMethods.ConvertDecimal(r["discount_amount"]) / globalMethods.ConvertDecimal(r["INVOICE_QUANTITY"]));
                            //if (cRoundOff_Item_At == "1")
                            nNet = Math.Round(nNet);
                            r["NET_RATE"] = globalMethods.ConvertDecimal(r["RATE"]) - nNet;

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
                    DataTable dtCmdIncl = dtCmd.Select("isnull(tax_method,0) in (0,1) AND (net_rate-INMDISCOUNTAMOUNT)>=" + nInclRangeFrom.ToString() + " AND (net_rate-INMDISCOUNTAMOUNT)<=" + nInclRangeTo.ToString(), "").CopyToDataTable();

                    if (dtCmdIncl.Rows.Count > 0)
                    {
                        dtCmdIncl.AsEnumerable().ToList().ForEach(r =>
                        {
                            r["DISCOUNT_AMOUNT"] = globalMethods.ConvertDecimal(r["DISCOUNT_AMOUNT"]) + (globalMethods.ConvertDecimal(r["net_rate"]) - globalMethods.ConvertDecimal(r["INMDISCOUNTAMOUNT"])
                            - (nInclRangeNet * (globalMethods.ConvertDecimal(r["invoice_quantity"]) > 0 ? 1 : -1)));
                            r["DISCOUNT_PERCENTAGE"] = Math.Abs(Math.Round(globalMethods.ConvertDecimal(r["DISCOUNT_AMOUNT"]) / (globalMethods.ConvertDecimal(r["RATE"]) * globalMethods.ConvertDecimal(r["invoice_quantity"]) * 100), 3));

                            //r["DISCOUNT_AMOUNT"] = globalMethods.ConvertDecimal(r["BASIC_DISCOUNT_AMOUNT"]) + globalMethods.ConvertDecimal(r["CARD_DISCOUNT_AMOUNT"]);
                            //r["DISCOUNT_PERCENTAGE"] = Math.Round((globalMethods.ConvertDecimal(r["DISCOUNT_AMOUNT"]) / (globalMethods.ConvertDecimal(r["RATE"]) * globalMethods.ConvertDecimal(r["invoice_quantity"]))) * 100, 3);

                            //r["NET_RATE"] = (globalMethods.ConvertDecimal(r["RATE"]) * globalMethods.ConvertDecimal(r["invoice_quantity"])) - globalMethods.ConvertDecimal(r["DISCOUNT_AMOUNT"]);
                            Decimal nNet = 0;
                            if (globalMethods.ConvertDecimal(r["discount_amount"]) > 0)
                                nNet = (globalMethods.ConvertDecimal(r["discount_amount"]) / globalMethods.ConvertDecimal(r["INVOICE_QUANTITY"]));
                            //if (cRoundOff_Item_At == "1")
                            nNet = Math.Round(nNet);
                            r["NET_RATE"] = globalMethods.ConvertDecimal(r["RATE"]) - nNet;

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
                        dr["discount_amount"] = Math.Round(globalMethods.ConvertDecimal(dr["discount_amount"]), MidpointRounding.AwayFromZero);
                        //dr["NET_RATE"] = Math.Round((globalMethods.ConvertDecimal(dr["invoice_quantity"]) * globalMethods.ConvertDecimal(dr["RATE"])) - globalMethods.ConvertDecimal(dr["basic_discount_amount"]), 2);
                        //dr["NET_RATE"] = Math.Round((globalMethods.ConvertDecimal(dr["invoice_quantity"]) * globalMethods.ConvertDecimal(dr["RATE"]))) - globalMethods.ConvertDecimal(dr["discount_amount"]);
                        Decimal nNet = 0;
                        if (globalMethods.ConvertDecimal(dr["discount_amount"]) > 0)
                            nNet = (globalMethods.ConvertDecimal(dr["discount_amount"]) / globalMethods.ConvertDecimal(dr["INVOICE_QUANTITY"]));
                        //if (cRoundOff_Item_At == "1")
                        nNet = Math.Round(nNet);
                        dr["NET_RATE"] = globalMethods.ConvertDecimal(dr["RATE"]) - nNet;
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
                r["INMDISCOUNTAMOUNT"] = 0;
            });

            if (nCmmDiscount == 0)
                return "";

            decimal nSubtotal, nSlsSubtotal, nSlrSubtotal;

            commonMethods globalMethods = new commonMethods();

            nSubtotal = globalMethods.ConvertDecimal(dtCmd.Compute("SUM(net_rate)", ""));
            nSlsSubtotal = globalMethods.ConvertDecimal(dtCmd.Compute("SUM(net_rate)", "invoice_quantity>0"));
            nSlrSubtotal = globalMethods.ConvertDecimal(dtCmd.Compute("SUM(net_rate)", "invoice_quantity<0"));

            if (nSubtotal > 0)
            {

                dtCmd.AsEnumerable().ToList().ForEach(r =>
                {
                    if (globalMethods.ConvertDecimal(r["invoice_quantity"]) > 0)
                        r["INMDISCOUNTAMOUNT"] = Math.Round((globalMethods.ConvertDecimal(r["NET_RATE"]) * nCmmDiscount) / nSlsSubtotal, 2);

                });
            }
            else
            {
                dtCmd.AsEnumerable().ToList().ForEach(r =>
                {
                    if (globalMethods.ConvertDecimal(r["invoice_quantity"]) < 0)
                        r["INMDISCOUNTAMOUNT"] = Math.Round((globalMethods.ConvertDecimal(r["NET_RATE"]) * nCmmDiscount) / nSlrSubtotal, 2);

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
                drGst["RATE"] = dr["RATE"];
                drGst["invoice_quantity"] = dr["invoice_quantity"];
                drGst["net_value"] = globalMethods.ConvertDecimal(dr["NET_RATE"]) - globalMethods.ConvertDecimal(dr["INMDISCOUNTAMOUNT"]);
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

                dtHsnMstFiltered.AsEnumerable().ToList().ForEach(r =>
                {
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
                    dr["net_value"] = (globalMethods.ConvertInt(dr["GST_CAL_BASIS"]) == 2 ? Convert.ToInt32(dr["RATE"]) * Convert.ToInt32(dr["invoice_quantity"]) : dr["net_value"]);
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
                            if ((globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) < (globalMethods.ConvertDecimal(r["NET_VALUE_WOTAX"]) / (globalMethods.ConvertDecimal(r["invoice_quantity"]))))
                                 || globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) == 0)
                                r["GST_PERCENTAGE"] = globalMethods.ConvertDecimal(r["tax_percentage"]);
                            else
                                r["GST_PERCENTAGE"] = globalMethods.ConvertDecimal(r["RATE_CUTOFF_TAX_PERCENTAGE"]);
                        });
                    }

                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        if ((globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) < (globalMethods.ConvertDecimal(r["NET_VALUE_WOTAX"]) / Math.Abs(globalMethods.ConvertDecimal(r["invoice_quantity"]))))
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
                r["rfnet"] = globalMethods.ConvertDecimal(r["NET_RATE"]) - globalMethods.ConvertDecimal(r["INMDISCOUNTAMOUNT"]) + (globalMethods.ConvertInt(r["tax_method"]) == 2 ?
                globalMethods.ConvertDecimal(r["igst_amount"]) + globalMethods.ConvertDecimal(r["cgst_amount"]) + globalMethods.ConvertDecimal(r["sgst_amount"]) +
                globalMethods.ConvertDecimal(r["gst_cess_amount"]) : 0);

                if (nAtdCharges == 0)
                    r["rfnet_with_other_charges"] = r["rfnet"];
                else
                    r["rfnet_with_other_charges"] = globalMethods.ConvertDecimal(r["rfnet"]) + ((nAtdCharges / nSubtotal) * globalMethods.ConvertDecimal(r["NET_RATE"]));
            });



            cMessage = CalcGstOC(ref dtCmm, dtCmd, bRegisteredDealer, cCurStateCode, cPartyStateCode, tConfig);

            return cMessage;
        }

        public string CalcGst_PackSize(SqlConnection conn, ref DataTable dtCmm, ref DataTable dtCmd, DataTable tConfig, DataTable dtHsnMst, DataTable dtHsnDet,
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
                drGst["RATE"] = dr["RATE"];
                drGst["invoice_quantity"] = dr["invoice_quantity"];
                drGst["net_value"] = Math.Round(globalMethods.ConvertDecimal(dr["NET_RATE"]) / globalMethods.ConvertDecimal(dr["ARTICLE_PACK_SIZE"]), 2) - (globalMethods.ConvertDecimal(dr["INMDISCOUNTAMOUNT"]) / globalMethods.ConvertDecimal(dr["ARTICLE_PACK_SIZE"]));
                drGst["cgst_amount"] = 0;
                drGst["sgst_amount"] = 0;
                drGst["igst_amount"] = 0;
                drGst["cess_amount"] = 0;
                drGst["gst_cess_amount"] = 0;
                drGst["gst_percentage"] = 0;
                drGst["tax_method"] = (globalMethods.ConvertInt(dr["tax_method"]) == 0 ? 1 : dr["tax_method"]);
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

                dtHsnMstFiltered.AsEnumerable().ToList().ForEach(r =>
                {
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
                    dr["net_value"] = (globalMethods.ConvertInt(dr["GST_CAL_BASIS"]) == 2 ? Convert.ToInt32(dr["RATE"]) * Convert.ToInt32(dr["invoice_quantity"]) : dr["net_value"]);
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
                            if ((globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) < (globalMethods.ConvertDecimal(r["NET_VALUE_WOTAX"]) / (globalMethods.ConvertDecimal(r["invoice_quantity"]))))
                                 || globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) == 0)
                                r["GST_PERCENTAGE"] = globalMethods.ConvertDecimal(r["tax_percentage"]);
                            else
                                r["GST_PERCENTAGE"] = globalMethods.ConvertDecimal(r["RATE_CUTOFF_TAX_PERCENTAGE"]);
                        });
                    }

                    dtGstCalc.AsEnumerable().ToList().ForEach(r =>
                    {
                        if ((globalMethods.ConvertDecimal(r["RATE_CUTOFF"]) < (globalMethods.ConvertDecimal(r["NET_VALUE_WOTAX"]) / Math.Abs(globalMethods.ConvertDecimal(r["invoice_quantity"]))))
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
                r["rfnet"] = globalMethods.ConvertDecimal(r["NET_RATE"]) - globalMethods.ConvertDecimal(r["INMDISCOUNTAMOUNT"]) + (globalMethods.ConvertInt(r["tax_method"]) == 2 ?
                globalMethods.ConvertDecimal(r["igst_amount"]) + globalMethods.ConvertDecimal(r["cgst_amount"]) + globalMethods.ConvertDecimal(r["sgst_amount"]) +
                globalMethods.ConvertDecimal(r["gst_cess_amount"]) : 0);

                if (nAtdCharges == 0)
                    r["rfnet_with_other_charges"] = r["rfnet"];
                else
                    r["rfnet_with_other_charges"] = globalMethods.ConvertDecimal(r["rfnet"]) + ((nAtdCharges / nSubtotal) * globalMethods.ConvertDecimal(r["NET_RATE"]));
            });



            cMessage = CalcGstOC(ref dtCmm, dtCmd, bRegisteredDealer, cCurStateCode, cPartyStateCode, tConfig);

            return cMessage;
        }
        private string applyBillLevelScheme(ref DataTable dtCmm, DataTable dtCmd, DataTable dtSlabs, DataRow drSchemeDet)
        {
            decimal nNetValue, nSchemeAppliedQty, nMrp, nQty, nDiscountFigure, nDiscountAmount = 0, nDiscountPercentage = 0;
            string cProductCode, filterTableData;

            commonMethods globalMethods = new commonMethods();

            // No need to apply Bill level scheme If already applied or Ecoupon is there
            if (!String.IsNullOrEmpty(dtCmm.Rows[0]["ecoupon_id"].ToString()))
                return "";

            dtCmm.Rows[0]["discount_percentage"] = 0;
            dtCmm.Rows[0]["discount_amount"] = 0;

            string cSchemeName = drSchemeDet["schemeName"].ToString();
            string cSchemeRowId = drSchemeDet["schemeRowId"].ToString();

            filterTableData = " schemeRowId='" + cSchemeRowId + "'";
            DataRow drSlabs = dtSlabs.Rows[0];

            int nBuyType = Convert.ToInt32(drSchemeDet["buyType"]);


            Decimal totals = 0, maxQtySlab = 0;
            foreach (DataRow dr in dtCmd.Rows)
            {
                if (nBuyType == 1)
                    totals = totals + globalMethods.ConvertDecimal(dr["invoice_quantity"]);
                else
                    totals = totals + ((globalMethods.ConvertDecimal(dr["RATE"]) * globalMethods.ConvertDecimal(dr["invoice_quantity"])) -
                                       globalMethods.ConvertDecimal(dr["basic_discount_amount"]) - globalMethods.ConvertDecimal(dr["card_discount_amount"]) -
                                       globalMethods.ConvertDecimal(dr["manual_Discount_amount"]));

            }

            maxQtySlab = globalMethods.ConvertDecimal(dtSlabs.Compute("max(buyToRange)", ""));

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
            }
            else if (globalMethods.ConvertDecimal(dtFilteredSlab.Rows[0]["discountAmount"]) > 0)
            {
                dtCmm.Rows[0]["discount_amount"] = dtFilteredSlab.Rows[0]["discountAmount"];

            }

            return "";

        }

        private string UpdateBNGNWtdAvgDisc(ref DataTable dtCmdSchemes, String cSlabRowId, bool donot_distribute_weighted_avg_disc_bngn)
        {
            string msg = "";
            try
            {

                DataTable dtBnGnCmd = new DataTable();
                commonMethods globalMethods = new commonMethods();

                var bngnRowIds = dtCmdSchemes.AsEnumerable()
                        .Select(row => row.Field<string>("BuynGetnRowId") ?? "").Distinct()
                        .ToList();

                foreach (var bngnRowId in bngnRowIds)
                {

                    if (string.IsNullOrEmpty(bngnRowId.ToString()))
                        continue;

                    dtBnGnCmd = dtCmdSchemes.Clone();
                    DataRow[] drowBnGn = dtCmdSchemes.Select("slabRowId='" + cSlabRowId + "' and addnlBnGnDiscount=false and BuynGetnRowId='" + bngnRowId.ToString() + "'", "");
                    if (drowBnGn.Length > 0)
                        dtBnGnCmd = drowBnGn.CopyToDataTable();

                    //Have to do this looping because compute on multiplication of 2 column values does not work
                    decimal GrossSale = 0, totalDiscount = 0;
                    foreach (DataRow dr in dtBnGnCmd.Rows)
                    {
                        GrossSale = GrossSale + (globalMethods.ConvertDecimal(dr["RATE"]) * globalMethods.ConvertDecimal(dr["invoice_quantity"]));
                        totalDiscount = totalDiscount + globalMethods.ConvertDecimal(dr["discount_amount"]);
                    }

                    decimal NetSale = GrossSale - totalDiscount;

                    decimal nTOTWTDDISC = 0, nWtdDisc = 0, nTotSchemeDisc = 0;
                    String cBnGnRowId = "";
                    foreach (DataRow dr in dtBnGnCmd.Rows)
                    {
                        if (dr["slabRowId"].ToString() == cSlabRowId)
                        {
                            nWtdDisc = Math.Round((globalMethods.ConvertDecimal(dr["RATE"]) * globalMethods.ConvertDecimal(dr["invoice_quantity"]) * ((GrossSale - NetSale) / GrossSale)), 2);
                            dr["WEIGHTED_AVG_DISC_AMT"] = nWtdDisc;
                            nTOTWTDDISC = nTOTWTDDISC + nWtdDisc;
                            nTotSchemeDisc = nTotSchemeDisc + globalMethods.ConvertDecimal(dr["discount_amount"]);
                            dr["WEIGHTED_AVG_DISC_PCT"] = Math.Round((nWtdDisc / (globalMethods.ConvertDecimal(dr["RATE"]) * globalMethods.ConvertDecimal(dr["invoice_quantity"]))) * 100, 3);
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
                             r["WEIGHTED_AVG_DISC_AMT"] = globalMethods.ConvertDecimal(r["WEIGHTED_AVG_DISC_AMT"]) + nTotSchemeDisc - nTOTWTDDISC;
                         });
                    }


                    if (!donot_distribute_weighted_avg_disc_bngn)
                    {
                        dtBnGnCmd.AsEnumerable().ToList().ForEach(r =>
                        {
                            r["discount_percentage"] = r["WEIGHTED_AVG_DISC_PCT"];
                            r["discount_amount"] = r["WEIGHTED_AVG_DISC_AMT"];
                            r["NET_RATE"] = Math.Round((globalMethods.ConvertDecimal(r["invoice_quantity"]) * globalMethods.ConvertDecimal(r["RATE"])) - globalMethods.ConvertDecimal(r["basic_discount_amount"]), 2);
                            Decimal nNet = 0;
                            if (globalMethods.ConvertDecimal(r["discount_amount"]) > 0)
                                nNet = (globalMethods.ConvertDecimal(r["discount_amount"]) / globalMethods.ConvertDecimal(r["INVOICE_QUANTITY"]));
                            //if (cRoundOff_Item_At == "1")
                                nNet = Math.Round(nNet);
                            r["NET_RATE"] = globalMethods.ConvertDecimal(r["RATE"]) - nNet;
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
                o.lMaster.SetField("discount_percentage", globalMethods.ConvertDecimal(o.lChild["discount_percentage"]));
                o.lMaster.SetField("discount_amount", globalMethods.ConvertDecimal(o.lChild["discount_amount"]));
                o.lMaster.SetField("NET_RATE", globalMethods.ConvertDecimal(o.lChild["NET_RATE"]));
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
                    o.lMaster.SetField("discount_percentage", (globalMethods.ConvertDecimal(o.lChild["discount_percentage"])));
                    o.lMaster.SetField("discount_amount", (globalMethods.ConvertDecimal(o.lChild["discount_amount"])));
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

                nPendingSchemeApplyAmount = globalMethods.ConvertDecimal(drCmd["pending_scheme_apply_amount"]);
                nQty = globalMethods.ConvertDecimal(drCmd["invoice_quantity"]);
                nBaseValue = Math.Abs(nPendingSchemeApplyAmount) * (nQty > 0 ? 1 : -1);
                nMrp = globalMethods.ConvertDecimal(drCmd["RATE"]);

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

                    nDiscountAmount = (nDiscountFigure > nMrp ? nMrp : nMrp - nDiscountFigure);
                    nDiscMethod = 3;
                }

                nDiscountAmount = Math.Abs(nDiscountAmount * nAppliedQty) * (nQty < 0 ? -1 : 1);

                drCmd["discount_amount"] = globalMethods.ConvertDecimal(drCmd["discount_amount"]) + nDiscountAmount;

                nDiscountPercentage = (nDiscMethod == 1 ? nDiscountFigure : Math.Round((globalMethods.ConvertDecimal(drCmd["discount_amount"]) / (nMrp * nQty)) * 100, 3));

                if (globalMethods.ConvertDecimal(drCmd["discount_amount"]) == 0)
                {
                    drCmd["discount_percentage"] = Math.Abs(nDiscountPercentage);
                    drCmd["slsdet_row_id"] = cSchemeRowId;
                    drCmd["NET_RATE"] = nNetValue;
                    drCmd["scheme_applied_amount"] = globalMethods.ConvertDecimal(drCmd["scheme_applied_amount"]) + nDiscountAmount;
                }
                else
                {

                    drCmd["scheme_applied_amount"] = globalMethods.ConvertDecimal(drCmd["scheme_applied_amount"]) + nDiscountAmount;
                    drCmd["discount_percentage"] = Math.Abs(Math.Round((globalMethods.ConvertDecimal(drCmd["discount_amount"]) / (nMrp * nQty)) * 100, 3));
                    drCmd["NET_RATE"] = (nMrp * nQty) - globalMethods.ConvertDecimal(drCmd["discount_amount"]);
                    drCmd["slsdet_row_id"] = cSchemeRowId;
                }
            }

            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);
                return "Error in applyDiscountasPerMethod at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();

            }

            return "";
        }

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

                nMrp = globalMethods.ConvertDecimal(drDetail["RATE"]);
                nQty = globalMethods.ConvertDecimal(drDetail["invoice_quantity"]);
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
                    else if (globalMethods.ConvertDecimal(dtBarCodeInfo.Rows[0]["flat_netPrice"]) > 0)
                    {
                        nDiscountFigure = globalMethods.ConvertDecimal(dtBarCodeInfo.Rows[0]["flat_netPrice"]);

                        nNetValue = (nDiscountFigure > nMrpValue ? 0 : nDiscountFigure);
                        nDiscMethod = 3;
                    }

                    nDiscountAmount = (nMrpValue - nNetValue);


                    nDiscountPercentage = (nDiscMethod == 1 ? nDiscountFigure : (nDiscountAmount / (nMrpValue)) * 100);

                    drDetail["discount_percentage"] = nDiscountPercentage;
                    drDetail["discount_amount"] = nDiscountAmount;
                    drDetail["slsdet_row_id"] = dtBarCodeInfo.Rows[0]["schemeRowId"];
                    drDetail["NET_RATE"] = nNetValue;
                    drDetail["scheme_applied_amount"] = nMrpValue;
                    drDetail["scheme_applied_qty"] = drDetail["invoice_quantity"];

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
                dr["scheme_applied_amount"] = globalMethods.ConvertDecimal(dr["scheme_applied_amount"]) + (globalMethods.ConvertDecimal(dr["scheme_applied_qty"]) * globalMethods.ConvertDecimal(dr["RATE"]));

                dr["pending_scheme_apply_qty"] = Math.Abs(globalMethods.ConvertDecimal(dr["invoice_quantity"]) - globalMethods.ConvertDecimal(dr["scheme_applied_qty"]));
                dr["pending_scheme_apply_amount"] = Math.Abs((globalMethods.ConvertDecimal(dr["invoice_quantity"]) *
                    globalMethods.ConvertDecimal(dr["RATE"])) - globalMethods.ConvertDecimal(dr["scheme_applied_amount"]));

                dr["scheme_name"] = cSchemeName;
                dr["slsdet_row_id"] = cSchemeRowId;
                dr["addnlBnGnDiscount"] = true;

                if (nItemsCnt >= nGetQty)
                    break;
            }

            synchCmdSchemes(dtcmdSchemesGet, ref dtCmdSchemes);

            return "";
        }

        private string applyEossFlatDiscount(ref DataTable dtCmdSchemes, DataRow drSchemeDet, DataTable dtSkuNames, string cCmdRowId = "")
        {
            try
            {

                decimal nNetValue, nSchemeAppliedQty, nMrp, nQty, nDiscountFigure, nDiscountAmount = 0, nDiscountPercentage = 0;
                string cProductCode, filterTableData;
                int nDiscMethod;

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
                    cProductCode = drDetail["product_code"].ToString();

                    nMrp = globalMethods.ConvertDecimal(drDetail["RATE"]);
                    nQty = globalMethods.ConvertDecimal(drDetail["invoice_quantity"]);
                    decimal nMrpValue = Math.Round(nMrp * nQty, 2);

                    nSchemeAppliedQty = globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]);
                    nNetValue = 0;
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
                    else if (globalMethods.ConvertDecimal(drSku[0]["flat_netPrice"]) > 0)
                    {
                        nDiscountFigure = globalMethods.ConvertDecimal(drSku[0]["flat_netPrice"]);

                        nNetValue = (nDiscountFigure > Math.Abs(nMrpValue) ? 0 : nDiscountFigure * (nMrpValue < 0 ? -1 : 1));
                        nDiscMethod = 3;
                    }

                    nDiscountAmount = (nMrpValue - nNetValue);


                    nDiscountPercentage = (nDiscMethod == 1 ? nDiscountFigure : (nDiscountAmount / (nMrpValue)) * 100);

                    if (nSchemeAppliedQty == 0)
                    {
                        drDetail["discount_percentage"] = nDiscountPercentage;
                        drDetail["discount_amount"] = nDiscountAmount;
                        drDetail["slsdet_row_id"] = drSchemeDet["schemeRowId"];
                        //drDetail["NET_RATE"] = nNetValue;
                        Decimal nNet = 0;
                        if (globalMethods.ConvertDecimal(drDetail["discount_amount"]) > 0)
                            nNet = (globalMethods.ConvertDecimal(drDetail["discount_amount"]) / globalMethods.ConvertDecimal(drDetail["INVOICE_QUANTITY"]));
                        //if (cRoundOff_Item_At == "1")
                        nNet = Math.Round(nNet);
                        drDetail["NET_RATE"] = globalMethods.ConvertDecimal(drDetail["RATE"]) - nNet;
                        if (string.IsNullOrEmpty(cCmdRowId))
                        {
                            drDetail["scheme_applied_amount"] = globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]) + nDiscountAmount;
                            drDetail["scheme_applied_qty"] = drDetail["invoice_quantity"];
                        }
                    }
                    else
                    {
                        drDetail["discount_amount"] = globalMethods.ConvertDecimal(drDetail["discount_amount"]) + nDiscountAmount;

                        drDetail["discount_percentage"] = Math.Round((globalMethods.ConvertDecimal(drDetail["discount_amount"]) / nMrpValue) * 100, 3);
                        //drDetail["NET_RATE"] = (nMrp * nQty) - globalMethods.ConvertDecimal(drDetail["discount_amount"]);
                        Decimal nNet = 0;
                        if (globalMethods.ConvertDecimal(drDetail["discount_amount"]) > 0)
                            nNet = (globalMethods.ConvertDecimal(drDetail["discount_amount"]) / globalMethods.ConvertDecimal(drDetail["INVOICE_QUANTITY"]));
                        //if (cRoundOff_Item_At == "1")
                        nNet = Math.Round(nNet);
                        drDetail["NET_RATE"] = globalMethods.ConvertDecimal(drDetail["RATE"]) - nNet;
                        if (string.IsNullOrEmpty(cCmdRowId))
                        {
                            drDetail["scheme_applied_amount"] = globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]) + nDiscountAmount;
                            drDetail["scheme_applied_qty"] = drDetail["invoice_quantity"];
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
                                            Int32 nSlrDiscountMode = 2, bool bDonotApplyHappyHours = false)
        {

            try
            {
                int nItemsLoop = 1, nProcessLoop = 1;
                if (nSlrDiscountMode >= 2)
                    nItemsLoop = 2;

                commonMethods globalMethods = new commonMethods();
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
                        cmdFilter = cmdFilter + " and invoice_quantity<0";
                    else
                        cmdFilter = cmdFilter + " and invoice_quantity>0";
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
                DateTime dXnDt = Convert.ToDateTime(dtCmm.Rows[0]["inv_dt"]);

                DataSet dsSchemeInfo = new DataSet();

                dsSchemeInfo.Tables.Add(tEossSchemes);
                dsSchemeInfo.Tables.Add(tEossSlabs);
                dsSchemeInfo.Tables.Add(tBcScheme);

                dsSchemeInfo.Tables[0].TableName = "schemeDetails";
                dsSchemeInfo.Tables[1].TableName = "slabDetails";
                dsSchemeInfo.Tables[2].TableName = "skuNames";


                if (dtCmdSchemes.Rows.Count == 0)
                    goto lblNextStep;

                string retMsgFromSchemeMethod = "";

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
                    filterSchemes = filterSchemes + " AND buytype=1";


                DataRow[] drSchemes = dsSchemeInfo.Tables[0].Select(filterSchemes, "");
                if (drSchemes.Length == 0)
                    return "";


                dtSchemeTitles = dsSchemeInfo.Tables[0].Select(filterSchemes, "").CopyToDataTable();

                String cSchemeName;
                Int32 nSchemeMode, nBuyType, nGetType;
                DataTable dFSlabsDetails = new DataTable();

                dtSchemeTitles.DefaultView.Sort = "titleProcessingOrder desc";

                DataTable dtSortedSchemes = dtSchemeTitles.DefaultView.ToTable();


                DataTable dtSlabs = dsSchemeInfo.Tables["slabDetails"].Clone();

                foreach (DataRow drTitles in dtSortedSchemes.Rows)
                {

                    cSchemeRowId = drTitles["schemeRowId"].ToString();
                    cSchemeName = drTitles["schemeName"].ToString();
                    nSchemeMode = Convert.ToInt32(drTitles["schemeMode"]);


                    if (nSchemeMode == 2)
                    {
                        //Call Flat discount Logic

                        retMsgFromSchemeMethod = applyEossFlatDiscount(ref dtCmdSchemes, drTitles, dsSchemeInfo.Tables["skuNames"], cCmdRowId);
                        if (!string.IsNullOrEmpty(retMsgFromSchemeMethod))
                            return retMsgFromSchemeMethod;

                        goto lblNextScheme;
                    }


                    // Need to commit the datatable changes before applying the next scheme
                    // as there may be need to rollback the changes within Buy n Get n scheme in case of failure/mismatch
                    dtCmdSchemes.AcceptChanges();

                    if (!(nProcessLoop == 1 && nItemsLoop == 2))
                    {
                        int nApplicableLevel = Convert.ToInt32(drTitles["schemeApplicableLevel"]);

                        if (nApplicableLevel == 2)
                        {
                            //Call Bill Level scheme of Buy More Pay Less Logic
                            dFSlabsDetails = dsSchemeInfo.Tables["slabDetails"].Select("schemeRowId='" + cSchemeRowId + "'", "").CopyToDataTable();

                            retMsgFromSchemeMethod = applyBillLevelScheme(ref dtCmm, dtCmdSchemes, dFSlabsDetails, drTitles);
                            goto lblNextScheme;
                        }
                    }



                    string filterSlabs = "schemeRowId='" + cSchemeRowId + "'";


                    if (bApplyFlatschemesOnly)
                        filterSlabs = filterSlabs + " AND gettype=3";

                    if (dsSchemeInfo.Tables["slabDetails"].Select(filterSlabs, "").Length > 0)
                        dtSlabs = dsSchemeInfo.Tables["slabDetails"].Select(filterSlabs, "").CopyToDataTable();

                    if (!dtSlabs.Columns.Contains("setValue"))
                        dtSlabs.Columns.Add("setValue", typeof(decimal));

                    dtSlabs.AsEnumerable().ToList().ForEach(r =>
                    {
                        r["setValue"] = globalMethods.ConvertDecimal(r["buyFromRange"]) + globalMethods.ConvertDecimal(r["getQty"]);
                    });

                    dtSlabs.DefaultView.Sort = "setValue desc";

                    DataTable dtSortedSlabs = dtSlabs.DefaultView.ToTable();

                    foreach (DataRow drSlabs in dtSortedSlabs.Rows)
                    {
                        nBuyType = Convert.ToInt32(drTitles["buyType"]);
                        nGetType = Convert.ToInt32(drSlabs["getType"]);
                        if (nBuyType == 2 && nGetType == 2 && globalMethods.ConvertDecimal(drSlabs["discountAmount"]) > 0)
                            retMsgFromSchemeMethod = applyEossBnGnAmount(ref dtCmdSchemes, drSlabs, drTitles, dsSchemeInfo.Tables["skuNames"]);
                        else
                            retMsgFromSchemeMethod = applyEossRangeBased(ref dtCmdSchemes, drSlabs, drTitles, dsSchemeInfo.Tables["skuNames"]);

                        if (!String.IsNullOrEmpty(retMsgFromSchemeMethod))
                            return retMsgFromSchemeMethod;
                    }

                lblNextScheme:
                    dFSlabsDetails.Rows.Clear();

                    // No need to proceed further if No pending bar code is left for Eoss applying
                    string filterTableData = " (invoice_quantity" + (/*AppConfigModel.*/EossReturnItemsProcessing ? "*-1)" : ")") +
                        "- scheme_applied_qty>0 or (rate*invoice_quantity" + (/*AppConfigModel.*/EossReturnItemsProcessing ? "*-1)" : ")") + "-scheme_applied_amount>0";
                    DataRow[] drPending = dtCmdSchemes.Select(filterTableData, "");
                    if (drPending.Length <= 0)
                        break;
                }


            lblNextStep:
                // Check for applying Max discount in case of Return Items by comparing Eoss discount and Last sold discount
                if (nProcessLoop == 2 && nSlrDiscountMode == 2 && !bApplyFlatschemesOnly)
                {

                    decimal nLastSoldDiscPct, nEossDiscPct;
                    foreach (DataRow dr in dtCmdSchemes.Rows)
                    {
                        nEossDiscPct = (globalMethods.ConvertDecimal(dr["WEIGHTED_AVG_DISC_PCT"]) > 0 ? globalMethods.ConvertDecimal(dr["WEIGHTED_AVG_DISC_PCT"]) : globalMethods.ConvertDecimal(dr["discount_percentage"]));
                        nLastSoldDiscPct = globalMethods.ConvertDecimal(dtCmd.Select("row_id='" + dr["row_id"] + "'".ToString())
                                        .CopyToDataTable().Rows[0]["discount_percentage"]);

                        if (nEossDiscPct < nLastSoldDiscPct)
                        {
                            dr["scheme_name"] = "";
                            dr["slsdet_row_id"] = "";
                            dr["discount_percentage"] = Math.Abs(nLastSoldDiscPct);
                            dr["discount_amount"] = Math.Round(globalMethods.ConvertDecimal(dr["invoice_quantity"]) * globalMethods.ConvertDecimal(dr["RATE"]) * nLastSoldDiscPct / 100, 2);
                            dr["NET_RATE"] = Math.Round((globalMethods.ConvertDecimal(dr["invoice_quantity"]) * globalMethods.ConvertDecimal(dr["RATE"])) - globalMethods.ConvertDecimal(dr["discount_amount"]), 2);
                            Decimal nNet = 0;
                            if (globalMethods.ConvertDecimal(dr["discount_amount"]) > 0)
                                nNet = (globalMethods.ConvertDecimal(dr["discount_amount"]) / globalMethods.ConvertDecimal(dr["INVOICE_QUANTITY"]));
                            //if (cRoundOff_Item_At == "1")
                                nNet = Math.Round(nNet);
                            dr["NET_RATE"] = globalMethods.ConvertDecimal(dr["RATE"]) - nNet;
                        }
                    }
                }

                nProcessLoop = nProcessLoop + 1;

                retMsgFromSchemeMethod = NormalizeCmdForEoss(ref dtCmdSchemes, true);

                if (!string.IsNullOrEmpty(retMsgFromSchemeMethod))
                    return retMsgFromSchemeMethod;

                if (nItemLevelRoundOff > 0)
                {
                    DataRow[] drSchemeRow = dtCmdSchemes.Select("scheme_name<>''", "");

                    if (drSchemeRow.Length > 0)
                        ProcessEossRoundOff(ref dtCmdSchemes, nItemLevelRoundOff);
                }




                synchCmdSchemes(dtCmdSchemes, ref dtCmd, true);

                if (nProcessLoop <= nItemsLoop)
                {
                    dsSchemeInfo.Tables.Clear();

                    goto lblStart;
                }
                if (dtCmd.Columns.Contains("WEIGHTED_AVG_DISC_AMT"))
                {
                    dtCmd.AsEnumerable().ToList().ForEach(r =>
                    {
                        r["WEIGHTED_AVG_DISC_AMT"] = (globalMethods.ConvertDecimal(r["WEIGHTED_AVG_DISC_AMT"]) == 0 ? r["discount_amount"] : r["WEIGHTED_AVG_DISC_AMT"]);
                        r["WEIGHTED_AVG_DISC_PCT"] = (globalMethods.ConvertDecimal(r["WEIGHTED_AVG_DISC_PCT"]) == 0 ? r["discount_percentage"] : r["WEIGHTED_AVG_DISC_PCT"]);
                    });
                }
            }
            catch (Exception ex)
            {
                int errLineNo = new commonMethods().GetErrorLineNo(ex);

                return "Error at Line#" + errLineNo.ToString() + " of UpdateSchemeDiscounts : " + ex.Message.ToString();
            }


        lblLast:
            return "";

        }
        private string applyEossRangeBased(ref DataTable dtCmdSchemes, DataRow drSlabs, DataRow drSchemeDet, DataTable dtSkuNames)
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

                commonMethods globalMethods = new commonMethods();

                Decimal nQty;
                // We need to do this so that we can process the scheme on the items ordered on pending scheme qty/amount desc
                foreach (DataRow dr in dtCmdSchemes.Rows)
                {
                    nQty = Math.Abs(globalMethods.ConvertDecimal(dr["invoice_quantity"])) - Math.Abs(globalMethods.ConvertDecimal(dr["scheme_applied_qty"]));
                    dr["pending_scheme_apply_qty"] = nQty;
                    dr["pending_scheme_apply_amount"] = (globalMethods.ConvertDecimal(dr["RATE"]) * Math.Abs(globalMethods.ConvertDecimal(dr["invoice_quantity"]))) - globalMethods.ConvertDecimal(dr["scheme_applied_amount"]);
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

                Decimal BuyItemsTotal = 0, GetItemsTotal = 0, AddnlGetItemsTotal = 0, nNetValue, nSchemeAppliedQty = 0, nLoopValue = 0, nAddValue = 0,
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

                filterTableData = (nBuyType == 1 ? " pending_scheme_apply_qty>0 " : " scheme_applied_amount=0 ");

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

                string cOrderColumn = (nBuyType == 1 ? "RATE" : "pending_scheme_apply_amount");

                DataView dv = dtFilteredCmdBuy.DefaultView;
                dv.Sort = cOrderColumn + " DESC ";// + (nBuyType == 1 ? " DESC " : "ASC"); //  string.Format("{0} {1}", cOrderColumn, "DES"); //sort descending

                dtFilteredCmdBuyOrdered = dv.ToTable();


                if (globalMethods.ConvertDecimal(drSlabs["discountAmount"]) != 0 && nGetType == 3)
                    bAPplyWtdDiscount = true;

                dtFilteredCmdBuyOrdered.Columns.Add("WtdDiscountBaseValue", typeof(decimal));

                foreach (DataRow drDetail in dtFilteredCmdBuyOrdered.Rows)
                {
                    if (nBuyType == 1)
                        nBaseQtyorAmount = globalMethods.ConvertDecimal(drDetail["pending_scheme_apply_qty"]);
                    else
                        nBaseQtyorAmount = globalMethods.ConvertDecimal(drDetail["pending_scheme_apply_amount"]);



                    if (nGetType == 3)
                    {
                        string cErr = "";
                        nAppliedValue = Math.Abs(((nLoopValue + nBaseQtyorAmount) > nSchemeToRange ? (nSchemeToRange - nLoopValue) : nBaseQtyorAmount));

                        if (!bAPplyWtdDiscount)
                            cErr = applyDiscountasPerMethod(drDetail, cSchemeRowId, globalMethods.ConvertDecimal(drSlabs["discountPercentage"]),
                            globalMethods.ConvertDecimal(drSlabs["discountAmount"]), globalMethods.ConvertDecimal(drSlabs["netPrice"]));
                        else
                            drDetail["WtdDiscountBaseValue"] = (nBuyType == 1 ? nBaseQtyorAmount * globalMethods.ConvertDecimal(drDetail["RATE"]) : nBaseQtyorAmount);

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
                        drDetail["scheme_applied_amount"] = globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]) +
                            (globalMethods.ConvertDecimal(drDetail["RATE"]) * nAppliedValue);

                    }
                    else
                    {
                        drDetail["scheme_applied_amount"] = globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]) + nAppliedValue;
                        drDetail["scheme_applied_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["invoice_quantity"]));
                    }


                    drDetail["pending_scheme_apply_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["invoice_quantity"])) - globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]);
                    drDetail["pending_scheme_apply_amount"] = Math.Abs((globalMethods.ConvertDecimal(drDetail["invoice_quantity"]) * globalMethods.ConvertDecimal(drDetail["RATE"])) -
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

                    drDetail["pending_scheme_apply_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["invoice_quantity"]) - globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]));
                    drDetail["pending_scheme_apply_amount"] = Math.Abs((globalMethods.ConvertDecimal(drDetail["invoice_quantity"]) *
                        globalMethods.ConvertDecimal(drDetail["RATE"])) - globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]));

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
                        //decimal nPendingQty = globalMethods.ConvertDecimal(dtCmdSchemes.Compute("sum(pending_scheme_apply_qty)", ""));
                        Decimal nPendingValue = 0;
                        if (nBuyType == 1)
                            nPendingValue = globalMethods.ConvertDecimal(dtCmdSchemes.Compute("sum(pending_scheme_apply_qty)", ""));
                        else
                            nPendingValue = globalMethods.ConvertDecimal(dtCmdSchemes.Compute("sum(pending_scheme_apply_amount)", ""));

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
                        retMsg = UpdateBNGNWtdAvgDisc(ref dtCmdSchemes, cSlabRowId, globalMethods.ConvertBool(drSchemeDet["donot_distribute_weighted_avg_disc_bngn"]));
                        if (!String.IsNullOrEmpty(retMsg))
                            goto lblEnd;

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
                                    drClubbed["invoice_quantity"] = globalMethods.ConvertDecimal(drClubbed["invoice_quantity"]) + globalMethods.ConvertDecimal(dr["invoice_quantity"]);
                                    drClubbed["discount_amount"] = globalMethods.ConvertDecimal(drClubbed["discount_amount"]) +
                                    globalMethods.ConvertDecimal(dr["discount_amount"]);
                                    drClubbed["WEIGHTED_AVG_DISC_AMT"] = globalMethods.ConvertDecimal(drClubbed["WEIGHTED_AVG_DISC_AMT"]) +
                                    globalMethods.ConvertDecimal(dr["WEIGHTED_AVG_DISC_AMT"]);

                                    //drClubbed["DISCOUNT_AMOUNT"] = globalMethods.ConvertDecimal(drClubbed["BASIC_DISCOUNT_AMOUNT"]) +
                                    //globalMethods.ConvertDecimal(drClubbed["CARD_DISCOUNT_AMOUNT"]);

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
                            dr["discount_percentage"] = Math.Abs(Math.Round((globalMethods.ConvertDecimal(dr["discount_amount"]) / (globalMethods.ConvertDecimal(dr["invoice_quantity"]) *
                                                              globalMethods.ConvertDecimal(dr["RATE"])) * 100), 3));

                            if (globalMethods.ConvertDecimal(dr["WEIGHTED_AVG_DISC_AMT"]) != 0)
                                dr["WEIGHTED_AVG_DISC_PCT"] = Math.Round((globalMethods.ConvertDecimal(dr["WEIGHTED_AVG_DISC_AMT"]) / (globalMethods.ConvertDecimal(dr["invoice_quantity"]) *
                                                                  globalMethods.ConvertDecimal(dr["RATE"])) * 100), 3);

                            dr["DISCOUNT_PERCENTAGE"] = Math.Round((globalMethods.ConvertDecimal(dr["DISCOUNT_AMOUNT"]) / (globalMethods.ConvertDecimal(dr["RATE"]) * globalMethods.ConvertDecimal(dr["invoice_quantity"]))) * 100, 3);

                        }

                        //dr["NET_RATE"] = (globalMethods.ConvertDecimal(dr["RATE"]) * globalMethods.ConvertDecimal(dr["invoice_quantity"])) - globalMethods.ConvertDecimal(dr["discount_amount"]);
                        Decimal nNet = 0;
                        if (globalMethods.ConvertDecimal(dr["discount_amount"]) > 0)
                            nNet = (globalMethods.ConvertDecimal(dr["discount_amount"]) / globalMethods.ConvertDecimal(dr["INVOICE_QUANTITY"]));
                        //if (cRoundOff_Item_At == "1")
                        nNet = Math.Round(nNet);
                        dr["NET_RATE"] = globalMethods.ConvertDecimal(dr["RATE"]) - nNet;

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
                    //dr["basic_discount_percentage"] = 0;
                    //dr["basic_discount_amount"] = 0;
                    dr["discount_percentage"] = 0;
                    dr["discount_amount"] = 0;
                    dr["WEIGHTED_AVG_DISC_AMT"] = 0;
                    dr["WEIGHTED_AVG_DISC_PCT"] = 0;
                    //dr["NET_RATE"] = globalMethods.ConvertDecimal(dr["RATE"]) * globalMethods.ConvertDecimal(dr["invoice_quantity"]);
                    Decimal nNet = 0;
                    if (globalMethods.ConvertDecimal(dr["discount_amount"]) > 0)
                        nNet = (globalMethods.ConvertDecimal(dr["discount_amount"]) / globalMethods.ConvertDecimal(dr["INVOICE_QUANTITY"]));
                    //if (cRoundOff_Item_At == "1")
                        nNet = Math.Round(nNet);
                    dr["NET_RATE"] = globalMethods.ConvertDecimal(dr["RATE"]) - nNet;
                    dr["scheme_name"] = "";
                    dr["scheme_applied_qty"] = 0;
                    dr["scheme_applied_amount"] = 0;
                    dr["pending_scheme_apply_qty"] = 0;
                    dr["pending_scheme_apply_amount"] = 0;
                    dr["addnlBnGnDiscount"] = false;
                    dr["cmdRowId"] = dr["row_id"];
                    if (globalMethods.ConvertDecimal(dr["invoice_quantity"]) > 1)
                    {
                        cOrgRowId = dr["row_id"].ToString();
                        nCmdQty = globalMethods.ConvertDecimal(dr["invoice_quantity"]);
                        nLoopQty = Math.Abs(nCmdQty);
                        while (nLoopQty > 0)
                        {
                            nRowNo = nRowNo + 1;
                            //dtCmdSchemesNormalized.Rows.Add(dr);

                            dtCmdSchemesNormalized.ImportRow(dr);

                            dtCmdSchemesNormalized.Rows[nRowNo]["invoice_quantity"] = (nLoopQty >= 1 ? 1 : nLoopQty) * (nCmdQty >= 1 ? 1 : nCmdQty);
                            //dtCmdSchemesNormalized.Rows[nRowNo]["NET_RATE"] = globalMethods.ConvertDecimal(dr["RATE"]) * (nCmdQty >= 1 ? 1 : nCmdQty);
                            nNet = 0;
                            if (globalMethods.ConvertDecimal(dtCmdSchemesNormalized.Rows[nRowNo]["discount_amount"]) > 0)
                                nNet = (globalMethods.ConvertDecimal(dtCmdSchemesNormalized.Rows[nRowNo]["discount_amount"]) / globalMethods.ConvertDecimal(dtCmdSchemesNormalized.Rows[nRowNo]["INVOICE_QUANTITY"]));
                            //if (cRoundOff_Item_At == "1")
                            nNet = Math.Round(nNet);
                            dtCmdSchemesNormalized.Rows[nRowNo]["NET_RATE"] = globalMethods.ConvertDecimal(dtCmdSchemesNormalized.Rows[nRowNo]["RATE"]) - nNet;
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


                decimal nMrpValue = globalMethods.ConvertDecimal(dtBuyItems.Select("isnull(WtdDiscountBaseValue,0)>0", "").CopyToDataTable().Compute("sum(net_rate)", ""));

                dtBuyItems.AsEnumerable().ToList().ForEach(r =>
                {
                    if (globalMethods.ConvertDecimal(r["WtdDiscountBaseValue"]) > 0)
                    {
                        r["discount_amount"] = globalMethods.ConvertDecimal(r["discount_amount"]) + Math.Round((nDiscountAmount / nMrpValue) *
                        globalMethods.ConvertDecimal(r["WtdDiscountBaseValue"]), 2);
                        r["discount_percentage"] = Math.Abs(Math.Round((globalMethods.ConvertDecimal(r["discount_amount"]) / (globalMethods.ConvertDecimal(r["RATE"]) *
                            globalMethods.ConvertDecimal(r["invoice_quantity"]))) * 100, 3));

                        //r["NET_RATE"] = ((globalMethods.ConvertDecimal(r["RATE"]) * globalMethods.ConvertDecimal(r["invoice_quantity"])) -
                        //globalMethods.ConvertDecimal(r["discount_amount"]));
                        Decimal nNet = 0;
                        if (globalMethods.ConvertDecimal(r["discount_amount"]) > 0)
                            nNet = (globalMethods.ConvertDecimal(r["discount_amount"]) / globalMethods.ConvertDecimal(r["INVOICE_QUANTITY"]));
                        //if (cRoundOff_Item_At == "1")
                        nNet = Math.Round(nNet);
                        r["NET_RATE"] = globalMethods.ConvertDecimal(r["RATE"]) - nNet;


                    }
                }
                );

                decimal nCalcDiscount = globalMethods.ConvertDecimal(dtBuyItems.Select("isnull(WtdDiscountBaseValue,0)>0", "").CopyToDataTable().Compute("sum(discount_amount)", ""));
                if (nCalcDiscount != nDiscountAmount)
                {
                    string cRowId = dtBuyItems.Select("isnull(WtdDiscountBaseValue,0)>0", "").CopyToDataTable().Rows[0]["row_id"].ToString();
                    dtBuyItems.Select("row_id='" + cRowId + "'", "").AsEnumerable().ToList().ForEach(r =>
                    {
                        r["discount_amount"] = globalMethods.ConvertDecimal(r["discount_amount"]) + nDiscountAmount - nCalcDiscount;
                    });
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
                    nQty = Math.Abs(globalMethods.ConvertDecimal(dr["invoice_quantity"])) - Math.Abs(globalMethods.ConvertDecimal(dr["scheme_applied_qty"]));
                    dr["pending_scheme_apply_qty"] = nQty;
                    dr["pending_scheme_apply_amount"] = (globalMethods.ConvertDecimal(dr["RATE"]) * Math.Abs(globalMethods.ConvertDecimal(dr["invoice_quantity"]))) - globalMethods.ConvertDecimal(dr["scheme_applied_amount"]);
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
                    drDetail["scheme_applied_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["invoice_quantity"]));

                    drDetail["pending_scheme_apply_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["invoice_quantity"])) - globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]);
                    drDetail["pending_scheme_apply_amount"] = Math.Abs((globalMethods.ConvertDecimal(drDetail["invoice_quantity"]) * globalMethods.ConvertDecimal(drDetail["RATE"])) -
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
                    drDetail["scheme_applied_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["invoice_quantity"]));

                    drDetail["discount_amount"] = nAppliedValue;
                    drDetail["discount_percentage"] = Math.Round((globalMethods.ConvertDecimal(drDetail["discount_amount"]) /
                        (globalMethods.ConvertDecimal(drDetail["RATE"]) * globalMethods.ConvertDecimal(drDetail["invoice_quantity"]))) * 100, 3);

                    //drDetail["NET_RATE"] = (globalMethods.ConvertDecimal(drDetail["RATE"]) * globalMethods.ConvertDecimal(drDetail["invoice_quantity"])) - globalMethods.ConvertDecimal(drDetail["discount_amount"]);
                    Decimal nNet = 0;
                    if (globalMethods.ConvertDecimal(drDetail["discount_amount"]) > 0)
                        nNet = (globalMethods.ConvertDecimal(drDetail["discount_amount"]) / globalMethods.ConvertDecimal(drDetail["INVOICE_QUANTITY"]));
                    //if (cRoundOff_Item_At == "1")
                    nNet = Math.Round(nNet);
                    drDetail["NET_RATE"] = globalMethods.ConvertDecimal(drDetail["RATE"]) - nNet;

                    drDetail["pending_scheme_apply_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["invoice_quantity"])) - globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]);

                    drDetail["pending_scheme_apply_amount"] = Math.Abs((globalMethods.ConvertDecimal(drDetail["invoice_quantity"]) * globalMethods.ConvertDecimal(drDetail["RATE"])) -
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
                    nQty = Math.Abs(globalMethods.ConvertDecimal(dr["invoice_quantity"])) - Math.Abs(globalMethods.ConvertDecimal(dr["scheme_applied_qty"]));
                    dr["pending_scheme_apply_qty"] = nQty;
                    dr["pending_scheme_apply_amount"] = (globalMethods.ConvertDecimal(dr["RATE"]) * Math.Abs(globalMethods.ConvertDecimal(dr["invoice_quantity"]))) - globalMethods.ConvertDecimal(dr["scheme_applied_amount"]);
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

                        drDetail["pending_scheme_apply_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["invoice_quantity"]) - globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]));
                        drDetail["pending_scheme_apply_amount"] = Math.Abs((globalMethods.ConvertDecimal(drDetail["invoice_quantity"]) *
                            globalMethods.ConvertDecimal(drDetail["RATE"])) - globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]));

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
                                (globalMethods.ConvertDecimal(drDetail["RATE"]) * nAppliedValue);

                        }
                        else
                        {
                            drDetail["scheme_applied_amount"] = globalMethods.ConvertDecimal(drDetail["scheme_applied_amount"]) + nAppliedValue;
                            drDetail["scheme_applied_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["invoice_quantity"]));
                        }


                        drDetail["pending_scheme_apply_qty"] = Math.Abs(globalMethods.ConvertDecimal(drDetail["invoice_quantity"])) - globalMethods.ConvertDecimal(drDetail["scheme_applied_qty"]);
                        drDetail["pending_scheme_apply_amount"] = Math.Abs((globalMethods.ConvertDecimal(drDetail["invoice_quantity"]) * globalMethods.ConvertDecimal(drDetail["RATE"])) -
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
}
