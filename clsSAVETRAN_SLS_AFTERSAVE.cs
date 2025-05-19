using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;

namespace WOWIntegration
{
    
    public class SAVETRAN_SLS_AFTERSAVE
    {
        DataTable dtMrpCalculation, dtCalculation, dtCDBase;

        DataSet dsetSAVETRAN_SLS_AFTERSAVE = new DataSet();

        internal Int32 _NUPDATEMODE = 1;

        internal String _ConStr, _NSPID, _CMEMONOPREFIX, _CFINYEAR, _CMACHINENAME, _CWINDOWUSERNAME, _CWIZAPPUSERCODE, _CMEMOID, _CLOCID,
            _CHODEPTID, _NLOGINSPID;

        internal Boolean _BCALLEDFROMESTIMATE, _BCALLEDFROMCASHIERMODULE, _BDIALOGRESULT, _BCALLEDFROMSLSSTOCKNACONVERT, _BSISLOC, _bcheckcreditlimit, _bCalledFromBulkImport
            , _bENABLEWIZCLIP, _BTILL_ENABLED, _bServerLoc, _BSLRRECONREQD, bValidateGvThruWizclip, bValidateGvThruHO, bValidategvLocal;

        #region User_Role_and_Authorization
        internal Boolean _bALLOW_TO_MODIFY_MEMO_OF_CLOSE_TILLS;
        #endregion User_Role_and_Authorization

        #region Config_Variables
        internal Boolean _bCONSIDER_TILL_PHY_CBS_AS_OPS;
        #endregion Config_Variables

        internal DateTime _CMEMODT = new DateTime();
        public SAVETRAN_SLS_AFTERSAVE(DataSet dset, String cConStr, DateTime CMEMODT, Int32 NUPDATEMODE = 1, String NSPID = "", String CMEMONOPREFIX = "", String CFINYEAR = "", String CMACHINENAME = "",
        String CWINDOWUSERNAME = "", String CWIZAPPUSERCODE = "0000000", String CMEMOID = "", Boolean BCALLEDFROMESTIMATE = false, Boolean BCALLEDFROMCASHIERMODULE = false,
        String CLOCID = "", Boolean BDIALOGRESULT = false, Boolean BCALLEDFROMSLSSTOCKNACONVERT = false, Boolean BSISLOC = false, String NLOGINSPID = "",
         Boolean bcheckcreditlimit = false, Boolean bCalledFromBulkImport = false)
        {
            dsetSAVETRAN_SLS_AFTERSAVE = dset.Copy();
            _ConStr = cConStr;
            _NUPDATEMODE = NUPDATEMODE;
            _NSPID = NSPID;
            _CMEMONOPREFIX = CMEMONOPREFIX;
            _CFINYEAR = CFINYEAR;
            _CMACHINENAME = CMACHINENAME;
            _CWINDOWUSERNAME = CWINDOWUSERNAME;
            _CWIZAPPUSERCODE = CWIZAPPUSERCODE;
            _CMEMOID = CMEMOID;
            _CMEMODT = CMEMODT;
            _BCALLEDFROMESTIMATE = BCALLEDFROMESTIMATE;
            _BCALLEDFROMCASHIERMODULE = BCALLEDFROMCASHIERMODULE;
            _CLOCID = CLOCID;
            _BDIALOGRESULT = BDIALOGRESULT;
            _BCALLEDFROMSLSSTOCKNACONVERT = BCALLEDFROMSLSSTOCKNACONVERT;
            _BSISLOC = BSISLOC;
            _NLOGINSPID = NLOGINSPID;
            _bcheckcreditlimit = bcheckcreditlimit;
            _bCalledFromBulkImport = bCalledFromBulkImport;

            bValidateGvThruWizclip = bValidateGvThruHO = bValidategvLocal = false;

            _bALLOW_TO_MODIFY_MEMO_OF_CLOSE_TILLS = false;
        }

        void CreateTable_For_Combo()
        {
            dtMrpCalculation = new DataTable();
            dtMrpCalculation.Columns.Add("DISPLAY_MEMBER", typeof(System.String));
            dtMrpCalculation.Columns.Add("VALUE_MEMBER", typeof(System.String));

            DataRow drow = dtMrpCalculation.NewRow();
            drow["DISPLAY_MEMBER"] = "MD";
            drow["VALUE_MEMBER"] = "1";
            dtMrpCalculation.Rows.Add(drow);
            drow = dtMrpCalculation.NewRow();
            drow["DISPLAY_MEMBER"] = "MU";
            drow["VALUE_MEMBER"] = "2";
            dtMrpCalculation.Rows.Add(drow);

            dtCalculation = new DataTable();
            dtCalculation = dtMrpCalculation.Clone();
            drow = dtCalculation.NewRow();
            drow["DISPLAY_MEMBER"] = "RSP From PP";
            drow["VALUE_MEMBER"] = "1";
            dtCalculation.Rows.Add(drow);
            drow = dtCalculation.NewRow();
            drow["DISPLAY_MEMBER"] = "PP From RSP";
            drow["VALUE_MEMBER"] = "2";
            dtCalculation.Rows.Add(drow);

            dtCDBase = new DataTable();
            dtCDBase = dtMrpCalculation.Clone();
            drow = dtCDBase.NewRow();
            drow["DISPLAY_MEMBER"] = "Taxable Amount";
            drow["VALUE_MEMBER"] = "1";
            dtCDBase.Rows.Add(drow);
            drow = dtCDBase.NewRow();
            drow["DISPLAY_MEMBER"] = "Bill Amount";
            drow["VALUE_MEMBER"] = "2";
            dtCDBase.Rows.Add(drow);


        }

        String GetConfigString()
        {
            String[] cCONFIGList = new String[]{"PICK_EOSS_DISC_ARTICLE", "exchange_tolerance_discount_diff_pct","GR_MINIMUM_AMOUNT_FOR_OTP"
                , "GENERATE_BATCH_WISE_BARCODE_FOR_FIX_ARTICLE", "EMAIL_PATH", "MSG_CENTER", "ENABLE_LUCKY_DRAW_PRINT", "LUCKY_DRAW_PRINT_COPY"
                ,"DONOT_ENFORCE_CREDIT_REFUND", "ONLINE_REDEMPTIONS_CN", "ENABLE_SLS_MINIMUM_AMOUNT", "ENABLE_FIX_MRP_SCAN", "MINIMUM_AMOUNT_VALUE",
            "GST_BILL_AMOUNT_LIMIT", "SHOW_BALANCE", "ENABLE_INVOICE", "SLS_DEBUG_MODE", "PICK_DISC_DURING_SCANNING", "DONOT_ALLOW_DUBLICATE_BARCODE",
            "ENABLE_AUTO_EXPORT_OF_REATAIL_SALE", "SHOW_CUST_PROFILE", "CONSIDER_TILL_PHY_CBS_AS_OPS",  "FOCUS", "PICK_LAST_SLS_DISC",
            "SHOW_SALE_RETURN_POPUP", "DISCOUNT_PICKMODE_SLR","SLS_ROUND_BILL_LEVEL", "SLS_SALES_PERSON_AT_ITEM_LEVEL", "SEARCH_ITEMS",
            "ALLOW_MULTIPLE_SALE_PERSON", "ENABLE_BILL_HOLD", "SEARCH_SPERSON", "ENABLE_ALTERATION", "TAKE_RETURN_MULTIPLE_UNQ_BARCODE",
            "DO_NOT_PICK_DISC_FROM_SALE_SETUP_IN_APP", "ACCEPT_NEGATIVE_VALUE_IN_OC", "ASK_CASH_TENDERED", "ASK_CASH_TENDERED_AFP",
            "ASK_FOR_PAN", "AMOUNT_FOR_PAN", "ENABLE_EXCHANGE_BILL_DISC", "MIN_AV_DAYS","CN_VALIDITY_DAYS","ESTIMATED_DELIVERY_DAYS","DT_CODE"
            ,"CREDIT_CARD_CODE", "PICK_CUSTOMER_FLAT_DISC", "PICK_CUSTOMER_DISCOUNTED_SALE", "PICK_CUSTOMER_DISCOUNTED_SALE_CUSTDYM", "PICK_LAST_SALES_PERSON"
            , "PROCESS_FLAT_DISCOUNT", "PROCESS_BRAND_DISCOUNT", "ENABLE_GRSERVICE", "SHOW_SALE_PERSON_ALIAS",  "ISSUE_ADDNL_DISCCOUPON_FIRSTPUR",
            "PRIVILEGE_LEVEL", "POINTS_CAMPAIGN_APPLICABLE", "MIN_POINTS_ENROLL_AMT", "MIN_OTHER_ENROLL_AMT", "CONSIDER_DISCITEMS_PRIVILEGEMARK",
            "PICK_WIZCLIPDISC_BILLSAVE", "INDEPENDANT_CAMPAIGN_GRP_CODE", "CAMPAIGN_GRP_CODE", "CREDIT_NOTE_REPORT_NAME","FOCUS_ON_ITEM_DESC_STOCK_NA"
            ,"MAINTAIN_01_QTY_UNQ_CODING", "OPEN_JOB_WORK_WINDOW","CHECK_BUYER_ORDER","SLS_MEMO_LEN", "APPLY_SALESETUP_PACKSLIP",
            "APPLY_SALESETUP_PACKSLIP", "ENABLE_FOCQTY", "PARA_NAME_FOR_BRAND","PARA_NAME_FOR_DISCOUNT", "DEFAULT_CREDIT_CARD",
            "FORCE_FEED_REMARKS", "ENABLE_PASSENGER_DETAILS", "DONOT_CHECK_MONITOR", "PRINT_GIFT_VOUCHER",
            "PRINT_GIFT_VOUCHER_REP_NAME", "PRINT_GIFT_VOUCHER_REP_NAME", "AUTO_HIDE_FILTER", "SHOW_CUSTOMER_BALANCE_ON_LABEL",
            "DEFAULT_DISCOUNT_TYPE",  "PICK_DISCOUNT_CUSTDYM_MODE","DONOT_RAISE_BILL_IF_CARD_EXPIRED", "ENABLE_THIRD_PARTY_LOYALTY",
            "LOYALTY_VENDOR","LOYALTY_URL_ADD","LOYALTY_USERID","LOYALTY_PWD","LOYALTY_POINTS_REDEEM_AS","ENABLE_MULTIPLE_WIZCLIPHO",
            "OPEN_PAYMENT_WINDOW_BEFORE_SAVING", "F11_TOGGLE", "GRID_AUTO_SIZE_LIST", "CALCULATE_EXCLUSIVE_TAX_ON_OTH_CHARGES",
            "DO_NOT_ALLOW_PRINT_WITHOUT_IRN_SLS", "STOCK_NA_BARCODE_FOR_SIS_IMPORT","PARA_NAME_FOR_BRAND","PARA_NAME_FOR_DISCOUNT"
            , "PICK_SLS_ROUND_OFF_FROMLOC", "DEFAULT_CASH_MEMO_PRINT_NAME",  "DONT_ASK_FOR_EWAY_BILL_GENERATION_DURING_EINVOICING","PAYMENT_MODE",
            "CREDIT_NOTE_PRINT_COPIES"};

            String cConfig = String.Join("','", cCONFIGList);
            String cExpr = "SELECT CONFIG_OPTION,VALUE FROM CONFIG (NOLOCK) WHERE CONFIG_OPTION IN ('" + cConfig + "') ORDER BY CONFIG_OPTION";
            return cExpr;
        }

        String GetModuleAuthorizationString(String LoggedLocation, String LoggedUserCode)
        {
            String cExpr = @"Select a.user_code,form_name,form_option,value,group_name 
                                From  users a (NOLOCK) 
                                join USER_ROLE_MST b (NOLOCK) on a.ROLE_ID = b.ROLE_ID 
                                join USER_ROLE_DET c (NOLOCK) on b.ROLE_id = c.ROLE_ID "+
                                //JOIN locusers d  (NOLOCK) ON d.user_code=a.user_code 
                                @"where form_name='FRMSALE' and a.user_code='" + LoggedUserCode + "'";
            //AND isnull(d.dept_id,'" + LoggedLocation + "') = '" + LoggedLocation + @"'
            return cExpr;
        }

        String GetLocationString(String LoggerLocation)
        {
            String cStrQuery = @"SELECT Enable_EInvoice,till_enabled, ENFORCE_OTP_BASED_GR ,Enable_Qr_Code_sale ,Qr_Code_Paymode ,wizclip_dept_id ,ISNULL(ask_for_consumables_billsaving,0) AS ask_for_consumables_billsaving,
                                ISNULL(bill_count_restriction,0) AS bill_count_restriction,ISNULL(AUTO_CALCULATION_OF_ALTERATION_CHARGES,0) AS AUTO_CALCULATION_OF_ALTERATION_CHARGES
                                ,ISNULL(sls_round_item_level,0) AS sls_round_item_level,ISNULL(sls_round_bill_level,0) AS sls_round_bill_level
                                ,PICK_CUSTOMER_GST_STATE_IN_RETAIL_SALE, 
                                bENABLEWIZCLIP = isnull(wizclip, 0),BSISLOC = ISNULL(sis_loc, 0),bServerLoc = isnull(server_loc, 0),
	                            BSLRRECONREQD = ISNULL(SLR_RECON_REQD, 0) FROM location (NOLOCK) WHERE DEPT_ID ='" + LoggerLocation + "'";

            return cStrQuery;
            //AppSLS.dmethod.SelectCmdTOSql(ref AppSLS.dset, cStrQuery, "tLocationSettingSLS", false, true);
        }
        String GetUserString(String LoggedUserCode)
        {
            String cStrQuery = @"select user_code, cash_refund_mode , cash_refund_limit ,ISNULL(Allow_access_retail_sale_All_users,0) Allow_access_retail_sale_All_users
                        ,ISNULL(discount_percentage_level,0) discount_percentage_level,ISNULL(LIMIT_DAY_FOR_GR,0) AS LIMIT_DAY_FOR_GR
                        FROM users (NOLOCK) WHERE user_code='" + LoggedUserCode + "'";

            return cStrQuery;
            //AppSLS.dmethod.SelectCmdTOSql(ref AppSLS.dset, cStrQuery, "tUserSettingSLS", false, true);
        }

        String STEP_1_Initialize_BasicValue_From_LocationMaster(String cDeptID)
        {
            String cretval = "";
            APIBaseClass clsBaseClass = new APIBaseClass(_ConStr);
            try
            {
                clsBaseClass.SelectCmdToSql_New(dsetSAVETRAN_SLS_AFTERSAVE, GetLocationString(cDeptID), "tLocationSaveTran", out cretval);
                if (String.IsNullOrEmpty(cretval))
                {
                    if (dsetSAVETRAN_SLS_AFTERSAVE.Tables.Contains("tLocationSaveTran"))
                    {
                        if (dsetSAVETRAN_SLS_AFTERSAVE.Tables["tLocationSaveTran"].Rows.Count > 0)
                        {
                            _bENABLEWIZCLIP = clsBaseClass.ConvertBool(dsetSAVETRAN_SLS_AFTERSAVE.Tables["tLocationSaveTran"].Rows[0]["bENABLEWIZCLIP"]);
                            _BSISLOC = clsBaseClass.ConvertBool(dsetSAVETRAN_SLS_AFTERSAVE.Tables["tLocationSaveTran"].Rows[0]["BSISLOC"]);
                            _BTILL_ENABLED = clsBaseClass.ConvertBool(dsetSAVETRAN_SLS_AFTERSAVE.Tables["tLocationSaveTran"].Rows[0]["TILL_ENABLED"]);
                            _bServerLoc = clsBaseClass.ConvertBool(dsetSAVETRAN_SLS_AFTERSAVE.Tables["tLocationSaveTran"].Rows[0]["bServerLoc"]);
                            _BSLRRECONREQD = clsBaseClass.ConvertBool(dsetSAVETRAN_SLS_AFTERSAVE.Tables["tLocationSaveTran"].Rows[0]["BSLRRECONREQD"]);
                        }

                    }
                }
            }
            catch (Exception ex)
            {

                cretval = "STEP_1_Initialize_BasicValue_From_LocationMaster : " + ex.Message;
            }
            return cretval;
        }

        String STEP_1_Initialize_BasicValue_From_UserRole(String cUserCode)
        {
            String cretval = "";
            APIBaseClass clsBaseClass = new APIBaseClass(_ConStr);
            try
            {
                if (String.Compare(cUserCode, "0000000", true) != 0)
                {
                    clsBaseClass.SelectCmdToSql_New(dsetSAVETRAN_SLS_AFTERSAVE, GetModuleAuthorizationString("",cUserCode), "tUserRoleSaveTran", out cretval);
                    if (String.IsNullOrEmpty(cretval))
                    {
                        if (dsetSAVETRAN_SLS_AFTERSAVE.Tables.Contains("tUserRoleSaveTran"))
                        {
                                DataRow[] drowUserRole = dsetSAVETRAN_SLS_AFTERSAVE.Tables["tUserRoleSaveTran"].Select("FORM_OPTION='ALLOW_TO_MODIFY_MEMO_OF_CLOSE_TILLS'");
                                if (drowUserRole.Length > 0)
                                    _bALLOW_TO_MODIFY_MEMO_OF_CLOSE_TILLS = clsBaseClass.ConvertBool(drowUserRole[0]["VALUE"]);
                        }
                    }
                }
                else
                {
                    _bALLOW_TO_MODIFY_MEMO_OF_CLOSE_TILLS = true;
                }
            }
            catch (Exception ex)
            {
                cretval = "STEP_1_Initialize_BasicValue_From_UserRole : " + ex.Message;
            }
            return cretval;
        }
        String STEP_1_Initialize_BasicValue_From_Config()
        {
            String cretval = "";
            APIBaseClass clsBaseClass = new APIBaseClass(_ConStr);
            try
            {

                clsBaseClass.SelectCmdToSql_New(dsetSAVETRAN_SLS_AFTERSAVE, GetConfigString(), "tConfigSaveTran", out cretval);
                if (String.IsNullOrEmpty(cretval))
                {
                    if (dsetSAVETRAN_SLS_AFTERSAVE.Tables.Contains("tConfigSaveTran"))
                    {
                        DataRow[] drowConfig = dsetSAVETRAN_SLS_AFTERSAVE.Tables["tConfigSaveTran"].Select("CONFIG_OPTION='CONSIDER_TILL_PHY_CBS_AS_OPS'");
                        if (drowConfig.Length > 0)
                            _bCONSIDER_TILL_PHY_CBS_AS_OPS = clsBaseClass.ConvertBool(drowConfig[0]["VALUE"]);
                    }
                }
            }
            catch (Exception ex)
            {
                cretval = "STEP_1_Initialize_BasicValue_From_Config : " + ex.Message;
            }
            return cretval;
        }
        String Step_2_Coupon_Validation()
        {
            String cRetval = "";
            try
            {


                if (_NUPDATEMODE == 1 || _NUPDATEMODE == 2)
                {

                    DataTable dtPayment = dsetSAVETRAN_SLS_AFTERSAVE.Tables["tPaymode_xn_det"];
                    if (dtPayment.Select("paymode_code = 'GVC0001' AND LEFT(gv_srno,2)= 'WC' AND amount<>0").Length > 0)

                        bValidateGvThruWizclip = true;
                    if (dtPayment.Select("paymode_code = 'GVC0001' AND LEFT(a.gv_srno,2)<> 'WC' AND amount<>0").Length > 0)
                        bValidateGvThruHO = true;


                    if (bValidateGvThruHO && bValidateGvThruWizclip)
                    {
                        cRetval = "Wizclip and Non-Wizcllip Gv(s) cannot be Redeemed in One Memo..";
                    }
                    if (!_bENABLEWIZCLIP && bValidateGvThruWizclip)
                    {
                        cRetval = "Wizclip Gv(s) cannot be Redeemed as Wizclip is not Enabled..";
                    }

                    if (bValidateGvThruHO)
                    {
                        if (_CLOCID == _CHODEPTID || _bServerLoc)
                        {

                            bValidategvLocal = true;

                            bValidateGvThruHO = false;

                            // INSERT validate_sls_gvredemption_upload(sp_id, gv_adj_amount, GV_SCRATCH_NO, GV_SRNO, CM_ID, denomination,
                            // redemption_customer_code  )

                            // SELECT a.sp_id, a.amount as gv_amount, b.GV_SCRATCH_NO, a.GV_SRNO, a.memo_id, a.amount gv_amount, d.customer_code
                            // FROM  SLS_PAYMODE_XN_DET_UPLOAD A(NOLOCK)
                            //         JOIN gv_mst_info b(NOLOCK) ON a.gv_srno = b.gv_srno

                            // JOIN sku_gv_mst c(NOLOCK) ON a.gv_srno = c.gv_srno

                            // JOIN sls_cmm01106_upload d(NOLOCK) ON d.sp_id = a.sp_id

                            // WHERE a.sp_id = @nSpId


                            //SET @cSTEP = '7.5'

                            //EXEC SP_CHKXNSAVELOG 'SLS_after', @CStep,1, @nSpId,'',1


                            //EXEC SP3S_VALIDATE_GVREDEMPTION_SINGLECHANNEL

                            //@NSPID = @nSpid,
                            //@nMode = 2,
                            //@cLocId = @cLocationId,
                            //@bCalledfromSaveTran = 1


                            //            IF EXISTS(SELECT TOP 1 gv_srno FROM validate_sls_gvredemption_upload(NOLOCK)
                            //                                WHERE sp_id = @nSpId AND ISNULL(errmsg,'')<> '')
                            //BEGIN
                            //                SELECT TOP 1 @cErrormsg = errmsg FROM validate_sls_gvredemption_upload(NOLOCK)

                            //                WHERE sp_id = @nSpId AND ISNULL(errmsg,'')<> ''


                            //                set @bValidationGvLocalFailed = 1


                            //                select* from validate_sls_gvredemption_upload(NOLOCK) WHERE sp_id = @nSpId

                            //                GOTO END_PROC

                            //            END


                        }
                    }
                }


            }
            catch (Exception ex)
            {
                cRetval = "Step_2_Coupon_Validation : " + ex.Message;
            }
            return cRetval;
        }

        String Step_2_1_SP3S_VALIDATE_GVREDEMPTION_SINGLECHANNEL(Int32 NMODE, String CLOCID, Boolean bCalledfromSavetran)
        {
            String cRetVal = "", cQueryString = "";
            APIBaseClass clsBaseClass = new APIBaseClass(_ConStr);
            try
            {
                StringBuilder sb_without_WC = new StringBuilder();
                StringBuilder sb_WC = new StringBuilder();
                StringBuilder sb_without_WC_CTE = new StringBuilder();
                StringBuilder sb_WC_CTE = new StringBuilder();
                sb_without_WC_CTE.Append(@"
                ;WITH without_WC_CTE
                AS
                (");
                sb_WC_CTE.Append(@"
                ;WITH WC_CTE
                AS
                (");
                /*
                drownew["amount"] = Math.Round(clsCommon.ConvertDouble(drow["denomination"]) * nConvRate, 2).ToString();
                drownew["ref_no"] = drow["gv_srno"];
                drownew["gv_srno"] = drow["gv_srno"];
                //drownew["gv_scratch_no"] = WizAppInfo.WizAppInfo.EncryptStringChar(Convert.ToString(drow["gv_scratch_no"]));
                drownew["gv_scratch_no"] = Convert.ToString(drow["gv_scratch_no"]);
                drownew["cc_name"] = Convert.ToString(drow["scheme_id"]);
                */ 
                String cCM_ID = "";
                if (dsetSAVETRAN_SLS_AFTERSAVE.Tables.Contains("tPAYMENTDETAILS"))
                {

                    int i = 0, i1 = 0;
                    foreach (DataRow drow in dsetSAVETRAN_SLS_AFTERSAVE.Tables["tPAYMENTDETAILS"].Select("ISNULL(gv_srno,'')<>''"))
                    {
                        cCM_ID = Convert.ToString(drow["memo_id"]);
                        if (Convert.ToString(drow["gv_srno"]).ToUpper().StartsWith("WC"))
                        {

                            sb_WC.Append(Convert.ToString(drow["gv_srno"]));
                            if (i == 1) sb_WC_CTE.Append("UNION ALL");
                            sb_WC_CTE.Append(@"'" + Convert.ToString(drow["gv_srno"]) + "' AS gv_srno");
                            i = 1;
                        }
                        else
                        {

                            sb_without_WC.Append(Convert.ToString(drow["gv_srno"]));
                            if (i == 1) sb_without_WC_CTE.Append("UNION ALL");
                            sb_without_WC_CTE.Append(@"'" + Convert.ToString(drow["gv_srno"]) + "' AS gv_srno");
                            i = 1;
                        }
                    }

                    sb_without_WC_CTE.Append(@" )");
                    sb_WC_CTE.Append(@" )");
                    if (sb_without_WC.Length > 0)
                    {
                        cQueryString = sb_without_WC_CTE.ToString() + @"
                        SELECT TOP 1 ERRMSG = 'INVALID GV NO. ENTERED....CANNOT REDEEM' 
                        FROM without_WC_CTE A
                        LEFT OUTER JOIN SKU_GV_MST B (NOLOCK)  ON a.gv_srno = B.gv_srno
                        WHERE B.GV_SRNO B.GV_SRNO IS NULL";

                        cQueryString = sb_without_WC_CTE.ToString() + @" 
                    SELECT ERRMSG='Gv is marked as Cancelled....CANNOT REDEEM' 
                    FROM without_WC_CTE A
                    JOIN GV_GEN_DET B (NOLOCK) ON A.GV_SRNO=B.GV_SRNO
                    JOIN gv_gen_mst c (NOLOCK) ON c.memo_id=b.memo_id
                    left JOIN 
                    (
                        select a.gv_srno from without_WC_CTE a 
	                    join GV_GEN_DET d (NOLOCK) ON A.GV_SRNO=d.GV_SRNO
	                    JOIN gv_gen_mst e (NOLOCK) ON e.memo_id=d.memo_id 
	                    where  e.cancelled=0 
                    ) d on d.GV_SRNO=a.GV_SRNO
	                WHERE c.cancelled=1 AND d.gv_srno IS NULL
                    ";

                        cQueryString = @"UPDATE A SET GV_TYPE=(CASE WHEN B.GV_TYPE IS NULL THEN 1 ELSE b.gv_type END)
	                FROM validate_sls_gvredemption_upload A WITH (ROWLOCK)
	                LEFT OUTER JOIN SKU_GV_MST B (NOLOCK) ON A.GV_SRNO=B.GV_SRNO
	                WHERE sp_id=@nSpId";

                        cQueryString = sb_without_WC_CTE.ToString() + @"
                    SELECT ERRMSG='GV HAS BEEN ADJUSTED IN THE BILL NO.:'+C.CM_NO+' DATED:'+CONVERT(VARCHAR,C.CM_DT,105)
                    FROM without_WC_CTE A 
                    JOIN PAYMODE_XN_DET B (NOLOCK) ON A.GV_SRNO=B.GV_SRNO
                    JOIN CMM01106 C (NOLOCK) ON B.MEMO_ID=C.CM_ID
                    JOIN sku_gv_mst d (NOLOCK) ON d.gv_srno=a.gv_srno
                    WHERE B.XN_TYPE='SLS' AND C.CANCELLED=0 AND C.CM_ID<>'" + cCM_ID + @"'
                    AND ISNULL(d.allow_partial_redemption,0)<>1 

                    ";

                        cQueryString = sb_without_WC_CTE.ToString() + @"
                        SELECT ERRMSG='GV adjusted amount cannot be more than '+
	                    (CASE WHEN ISNULL(c.gv_type,1) IN (0,1)  THEN LTRIM(RTRIM(STR(ISNULL(d.gv_issue_amount,0)-ISNULL(b.gv_adj_amount,0))))
	                          ELSE  LTRIM(RTRIM(STR(c.denomination))) END)+'....CANNOT REDEEM'
	                    FROM without_WC_CTE A 
	                    LEFT JOIN 
	                    (
                            SELECT a.GV_SRNO,SUM(gv_amount) gv_adj_amount 
                            FROM   GV_MST_REDEMPTION a (NOLOCK)
                            JOIN without_WC_CTE  B  ON A.GV_SRNO=B.GV_SRNO
                            JOIN sku_gv_mst c (NOLOCK) ON c.gv_srno=b.gv_srno
                            WHERE ISNULL(c.allow_partial_redemption,0)=1
                            AND ISNULL(a.REDEMPTION_CM_ID,'')<>'" + cCM_ID + @"'
                            GROUP BY a.gv_srno
                        ) b ON a.GV_SRNO=b.gv_srno
	                     JOIN sku_gv_mst c (NOLOCK) ON c.gv_srno=a.gv_srno
	                     LEFT JOIN 
	                     (
                            SELECT a.gv_srno,SUM(a.denomination) gv_issue_amount 
                            FROM arc_gvsale_details a (NOLOCK)
                            JOIN arc01106 b (NOLOCK) ON b.adv_rec_id=a.adv_rec_id
                            JOIN without_WC_CTE c ON c.GV_SRNO=a.gv_srno
                            WHERE cancelled=0
                            GROUP BY a.gv_srno
                        ) d ON d.gv_srno=a.GV_SRNO
 	 
	                    WHERE ISNULL(c.allow_partial_redemption,0)=1 AND 
	                    ((ISNULL(c.gv_type,1) IN (0,1) AND isnull(a.gv_adj_amount,0)>(ISNULL(d.gv_issue_amount,0)-ISNULL(b.gv_adj_amount,0))) OR 
	                     (ISNULL(c.gv_type,1)=2 AND a.gv_adj_amount>c.denomination))
                    ";
                    }
                }
                //UPDATE A WITH(ROWLOCK) SET ERRMSG = 'INVALID GV NO. ENTERED....CANNOT REDEEM'

                //FROM validate_sls_gvredemption_upload A
                //LEFT OUTER JOIN SKU_GV_MST B(NOLOCK) ON A.GV_SRNO = B.GV_SRNO

                //WHERE sp_id = @nSpId AND B.GV_SRNO IS NULL AND LEFT(a.gv_srno, 2) <> 'WC'
            }
            catch (Exception ex)
            {
                cRetVal = "Step_2_1_SP3S_VALIDATE_GVREDEMPTION_SINGLECHANNEL : " + ex.Message;
            }
            return cRetVal;
        }

        String SP3S_UPDATE_CUSTOMERBALANCES(SqlConnection sqlcon,SqlTransaction sqlTran, Boolean bREVERTFLAG, DataTable dtMST, DataTable dtDet, DataTable dtPayment)
        {
            String cRetVal = "";
            APIBaseClass clsBaseClass = new APIBaseClass();
            try
            {
                Decimal nTotal, nTotalTemp;
                nTotal = nTotalTemp = 0;
                String cCustomerCode = Convert.ToString(dtMST.Rows[0]["customer_code"]);
                if (String.IsNullOrEmpty(cCustomerCode) || cCustomerCode.StartsWith("000000000000"))
                    return cRetVal;
                /*
                    INSERT INTO #CUS_BAL(CUS_CODE,BAL)
                    SELECT CUSTOMER_CODE,ABS(ISNULL(SUM(B.AMOUNT),0)) AS CREDIT_ISSUE_AMT 
                    FROM CMM01106 A (NOLOCK) JOIN PAYMODE_XN_DET B (NOLOCK) ON A.CM_ID=B.MEMO_ID  
                    WHERE    A.CM_ID =(CASE WHEN @CMEMO_ID='' THEN  A.CM_ID ELSE @CMEMO_ID END )
                    and CANCELLED=0 AND PAYMODE_CODE='0000004' AND XN_TYPE='SLS'  
                    AND SUBSTRING(A.CM_NO,5,1)<>'N'
                    GROUP BY CUSTOMER_CODE
                 */
                nTotalTemp = clsBaseClass.ConvertDecimal(dtPayment.Compute("SUM(AMOUNT)", "PAYMODE_CODE='0000004' AND ISNULL(amount,0)>0"));
                nTotalTemp = Math.Abs(nTotalTemp);
                nTotal += nTotalTemp;
                /*
                          PRINT '***LBLCREDIT_NOTE_ISSUE:****'


                           INSERT INTO #CUS_BAL(CUS_CODE,BAL)
                           SELECT CUSTOMER_CODE,-1*ABS(ISNULL(SUM(B.AMOUNT),0)) AS CREDITNOTEISSUE_AMT FROM CMM01106 A  (NOLOCK) 
                           JOIN PAYMODE_XN_DET B (NOLOCK) ON A.CM_ID=B.MEMO_ID  
                           WHERE A.CM_ID =(CASE WHEN @CMEMO_ID='' THEN  A.CM_ID ELSE @CMEMO_ID END ) AND  
                           CANCELLED=0 AND PAYMODE_CODE='0000004' AND XN_TYPE='SLS' 
                           AND SUBSTRING(A.CM_NO,5,1)='N' 
                           GROUP BY CUSTOMER_CODE
                */
                nTotalTemp = clsBaseClass.ConvertDecimal(dtPayment.Compute("SUM(AMOUNT)", "PAYMODE_CODE='0000004' AND ISNULL(amount,0)<0"));
                nTotalTemp = Convert.ToDecimal(-1) * Math.Abs(nTotalTemp);
                nTotal += nTotalTemp;
                /*
                          PRINT '***LBLCREDITNOTEADJ:****'


                            INSERT INTO #CUS_BAL(CUS_CODE,BAL)
                           SELECT CUSTOMER_CODE,ABS(ISNULL(SUM(B.AMOUNT),0)) FROM CMM01106 A (NOLOCK)  
                           JOIN PAYMODE_XN_DET B (NOLOCK)  ON A.CM_ID=B.MEMO_ID  
                           WHERE A.CM_ID =(CASE WHEN @CMEMO_ID='' THEN  A.CM_ID ELSE @CMEMO_ID END ) AND  
                           CANCELLED=0 AND PAYMODE_CODE='0000001'AND XN_TYPE='SLS' 
                           GROUP BY CUSTOMER_CODE
                */
                nTotalTemp = clsBaseClass.ConvertDecimal(dtPayment.Compute("SUM(AMOUNT)", "PAYMODE_CODE='0000001'"));
                nTotalTemp = Math.Abs(nTotalTemp);
                nTotal += nTotalTemp;
                /*
                         

                         PRINT '*** ADAVANCE_AMT_SLSADJ:****'

                           INSERT INTO #CUS_BAL(CUS_CODE,BAL)
                           SELECT CUSTOMER_CODE,ABS(ISNULL(SUM(B.AMOUNT),0)) AS ADAVANCE_AMT_ADJ 
                           FROM CMM01106 A (nolock) JOIN PAYMODE_XN_DET B (nolock) ON A.CM_ID=B.MEMO_ID  
                           WHERE A.CM_ID =(CASE WHEN @CMEMO_ID='' THEN  A.CM_ID ELSE @CMEMO_ID END ) AND  
                           CANCELLED=0 AND PAYMODE_CODE='0000002' AND XN_TYPE='SLS'
                           GROUP BY CUSTOMER_CODE
                */
                nTotalTemp = clsBaseClass.ConvertDecimal(dtPayment.Compute("SUM(AMOUNT)", "PAYMODE_CODE='0000002'"));
                nTotalTemp = Math.Abs(nTotalTemp);
                nTotal += nTotalTemp;
                /*

                          PRINT '*** CREDIT_REFUND_AMT:****'

                           INSERT INTO #CUS_BAL(CUS_CODE,BAL)
                           SELECT CUSTOMER_CODE,-1*ABS(ISNULL(SUM(B.AMOUNT),0)) 
                           FROM CMM01106 A (nolock) 
                           JOIN PAYMODE_XN_DET B (nolock) ON A.CM_ID=B.MEMO_ID  
                           WHERE A.CM_ID =(CASE WHEN @CMEMO_ID='' THEN  A.CM_ID ELSE @CMEMO_ID END ) AND  
                           CANCELLED=0 AND PAYMODE_CODE='CMR0001'AND XN_TYPE='SLS'
                           GROUP BY CUSTOMER_CODE
                */
                nTotalTemp = clsBaseClass.ConvertDecimal(dtPayment.Compute("SUM(AMOUNT)", "PAYMODE_CODE='CMR0001'"));
                nTotalTemp = Convert.ToDecimal(-1) * Math.Abs(nTotalTemp);
                nTotal += nTotalTemp;
                /*
                    IF @NREVERTFLAG = 0
                        SET @NADDBAL = 1
                    ELSE
                        SET @NADDBAL = -1
                    UPDATE A SET CUST_BAL = case when @NADDBAL = 1 then ISNULL(CUST_BAL,0)+(@naddbal * ISNULL(B.BAL, 0))
                    when @NADDBAL = -1 and ISNULL(CUST_BAL,0)>= 0 then ISNULL(CUST_BAL,0)+(@naddbal * ISNULL(B.BAL, 0))
                    when @NADDBAL = -1 and ISNULL(CUST_BAL,0)< 0 then ISNULL(CUST_BAL,0)-(@naddbal * ISNULL(B.BAL, 0))
                    else 0 end
                    FROM CUSTDYM A(NOLOCK)
                    JOIN 
		           ( 
		             SELECT CUS_CODE,SUM(ISNULL(BAL,0)) AS BAL
		             FROM #CUS_BAL
			         GROUP BY CUS_CODE
			        )B ON A.CUSTOMER_CODE=B.CUS_CODE 

                */
                Decimal NREVERTFLAG = 1;
                if (bREVERTFLAG)
                {
                    NREVERTFLAG = -1;
                }
                nTotal = NREVERTFLAG * nTotal;
                String cQueryStr = @"UPDATE CUSTDYM SET CUST_BAL = case when " + NREVERTFLAG.ToString() + @"= 1 then ISNULL(CUST_BAL,0)+" + nTotal.ToString() +
                                      @" when " + NREVERTFLAG.ToString() + @" = -1 and ISNULL(CUST_BAL,0)>= 0 then ISNULL(CUST_BAL,0)+" + nTotal.ToString() +
                                      @" when " + NREVERTFLAG.ToString() + @" = -1 and ISNULL(CUST_BAL,0)< 0 then ISNULL(CUST_BAL,0)-" + nTotal.ToString() +
                                      @" else 0 end
                WHERE CUSTOMER_CODE='" + cRetVal + "'";
                clsBaseClass.RunSQLCommandWithSQLTran(cQueryStr, sqlcon, sqlTran, out cRetVal);
            }
            catch (Exception ex)
            {
                cRetVal = "SP3S_UPDATE_CUSTOMERBALANCES : " + ex.Message;
            }
            return cRetVal;
        }
        String SP3S_UPDATESHIFT_AMOUNT(SqlConnection sqlcon, SqlTransaction sqlTran, Boolean bREVERTFLAG, DataTable dtMST, DataTable dtDet, DataTable dtPayment)
        {
            String cRetVal = "";
            APIBaseClass clsBaseClass = new APIBaseClass();
            try
            {
                Decimal nTotal, nTotalTemp;
                String CSHIFT_ID, CUSER_CODE;
                nTotal = nTotalTemp = 0;
                CSHIFT_ID = CUSER_CODE = "";
                if (_BTILL_ENABLED)
                {
                    DataRow[] drowUserRole = dsetSAVETRAN_SLS_AFTERSAVE.Tables["tConfigSaveTran"].Select("FORM_OPTION='ALLOW_TO_MODIFY_MEMO_OF_CLOSE_TILLS'");
                    if (drowUserRole.Length > 0)
                        _bALLOW_TO_MODIFY_MEMO_OF_CLOSE_TILLS = clsBaseClass.ConvertBool(drowUserRole[0]["VALUE"]);
                    DataRow[] drowConfig = dsetSAVETRAN_SLS_AFTERSAVE.Tables["tConfigSaveTran"].Select("CONFIG_OPTION='CONSIDER_TILL_PHY_CBS_AS_OPS'");
                    if (drowConfig.Length > 0)
                        _bCONSIDER_TILL_PHY_CBS_AS_OPS = clsBaseClass.ConvertBool(drowConfig[0]["VALUE"]);
                    CSHIFT_ID = Convert.ToString(dtMST.Rows[0]["SHIFT_ID"]);
                    CUSER_CODE = Convert.ToString(dtMST.Rows[0]["USER_CODE"]);
                    if (!(_bALLOW_TO_MODIFY_MEMO_OF_CLOSE_TILLS && _bCONSIDER_TILL_PHY_CBS_AS_OPS))
                    {
                        if (!String.IsNullOrEmpty(Convert.ToString(clsBaseClass.ExecuteScalar(@"SELECT TOP 1 'U' FROM TILL_SHIFT_MST (NOLOCK) WHERE SHIFT_ID='" + CSHIFT_ID + @" AND ISNULL(CLOSE_DATE,'')=''"))))
                        {
                            cRetVal = @"SHIFT IS NOT OPEN FOR THE TILL.TRANSACTION (SALES) ." + CUSER_CODE;// + ':' + STR(@NUPDATEMODE) + ':' + @CVALUE1 + ':' + @CVALUE2
                        }
                    }
                    /*
                     SET @CVALUE=0
	--GET THE SHIFT_ID FROM CMM01106
	IF @NUPDATEMODE=2
		SELECT @CSHIFT_ID=SHIFT_ID,@CUSER_CODE=EDT_USER_CODE FROM SLS_CMM01106_UPLOAD WHERE CM_ID=@CXN_ID
		AND SP_ID=@NSPID
	ELSE
		SELECT @CSHIFT_ID=SHIFT_ID,@CUSER_CODE=EDT_USER_CODE FROM CMM01106 WHERE CM_ID=@CXN_ID
	PRINT '125'
	DECLARE @CVALUE1 VARCHAR(2),@CVALUE2 VARCHAR(2)
	
	SELECT TOP 1 @CVALUE1=VALUE  
	FROM CONFIG WHERE CONFIG_OPTION='CONSIDER_TILL_PHY_CBS_AS_OPS' 

	
	IF ISNULL(@CVALUE1,0)='1'
	BEGIN
		SELECT TOP 1 @CVALUE2=VALUE FROM USER_ROLE_DET A
		JOIN USERS B ON A.ROLE_ID=B.ROLE_ID 
		WHERE USER_CODE=@CUSER_CODE 
		AND FORM_NAME='FRMSALE' 
		AND FORM_OPTION='ALLOW_TO_MODIFY_MEMO_OF_CLOSE_TILLS'
	END
	
	SET @CVALUE=(CASE WHEN ISNULL(@CVALUE1,'')='1' AND ISNULL(@CVALUE2,'')='1' THEN '1' ELSE '' END)
	
	IF ISNULL(@CVALUE,0)=0 OR @NUPDATEMODE=1
	BEGIN
	IF NOT EXISTS(SELECT TOP 1 'U' FROM TILL_SHIFT_MST WHERE SHIFT_ID=@CSHIFT_ID AND ISNULL(CLOSE_DATE,'')='')
	BEGIN
		SELECT @CERRMSGOUT='SHIFT IS NOT OPEN FOR THE TILL.TRANSACTION (SALES) .'+@CUSER_CODE+':'+STR(@NUPDATEMODE)+':'+@CVALUE1+':'+@CVALUE2
		GOTO END_PROC
	END
	END
                     */

                }


            }
            catch (Exception ex)
            {
                cRetVal = "SP3S_UPDATESHIFT_AMOUNT : " + ex.Message;
            }
            return cRetVal;
        }

        String SP3S_GETENINVOICE_MEMOPREFIX(SqlConnection sqlcon, SqlTransaction sqlTran, Boolean bREVERTFLAG, DataTable dtMST, DataTable dtDet, DataTable dtPayment
            ,String cXnType ,String cPartyGstNo ,String cSourceLocId ,String cFinyear ,String cInputMemoPrefix ,String nSpId ,out String cErrormsg ,out String cOutputMemoPrefix)
        {
            String cRetVal = "";
            cErrormsg = cOutputMemoPrefix = "";
            APIBaseClass clsBaseClass = new APIBaseClass();
            try
            {

            }
            catch (Exception ex)
            {
                cRetVal = "SP3S_GETENINVOICE_MEMOPREFIX : " + ex.Message;
            }
            return cRetVal;
        }

        String SP3S_CHECK_PREVMEMO(SqlConnection sqlcon, SqlTransaction sqlTran, Boolean bREVERTFLAG, DataTable dtMST, DataTable dtDet, DataTable dtPayment
            , String CFINYEAR, String CMEMONO,  out String cErrormsg)
        {
            String cRetVal = "";
            cErrormsg =  "";
            APIBaseClass clsBaseClass = new APIBaseClass();
            try
            {

            }
            catch (Exception ex)
            {
                cRetVal = "SP3S_CHECK_PREVMEMO : " + ex.Message;
            }
            return cRetVal;
        }

        String SP_VALIDATE_MEMODATE_SLS(SqlConnection sqlcon, SqlTransaction sqlTran, Boolean bREVERTFLAG, DataTable dtMST, DataTable dtDet, DataTable dtPayment
            , String CXNTYPE, String nSpId,String cKeysTable, out String cErrormsg)
        {
            String cRetVal = "";
            cErrormsg = "";
            APIBaseClass clsBaseClass = new APIBaseClass();
            try
            {
                DateTime DMEMODT = clsBaseClass.ConvertDateTime(dtMST.Rows[0]["cm_dt"]);
                String CCURMEMONO = Convert.ToString(dtMST.Rows[0]["cm_no"]).Trim();
                String CFINYEAR = "011" + (DMEMODT.Month >= 4 && DMEMODT.Month <= 12 ? DMEMODT.AddYears(1).Year.ToString().Substring(2) : DMEMODT.Year.ToString().Substring(2));
                Int32 NMEMOPREFIXLEN = CCURMEMONO.IndexOf('-');
                String cMemonoPrefix = CCURMEMONO.Substring(0, NMEMOPREFIXLEN);
                DateTime DMAXMEMODATE;
               String cQueryStr = @"SELECT DMAXMEMODATE = last_cm_dt FROM " + cKeysTable + @"
                WHERE prefix = '" + cMemonoPrefix + "' AND FINYEAR = '" + @CFINYEAR + "'";

                DMAXMEMODATE=clsBaseClass.ConvertDateTime( clsBaseClass.ExecuteScalar(cQueryStr));


                if (DMEMODT < DMAXMEMODATE)
                {
                    cErrormsg = "CURRENT MEMO DATE CANNOT BE LESS THAN - " + DMAXMEMODATE.ToString("yyyy-MM-dd");
                }
            }
            catch (Exception ex)
            {
                cRetVal = "SP_VALIDATE_MEMODATE_SLS : " + ex.Message;
            }
            return cRetVal;
        }

        String SP3S_UPD_SKUXFPNEW(SqlConnection sqlcon, SqlTransaction sqlTran, Boolean bREVERTFLAG, DataTable dtMST, DataTable dtDet, DataTable dtPayment
            , String CXNTYPE, String CXN_ID, Boolean BCALLEDFROMREBUILD, out String cErrormsg)
        {
            String cRetVal = "";
            cErrormsg = "";
            APIBaseClass clsBaseClass = new APIBaseClass();
            try
            {

            }
            catch (Exception ex)
            {
                cRetVal = "SP_VALIDATE_MEMODATE_SLS : " + ex.Message;
            }
            return cRetVal;
        }

    }
    public class validate_sls_gvredemption_upload
    {
        public string sp_id { get; set; }
        public string GV_SRNO { get; set; }
        public string GV_SCRATCH_NO { get; set; }
        public decimal? gv_adj_amount { get; set; }
        public bool? gv_sold { get; set; }
        public string errmsg { get; set; }
        public bool? allow_partial_redemption { get; set; }
        public decimal? gv_type { get; set; }
        public string cm_id { get; set; }
        public decimal? bill_amount { get; set; }
        public decimal? denomination { get; set; }
        public string sold_TO_customer_code { get; set; }
        public string redemption_customer_code { get; set; }
        public decimal? redemption_usage_type { get; set; }
    }
}
