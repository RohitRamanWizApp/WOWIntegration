using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;


namespace WOWIntegration
{
    public class RUN_QUERY :IDisposable
    {
        string cPath, Groupcode, LoggedLocation, LoggedUserCode, LoggedBinID, GHOLOCATION, cCredentialName, cCredentialPassword, cAPIAddress, cConStr;
        QueryStr QS;
        Boolean bThroughCloud;
        DateTime GTODAYDATE;
        MessageContent MC;
        public string _Path { get => cPath; set => cPath = value; }
        public string _GroupCode { get => Groupcode; set => Groupcode = value; }
        public string _LoggedLocation { get => LoggedLocation; set => LoggedLocation = value; }
        public string _LoggedUserCode { get => LoggedUserCode; set => LoggedUserCode = value; }
        public string _CredentialName { get => cCredentialName; set => cCredentialName = value; }
        public string _CredentialPassword { get => cCredentialPassword; set => cCredentialPassword = value; }
        public string _APIAddress { get => cAPIAddress; set => cAPIAddress = value; }
        public string _ConnnectionString { get => cConStr; set => cConStr = value; }
        public bool _ThroughCloud { get => bThroughCloud; set => bThroughCloud = value; }
        public QueryStr _QS { get => QS; set => QS = value; }
        public MessageContent _MC { get => MC; set => MC = value; }
        public string _LoggedBinID { get => LoggedBinID; set => LoggedBinID = value; }
        public DateTime _GTODAYDATE { get => GTODAYDATE; set => GTODAYDATE = value; }
        public string _GHOLOCATION { get => GHOLOCATION; set => GHOLOCATION = value; }
        public string _FINYEAR { get ; set ; }
        public Boolean _CENTRALIZED_LOC { get; set; }
        public String _UserForLocs { get; set; }
        public Boolean ShowDataForAllUser { get; set; }

        public RUN_QUERY()
        {
            cPath = Groupcode = LoggedLocation = LoggedUserCode = cCredentialName = cCredentialPassword = cAPIAddress =cConStr= "";
            cAPIAddress = "https://wizapp.in/wowservice";
            QS = new QueryStr();
            bThroughCloud = false;
        }
        public DataTable GetCountForWhatsApp(String GroupCode,String SenderMobile)
        {
            String APIAddress = "https://wizapp.in/restregservice";
            DataTable dataTable = new DataTable(nameof(GetCountForWhatsApp));
            dataTable.Columns.Add("err_msg", typeof(string));
            dataTable.Rows.Add();
            try
            {
                string endpoint = APIAddress;
                string xmlRedeemCoupon = "";// Newtonsoft.Json.JsonConvert.SerializeObject(_QS);
                File.WriteAllText(_Path + "\\_GetCountForWhatsApp.txt", Convert.ToString(xmlRedeemCoupon));
                string cReturnedStr = new RestAPIRestClient(endpoint).MakeRequestWizApp("/ValidateWhatsAppCount?GroupCode="+GroupCode+"&SenderMobile="+SenderMobile, xmlRedeemCoupon, "", "");
                File.WriteAllText(_Path + "\\_GetCountForWhatsApp_Returned.txt", Convert.ToString(cReturnedStr));
                dataTable.Rows[0]["err_msg"] = cReturnedStr;
            }
            catch (Exception ex)
            {
                dataTable.Rows[0]["err_msg"] = (object)ex.Message;
            }
            if (!dataTable.Columns.Contains("err_msg")) dataTable.Columns.Add("err_msg", typeof(string));
            return dataTable;
        }
        public DataTable SendLogForWhatsApp(String  GroupCode  ,String XnType ,String MemoID, String SenderMobile ,String ReceiverMobile)
        {
            
            String APIAddress = "https://wizapp.in/restregservice";
            DataTable dataTable = new DataTable(nameof(SendLogForWhatsApp));
            dataTable.Columns.Add("errmsg", typeof(string));
            dataTable.Rows.Add();
            try
            {
                
                string endpoint = APIAddress;
                string xmlRedeemCoupon = Newtonsoft.Json.JsonConvert.SerializeObject(_MC);
                File.WriteAllText(_Path + "\\_SendLogForWhatsApp.txt", Convert.ToString(xmlRedeemCoupon));
                string cReturnedStr = new RestAPIRestClient(endpoint, HttpVerb.POST, xmlRedeemCoupon).MakeRequestWizApp("/WhatsAppLog?GroupCode="+GroupCode+"&XnType="+XnType+"&MemoID="+MemoID+"&SenderMobile="+SenderMobile+"&ReceiverMobile="+ReceiverMobile, xmlRedeemCoupon, _CredentialName, "");
                File.WriteAllText(_Path + "\\_SendLogForWhatsApp_Returned.txt", Convert.ToString(cReturnedStr));
                DataSet dsetdataTable = Newtonsoft.Json.JsonConvert.DeserializeObject<DataSet>(cReturnedStr);
                if (dsetdataTable.Tables.Count > 0)
                {
                    dataTable = dsetdataTable.Tables[0];
                }
            }
            catch (Exception ex)
            {
                dataTable.Rows[0]["errmsg"] = (object)ex.Message;
            }
            if (!dataTable.Columns.Contains("errmsg")) dataTable.Columns.Add("errmsg", typeof(string));
            return dataTable;
        }
        
        private DataTable GetDetailsFromAPI_RunQuery_Cloud(string cReturnedStr)
        {
            DataTable fromApiRunQuery_Cloud = new DataTable(nameof(GetDetailsFromAPI_RunQuery_Cloud));
            fromApiRunQuery_Cloud.Columns.Add("err_msg", typeof(string));
            fromApiRunQuery_Cloud.Rows.Add();
            if (string.IsNullOrEmpty(cReturnedStr))
            {
                fromApiRunQuery_Cloud.Rows[0]["err_msg"] = (object)"String not return by API";
                return fromApiRunQuery_Cloud;
            }
            else
            {
                try
                {
                    fromApiRunQuery_Cloud = Newtonsoft.Json.JsonConvert.DeserializeObject<DataTable>(cReturnedStr);
              //      string str1 = Convert.ToString(cReturnedStr);
              //      string[] separator1 = new string[1] { "{" };
              //      foreach (string str2 in str1.Split(separator1, StringSplitOptions.RemoveEmptyEntries))
              //      {
              //          string[] separator2 = new string[1] { "," };
              //          foreach (string str3 in str2.Split(separator2, StringSplitOptions.RemoveEmptyEntries))
              //          {
              //              string[] strArray = str3.Replace("\"", "").Replace("}", "").Split(new string[1]
              //              {
              //":"
              //              }, StringSplitOptions.RemoveEmptyEntries);
              //              if (strArray.Length != 0)
              //                  fromApiRedeemCoupon.Columns.Add(strArray[0], typeof(string));
              //              if (strArray.Length > 1)
              //                  fromApiRedeemCoupon.Rows[0][strArray[0]] = (object)strArray[1];
              //          }
              //      }
              //      if (fromApiRedeemCoupon.Columns.Contains("ReturnCode"))
              //      {
              //          if (Convert.ToString(fromApiRedeemCoupon.Rows[0]["ReturnCode"]) != "0")
              //              fromApiRedeemCoupon.Rows[0]["err_msg"] = (object)Convert.ToString(fromApiRedeemCoupon.Rows[0]["ReturnMessage"]);
              //      }
                }
                catch (Exception ex)
                {
                    fromApiRunQuery_Cloud.Rows[0]["err_msg"] = (object)("GetDetailsFromAPI_RedeemCoupon : " + ex.Message);
                }
            }
            return fromApiRunQuery_Cloud;
        }

        public DataTable RunQuery_Cloud()
        {
            if (String.IsNullOrEmpty(_APIAddress))
                _APIAddress = "https://wizapp.in/wowservice";
            DataTable dataTable = new DataTable(nameof(RunQuery_Cloud));
            dataTable.Columns.Add("err_msg", typeof(string));
            dataTable.Rows.Add();
            try
            {
                if (string.IsNullOrEmpty(GET_TOKEN._SecurityToken))
                {
                    new GET_TOKEN().Token(_Path, _GroupCode, _CredentialName, _CredentialPassword, _APIAddress);
                    if (string.IsNullOrEmpty(GET_TOKEN._SecurityToken))
                    {
                        dataTable.Rows[0]["err_msg"] = (object)"REDEEM_COUPON.RedeemCoupon : Token Not Found";
                        dataTable.Rows[0].EndEdit();
                        return dataTable;
                    }
                }
                string endpoint = _APIAddress;
                string xmlRedeemCoupon = Newtonsoft.Json.JsonConvert.SerializeObject(_QS);
                File.WriteAllText(_Path + "\\_RedeemCoupon.txt", Convert.ToString(xmlRedeemCoupon));
                string cReturnedStr = new RestAPIRestClient(endpoint, HttpVerb.POST, xmlRedeemCoupon).MakeRequestWizApp("/RunQuery?GroupCode=" + _GroupCode+ "&LoggedLocation="+_LoggedLocation+"&LoggedUserCode="+_LoggedUserCode, xmlRedeemCoupon, _CredentialName,GET_TOKEN._SecurityToken );
                File.WriteAllText(_Path + "\\_RedeemCoupon_Returned.txt", Convert.ToString(cReturnedStr));
                dataTable = this.GetDetailsFromAPI_RunQuery_Cloud(cReturnedStr);
            }
            catch (Exception ex)
            {
                dataTable.Rows[0]["err_msg"] = (object)ex.Message;
            }
            if (!dataTable.Columns.Contains("err_msg")) dataTable.Columns.Add("err_msg", typeof(string));
            return dataTable;
        }

        public DataTable UPDATEWHATSAPPMSG()
        {
            if (String.IsNullOrEmpty(_APIAddress))
                _APIAddress = "https://wizapp.in/wowservice";
            DataTable dataTable = new DataTable(nameof(RunQuery_Cloud));
            dataTable.Columns.Add("err_msg", typeof(string));
            dataTable.Rows.Add();
            try
            {
                if (string.IsNullOrEmpty(GET_TOKEN._SecurityToken))
                {
                    new GET_TOKEN().Token(_Path, _GroupCode, _CredentialName, _CredentialPassword, _APIAddress);
                    if (string.IsNullOrEmpty(GET_TOKEN._SecurityToken))
                    {
                        dataTable.Rows[0]["err_msg"] = (object)"REDEEM_COUPON.RedeemCoupon : Token Not Found";
                        dataTable.Rows[0].EndEdit();
                        return dataTable;
                    }
                }
                string endpoint = _APIAddress;
                string xmlRedeemCoupon = Newtonsoft.Json.JsonConvert.SerializeObject(_QS);
                File.WriteAllText(_Path + "\\_RedeemCoupon.txt", Convert.ToString(xmlRedeemCoupon));
                string cReturnedStr = new RestAPIRestClient(endpoint, HttpVerb.POST, xmlRedeemCoupon).MakeRequestWizApp("/RunQuery?GroupCode=" + _GroupCode + "&LoggedLocation=" + _LoggedLocation + "&LoggedUserCode=" + _LoggedUserCode, xmlRedeemCoupon, _CredentialName, GET_TOKEN._SecurityToken);
                File.WriteAllText(_Path + "\\_RedeemCoupon_Returned.txt", Convert.ToString(cReturnedStr));
                dataTable = this.GetDetailsFromAPI_RunQuery_Cloud(cReturnedStr);
            }
            catch (Exception ex)
            {
                dataTable.Rows[0]["err_msg"] = (object)ex.Message;
            }
            if (!dataTable.Columns.Contains("err_msg")) dataTable.Columns.Add("err_msg", typeof(string));
            return dataTable;
        }

        String BadRequest(String cMsg)
        {
            return cMsg;
        }

        public DataTable RunQuery_Local()
        {
            DataTable fromApiExecuteQuery = new DataTable(nameof(RunQuery_Local));
            fromApiExecuteQuery.Columns.Add("err_msg", typeof(string));
            fromApiExecuteQuery.Rows.Add();


            DataSet dset = null;
            if (Equals(dset, null)) dset = new DataSet();
            try
            {
                DataTable DtM = new DataTable();

                if (string.IsNullOrEmpty(_ConnnectionString))
                    fromApiExecuteQuery.Rows[0]["err_msg"] = BadRequest("Connection String Not Found");

                if (string.IsNullOrEmpty(_LoggedLocation))
                    fromApiExecuteQuery.Rows[0]["err_msg"] = BadRequest("Logged Location ID should not be Empty");

                if (string.IsNullOrEmpty(_LoggedUserCode))
                    fromApiExecuteQuery.Rows[0]["err_msg"] = BadRequest("Logged User Code should not be Empty");

                if (string.IsNullOrEmpty(_QS._QueryStr))
                    fromApiExecuteQuery.Rows[0]["err_msg"] = BadRequest("Query String should not be Empty");

                if (string.IsNullOrEmpty(_QS._TableAlias))
                    _QS._TableAlias = "tDATA";

                //SqlConnection con = new SqlConnection(_ConStr);
                //SqlCommand cmd = new SqlCommand();
                //SqlDataAdapter sda = new SqlDataAdapter();
                APIBaseClass clsBase = new APIBaseClass(_ConnnectionString);


                String cExpr = _QS._QueryStr;
                String cErrMSg = "";
                if (_QS._ExecuteNonQuery)
                {
                    clsBase.ExecuteNonQuery(cExpr, out cErrMSg);
                }
                else
                {
                    clsBase.SelectCmdToSql_New(dset, cExpr, _QS._TableAlias, out cErrMSg);
                    //clsBase.SelectCmdToSql_New(dset, cExpr, queryStr.cTableAlias, out cErrMSg);
                }

                if (String.IsNullOrEmpty(cErrMSg))
                {
                    if (_QS._ExecuteNonQuery)
                    {
                        fromApiExecuteQuery.Rows[0]["err_msg"] = "";
                    }
                    else
                    {
                        fromApiExecuteQuery = dset.Tables[_QS._TableAlias];
                    }
                }
                else
                    fromApiExecuteQuery.Rows[0]["err_msg"] = BadRequest(cErrMSg);

            }
            catch (Exception ex)
            {
                fromApiExecuteQuery.Rows[0]["err_msg"] = BadRequest(ex.Message);
            }
            if (!fromApiExecuteQuery.Columns.Contains("err_msg")) fromApiExecuteQuery.Columns.Add("err_msg", typeof(string));
            return fromApiExecuteQuery;
        }
        public DataTable ExecuteQuery()
        {
            DataTable fromApiExecuteQuery = new DataTable(nameof(ExecuteQuery));
            fromApiExecuteQuery.Columns.Add("err_msg", typeof(string));
            fromApiExecuteQuery.Rows.Add();

            if (_ThroughCloud)
            {
                fromApiExecuteQuery = RunQuery_Cloud();
            }
            else
            {
                fromApiExecuteQuery = RunQuery_Local();
            }
            if (!fromApiExecuteQuery.Columns.Contains("err_msg")) fromApiExecuteQuery.Columns.Add("err_msg", typeof(string));
            return fromApiExecuteQuery;
        }

        public void Dispose()
        {
            //throw new NotImplementedException();
        }

        public String OpenTable()
        {
            String cexpr = "";
            String cTableAlias, cWhere, cRefMemoID, cRefMemoDt, cDepID;
            cTableAlias = _QS._TableAlias;
            cWhere = _QS._Where;
            cRefMemoID = _QS._RefMemoID;
            cRefMemoDt = _QS._RefMemoDt;
            cDepID = LoggedLocation;
            Decimal nNav = _QS._Nav;
            Decimal cQuantity = _QS._Quantity;
            switch (cTableAlias.ToUpper())
            {
                case "TLOCATIONSETTINGSLS":
                    cexpr = @"SELECT HAPPYHOURS_MAXDISCOUNT,ISNULL(hsn_last_updated_on,'') AS hsn_last_updated_on,ISNULL(DISCOUNT_PICKMODE_SLR,0) AS DISCOUNT_PICKMODE_SLR,ENFORCE_OTP_BASED_GR ,Enable_Qr_Code_sale ,Qr_Code_Paymode ,wizclip_dept_id ,ISNULL(ask_for_consumables_billsaving,0) AS ask_for_consumables_billsaving,
                                ISNULL(bill_count_restriction,0) AS bill_count_restriction,ISNULL(AUTO_CALCULATION_OF_ALTERATION_CHARGES,0) AS AUTO_CALCULATION_OF_ALTERATION_CHARGES
                                ,ISNULL(sls_round_item_level,0) AS sls_round_item_level,ISNULL(sls_round_bill_level,0) AS sls_round_bill_level
                                ,PICK_CUSTOMER_GST_STATE_IN_RETAIL_SALE FROM location (NOLOCK) WHERE DEPT_ID ='" + LoggedLocation + "'";
                    break;
                case "TUSERSETTINGSLS":
                    cexpr = @"select cash_refund_mode , cash_refund_limit ,ISNULL(Allow_access_retail_sale_All_users,0) Allow_access_retail_sale_All_users
                        ,ISNULL(discount_percentage_level,0) discount_percentage_level,ISNULL(LIMIT_DAY_FOR_GR,0) AS LIMIT_DAY_FOR_GR
                        FROM users (NOLOCK) WHERE user_code='" + LoggedUserCode + "'";
                    break;
                case "tModuleAccess":
                    cexpr = "Select a.user_code,form_name,form_option,value,group_name  \n" +
                    "From  users a (NOLOCK) \n" +
                    "join USER_ROLE_MST b (NOLOCK) on a.ROLE_ID = b.ROLE_ID \n" +
                    "join USER_ROLE_DET c (NOLOCK) on b.ROLE_id = c.ROLE_ID \n" +
                    "JOIN locusers d  (NOLOCK) ON d.user_code=a.user_code \n" +
                    "where isnull(d.dept_id,'" + LoggedLocation + "') = '" + LoggedLocation + "' \n" +
                    "and a.user_code='" + LoggedUserCode + "'";
                    break;
                case "TMAXCMDT":
                    cexpr = " select isnull(MAX(cm_dt),'') as cm_dt from cmm01106 (NOLOCK) where CANCELLED=0 and location_code='" + LoggedLocation + "' and CM_MODE=1";
                    break;
                case "TINSERTGRHISTORY":
                    cexpr = " INSERT gr_history	( product_code, status, memo_dt )  \n" +
                                    " SELECT  '" + cWhere + "' AS product_code, NULL AS Status, getdate() as memo_dt";
                    break;
                case "TBUYER_ITEM":
                    cexpr=@"EXEC SP_GETBUYER_UNQITEM '" + cWhere + "'";
                    break;
                case "TCUSTOMER":
                    cexpr = @"EXEC SP_RETAILSALE_55 '" + cWhere + "'";
                    break;
                case "TCUSTOMER_ITEM":
                    cexpr =@"EXEC SP_GETCUSTOMER_UNQITEM '" + cWhere + "'";
                    break;
                case "CHECKSOLDSTATUS":
                    cexpr = @"SELECT TOP 1 cmm.cm_dt
                                        FROM cmd01106 cmd  (NOLOCK)
                                        JOIN cmm01106 cmm (NOLOCK) on cmd.cm_id=cmm.cm_id
                                        WHERE cmm.cancelled=0 AND cmd.quantity>0 AND cmd.product_code='" +cWhere+ @"' 
                                        ORDER BY cm_dt DESC,cm_time desc";
                    break;
                case "PICKLASTDISCOUNT":
                    cexpr = @"SELECT COUNT(*) FROM cmd01106 a (NOLOCK)
                            JOIN cmm01106 b (NOLOCK) ON b.cm_id=a.cm_id 
                            WHERE b.cancelled=0 AND QUANTITY>0 and a.product_code='" + cWhere + "'";
                    break;
                case "PICKLASTDISCOUNT1":
                    cexpr = @"EXEC SP_APPLY_SLSDISCTAX '" + cWhere + "','" + cRefMemoDt + "',0,0,0" + ",0,'" + LoggedUserCode + "',1,'" + cWhere + "'";
                    break;
                case "TPROCUCT_DISC_NEW":
                    cTableAlias = "TPROCUCT_DISC";
                    cexpr = "EXEC SPWOW_GETBARCODE_FLATDISCOUNT @CREFMEMODT = '" + cRefMemoDt + "',@cLocationId = '" + LoggedLocation + "',@cProductCode = '" + cWhere + "',@nQty = " + nNav;
                    cexpr = @"
                     DECLARE @CREFMEMODT DATETIME,@cLocationId	VARCHAR(4),@cProductCode VARCHAR(50),@nQty NUMERIC(5,2),@CERRORMSG VARCHAR(MAX)
                    SELECT @CREFMEMODT = '" + cRefMemoDt + "',@cLocationId = '" + LoggedLocation + "',@cProductCode = '" + cWhere + "',@nQty = " + nNav+ @"
                     DECLARE @tSlsBc TABLE (DISCOUNT_PERCENTAGE NUMERIC(6,2),net NUMERIC(10,2),discount_amount NUMERIC(10,2),slsdet_row_id varchar(100),
	                 scheme_Name varchar(100),happy_hours_applied BIT,happyHoursAapplicable bit,additionalScheme bit)

	                INSERT @tSlsBc (DISCOUNT_PERCENTAGE,net,discount_amount,slsdet_row_id ,scheme_Name,happyHoursAapplicable)
	                SELECT TOP 1 a.discountPercentage ,a.netPrice*@Nqty as net,a.discountAmount*@Nqty as discount_amount,a.schemeRowId ,
	                schemeName,b.happy_hours_applicable,b.additionalScheme
	                FROM wow_SchemeSetup_slsbc_flat a (NOLOCK)
	                JOIN wow_SchemeSetup_Title_Det b (NOLOCK) on  a.schemeRowId=b.schemeRowId
	                JOIN wow_schemesetup_mst c (NOLOCK) ON c.setupId=b.setupId
	                left JOIN wow_SchemeSetup_locs d (NOLOCK) ON d.schemeRowId=b.schemeRowId AND d.locationId=@cLocationId
	                WHERE (a.PRODUCT_CODE = @cProductCode or a.product_code=LEFT(@cProductCode, ISNULL(NULLIF(CHARINDEX ('@',@cProductCode)-1,-1),LEN(@cProductCode))))
	                AND @CREFMEMODT BETWEEN d.applicableFromDt AND d.applicableToDt AND
	                (b.locApplicableMode=1 OR d.schemeRowId IS NOT NULL) AND schememode=2 and b.buyFilterMode=2
	 

	                 IF EXISTS (SELECT TOP 1 DISCOUNT_PERCENTAGE FROM @tSlsBc WHERE ISNULL(happyHoursAapplicable,0)=1)
	                 BEGIN
		                 --select * from   @tSlsbc
		                 declare @CurTime DATETIME

		                 SELECT @CurTime = CONVERT(DATETIME,'1900-01-01 '+LTRIM(RTRIM(STR(DATEPART(HH,GETDATE()))))+':'+
				                LTRIM(RTRIM(STR(DATEPART(MI,GETDATE()))))+':00')

		                 IF EXISTS (SELECT TOP 1 schemeRowId FROM  wow_schemesetup_happyhours a (NOLOCK)
					                    JOIN @tSlsBc b ON a.schemeRowId=b.slsdet_row_id WHERE @CurTime BETWEEN a.from_time AND a.to_time)
			                UPDATE  @tSlsBc SET happy_hours_applied=1
		                ELSE
			                DELETE FROM @tSlsbc
	                 END

	                 SELECT *,isnull(@cErrormsg,'') errmsg FROM @tSlsBc";
                    break;
                case "TCUSTOMER_CARD_DISCOUNT":
                    cexpr = "EXEC SP3S_GETCUSTOMER_CARD_DISCOUNT @dXnDt='" + cRefMemoDt + "',@cCUSTOMER_CODE ='" + cWhere + "'";
                    break;
                case "TACTIVE_SCHEMES":
                    cexpr = "EXEC SPWOW_GET_ACTIVE_SCHEME @nQueryId=1, @dXnDt='" + cWhere + "',@cLocId ='" + cDepID + "'";
                    cexpr = @"
                            DECLARE @dXnDt datetime,  @cLocId char(2)
                            SELECT  @dXnDt='" + cWhere + "',@cLocId ='" + cDepID + @"'
                            declare @cCurLocId VARCHAR(4),@cHoLocId VARCHAR(4)
	                    SELECT distinct isnull(memoProcessingOrder,0) memoProcessingOrder,isnull(titleProcessingOrder,0) titleProcessingOrder, a.schemeRowId ,a.buyFilterRepId,
	                    a.buyFilterCriteria, a.getFilterCriteria,a.getFilterRepId,buyFilterCriteria_exclusion,
	                    a.buyFilterMode, a.getFilterMode,a.schemeName,isnull(a.schemebuyType,0) buytype,
	                    a.schemeMode,CONVERT(BIT,(CASE WHEN a.schemeMode=1 AND a.buyFilterMode=2 
	                    THEN 1 ELSE 0 END)) barcodewise_flat_scheme ,a.incrementalScheme,isnull(donot_distribute_weighted_avg_disc_bngn,0) donot_distribute_weighted_avg_disc_bngn,
	                    a.setTotalQty,isnull(a.wizclip_based_scheme,0) wizclip_based_scheme,isnull(schemeApplicableLevel,1) schemeApplicableLevel,
	                    isnull(a.addnlGetFilterCriteria,'') addnlGetFilterCriteria,weekday_wise_applicable,a.applicable_on_friday,a.applicable_on_monday,a.applicable_on_tuesday,
	                    a.applicable_on_wednesday,a.applicable_on_thursday,a.applicable_on_saturday,a.applicable_on_sunday,l.applicableFromDt,l.applicableToDt,a.happy_hours_applicable
                        ,a.additionalScheme
	                    into #tActiveTitles
	                    from wow_SchemeSetup_Title_Det a  (NOLOCK)
	                    JOIN wow_SchemeSetup_mst b  (NOLOCK) ON a.setupId=b.setupId
	                    LEFT JOIN wow_SchemeSetup_locs l (NOLOCK) ON l.schemeRowId=a.schemeRowId AND l.locationId=@cLocId
	                    LEFT JOIN wow_schemesetup_happyhours hh (NOLOCK) ON hh.schemerowid=a.schemerowid
	                    WHERE (@dXnDt BETWEEN l.applicableFromDt AND l.applicableToDt OR @dXnDt='') AND (b.locApplicableMode=1  OR l.schemeRowId IS NOT NULL) 
	                    AND ISNULL(a.inactive,0)=0
	                    AND NOT (a.schemeMode=2 AND a.buyFilterMode=2) 
		
	                    IF EXISTS (SELECT TOP 1 * FROM #tActiveTitles WHERE ISNULL(weekday_wise_applicable,0)=1)
	                    BEGIN
		                    DELETE FROM #tActiveTitles WHERE ISNULL(weekday_wise_applicable,0)=1 AND ((DateName(w,@dXnDt)='Sunday' and isnull(applicable_on_sunday,0)=0) OR
		                    (DateName(w,@dXnDt)='Monday' and isnull(applicable_on_monday,0)=0) OR (DateName(w,@dXnDt)='Tuesday' and isnull(applicable_on_tuesday,0)=0) OR
		                    (DateName(w,@dXnDt)='Wednesday' and isnull(applicable_on_wednesday,0)=0) OR (DateName(w,@dXnDt)='Thursday' and isnull(applicable_on_thursday,0)=0) OR
		                    (DateName(w,@dXnDt)='Friday' and isnull(applicable_on_friday,0)=0) OR (DateName(w,@dXnDt)='Saturday' and isnull(applicable_on_saturday,0)=0)) 
	                    END

	                    select * from #tActiveTitles  ORDER BY titleProcessingOrder DESC
	                    --for json path

	                    SELECT DISTINCT a.schemeRowId,getQty,buyFromRange,buyToRange,discountPercentage,discountAmount,netPrice,rowId,
	                    addnlgetQty,addnldiscountPercentage,addnldiscountAmount,a.schemegetType getType,happy_hours_applicable from wow_SchemeSetup_slabs_Det a  (NOLOCK)
	                    JOIN #tActiveTitles b ON b.schemeRowId=a.schemeRowId
	                    --for json path

	                    select a.schemeRowId,section_name,sub_Section_name,article_no,para1_name,para2_name,para3_name,para4_name,para5_name,para6_name,
	                    a.discountPercentage flat_discountPercentage,a.discountAmount flat_discountAmount,a.netPrice flat_netPrice,
	                    convert(bit,0) buybc,convert(bit,0) getbc,0 targettype,happy_hours_applicable from wow_SchemeSetup_para_combination_flat a
	                    JOIN #tActiveTitles b ON a.schemeRowId=b.schemeRowId
	                    UNION ALL
	                    select a.schemeRowId,section_name,sub_Section_name,article_no,para1_name,para2_name,para3_name,para4_name,para5_name,para6_name,
	                    0 flat_discountPercentage,0 flat_discountAmount,0 flat_netPrice,
	                    convert(bit,1) buybc,convert(bit,0) getbc,0 targettype,happy_hours_applicable from wow_SchemeSetup_para_combination_buy a
	                    JOIN #tActiveTitles b ON a.schemeRowId=b.schemeRowId
	                    UNION ALL
	                    select a.schemeRowId,section_name,sub_Section_name,article_no,para1_name,para2_name,para3_name,para4_name,para5_name,para6_name,
	                    0 flat_discountPercentage,0 flat_discountAmount,0 flat_netPrice,
	                    convert(bit,0) buybc,convert(bit,1) getbc,targettype,happy_hours_applicable from wow_SchemeSetup_para_combination_get a
	                    JOIN #tActiveTitles b ON a.schemeRowId=b.schemeRowId

	                    SELECT a.*,happy_hours_applicable FROM wow_SchemeSetup_para_combination_config a
	                    JOIN #tActiveTitles b ON a.schemeRowId=b.schemeRowId
	
	                    SELECT a.product_code,a.schemeRowId,convert(bit,1) flatdiscount,convert(bit,0) buybc,convert(bit,0) getbc,
	                    a.discountPercentage flat_discountPercentage,a.discountAmount flat_discountAmount,
	                    a.netPrice flat_netprice,0 flat_addnl_discountpercentage,
	                    convert(int,0) schemeMode,convert(bit,0) getbcAddnl
	                    from wow_SchemeSetup_slsbc_flat a (NOLOCK)
	                    JOIN wow_SchemeSetup_slabs_Det c (NOLOCK) ON c.schemeRowId=a.schemeRowId
	                    WHERE 1=2
	
	                    SELECT a.schemerowId,from_time,to_time from  wow_schemesetup_happyhours a 
	                    JOIN #tActiveTitles b ON a.schemeRowId=b.schemeRowId
	                    UNION ALL
	                    SELECT a.schemerowId,from_time,to_time from  wow_schemesetup_happyhours a 
	                    JOIN wow_schemesetup_locs b ON a.schemeRowId=b.schemeRowId
	                    where b.locationId=@cLocId AND @dXnDt BETWEEN b.applicableFromDt AND b.applicableToDt

	                    DROP TABLE #tActiveTitles";
                    break;
                case "TSPWOWGSTCALCBASE":
                    cexpr = "EXEC SPWOW_GST_CALC_QUERIES @nQueryId=1,@dMemoDt='" + GTODAYDATE.ToString("yyyy-MM-dd") + "',@cLocationId ='" + LoggedLocation + "'";
                    cexpr = @"select row_id,hsn_code,mrp,quantity,xn_value_without_gst,xn_value_with_gst,
		                    NET_VALUE_WOTAX,net_value,cgst_amount,sgst_amount,igst_amount,gst_percentage,gst_percentage tax_percentage,
		                    gst_percentage RATE_CUTOFF_TAX_PERCENTAGE,Gst_Cess_Percentage,Gst_Cess_Percentage Rate_CutOff_Gst_Cess_Percentage,
		                    Gst_Cess_Percentage Cess_Percentage,gst_percentage RATE_CUTOFF,
		                    cess_amount,XN_VALUE_WITH_GST,CESS_AMOUNT Gst_Cess_Amount,CONVERT(INT,0) GST_CAL_BASIS,convert(int,0) tax_method
		                    from GST_TAXINFO_CALC (NOLOCK) WHERE 1=2";
                    break;
                case "TSPWOW_GST_CALC_QUERIES":
                    cexpr = "EXEC SPWOW_GST_CALC_QUERIES @nQueryId=2,@dMemoDt='" + cWhere + "',@cLocationId ='" + LoggedLocation + "'";
                    cexpr = @"DECLARE @cLocationId VARCHAR(50),@dMemoDt VARCHAR(20)
                            SELECT @cLocationId = '" + LoggedLocation + @"',@dMemoDt VARCHAR(50) = '" + cWhere + @"'
                            DECLARE @CFC_CODE VARCHAR(7),@CALL_XN_IGST VARCHAR(5),@CALWAYS_PICK_GST_MODE_IN_RETAIL VARCHAR(10),
		                    @OTHER_CHARGES_HSN_CODE VARCHAR(20),@nOHGstPercentage NUMERIC(6,2)
		                    SELECT @CFC_CODE=fc_code  FROM location (NOLOCK) WHERE DEPT_ID=@cLocationId
       
                           SELECT TOP 1 @CALL_XN_IGST=VALUE  FROM CONFIG (NOLOCK) WHERE CONFIG_OPTION ='ALL_XN_IGST' 
       
                           IF ISNULL(@CFC_CODE,'') NOT IN('','0000000')
	       	                    SET @CALL_XN_IGST='1'
		
                           DECLARE @CREUPDATENETEXCL VARCHAR(5),@CREUPDATENETINCL VARCHAR(5),@CEXCLRANGEFROM VARCHAR(15),@CEXCLRANGETO VARCHAR(15),
	                       @CEXCLRANGENET VARCHAR(15),@CINCLRANGEFROM VARCHAR(15),@CINCLRANGETO VARCHAR(15),@CINCLRANGENET VARCHAR(15)

		                    SELECT TOP 1 @CREUPDATENETEXCL=VALUE FROM CONFIG (NOLOCK) WHERE CONFIG_OPTION='REUPDATE_NET_FOR_RANGE_EXCLUSIVE'
		                    SELECT TOP 1 @CREUPDATENETINCL=VALUE FROM CONFIG (NOLOCK) WHERE CONFIG_OPTION='REUPDATE_NET_FOR_RANGE_INCLUSIVE'

	                       SELECT TOP 1 @CEXCLRANGEFROM=VALUE FROM CONFIG (NOLOCK) WHERE CONFIG_OPTION='FROM_NET_RANGE_EXCLUSIVE'
	                       SELECT TOP 1 @CEXCLRANGETO=VALUE FROM CONFIG (NOLOCK) WHERE CONFIG_OPTION='TO_NET_RANGE_EXCLUSIVE'
	                       SELECT TOP 1 @CEXCLRANGENET=VALUE FROM CONFIG (NOLOCK) WHERE CONFIG_OPTION='RANGE_EXCLUSIVE_CONVERT_NET'
	                       SELECT TOP 1 @CINCLRANGEFROM=VALUE FROM CONFIG (NOLOCK) WHERE CONFIG_OPTION='FROM_NET_RANGE_INCLUSIVE'
	                       SELECT TOP 1 @CINCLRANGETO=VALUE FROM CONFIG (NOLOCK) WHERE CONFIG_OPTION='TO_NET_RANGE_INCLUSIVE'
	                       SELECT TOP 1 @CINCLRANGENET=VALUE FROM CONFIG (NOLOCK) WHERE CONFIG_OPTION='RANGE_INCLUSIVE_CONVERT_NET'

	                       SELECT @CALWAYS_PICK_GST_MODE_IN_RETAIL=value  FROM CONFIG (NOLOCK) WHERE CONFIG_OPTION='ALWAYS_PICK_GST_MODE_IN_RETAIL_SALE_FROM_HSN_MASTER'

	                       IF EXISTS (SELECT TOP 1 HSN_CODE FROM GST_OH_CONFIG (NOLOCK) WHERE OH_NAME='OC' AND XN_TYPE='SLS')
		                       SELECT TOP 1 @OTHER_CHARGES_HSN_CODE=HSN_CODE,@nOHGstPercentage=GST_PERCENTAGE FROM GST_OH_CONFIG (NOLOCK) WHERE OH_NAME='OC' 
		                       AND XN_TYPE='SLS'
	                       ELSE
	   	                       SELECT TOP 1 @OTHER_CHARGES_HSN_CODE=HSN_CODE,@nOHGstPercentage=GST_PERCENTAGE FROM GST_OH_CONFIG (NOLOCK) WHERE OH_NAME='OC' 

	                       SELECT TOP 1 ISNULL(a.GST_STATE_CODE,'') CURSTATE_CODE,LOC_GST_NO,ISNULL(REGISTERED_GST,0) REGISTERED_DEALER,
	                       ISNULL(CESS_APPLICABLE,0) CESS_APPLICABLE,ISNULL(b.CESS_PERCENTAGE,0) CESS_PERCENTAGE,ISNULL(@CALL_XN_IGST,'') ALL_XN_IGST,
	                       ISNULL(@CALWAYS_PICK_GST_MODE_IN_RETAIL,'') ALWAYS_PICK_GST_MODE_IN_RETAIL_SALE_FROM_HSN_MASTER,
	                       CONVERT(VARCHAR(20),'') party_gst_no,CONVERT(VARCHAR(20),'') party_state_code,
	                       CONVERT(BIT,0) custdym_export_gst_percentage_Applicable,convert(numeric(7,2),0) custdym_export_gst_percentage,
	                       ISNULL(@OTHER_CHARGES_HSN_CODE,'') OTHER_CHARGES_HSN_CODE,ISNULL(@nOHGstPercentage,0) OTHER_CHARGES_Gst_Percentage,
	                       @dMemoDt memo_dt,@CREUPDATENETEXCL REUPDATE_NET_FOR_RANGE_EXCLUSIVE,@CREUPDATENETINCL REUPDATE_NET_FOR_RANGE_INCLUSIVE,@CEXCLRANGEFROM  FROM_NET_RANGE_EXCLUSIVE,
	                       @CEXCLRANGETO TO_NET_RANGE_EXCLUSIVE,@CEXCLRANGENET RANGE_EXCLUSIVE_CONVERT_NET,@CINCLRANGEFROM FROM_NET_RANGE_INCLUSIVE,@CINCLRANGETO TO_NET_RANGE_INCLUSIVE,
	                       @CINCLRANGENET  RANGE_INCLUSIVE_CONVERT_NET

	                       FROM LOCATION A (NOLOCK)
	                       LEFT JOIN GST_STATE_DET b (NOLOCK) ON a.gst_state_code=b.GST_STATE_CODE
	                       AND @dMemoDt BETWEEN B.FM_DT AND B.TO_DT
	                       WHERE A.DEPT_ID =@cLocationId 
	                       --FOR JSON Path
       
                           IF ISNULL(@CFC_CODE,'') NOT IN('','0000000')
       		                    SET @CALL_XN_IGST='1'
	
		                    SELECT hsn_code,RETAILSALE_TAX_METHOD FROM hsn_mst (NOLOCK)
		                    --FOR JSON Path

		                    ;WITH CTE AS
			                    (
			                    SELECT a.HSN_CODE,ISNULL(C.TAX_PERCENTAGE,0) AS TAX_PERCENTAGE,ISNULL(C.RATE_CUTOFF,0) AS RATE_CUTOFF,
					                    ISNULL(C.RATE_CUTOFF_TAX_PERCENTAGE,0) AS RATE_CUTOFF_TAX_PERCENTAGE,
					                    ISNULL(C.WEF,'') AS WEF,
					                    SR=ROW_NUMBER() OVER (PARTITION BY A.hsn_code ORDER BY C.WEF DESC),
					                    ISNULL(C.GST_CAL_BASIS,1) AS GST_CAL_BASIS,
					                    isnull(c.Rate_CutOff_Gst_Cess_Percentage,0) as Rate_CutOff_Gst_Cess_Percentage,
					                    isnull(c.Gst_Cess_Percentage,0) as Gst_Cess_Percentage
			                    FROM hsn_mst A (NOLOCK)
			                    LEFT JOIN HSN_DET C (NOLOCK) ON a.HSN_CODE =C.HSN_CODE AND C.WEF  <=@dMemoDt AND ISNULL(C.DEPT_ID,'')=
			                    (CASE WHEN ISNULL(@CFC_CODE,'')IN('','0000000') THEN  '' ELSE @cLocationId END)
			                    )
			                    SELECT *
			                    FROM CTE WHERE SR=1
			                    --FOR JSON Path";
                    break;
                case "DTPENDINGITEMS":
                    cexpr = @"EXEC SP_GETPENDINGS_APM " + nNav + ",'" + cWhere + "','" + LoggedLocation + "','" + LoggedUserCode + "','" + LoggedBinID + "'";
                    break;

                case "TAPPLU":

                    String cWhere1 = (ShowDataForAllUser || LoggedUserCode == "0000000" ? "" : "USER_CODE='" + LoggedUserCode + "'");
                    cWhere1 = cWhere1.Replace("'", "''");
                    String cWhereClause = "";


                    if ((LoggedLocation != _GHOLOCATION))
                    {
                        if (_CENTRALIZED_LOC)
                            cWhereClause = " location_code= ''" + LoggedLocation + "'' ";
                    }
                    else
                    {
                        //if (LoggedUserCode != "0000000")
                        //    cWhereClause = " LEFT(CM_ID,2) IN (" + GlobalCls.cUserForLocs.Replace("'", "''") + ") ";
                    }

                    if (cWhereClause.Trim() == "") cWhereClause = cWhere1; else cWhereClause += cWhere1.Trim() == "" ? "" : " AND " + cWhere1;
                    if (cWhere.Trim() == "") cWhere = cWhereClause;
                    else cWhere += cWhereClause.Trim() == "" ? "" : " AND " + cWhereClause;


                    cexpr = "EXEC SP_RTLSL " +
                             " @cQueryID=1" +
                             ",@cWhere='" + cWhere + "'" +
                             ",@cFinYear='" + _FINYEAR + "'" +
                             ",@cDeptID='" + LoggedLocation + "'" +
                             ",@nNavMode=" + nNav.ToString() +
                             ",@cWizAppUserCode='" + LoggedUserCode + "'" +
                             ",@cRefMemoId='" + cRefMemoID + "'" +
                             ",@cRefMemoDt='" + cRefMemoDt + "'" +
                             ",@bIncludeEstimate=1";// +(GlobalCls.bStatus ? 1 : 0).ToString() + "";

                    // cexpr = "EXEC SP_RTLSL 1,'','" + cWhere + "',''," + nNav.ToString() + ",'" + AppAIVAL.GC_C + AppAIVAL.GFIN_YEAR + "'";
                    break;
                case "TQBF":
                    cexpr = "SELECT DISTINCT cm_id as cm_id,mst_cm_no as cm_no --,mst_cm_dt as xn_dt  \n" +
                            "FROM VW_WL_RPSMST_QBF " + cWhere;
                    cTableAlias = "tAPPLU";
                    break;
                case "TAPP_MST":
                    cexpr = "EXEC SP_RTLSL 3,'" + cWhere + "',''," + nNav.ToString();
                    cexpr = @"
                        SELECT CMM.CM_NO AS REF_SLS_MEMO_NO,CMM.CM_DT AS REF_SLS_MEMO_DT,  A.*, C.USERNAME,ISNULL(X.EMP_CODE,'0000000') AS [EMP_CODE],
                        ISNULL(X.EMP_CODE1,'0000000')  AS [EMP_CODE1],ISNULL(X.EMP_CODE2,'0000000') AS [EMP_CODE2],
                        ISNULL(X.EMP_NAME,'') AS [EMP_NAME],ISNULL(X.EMP_NAME1,'') AS [EMP_NAME1],
                        ISNULL(X.EMP_NAME2,'') AS [EMP_NAME2]   ,ISNULL(BIN.BIN_NAME,'')  AS [BIN_NAME]
                        ,CAST(ISNULL(TAX,0) AS NUMERIC(14,2)) AS [TOTAL_TAX] ,CUST.*, '' AS CUSTOMER_NAME,'' AS ADDRESS,
                        CAST('' AS VARCHAR(40)) AS SP_ID,CAST(0 AS NUMERIC(14,2)) AS SUBTOTAL_R,A.SUBTOTAL AS SUBTOTAL_T,CAST(0 AS BIT ) AS dp_changed
                        FROM RPS_MST A    
                        JOIN USERS C ON C.USER_CODE=A.USER_CODE  
                        LEFT OUTER JOIN
                        (
                            SELECT TOP 1 CM_ID,E1.EMP_CODE,E2.EMP_CODE  AS [EMP_CODE1],E3.EMP_CODE AS [EMP_CODE2],E1.EMP_NAME AS [EMP_NAME]
                            ,E2.EMP_NAME AS [EMP_NAME1],E3.EMP_NAME AS [EMP_NAME2]
                            FROM RPS_DET A (NOLOCK)
                            JOIN EMPLOYEE E1 (NOLOCK) ON E1.EMP_CODE=A.EMP_CODE
                            JOIN EMPLOYEE E2 (NOLOCK) ON E2.EMP_CODE=A.EMP_CODE1
                            JOIN EMPLOYEE E3 (NOLOCK) ON E3.EMP_CODE=A.EMP_CODE2
                            WHERE A.CM_ID='" + cWhere + @"' AND (A.EMP_CODE<>'0000000' OR A.EMP_CODE2<>'0000000' OR A.EMP_CODE2<>'0000000')
                        )X ON X.CM_ID=A.CM_ID
                        LEFT OUTER JOIN
                        (
                            SELECT SUM(TAX_AMOUNT) AS [TAX] ,CM_ID	FROM RPS_DET A (NOLOCK)
                            WHERE TAX_METHOD=2 AND CM_ID='" + cWhere + @"'
                            GROUP BY CM_ID
                        )X1 ON X1.CM_ID=A.CM_ID  
                        LEFT OUTER JOIN BIN ON BIN.BIN_ID=A.BIN_ID
                        LEFT OUTER JOIN CUSTDYM  CUST ON A.CUSTOMER_CODE=CUST.CUSTOMER_CODE
                        LEFT OUTER JOIN CMM01106 CMM  (NOLOCK) ON CMM.CM_ID= A.ref_cm_id
                        WHERE  A.CM_ID= '" + cWhere + "'";
                    break;
                case "TAPP_DET":
                    cexpr = "EXEC SP_RTLSL 4,'" + cWhere + "','',''";
                    cexpr = @"
                    SELECT  CMM.CM_NO AS REF_SLS_MEMO_NO,CMM.CM_DT AS REF_SLS_MEMO_DT, A.*,ROW_NUMBER() OVER (ORDER BY A.TS) AS SRNO,    
                    EMP.EMP_NAME, A.PRODUCT_CODE, B.ARTICLE_CODE, B.ARTICLE_NO, B.ARTICLE_NAME, S.PARA1_CODE,  
                    SN.PARA1_NAME, S.PARA2_CODE, SN.PARA2_NAME, S.PARA3_CODE, SN.PARA3_NAME, E.UOM_NAME,     
                    A.DEPT_ID, S.BARCODE_CODING_SCHEME AS CODING_SCHEME,  B.INACTIVE, ISNULL(P.QUANTITY_IN_STOCK,0) AS QUANTITY_IN_STOCK,  
                    S.PURCHASE_PRICE,  S.MRP,S.WS_PRICE,  '' AS SCHEME_ID, SN.SECTION_NAME, SN.SUB_SECTION_NAME,  
                    S.PARA4_CODE,S.PARA5_CODE,S.PARA6_CODE,  
                    PARA4_NAME,PARA5_NAME,PARA6_NAME,E.UOM_CODE,ISNULL(E.UOM_TYPE,0) AS [UOM_TYPE],  
                    B.DT_CREATED AS [ART_DT_CREATED],'' AS [PARA3_DT_CREATED],S.DT_CREATED AS [SKU_DT_CREATED],  
                    B.STOCK_NA,    
                    CONVERT (BIT,(CASE WHEN A.QUANTITY <0 THEN 1 ELSE 0 END)) AS SALERETURN ,CAST(0 AS BIT) AS CREDIT_REFUND,
	                    A.MRP AS [LOCSKU_MRP],S.PRODUCT_NAME,  
                    EMP1.EMP_NAME AS EMP_NAME1 ,EMP2.EMP_NAME AS EMP_NAME2  ,
                    (CASE WHEN ISNULL(S.FIX_MRP,0)=0 THEN S.MRP ELSE S.FIX_MRP END) AS [FIX_MRP],
                    (CASE WHEN ISNULL(A.HOLD_FOR_ALTER,0)=0 THEN 'N' ELSE 'Y' END) AS [HOLD_FOR_ALTER_TXT] ,
                    ISNULL(BIN.BIN_NAME,'')  AS [BIN_NAME],B.ALIAS AS ARTICLE_ALIAS  ,'' SUB_SECTION_CODE,'' SECTION_CODE,
                    ISNULL(S1.SLS_TITLE,'') AS [SLS_TITLE] ,CAST('' AS VARCHAR(40)) AS SP_ID,
                    (CASE WHEN ISNULL(A.REF_APPROVAL_MEMO_ID,'')='' THEN '' ELSE RIGHT(A.REF_APPROVAL_MEMO_ID,10 ) END) AS REF_APPROVAL_MEMO_NO
                    ,CAST(0 AS NUMERIC(14,2)) AS manual_discount_percentage,
                    CAST(0 AS NUMERIC(14,2)) AS manual_discount_amount,CAST(0 AS BIT) AS ManualDA_changed,CAST(0 AS VARCHAR(100)) AS DET_REMARKS,CAST(0 AS BIT) AS  manual_mrp
                    ,CAST(CASE WHEN CHARINDEX('@',A.PRODUCT_CODE)=0 THEN '' ELSE 
                    (SUBSTRING(A.PRODUCT_CODE,CHARINDEX('@',A.PRODUCT_CODE)+1,15)) END AS VARCHAR(100))  AS BATCH_LOT_NO,
                    S.BATCH_NO,S.EXPIRY_DT,CAST('' AS DATETIME) AS [rps_last_update],S.er_flag
                    ,CAST(0 AS BIT) AS barcodebased_flatdisc_applied,CAST(0 AS BIT) AS bngn_not_applied,CAST(0 AS BIT) AS happy_hours_applied,SN.sku_item_type AS  ITEM_TYPE,'' as scheme_name,SN.*
                    FROM  RPS_DET  A (NOLOCK)  
                    JOIN RPS_MST MST(NOLOCK) ON MST.cm_id=A.cm_id
                    LEFT OUTER JOIN PMT01106 P (NOLOCK) ON A.PRODUCT_CODE = P.PRODUCT_CODE AND A.BIN_ID = P.BIN_ID   and MST.LOCATON_CODE= P.dept_id
                    LEFT OUTER JOIN BIN (NOLOCK) ON BIN.BIN_ID = A.BIN_ID   
                    JOIN SKU S (NOLOCK) ON S.PRODUCT_CODE=A.PRODUCT_CODE  
                    LEFT JOIN sku_names sn (NOLOCK) ON sn.product_code=a.product_code
                    JOIN ARTICLE B (NOLOCK) ON S.ARTICLE_CODE = B.ARTICLE_CODE      
                    JOIN UOM   E (NOLOCK) ON B.UOM_CODE = E.UOM_CODE   
                    JOIN EMPLOYEE EMP (NOLOCK) ON A.EMP_CODE = EMP.EMP_CODE	
                    LEFT OUTER JOIN EMPLOYEE EMP1  (NOLOCK) ON A.EMP_CODE1= EMP1.EMP_CODE     
                    LEFT OUTER JOIN EMPLOYEE EMP2  (NOLOCK) ON A.EMP_CODE2= EMP2.EMP_CODE     
                    LEFT OUTER JOIN SLSDET S1 (NOLOCK) ON S1.ROW_ID= A.SLSDET_ROW_ID 
                    LEFT OUTER JOIN CMM01106 CMM  (NOLOCK) ON CMM.CM_ID= MST.ref_cm_id   
                    WHERE A.CM_ID='" + cWhere + @"'  
                    ORDER BY  ROW_NUMBER() OVER (ORDER BY A.TS) ";
                    break;
                case "TPRODUCT_CODE":
                    //cexpr = "EXEC SP_RTLSL 10, '" + LoggedLocation + "','',''";
                    cexpr = @"
                    SELECT DISTINCT TOP 50 SKU.PRODUCT_CODE,ARTICLE_CODE,P.QUANTITY_IN_STOCK AS [QUANTITY],P.DEPT_ID
                    FROM SKU  (NOLOCK)    
                    JOIN PMT01106 P (NOLOCK) ON P.PRODUCT_CODE=SKU.PRODUCT_CODE  
                    WHERE 1=2
                    ";
                    break;
                case "TEMPLOYEE":
                    //cexpr = "EXEC SP_RTLSL 6, '" + LoggedLocation + "','',''";
                    cexpr = @"
                    SELECT A.EMP_CODE, EMP_NAME, EMP_NAME AS EMP_NAME_ORG,0 AS ALIASENTRY   
                    FROM EMPLOYEE  A(NOLOCK) 
                    JOIN EMP_GRP_LINK B (NOLOCK) ON A.emp_code=B.EMP_CODE 
                    Join EMPLOYEE_GRP C on B.EMP_GRP_CODE = C.EMP_GRP_CODE 
                    WHERE A.INACTIVE = 0    AND EMP_TYPE in (1,3) AND C.dept_id = '" + LoggedLocation + @"' 
                    UNION   
                    SELECT A.EMP_CODE, EMP_ALIAS AS EMP_NAME, EMP_NAME AS EMP_NAME_ORG,1 AS ALIASENTRY    
                    FROM EMPLOYEE  A(NOLOCK) 
                    JOIN EMP_GRP_LINK B (NOLOCK) ON A.emp_code=B.EMP_CODE  
                    Join EMPLOYEE_GRP C on B.EMP_GRP_CODE = C.EMP_GRP_CODE 
                    WHERE A.INACTIVE = 0 AND EMP_TYPE in (1,3) AND EMP_ALIAS <>''   AND C.dept_id = '" + LoggedLocation + @"'
                    ORDER BY EMP_NAME   ";
                    break;
                case "TSTOCK":
                    DateTime Dt = GTODAYDATE;
                    TimeSpan t = new TimeSpan(System.DateTime.Now.Hour, System.DateTime.Now.Minute, System.DateTime.Now.Second);
                    Dt = Dt + t;
                    cexpr = "EXEC SP_CHECKSTOCK_BATCH '" + cWhere + "'," + nNav + ",'" + LoggedBinID + "','" + Dt.ToString("yyyy-MM-dd HH:mm") + "'," + (cQuantity).ToString() + ",'" + LoggedUserCode + "',0,'SLS',1,0,1,'" + LoggedLocation + "'";
                    break;
                
                case "TSCHEME_DET":
                    //cexpr = "EXEC SP_RTLSL 19, '" + cWhere + "','',''";
                    cexpr = @"
                    SELECT A.* ,CAST(0 AS INT) AS SP_ID FROM CMD_SCHEME_DET A    
                    JOIN RPS_DET B ON B.ROW_ID=A.CMD_ROW_ID     
                    WHERE B.CM_ID='" + cWhere + "'";
                    break;
                case "TSUMQUNTITY":
                    //cexpr = "EXEC SP_RTLSL 20, '" + cWhere + "','','" + cDepID + "'," + nNav + ",'','" + cRefMemoID + "','',''," + cQuantity + "";
                    cexpr = @"
                    DECLARE @NPENDINGQTY NUMERIC(10,3),@NSTOCKQTY NUMERIC(10,3),@CERRORMSG VARCHAR(MAX)  
   
                    SELECT @NPENDINGQTY=SUM(A.QUANTITY) 
                    FROM RPS_DET A 
                    JOIN RPS_MST B ON B.CM_ID=A.CM_ID   
                    WHERE A.PRODUCT_CODE='" + cWhere + @"'
                    AND ISNULL(B.REF_CM_ID,'')=''
                    AND  B.CM_ID<>'" + cRefMemoID + @"' AND B.CANCELLED=0  


                    SELECT @NSTOCKQTY=QUANTITY_IN_STOCK FROM PMT01106 WHERE PRODUCT_CODE='" + cWhere + @"' AND DEPT_ID='" + cDepID + @"'


                    IF @NSTOCKQTY-ISNULL(@NPENDINGQTY,0)-@NQUANTITY<0  
	                    SET @CERRORMSG=' BAR CODE : '" + cWhere + @"' QUANTITY IN STOCK IS GOING NEGATIVE'  
                    ELSE  
	                    SET @CERRORMSG=''   
		
                    SELECT @CERRORMSG AS ERRMSG  
                    ";
                    break;
                case "TPROCUCT_DISC":
                    cexpr = "EXEC SP_RETAILSALE 23, '" + cWhere + "','',''";
                    cexpr = "EXEC SP_RETAILSALE_23 23, '" + cWhere + "','','" + LoggedLocation + "',0,'','','" + cRefMemoDt + "',0,'','',0,'" + cRefMemoID + "'," + nNav;
                    break;
                case "TPACK_SLIP_REF":
                    cexpr = "EXEC SP_RTLSL 27, '" + cWhere + "','',''";
                    cexpr = @"
                    SELECT REF_CM_ID AS  CM_ID ,CM_ID AS  PACK_SLIP_ID, LAST_UPDATE 
                    FROM RPS_MST (NOLOCK) 
                    WHERE CM_ID='" + cWhere + @"' AND  ISNULL(REF_CM_ID,'')<>''";
                    break;

                case "TLASTSALESPERSON":
                    cexpr = "EXEC SP_RTLSL 28, '" + cWhere + "','',''";
                    cexpr = @"
                    SELECT TOP 1 A.CM_ID ,A.EMP_CODE,A.EMP_CODE1,A.EMP_CODE2,ISNULL(EMP1.EMP_NAME,'') AS EMP_NAME,
                    ISNULL(EMP2.EMP_NAME,'') AS EMP_NAME1 ,ISNULL(EMP3.EMP_NAME,'') AS EMP_NAME2
                    FROM RPS_DET A (NOLOCK)
                    JOIN RPS_MST B (NOLOCK) ON A.CM_ID= B.CM_ID 
                    LEFT OUTER JOIN EMPLOYEE EMP1 ON EMP1.EMP_CODE=A.EMP_CODE
                    LEFT OUTER JOIN EMPLOYEE EMP2 ON EMP2.EMP_CODE=A.EMP_CODE1
                    LEFT OUTER JOIN EMPLOYEE EMP3 ON EMP3.EMP_CODE=A.EMP_CODE2
                    WHERE B.CANCELLED=0 AND A.QUANTITY>0 AND A.PRODUCT_CODE = '" + cWhere + @"'
                    ORDER BY B.CM_DT DESC";
                    break;

                case "TBILL":
                    cexpr = "EXEC SP_RETAILSALE @CQUERYID=20, @CWHERE='" + cWhere + "',@cCustCode='" + cRefMemoDt + "'";
                    break;
                case "TBILL_ITEMS":
                    //bAppend = (cWhere.Trim()!="");
                    cexpr = "EXEC SP_RETAILSALE @CQUERYID=21,@CWHERE='" + cWhere + "',@CREFMEMOID='" + cRefMemoID + "'";
                    break;
                case "TBILL_CONDITION":
                    cexpr = "EXEC SP_RETAILSALE @CQUERYID=22, @CWHERE='" + cWhere + "',@cCustCode='" + cRefMemoDt + "'";
                    break;
                case "TSLS_LIST_FILTER":
                    cexpr = "EXEC SP3S_RPS_FILTER " + cWhere;
                    break;
                case "TBILLSTATUS2":
                    cexpr = "EXEC SP3S_GET_COMBOITEMS @cXnType='SLS',@cComboType ='BILL_STATUS', @nMode=1";
                    break;
                case "TCANCELSATUS2":
                    cexpr = "EXEC SP3S_GET_COMBOITEMS @cXnType='SLS',@cComboType ='CANCELLED', @nMode=1";
                    break;
                case "DTCOLLIST":
                    cexpr = "SELECT * FROM GRID_COLUMN_LIST WHERE MODULENAME='" + cWhere.Trim() + "' AND USER_CODE='" + LoggedUserCode + "'";
                    break;
                case "TCUSTOMER1":
                    cexpr = "SELECT TOP 50 customer_fname,customer_lname,customer_code," +
                            "user_customer_code FROM custdym  WHERE customer_code <>'000000000000' AND 1=2";
                    break;
            }
            return cexpr;
        }
    }


    

    





}
