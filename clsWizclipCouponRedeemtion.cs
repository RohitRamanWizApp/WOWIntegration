
using Newtonsoft.Json;
using System;
using System.Data;
using System.IO;
using System.Net;
using System.Text;


namespace WOWIntegration
{
    //internal class RestAPIRestClient
    //{
    //    public string EndPoint { get; set; }

    //    public HttpVerb Method { get; set; }

    //    public string ContentType { get; set; }

    //    public string PostData { get; set; }

    //    public RestAPIRestClient()
    //    {
    //        this.EndPoint = "";
    //        this.Method = HttpVerb.GET;
    //        this.ContentType = "text/xml";
    //        this.PostData = "";
    //    }

    //    public RestAPIRestClient(string endpoint)
    //    {
    //        this.EndPoint = endpoint;
    //        this.Method = HttpVerb.GET;
    //        this.ContentType = "text/xml";
    //        this.PostData = "";
    //    }

    //    public RestAPIRestClient(string endpoint, HttpVerb method)
    //    {
    //        this.EndPoint = endpoint;
    //        this.Method = method;
    //        this.ContentType = "application/xml";
    //        this.PostData = "";
    //    }

    //    public RestAPIRestClient(string endpoint, HttpVerb method, string postData)
    //    {
    //        this.EndPoint = endpoint;
    //        this.Method = method;
    //        this.ContentType = "application/json";
    //        this.PostData = postData;
    //    }

    //    public string MakeRequest() => this.MakeRequest(string.Empty, string.Empty, string.Empty, string.Empty);

    //    public string MakeRequest(
    //      string parameters,
    //      string postParameters,
    //      string username,
    //      string password)
    //    {
    //        try
    //        {
    //            string requestUriString = this.EndPoint + parameters;
    //            string str1 = password;
    //            string userName = username;
    //            string password1 = str1;
    //            HttpWebRequest httpWebRequest = (HttpWebRequest)WebRequest.Create(requestUriString);
    //            NetworkCredential networkCredential = new NetworkCredential(userName, password1);
    //            httpWebRequest.Credentials = (ICredentials)networkCredential;
    //            httpWebRequest.Method = this.Method.ToString();
    //            httpWebRequest.ContentLength = 0L;
    //            httpWebRequest.ContentType = this.ContentType;
    //            httpWebRequest.Timeout =  30000;
    //            httpWebRequest.KeepAlive = false;
    //            httpWebRequest.PreAuthenticate = true;
    //            httpWebRequest.Headers.Add(HttpRequestHeader.AcceptCharset, "utf-8");
    //            ServicePointManager.Expect100Continue = false;
    //            httpWebRequest.MediaType = "text/xml;charset=\"utf-8\"";
    //            if (!string.IsNullOrEmpty(postParameters) && this.Method == HttpVerb.POST)
    //            {
    //                httpWebRequest.Method = "POST";
    //                byte[] bytes = Encoding.UTF8.GetBytes(postParameters.ToString());
    //                httpWebRequest.ContentLength = (long)bytes.Length;
    //                Stream requestStream = httpWebRequest.GetRequestStream();
    //                try
    //                {
    //                    requestStream.Write(bytes, 0, bytes.Length);
    //                    requestStream.Close();
    //                }
    //                catch (Exception ex)
    //                {
    //                }
    //            }
    //            using (HttpWebResponse response = (HttpWebResponse)httpWebRequest.GetResponse())
    //            {
    //                string str2 = string.Empty;
    //                if (response.StatusCode != HttpStatusCode.OK)
    //                    throw new ApplicationException(string.Format("Request failed. Received HTTP {0}", (object)response.StatusCode));
    //                using (Stream responseStream = response.GetResponseStream())
    //                {
    //                    if (responseStream != null)
    //                    {
    //                        using (StreamReader streamReader = new StreamReader(responseStream))
    //                            str2 = streamReader.ReadToEnd();
    //                    }
    //                }
    //                return str2;
    //            }
    //        }
    //        catch (Exception ex)
    //        {
    //            return (string)null;
    //        }
    //    }
    //}
    
    public class WIZCLIP_REDEEM_COUPON
    {
        public class MEMBER_DETAILS
        {
            public String mode { get; set; }
            public String ecoupon_Id { get; set; }
            public String customer_Code { get; set; }
            public String mobile { get; set; }
            public String location_Id { get; set; }
            public String cm_Id { get; set; }
            public String cm_no { get; set; }
            public String cm_dt { get; set; }
        }
        private string GenerateXML_RedeemCoupon(MEMBER_DETAILS MD, string cToken)
        {
            StringBuilder stringBuilder1 = new StringBuilder();
            StringBuilder stringBuilder2 = new StringBuilder();
            stringBuilder1.AppendLine("{");
            stringBuilder1.AppendLine("ecoupon_Id : \"" +  MD.ecoupon_Id + "\",");
            stringBuilder1.AppendLine("customer_Code : \"" + MD.customer_Code + "\",");
            stringBuilder1.AppendLine("mobile : \"" + MD.mobile + "\",");
            stringBuilder1.AppendLine("location_Id : \"" + MD.location_Id + "\",");
            stringBuilder1.AppendLine("cm_id : \"" + MD.cm_Id + "\",");
            stringBuilder1.AppendLine("cm_no : \"" + MD.cm_no + "\",");
            stringBuilder1.AppendLine("cm_dt : \"" + MD.cm_dt + "\"");
            stringBuilder1.AppendLine("}");
            return stringBuilder1.ToString();
        }

        public DataTable GetDetailsFromAPI_RedeemCoupon(string cReturnedStr)
        {
            DataTable fromApiRedeemCoupon = new DataTable("RedeemCoupon");
            fromApiRedeemCoupon.Columns.Add("err_msg", typeof(string));
            fromApiRedeemCoupon.Rows.Add();
            if (string.IsNullOrEmpty(cReturnedStr))
            {
                fromApiRedeemCoupon.Rows[0]["err_msg"] = (object)"String not return by API";
                return fromApiRedeemCoupon;
            }
            try
            {
               
                string str1 = Convert.ToString(cReturnedStr);
                //string serializedObject = Newtonsoft.Json.JsonConvert.SerializeObject(str1, Newtonsoft.Json.Formatting.Indented);
                //serializedObject =Convert.ToString( Body);
                //Object loc = Newtonsoft.Json.JsonConvert.DeserializeObject<Object>(serializedObject);
                fromApiRedeemCoupon =JsonConvert.DeserializeObject<DataTable>(str1);
                //string[] separator1 = new string[1] { "{" };
                //foreach (string str2 in str1.Split(separator1, StringSplitOptions.RemoveEmptyEntries))
                //{
                //    string[] separator2 = new string[1] { "," };
                //    foreach (string str03 in str2.Split(separator2, StringSplitOptions.RemoveEmptyEntries))
                //    {
                //        String str3 = str03.Replace("[", "").Replace("]", "");
                //        string[] strArray = str3.Replace("\"", "").Replace("}", "").Split(new string[1]
                //        {
                //            ":"
                //        }, StringSplitOptions.RemoveEmptyEntries);
                //        if (strArray.Length != 0)
                //            fromApiRedeemCoupon.Columns.Add(strArray[0], typeof(string));
                //        if (strArray.Length > 1)
                //            fromApiRedeemCoupon.Rows[0][strArray[0]] = (object)strArray[1];
                //    }
                //}
                if (!fromApiRedeemCoupon.Columns.Contains("err_msg"))
                {
                    fromApiRedeemCoupon.Columns.Add("err_msg", typeof(string));                  
                }
                if (fromApiRedeemCoupon.Columns.Contains("error_msg"))
                {
                    if (!String.IsNullOrEmpty(Convert.ToString(fromApiRedeemCoupon.Rows[0]["error_msg"])))
                        fromApiRedeemCoupon.Rows[0]["err_msg"] = Convert.ToString(fromApiRedeemCoupon.Rows[0]["error_msg"]);
                }
                if (fromApiRedeemCoupon.Columns.Contains("Message"))
                {
                    if (!String.IsNullOrEmpty(Convert.ToString(fromApiRedeemCoupon.Rows[0]["Message"])))
                        fromApiRedeemCoupon.Rows[0]["err_msg"] = Convert.ToString(fromApiRedeemCoupon.Rows[0]["Message"]);
                }

            }
            catch (Exception ex)
            {
                fromApiRedeemCoupon.Rows[0]["err_msg"] = (object)("GetDetailsFromAPI_RedeemCoupon : " + ex.Message);
            }
            return fromApiRedeemCoupon;
        }

        public DataTable RedeemCoupon(
          string cPath,
          MEMBER_DETAILS MD,
          string cCredentialName,
          string cCredentialPassword,
          string cAPIAddress)
        {
            DataTable dataTable = new DataTable(nameof(RedeemCoupon));
            dataTable.Columns.Add("err_msg", typeof(string));
            dataTable.Rows.Add();
            try
            {
                //if (string.IsNullOrEmpty(GET_TOKEN._SecurityToken))
                //{
                //    new GET_TOKEN().Token(cPath, "", cCredentialName, cCredentialPassword, cAPIAddress);
                //    if (string.IsNullOrEmpty(GET_TOKEN._SecurityToken))
                //    {
                //        dataTable.Rows[0]["err_msg"] = (object)"REDEEM_COUPON.RedeemCoupon : Token Not Found";
                //        dataTable.Rows[0].EndEdit();
                //        return dataTable;
                //    }
                //}
                string xmlRedeemCoupon = this.GenerateXML_RedeemCoupon(MD, ""/*GET_TOKEN._SecurityToken*/);
                File.WriteAllText(cPath + "\\_RedeemCoupon.txt", Convert.ToString(xmlRedeemCoupon));
                string cReturnedStr = new RestAPIRestClient(cAPIAddress, HttpVerb.POST, xmlRedeemCoupon).MakeRequestWizApp("/RedeemECoupon?GroupCode=" + cCredentialName + "&nMode=" + MD.mode, xmlRedeemCoupon, cCredentialName, cCredentialPassword);
                File.WriteAllText(cPath + "\\_RedeemCoupon_Returned.txt", Convert.ToString(cReturnedStr));
                dataTable = this.GetDetailsFromAPI_RedeemCoupon(cReturnedStr);
            }
            catch (Exception ex)
            {
                dataTable.Rows[0]["err_msg"] = (object)ex.Message;
            }
            return dataTable;
        }
    }
}