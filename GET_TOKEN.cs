using System;
using System.Data;
using System.IO;
using System.Text;

namespace WOWIntegration
{
  public class GET_TOKEN
  {
    public static string _SecurityToken = "";

        private string GenerateXML_Token(string cUserName, string cPassword)
        {
            StringBuilder stringBuilder1 = new StringBuilder();
            StringBuilder stringBuilder2 = new StringBuilder();
            USER_DETAILS UD = new USER_DETAILS();
            UD.userName = cUserName;
            UD.passwd = cPassword;
            stringBuilder1.Append(Newtonsoft.Json.JsonConvert.SerializeObject(UD));
            return stringBuilder1.ToString();
        }

        private DataTable GetDetailsFromAPI_Token(string cReturnedStr)
        {
            DataTable detailsFromApiToken = new DataTable("TOKEN");
            detailsFromApiToken.Columns.Add("err_msg", typeof(string));
            detailsFromApiToken.Rows.Add();
            if (string.IsNullOrEmpty(cReturnedStr))
                detailsFromApiToken.Rows[0]["err_msg"] = (object)"String not return by API";
            else
            {
                try
                {
                    string str1 = Convert.ToString(cReturnedStr);
                    string[] separator1 = new string[1] { "{" };
                    foreach (string str2 in str1.Split(separator1, StringSplitOptions.RemoveEmptyEntries))
                    {
                        string[] separator2 = new string[1] { "," };
                        foreach (string str3 in str2.Split(separator2, StringSplitOptions.RemoveEmptyEntries))
                        {
                            string[] strArray = str3.Replace("\"", "").Replace("}", "").Split(new string[1]{":"}, StringSplitOptions.RemoveEmptyEntries);
                            if (strArray.Length != 0)
                                detailsFromApiToken.Columns.Add(strArray[0], typeof(string));
                            if (strArray.Length > 1)
                                detailsFromApiToken.Rows[0][strArray[0]] = (object)strArray[1];
                        }
                    }
                    if (detailsFromApiToken.Columns.Contains("ReturnCode"))
                    {
                        if (Convert.ToString(detailsFromApiToken.Rows[0]["ReturnCode"]) != "0")
                            detailsFromApiToken.Rows[0]["err_msg"] = (object)Convert.ToString(detailsFromApiToken.Rows[0]["ReturnMessage"]);
                    }
                }
                catch (Exception ex)
                {
                    detailsFromApiToken.Rows[0]["err_msg"] = (object)("GetDetailsFromAPI_Token : " + ex.Message);
                }
            }
            return detailsFromApiToken;
        }

    public DataTable Token(
      string cPath,
      string Groupcode,
      string cCredentialName,
      string cCredentialPassword,
      string cAPIAddress="")
    {
            if (String.IsNullOrEmpty(cAPIAddress))
                cAPIAddress = "https://wizapp.in/wowservice";
      DataTable dataTable = new DataTable("TOKEN");
      dataTable.Columns.Add("err_msg", typeof (string));
      dataTable.Rows.Add();
      try
      {
        string endpoint = cAPIAddress;
        string xmlToken = this.GenerateXML_Token(cCredentialName, cCredentialPassword);
        File.WriteAllText(cPath + "\\_Token.txt", Convert.ToString(xmlToken) + "\n" + endpoint + "/validateuser?GroupCode="+Groupcode);
        string cReturnedStr = new RestAPIRestClient(endpoint, HttpVerb.POST, xmlToken).MakeRequestWizApp("/validateuser?GroupCode=" + Groupcode, xmlToken, cCredentialName, cCredentialPassword);
        File.WriteAllText(cPath + "\\_Token_Returned.txt", Convert.ToString(cReturnedStr));
        dataTable = this.GetDetailsFromAPI_Token(cReturnedStr);
      }
      catch (Exception ex)
      {
        dataTable.Rows[0]["err_msg"] = (object) ex.Message;
      }
      if (dataTable.Rows.Count > 0 && string.IsNullOrEmpty(Convert.ToString(dataTable.Rows[0]["err_msg"]))/* && string.IsNullOrEmpty(Convert.ToString(dataTable.Rows[0]["ReturnMessage"]))*/)
        GET_TOKEN._SecurityToken = Convert.ToString(dataTable.Rows[0]["RefreshToken"]);
      return dataTable;
    }
  }
}
