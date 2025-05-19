// Decompiled with JetBrains decompiler
// Type: WARPSPDIntegration.RestAPIRestClient
// Assembly: WARPSPDIntegration, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// MVID: BA6EEE9B-2A17-48D3-AD02-F192AFC6A54D
// Assembly location: C:\WizApp2020\WARPSPDIntegration.dll

using System;
using System.IO;
using System.Net;
using System.Text;

namespace WOWIntegration
{
  internal class RestAPIRestClient
  {
    public string EndPoint { get; set; }

    public HttpVerb Method { get; set; }

    public string ContentType { get; set; }

    public string PostData { get; set; }

    public RestAPIRestClient()
    {
      this.EndPoint = "";
      this.Method = HttpVerb.GET;
      this.ContentType = "text/xml";
      this.PostData = "";
    }

    public RestAPIRestClient(string endpoint)
    {
      this.EndPoint = endpoint;
      this.Method = HttpVerb.GET;
      this.ContentType = "text/xml";
      this.PostData = "";
    }

    public RestAPIRestClient(string endpoint, HttpVerb method)
    {
      this.EndPoint = endpoint;
      this.Method = method;
      this.ContentType = "application/xml";
      this.PostData = "";
    }

    public RestAPIRestClient(string endpoint, HttpVerb method, string postData)
    {
      this.EndPoint = endpoint;
      this.Method = method;
            this.ContentType = "application/json-patch+json";// "application /json";
      this.PostData = postData;
    }

    //public string MakeRequest() => this.MakeRequest(string.Empty, string.Empty, string.Empty, string.Empty);

        public string MakeRequestWizApp(
          string parameters,
          string postParameters,
          string username,
          string cToken)
        {
            try
            {
                string requestUriString = this.EndPoint + parameters;
                string str1 = cToken;
                string userName = username;
                string password1 = str1;
               /*
                var httpWebRequest = (HttpWebRequest)WebRequest.Create(requestUriString);
                httpWebRequest.ContentType = "application/json";
                //httpWebRequest.Headers["userid"] = cCredentialName;// "mob_usr";
                //httpWebRequest.Headers["pwd"] = cCredentialPassword;// "@pa$$w0rd";
                httpWebRequest.Method = "POST";

                using (var streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
                {
                    streamWriter.Write(postParameters);
                    streamWriter.Flush();
                }

                var httpResponse = (HttpWebResponse)httpWebRequest.GetResponse();
                using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
                {
                    var result = streamReader.ReadToEnd();
                }
                return "";
                */

                                    HttpWebRequest httpWebRequest = (HttpWebRequest) WebRequest.Create(requestUriString);
                        //NetworkCredential networkCredential = new NetworkCredential(userName, password1);
                        //httpWebRequest.Credentials = (ICredentials) networkCredential;
                        httpWebRequest.Method = this.Method.ToString();
                        httpWebRequest.ContentLength = 0L;
                        httpWebRequest.ContentType = this.ContentType;
                        httpWebRequest.Timeout = 30000;
                        httpWebRequest.KeepAlive = false;
                        httpWebRequest.PreAuthenticate = true;
                        httpWebRequest.Headers.Add(HttpRequestHeader.AcceptCharset, "utf-8");
                if (!String.IsNullOrEmpty(cToken))
                    httpWebRequest.Headers.Add(HttpRequestHeader.Authorization, "Bearer : " + cToken);
                ServicePointManager.Expect100Continue = false;
                        httpWebRequest.MediaType = "text/xml;charset=\"utf-8\"";
                        if (!string.IsNullOrEmpty(postParameters) && this.Method == HttpVerb.POST)
                        {
                          httpWebRequest.Method = "POST";
                          byte[] bytes = Encoding.UTF8.GetBytes(postParameters.ToString());
                          httpWebRequest.ContentLength = (long) bytes.Length;
                          Stream requestStream = httpWebRequest.GetRequestStream();
                          try
                          {
                            requestStream.Write(bytes, 0, bytes.Length);
                            requestStream.Close();
                          }
                          catch (Exception ex)
                          {
                          }
                        }
                        using (HttpWebResponse response = (HttpWebResponse) httpWebRequest.GetResponse())
                        {
                          string str2 = string.Empty;
                          if (response.StatusCode != HttpStatusCode.OK)
                            throw new ApplicationException(string.Format("Request failed. Received HTTP {0}", (object) response.StatusCode));
                          using (Stream responseStream = response.GetResponseStream())
                          {
                            if (responseStream != null)
                            {
                              using (StreamReader streamReader = new StreamReader(responseStream))
                                str2 = streamReader.ReadToEnd();
                            }
                          }
                          return str2;
                        }
                
            }
            catch (Exception ex)
            {
                return (string)null;
            }
        }

        public string MakeRequestThirdParty(
          string parameters,
          string postParameters,
          string username,
          string password)
        {
            try
            {
                string requestUriString = this.EndPoint + parameters;
                //string str1 = password;
                string userName = username;
                

                HttpWebRequest httpWebRequest = (HttpWebRequest)WebRequest.Create(requestUriString);
                //NetworkCredential networkCredential = new NetworkCredential(userName, password1);
                //httpWebRequest.Credentials = (ICredentials) networkCredential;
                httpWebRequest.Headers["userid"] = username;// "mob_usr";
                httpWebRequest.Headers["pwd"] = password;// "@pa$$w0rd"
                httpWebRequest.Method = this.Method.ToString();
                httpWebRequest.ContentLength = 0L;
                httpWebRequest.ContentType = this.ContentType;
                httpWebRequest.Timeout = 30000;
                httpWebRequest.KeepAlive = false;
                httpWebRequest.PreAuthenticate = true;
                httpWebRequest.Headers.Add(HttpRequestHeader.AcceptCharset, "utf-8");
                ServicePointManager.Expect100Continue = false;
                httpWebRequest.MediaType = "text/xml;charset=\"utf-8\"";
                if (!string.IsNullOrEmpty(postParameters) && this.Method == HttpVerb.POST)
                {
                    httpWebRequest.Method = "POST";
                    byte[] bytes = Encoding.UTF8.GetBytes(postParameters.ToString());
                    httpWebRequest.ContentLength = (long)bytes.Length;
                    Stream requestStream = httpWebRequest.GetRequestStream();
                    try
                    {
                        requestStream.Write(bytes, 0, bytes.Length);
                        requestStream.Close();
                    }
                    catch (Exception ex)
                    {
                    }
                }
                using (HttpWebResponse response = (HttpWebResponse)httpWebRequest.GetResponse())
                {
                    string str2 = string.Empty;
                    if (response.StatusCode != HttpStatusCode.OK)
                        throw new ApplicationException(string.Format("Request failed. Received HTTP {0}", (object)response.StatusCode));
                    using (Stream responseStream = response.GetResponseStream())
                    {
                        if (responseStream != null)
                        {
                            using (StreamReader streamReader = new StreamReader(responseStream))
                                str2 = streamReader.ReadToEnd();
                        }
                    }
                    return str2;
                }

            }
            catch (Exception ex)
            {
                return (string)null;
            }
        }
    }
}
