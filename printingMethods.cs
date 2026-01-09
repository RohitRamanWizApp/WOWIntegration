using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

namespace WOWIntegration
{
    public class printingMethods
    {

        private string ReplaceValue(string cOldValue, DataTable dt_variable, DataTable dt_record, int nRecordNo)
        {
            string cNewValue = "";
            string cColumnValue = "";
            bool bFindVariable = true;

            if (dt_record.Rows.Count == nRecordNo)
                nRecordNo = nRecordNo - 1;

            if (cOldValue.Trim().Length > 0)
            {
                while (bFindVariable)
                {
                    int nStartIndex = cOldValue.IndexOf("[");
                    int nEndIndex = cOldValue.IndexOf("]");

                    if (nStartIndex >= 0 && nEndIndex >= 0 && nStartIndex <= nEndIndex)
                    {
                        string cColumn = cOldValue.Substring(nStartIndex, (nEndIndex - nStartIndex) + 1);
                        DataRow[] rows = dt_variable.Select("Name = '" + cColumn + "'");

                        if (rows.Length > 0)
                            cColumnValue = Convert.ToString(rows[0]["Value"]);

                        cNewValue = cOldValue.Replace(cColumn, Convert.ToString(dt_record.Rows[nRecordNo][cColumnValue]).Replace("[", "(").Replace("]", ")"));
                        cOldValue = cNewValue;
                    }
                    else
                    {
                        cNewValue = cOldValue;
                        bFindVariable = false;
                    }
                }
            }

            return cNewValue;
        }

        private string CleanPath(string path)
        {
            if (string.IsNullOrEmpty(path))
                return path;

            // Replace all double backslashes with single (iteratively)
            while (path.Contains("\\\\"))
                path = path.Replace("\\\\", "\\");

            // Ensure UNC starts with double backslash
            if (!path.StartsWith(@"\\"))
                path = @"\" + path;

            return path;
        }

        public string GetBarcodePrintPRN(string cGroupCode, string cMemoId, string cReportId,
          string cExpr, string cRepFileName, SqlCommand cmd,string printDocsPath,string cURLPath, ref string cErr,ref string cOutputFile, int nNoOfCopies = 1)
        {
            string cPrintURL = "";

            try
            {
                string docFilePath = printDocsPath;
                if (!docFilePath.EndsWith("\\"))
                    docFilePath += "\\";

                if (!String.IsNullOrEmpty(cMemoId))
                {
                    if (!Directory.Exists(docFilePath + cGroupCode))
                        Directory.CreateDirectory(docFilePath + cGroupCode);

                    docFilePath = docFilePath + cGroupCode;
                    if (!Directory.Exists(docFilePath + @"\DocReports\PO\"))
                        Directory.CreateDirectory(docFilePath + @"\DocReports\PO\");

                    if (!Directory.Exists(docFilePath + @"\PrintedMemo\PO\"))
                        Directory.CreateDirectory(docFilePath + @"\PrintedMemo\PO\");
                }


                string cFormatFile = $@"{docFilePath}\DocReports\PO\{cRepFileName}.xml";
                DataSet dset_xml = new DataSet();
                if (System.IO.File.Exists(cFormatFile))
                {
                    dset_xml.ReadXml(cFormatFile);
                }
                else
                {
                    cErr = $"Barcode PRN file{cFormatFile} not found, Please check !";
                    goto lblLast;
                }

                StringBuilder cPrintText = new StringBuilder();
                StringBuilder cHeaderText = new StringBuilder();
                StringBuilder cFooterText = new StringBuilder();
                StringBuilder cBodyText = new StringBuilder();

                int nAcrossLabel = 0;

                cmd.CommandText = cExpr;
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                DataTable dtRecord = new DataTable();
                sda.Fill(dtRecord);

                DataTable dtRecordCopy = new DataTable();


                if (nNoOfCopies > 1)
                {
                    dtRecordCopy = dtRecord.Clone();

                    foreach (DataRow row in dtRecord.Rows)
                    {
                        for (int t = 1; t <= nNoOfCopies; t++)
                            dtRecordCopy.Rows.Add(row.ItemArray);
                    }
                }
                else

                {
                    dtRecordCopy = dtRecord;
                }

                foreach (DataTable dt in dset_xml.Tables)
                {
                    if (dt.TableName.StartsWith("dt_label_body"))
                        nAcrossLabel = nAcrossLabel + 1;
                }


                int nRecordNo = 0;
                string cRowValue = "";
                string cRowValueNew = "";

                //---------------------------------Converting Code--------------------------------------

                //Page Header
                if (dset_xml.Tables.Contains("dt_page_header"))
                {
                    foreach (DataRow row in dset_xml.Tables["dt_page_header"].Rows)
                    {
                        cRowValue = Convert.ToString(row["Value"]).Trim();

                        if (!String.IsNullOrEmpty(cRowValue))
                            cHeaderText.AppendLine(cRowValue);
                    }
                }


                try
                {
                    for (int Ki = 0; Ki <= dtRecordCopy.Rows.Count - 1; Ki++)

                    {


                        if (nRecordNo == dtRecordCopy.Rows.Count)
                            break;


                        if (dset_xml.Tables.Contains("dt_label_row_header"))
                        {
                            foreach (DataRow row in dset_xml.Tables["dt_label_row_header"].Rows)
                            {
                                cRowValue = Convert.ToString(row["Value"]).Trim();

                                if (!String.IsNullOrEmpty(cRowValue))
                                    cBodyText.AppendLine(cRowValue);
                            }
                        }

                        //Label body

                        //  DataTable dt_Var = dset_xml.Tables["dt_variable"];

                        if (dset_xml.Tables["dt_variable"] == null)
                        {
                            cErr = "Varible table is not defined, please check !";
                            goto lblLast;
                        }

                        for (int i = 1; i <= nAcrossLabel; i++)
                        {
                            if (dset_xml.Tables.Contains("dt_label_body" + i))
                            {
                                foreach (DataRow row in dset_xml.Tables["dt_label_body" + i].Rows)
                                {
                                    cRowValue = Convert.ToString(row["Value"]).Trim();

                                    //cRowValueNew = ReplaceValue(cRowValue, ref dt_Var, ref dtRecordCopy, nRecordNo);
                                    cRowValueNew = ReplaceValue(cRowValue, dset_xml.Tables["dt_variable"], dtRecordCopy, nRecordNo);
                                    cBodyText.AppendLine(cRowValueNew);
                                }

                                nRecordNo = nRecordNo + 1;

                                if (nRecordNo == dtRecordCopy.Rows.Count)
                                    break;
                            }
                        }

                        //Label Row footer
                        if (dset_xml.Tables.Contains("dt_label_row_footer"))
                        {
                            foreach (DataRow row in dset_xml.Tables["dt_label_row_footer"].Rows)
                            {
                                cRowValue = Convert.ToString(row["Value"]).Trim();

                                if (!String.IsNullOrEmpty(cRowValue))
                                    cBodyText.AppendLine(cRowValue);
                            }
                        }

                        cBodyText.AppendLine();
                    }

                }
                catch (Exception ex)
                {
                    int errLineNo = clsGLobal.GetErrorLineNo(ex);
                    cErr = $"Error in GetBarcodePrintPRN method at Line#{errLineNo} while printing Barcode Record#{nRecordNo} :{ex.Message}";
                    goto lblLast;
                }

                // Page Footer
                if (dset_xml.Tables.Contains("dt_page_footer"))
                {
                    foreach (DataRow row in dset_xml.Tables["dt_page_footer"].Rows)
                    {
                        cRowValue = Convert.ToString(row["Value"]).Trim();

                        if (!String.IsNullOrEmpty(cRowValue))
                            cFooterText.AppendLine(cRowValue);

                    }
                }

                cPrintText.AppendLine(cHeaderText.ToString());
                cPrintText.AppendLine(cBodyText.ToString());
                cPrintText.AppendLine(cFooterText.ToString());
                //-----------------------------------------------------------------------------------------


                //----------------------------------------Printing-------------------------------------------------
                cOutputFile = docFilePath + $"\\PrintedMemo\\PO\\{cMemoId}.txt";

                if (!String.IsNullOrEmpty(cPrintText.ToString()))
                {
                    try
                    {
                        System.IO.File.Delete(cOutputFile);
                        char[] ch = { '\n' };
                        string[] str = cPrintText.ToString().Split(ch);
                        System.IO.File.WriteAllText(cOutputFile, cPrintText.ToString(), System.Text.Encoding.Default);
                        cPrintURL = cURLPath + "/" + cGroupCode + @"/PrintedMemo/PO/" + cMemoId + ".txt";
                    }
                    catch (Exception ex)
                    {
                        int errLineNo = clsGLobal.GetErrorLineNo(ex);
                        cErr = "Error in GetBarcodePrintPRN method while writing output data in file at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();
                        goto lblLast;
                    }
                }
            }

            catch (Exception ex)
            {
                int errLineNo = clsGLobal.GetErrorLineNo(ex);
                cErr = "Error in GetBarcodePrintPRN method at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();
            }

        lblLast:
            return cPrintURL;
        }

        public string GetBarcodePrintPDF(string GroupCode, string cMemoId, string cReportId, String cExpr,
            string cPrintFilePath,string cUrlPath,ref string cErr,ref string cOutputFile)
        {
            string cPrintURL = "" ;
            try
            {
                string cSqlExprFilePath = cPrintFilePath+ "\\" + GroupCode + @"\PrintedMemo\PO\";
                
                if (!Directory.Exists(cSqlExprFilePath))
                    Directory.CreateDirectory(cSqlExprFilePath);

                string cSqlExprFile=cSqlExprFilePath+$"//{cMemoId}_sqlexpr.txt";

                System.IO.File.WriteAllText(cSqlExprFile, cExpr, System.Text.Encoding.Default);

                String strURL = "https://wizapp.in/RestMirrorService/api/Print/PrintPOBarcodes?GroupCode=" + GroupCode + "&MemoID=" + cMemoId + "&cReportId=" + cReportId + "&cSqlExprFile=" + cSqlExprFile;

                var httpWebRequest = WebRequest.Create(strURL);
                var httpResponse = (HttpWebResponse)httpWebRequest.GetResponse();
                using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
                {

                    var result1 = streamReader.ReadToEnd();

                    if (httpResponse.StatusCode == HttpStatusCode.OK)
                    {
                        var jsonObj = Newtonsoft.Json.Linq.JObject.Parse(result1);

                        cErr = (string)jsonObj["error"];
                     
                        if (!string.IsNullOrEmpty(cErr))
                            goto lblLast;

                        cOutputFile = (string)jsonObj["networkFile"];

                        cPrintURL = cUrlPath + "/" + GroupCode + @"/PrintedMemo/PO/" + cMemoId + ".pdf";
                    }
                    else
                    {
                        cErr = result1;
                    }
                }
            }
            catch (Exception ex)
            {
                int errLineNo = clsGLobal.GetErrorLineNo(ex);
                cErr = $"Error in GetBarcodePrintPDF method at Line#{errLineNo}:{ex.Message}";
                goto lblLast;

            }

        lblLast:
            return cPrintURL;
        }

    }
}
