using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace WOWIntegration
{
    public class clsApplyGST
    {
        public Boolean ApplyGST(String cConStr,DataTable dtGst, ref DataTable dtMst, ref DataTable dtDetails, DataTable dtConfig, DataTable dtHSN_MST, DataTable dtHSN_DET, out String cErrMsg)
        {
            cErrMsg = "";
            Boolean lRetVal = false;
            try
            {
                SqlConnection con = new SqlConnection(cConStr);
                SqlCommand cmd = new SqlCommand();
                SqlDataAdapter sda = new SqlDataAdapter();

                clsSaleMethods clssale = new clsSaleMethods();

                String cRetVal = clssale.CalcGst(con, ref dtMst, ref dtDetails, dtConfig, dtHSN_MST, dtHSN_DET, dtGst);
                if (String.IsNullOrEmpty(cRetVal))
                {
                    lRetVal = true;
                }
                else
                {
                    //MessageBox.Show(cRetVal, this.ProductName, MessageBoxButtons.OK, MessageBoxIcon.Information);
                    cErrMsg = cRetVal;
                    lRetVal = false;
                }
            }
            catch (Exception ex)
            {
                //MessageBox.Show("ApplyGST() : " + ex.Message, this.ProductName, MessageBoxButtons.OK, MessageBoxIcon.Information);
                cErrMsg = "ApplyGST() : " + ex.Message;
                lRetVal = false;
            }
            return lRetVal;
        }
        public Boolean ApplyGST_PackSize(String cConStr, DataTable dtGst, ref DataTable dtMst, ref DataTable dtDetails, DataTable dtConfig, DataTable dtHSN_MST, DataTable dtHSN_DET, out String cErrMsg)
        {
            cErrMsg = "";
            Boolean lRetVal = false;
            try
            {
                SqlConnection con = new SqlConnection(cConStr);
                SqlCommand cmd = new SqlCommand();
                SqlDataAdapter sda = new SqlDataAdapter();

                clsSaleMethods clssale = new clsSaleMethods();

                String cRetVal = clssale.CalcGst_PackSize(con, ref dtMst, ref dtDetails, dtConfig, dtHSN_MST, dtHSN_DET, dtGst);
                if (String.IsNullOrEmpty(cRetVal))
                {
                    lRetVal = true;
                }
                else
                {
                    //MessageBox.Show(cRetVal, this.ProductName, MessageBoxButtons.OK, MessageBoxIcon.Information);
                    cErrMsg = cRetVal;
                    lRetVal = false;
                }
            }
            catch (Exception ex)
            {
                //MessageBox.Show("ApplyGST() : " + ex.Message, this.ProductName, MessageBoxButtons.OK, MessageBoxIcon.Information);
                cErrMsg = "ApplyGST() : " + ex.Message;
                lRetVal = false;
            }
            return lRetVal;
        }

        public Boolean ApplyGST_WSL_PackSize(String cConStr, DataTable dtGst, ref DataTable dtMst, ref DataTable dtDetails, DataTable dtConfig, DataTable dtHSN_MST, DataTable dtHSN_DET, out String cErrMsg)
        {
            cErrMsg = "";
            Boolean lRetVal = false;
            try
            {
                SqlConnection con = new SqlConnection(cConStr);
                SqlCommand cmd = new SqlCommand();
                SqlDataAdapter sda = new SqlDataAdapter();

                clsSaleMethods clssale = new clsSaleMethods();

                String cRetVal = clssale.CalcGst_WSL_PackSize(con, ref dtMst, ref dtDetails, dtConfig, dtHSN_MST, dtHSN_DET, dtGst);
                if (String.IsNullOrEmpty(cRetVal))
                {
                    lRetVal = true;
                }
                else
                {
                    //MessageBox.Show(cRetVal, this.ProductName, MessageBoxButtons.OK, MessageBoxIcon.Information);
                    cErrMsg = cRetVal;
                    lRetVal = false;
                }
            }
            catch (Exception ex)
            {
                //MessageBox.Show("ApplyGST() : " + ex.Message, this.ProductName, MessageBoxButtons.OK, MessageBoxIcon.Information);
                cErrMsg = "ApplyGST_WSL_PackSize() : " + ex.Message;
                lRetVal = false;
            }
            return lRetVal;
        }
    }
}
