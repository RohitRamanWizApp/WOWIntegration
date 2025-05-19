// Decompiled with JetBrains decompiler
// Type: WARPSPDIntegration.MEMBER_DETAILS
// Assembly: WARPSPDIntegration, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// MVID: BA6EEE9B-2A17-48D3-AD02-F192AFC6A54D
// Assembly location: C:\WizApp2020\WARPSPDIntegration.dll

using System;

namespace WOWIntegration
{
    public class MessageContent
    {
        /// <summary>
        /// file in Byte Format
        /// </summary>
        public String Content { get; set; }
    }
    public class QueryStr
    {
        Boolean bExecuteNonQuery = false;
        public String _QueryStr { get; set; }

        public String _TableAlias { get; set; }

        public Boolean _ExecuteNonQuery { get => bExecuteNonQuery; set => bExecuteNonQuery = value; }

        public String _Where { get; set; }

        public Decimal _Nav { get; set; }

        public String _RefMemoID { get; set; }

        public String _RefMemoDt { get; set; }

        public Decimal _Quantity { get; set; }
    }
    public class USER_DETAILS
    {
       public string userName { get; set; }
        public string passwd { get; set; }
        public string roleCode { get; set; }
        public Boolean inactive { get; set; }
        public string loginId { get; set; }
        public DateTime refreshTokenValidity { get; set; }
        public string apiAccess { get; set; }
    }
  public class MEMBER_DETAILS
  {
    public string UserName { get; set; }

    public string StoreCode { get; set; }

    public string MobileNo { get; set; }

    public string EmailId { get; set; }

    public string FirstName { get; set; }

    public string LastName { get; set; }

    public string Gender { get; set; }

    public string DOB { get; set; }

    public string Address1 { get; set; }

    public string Address2 { get; set; }

    public string PinCode { get; set; }

    public string MemberShipCardNumber { get; set; }

    public string CountryCode
    {
      get => "91";
      set
      {
      }
    }

    public string CustomerTypeCode
    {
      get => "Loyalty";
      set
      {
      }
    }

    public string ActivityCode { get; set; }

    public int BillAmount { get; set; }

    public string TransactionCode { get; set; }

    public double Amount { get; set; }

    public int MemPoints { get; set; }

    public string RedemptionDate { get; set; }

    public string RedemptionType { get; set; }

    public string TransactionDescription { get; set; }

    public string SmsCode { get; set; }

    public string WithoutOTP { get; set; }

    public string OTPNumber { get; set; }

    public string NetAmount { get; set; }

    public string EOSSAmount { get; set; }

    public string NONEOSSAmount { get; set; }

    public string RedemptionCode { get; set; }

    public string NewTransactionCode { get; set; }

    public string RequestID { get; set; }

    public string CouponCode { get; set; }

    public string BillNo { get; set; }

    public string Discount { get; set; }

    public string TotalPaidAmount { get; set; }
  }
}
