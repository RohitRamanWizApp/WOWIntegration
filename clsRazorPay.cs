// Decompiled with JetBrains decompiler
// Type: WARPSPDIntegration.SAVE_SKU_BILL_DETAILS
// Assembly: WARPSPDIntegration, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// MVID: BA6EEE9B-2A17-48D3-AD02-F192AFC6A54D
// Assembly location: C:\WizApp2020\WARPSPDIntegration.dll

using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Net;
using System.Text;


namespace WOWIntegration
{
    public class clsBillsForRazorPay
    {
        public class RazorPayErrorDetails
        {
            public string code { get; set; }
            public string description { get; set; }
            public String field { get; set; }
            public string source { get; set; }
            public string step { get; set; }
            public string reason { get; set; }
            public Dictionary<string, object> metadata { get; set; }
        }

        public class RazorPayMetadata
        {
            //public Dictionary<string, object>
        }
        public class RazorPayError
        {
            public RazorPayErrorDetails error { get; set; }
        }

        public class RazorPayDeleteError
        {
            public string status { get; set; }
        }

        /// <summary>
        /// Customer's address. Required if your business_type is ecommerce
        /// </summary>
        public class RazorPayAddress
        {
            /// <summary>
            /// Name of the recipient.
            /// </summary>
            public string customer_name { get; set; }

            /// <summary>
            ///The mobile number of the recipient. 
            /// </summary>
            public String contact { get; set; }

            /// <summary>
            /// GST number of the recipient.
            /// </summary>
            public String gstin { get; set; }

            /// <summary>
            /// PAN number of the recipient.
            /// </summary>
            public String pan { get; set; }

            /// <summary>
            /// Customer billing address line 1.
            /// </summary>
            public String address_line_1 { get; set; }
            /// <summary>
            /// Customer billing address line 2.
            /// </summary>
            public String address_line_2 { get; set; }
            /// <summary>
            /// Customer billing address landmark.
            /// </summary>
            public String landmark { get; set; }
            /// <summary>
            /// Customer billing address city.
            /// </summary>
            public String city { get; set; }
            /// <summary>
            /// Customer billing address province.
            /// </summary>
            public String province { get; set; }
            /// <summary>
            /// Customer billing address PIN code.
            /// </summary>
            public String pin_code { get; set; }
            /// <summary>
            /// Customer billing address country.
            /// </summary>
            public String country { get; set; }
        }
        /// <summary>
        /// This is an array of objects containing details of the employees associated with the receipt.
        /// </summary>
        public class RazorPayEmployee
        {
            /// <summary>
            /// Employee ID/code.
            /// </summary>
            public string id { get; set; }
            /// <summary>
            /// Employee name.
            /// </summary>
            public string name { get; set; }
            /// <summary>
            /// Employee designation/role.
            /// </summary>
            public string role { get; set; }

        }
        /// <summary>
        /// Details of the customer. Required if receipt mode is digital or digital_and_print.
        /// </summary>
        public class RazorPayCustomer
        {
            /// <summary>
            /// The customer's phone number. Required if receipt mode is digital or digital_and_print and email is not present.
            /// </summary>
            public String contact { get; set; }
            /// <summary>
            /// The customer's name.
            /// </summary>
            public String name { get; set; }
            /// <summary>
            /// The customer's email address. Required if receipt mode is digital or digital_and_print and contact is not present.
            /// </summary>
            public String email { get; set; }
            /// <summary>
            /// The customer's customer_id. Required if receipt mode is digital or digital_and_print and neither contact nor email is present.
            /// </summary>
            public String customer_id { get; set; }
            /// <summary>
            /// Age of the customer.
            /// </summary>
            public Int32? age { get; set; }
            /// <summary>
            /// Customer's date of birth.
            /// </summary>
            public Int32? date_of_birth { get; set; }
            /// <summary>
            /// PAN number of the billed customer.
            /// </summary>
            public String pan { get; set; }
            /// <summary>
            /// Customer's current job profile name.
            /// </summary>
            public String profession { get; set; }
            /// <summary>
            /// Customer's current employer.
            /// </summary>
            public String company_name { get; set; }
            /// <summary>
            /// Customer's marital status. Possible values: married | unmarried
            /// </summary>
            public String marital_status { get; set; }
            /// <summary>
            /// Name of customer's spouse.
            /// </summary>
            public String spouse_name { get; set; }
            /// <summary>
            /// Customer's date of anniversary.
            /// </summary>
            public Int32? anniversary_date { get; set; }
            /// <summary>
            /// Customer gender. Possible values: male | female | other
            /// </summary>
            public String gender { get; set; }
            /// <summary>
            /// Customer's GST number.
            /// </summary>
            public String gstin { get; set; }
            /// <summary>
            /// Customer's billing address. Required if your business_type is ecommerce.
            /// </summary>
            public RazorPayAddress billing_address { get; set; }
            /// <summary>
            /// Customer's billing address. Required if your business_type is ecommerce.
            /// </summary>
            public RazorPayAddress shipping_address { get; set; }
        }
        /// <summary>
        /// Customer loyalty details. Optional
        /// </summary>
        public class RazorPayLoyalty
        {
            /// <summary>
            /// Customer loyalty type.
            /// </summary>
            public String type { get; set; }
            /// <summary>
            /// Hashed debit/credit card number provided by the customer.
            /// </summary>
            public String card_num { get; set; }
            /// <summary>
            /// Name of the card holder.
            /// </summary>
            public String card_holder_name { get; set; }
            /// <summary>
            /// Wallet amount after used rewards of the customer.
            /// </summary>
            public Decimal wallet_amount { get; set; }
            /// <summary>
            /// Amount saved by the customer.
            /// </summary>
            public Decimal amount_saved { get; set; }
            /// <summary>
            ///  Points earned by the customer after a transaction.
            /// </summary>
            public Decimal points_earned { get; set; }
            /// <summary>
            /// Points redeemed by the customer on a transaction.
            /// </summary>
            public Decimal points_redeemed { get; set; }
            /// <summary>
            /// Points available to the customer at the beginning of the transaction.
            /// </summary>
            public Decimal points_available { get; set; }
            /// <summary>
            /// Points available to the customer at the end of the transaction.
            /// </summary>
            public Decimal points_balance { get; set; }
        }
        /// <summary>
        /// This object contains IRN ( Invoice Reference Number ) related details. If irn is present, qr_code and irn_number are required.
        /// </summary>
        public class RazorPayIRN
        {
            /// <summary>
            /// Acknowledgement number of the generated IRN.
            /// </summary>
            public String acknowledgement_number { get; set; }
            /// <summary>
            /// Acknowledgement date of the generated IRN.
            /// </summary>
            public Int32? acknowledgement_date { get; set; }
            /// <summary>
            /// QR code associated with the IRN. Required if irn is present.
            /// </summary>
            public String qr_code { get; set; }
            /// <summary>
            /// E-invoice IRN. Required if IRN is present.
            /// </summary>
            public String irn_number { get; set; }
        }
        /// <summary>
        /// Optional : Details of the event booking. Required if business_category is events.
        /// </summary>
        public class RazorPayEvents
        {
            /// <summary>
            /// Name of the event.
            /// </summary>
            public String name { get; set; }
            /// <summary>
            /// The exact time in seconds when the event starts.
            /// </summary>
            public int start_timestamp { get; set; }
            /// <summary>
            /// The exact time in seconds when the event ends.
            /// </summary>
            public int end_timestamp { get; set; }
            /// <summary>
            /// The location/venue of the event.
            /// </summary>
            public String location { get; set; }
            /// <summary>
            /// The specific room where the event was held.
            /// </summary>
            public String room { get; set; }
            /// <summary>
            /// The number of seats booked for the event.
            /// </summary>
            public Object[] seats { get; set; }
        }
        /// <summary>
        /// Optional : Details of the financier.
        /// </summary>
        public class RazorPayFinancier
        {
            /// <summary>
            /// Unique id of the financier.
            /// </summary>
            public String reference { get; set; }
            /// <summary>
            /// Name of the financier.
            /// </summary>
            public String name { get; set; }
        }
        /// <summary>
        /// Mandatory : This is an array of objects containing the details of the payment.
        /// </summary>
        public class RazorPayPayments
        {
            /// <summary>
            /// Madatory : The mode of payment.
            /// </summary>
            public String method { get; set; }
            /// <summary>
            /// mandatory : The currency in which the payment was made.(INR)
            /// </summary>
            public String currency { get; set; }
            /// <summary>
            /// Mandatory : The amount of the payment in paise. For example, if the amount is 1200. The unit will be 120000.
            /// </summary>
            public Decimal amount { get; set; }
            /// <summary>
            /// Optional : The Unique id of the payment method.
            /// </summary>
            public String payment_reference_id { get; set; }
            /// <summary>
            /// Optional : Details of the financier.
            /// </summary>
            public RazorPayFinancier financier_data { get; set; }

        }
        /// <summary>
        /// This is an array of objects containing the details of the taxes applied. Required if receipt_type is tax_inovice, purchase_invoice or sales_invoice.
        /// </summary>
        public class RazorPayTaxes
        {
            /// <summary>
            /// Mandatory : Name of the tax. For example, CGST, SGST and so on.
            /// </summary>
            public String name { get; set; }
            /// <summary>
            /// Percentage of tax.
            /// </summary>
            public Decimal percentage { get; set; }
            /// <summary>
            /// Mandatory : Applicable tax calculated on total amount.
            /// </summary>
            public Decimal amount { get; set; }
        }
        /// <summary>
        /// An array of objects containing the sub-item details of the item.
        /// </summary>
        public class RazorpayItems
        {
            /// <summary>
            /// Mandatory : Name of the product.
            /// </summary>
            public String name { get; set; }
            /// <summary>
            /// Mandatory : Quantity of the product.
            /// </summary>
            public Decimal quantity { get; set; }
            /// <summary>
            /// Price of the product.
            /// </summary>
            public Decimal unit_amount { get; set; }
            /// <summary>
            /// Type of unit. Possible values: kg | g | mg | lt | ml | pc | cm | m | in | ft | set
            /// </summary>
            public String unit { get; set; }
            /// <summary>
            /// The total weight of the item, including all materials such as metal, stones, diamonds, and other embellishments.
            /// </summary>
            public Decimal gross_weight { get; set; }
            /// <summary>
            /// The weight of only the metal used in the item, excluding the weight of any diamonds, stones, or other materials
            /// </summary>
            public Decimal net_weight { get; set; }
            /// <summary>
            /// Product/Item description.
            /// </summary>
            public String description { get; set; }
            /// <summary>
            /// HSN code of the product.
            /// </summary>
            public String hsn_code { get; set; }
            /// <summary>
            /// Product/Item code.
            /// </summary>
            public String product_code { get; set; }
            /// <summary>
            /// Product/Item UID/SKU Code.
            /// </summary>
            public String product_uid { get; set; }
            /// <summary>
            /// Image URL of the product.
            /// </summary>
            public String image_url { get; set; }
            public Decimal discount { get; set; }
            public String discount_description { get; set; }
            /// <summary>
            /// Mandatory : Total amount of the product.
            /// </summary>
            public Decimal total_amount { get; set; }
            /// <summary>
            /// Brand name of the product.
            /// </summary>
            public String brand { get; set; }
            /// <summary>
            /// Product style.
            /// </summary>
            public String style { get; set; }
            /// <summary>
            /// Colour of the product.
            /// </summary>
            public String colour { get; set; }
            /// <summary>
            /// Size of the product in cm.
            /// </summary>
            public String size { get; set; }
            /// <summary>
            /// Data of the financier. This is applicable if the product is financed. For example, if the product is purchased on EMI.
            /// </summary>
            public RazorPayFinancier financier_data { get; set; }
            /// <summary>
            /// This is an array of objects containing the details of the taxes incurred.
            /// </summary>
            public List<RazorPayTaxes> taxes { get; set; }
            /// <summary>
            /// An array of strings representing relevant tags associated with the item.
            /// </summary>
            public String[] tags { get; set; }
            /// <summary>
            /// This is an array of objects containing details of any additional charges on the item.
            /// </summary>
            public RazorPayAdditionalcharges additional_charges { get; set; }
        }
        /// <summary>
        /// This is an array of objects containing the product data of the bill. Required if receipt_type is not credit_invoice or debit_invoice.
        /// </summary>
        public class RazorpayLineItems
        {
            /// <summary>
            /// Mandatory : Name of the product.
            /// </summary>
            public String name { get; set; }
            /// <summary>
            /// Mandatory : Quantity of the product.
            /// </summary>
            public Decimal quantity { get; set; }
            /// <summary>
            /// Price of the product.
            /// </summary>
            public Decimal unit_amount { get; set; }
            /// <summary>
            /// Type of unit. Possible values: kg | g | mg | lt | ml | pc | cm | m | in | ft | set
            /// </summary>
            public String unit { get; set; }
            /// <summary>
            /// The total weight of the item, including all materials such as metal, stones, diamonds, and other embellishments.
            /// </summary>
            public Decimal gross_weight { get; set; }
            /// <summary>
            /// The weight of only the metal used in the item, excluding the weight of any diamonds, stones, or other materials
            /// </summary>
            public Decimal net_weight { get; set; }
            /// <summary>
            /// Product/Item description.
            /// </summary>
            public String description { get; set; }
            /// <summary>
            /// HSN code of the product.
            /// </summary>
            public String hsn_code { get; set; }
            /// <summary>
            /// Product/Item code.
            /// </summary>
            public String product_code { get; set; }
            /// <summary>
            /// Product/Item UID/SKU Code.
            /// </summary>
            public String product_uid { get; set; }
            /// <summary>
            /// Image URL of the product.
            /// </summary>
            public String image_url { get; set; }
            //public Decimal discount { get; set; }
            //public String discount_description { get; set; }
            /// <summary>
            /// Mandatory : Total amount of the product.
            /// </summary>
            public Decimal total_amount { get; set; }
            /// <summary>
            /// Brand name of the product.
            /// </summary>
            public String brand { get; set; }
            /// <summary>
            /// Product style.
            /// </summary>
            public String style { get; set; }
            /// <summary>
            /// Colour of the product.
            /// </summary>
            public String colour { get; set; }
            /// <summary>
            /// Size of the product in cm.
            /// </summary>
            public String size { get; set; }
            /// <summary>
            /// This is an array of objects containing the details of the taxes incurred.
            /// </summary>
            public List<RazorPayTaxes> taxes { get; set; }
            /// <summary>
            /// Data of the financier. This is applicable if the product is financed. For example, if the product is purchased on EMI.
            /// </summary>
            public RazorPayFinancier financier_data { get; set; }
           
            /// <summary>
            /// An array of strings representing relevant tags associated with the item.
            /// </summary>
            public String[] tags { get; set; }
            /// <summary>
            /// This is an array of objects containing details of any additional charges on the item.
            /// </summary>
            //public RazorPayAdditionalcharges additional_charges { get; set; }

            /// <summary>
            /// An array of objects containing the sub-item details of the item.
            /// </summary>
            public List<RazorpayItems> sub_items { get; set; }

            public String employee_id { get; set; }
            public RazorPayAdditionalcharges additional_charges { get; set; }

        }
        /// <summary>
        /// This is an array of objects containing details of any additional charges on the item.
        /// </summary>
        public class RazorPayAdditionalcharges
        {
            /// <summary>
            /// Description of the additional charges.
            /// </summary>
            public String description { get; set; }
            /// <summary>
            /// Amount of additional charges.
            /// </summary>
            public Decimal amount { get; set; }
            /// <summary>
            /// Percent calculated on total amount.
            /// </summary>
            public Decimal percent { get; set; }
        }
        /// <summary>
        /// This is an array of objects containing the details of the discount. If product reference (product_code, product_uid, or hsn_code) is present in the object, then the discount will be on the item. If not, the discount will be on the invoice.
        /// </summary>
        public class RazorPayDiscounts
        {
            /// <summary>
            /// Name of the discount.
            /// </summary>
            public string name { get; set; }
            /// <summary>
            /// Amount of the applied discount.
            /// </summary>
            public Decimal amount { get; set; }
            /// <summary>
            /// Percentile value of the discounted amount.
            /// </summary>
            public Decimal percent { get; set; }
            /// <summary>
            /// Sub-item code.
            /// </summary>
            public String product_code { get; set; }
            /// <summary>
            /// Sub-item UID/SKU Code.
            /// </summary>
            public string product_uid { get; set; }
            /// <summary>
            /// HSN code of the product.
            /// </summary>
            public string hsn_code { get; set; }
            /// <summary>
            /// Reference ID of the discount.
            /// </summary>
            public string reference_id { get; set; }
        }       
        /// <summary>
        /// Mandatory : Details of the receipt.
        /// </summary>
        public class RazorPayReceipts
        {
            /// <summary>
            /// Mandatory : Total product quantity sold in the invoice.
            /// </summary>
            public Decimal total_quantity { get; set; }
            /// <summary>
            /// OPtional : The total amount before taxes, discounts and additional fees are added to the invoice.
            /// </summary>
            public Decimal sub_total_amount { get; set; }
            /// <summary>
            /// Mandatory : The currency of the invoice. Refer to this sheet for the list of supported currencies.(INR)
            /// </summary>
            public String currency { get; set; }
            /// <summary>
            /// Optional : Total tax amount in paise.
            /// </summary>
            public Decimal total_tax_amount { get; set; }
            /// <summary>
            /// Total tax percentage applied on the receipt.
            /// </summary>
            public Decimal total_tax_percent { get; set; }
            /// <summary>
            /// Mandatory : The total amount payable after adding taxes, discounts and additional fees to the invoice.
            /// </summary>
            public Decimal net_payable_amount { get; set; }
            /// <summary>
            /// OPtional : Status of the payment. Possible values: pending | authorized | failed | declined | refunded | cancelled | processed | settled | voided | success | paid | unpaid
            /// </summary>
            public String payment_status { get; set; }
            /// <summary>
            /// Optional : Delivery charges of the product. This is applicable if business_type is ecommerce.
            /// </summary>
            public Decimal delivery_charges { get; set; }
            /// <summary>
            /// Optional : Cash on Delivery charges of the product. This is applicable if business_type is ecommerce.
            /// </summary>
            public Decimal cod_charges { get; set; }
            /// <summary>
            /// Optional : Change amount to be returned to the customer if the payment was made in cash.
            /// </summary>
            public Decimal change_amount { get; set; }
            /// <summary>
            /// Optional : Change amount to be returned to the customer if the payment was made in cash.
            /// </summary>
            public Decimal roundup_amount { get; set; }
            /// <summary>
            /// Optional : Total percentage of the discount on the sub-total amount without the taxes.
            /// </summary>
            public Decimal total_discount_percent { get; set; }
            /// <summary>
            /// OPtional : Total value of the discount on the invoice.
            /// </summary>
            public Decimal total_discount_amount { get; set; }
            /// <summary>
            /// Optional : This is an array of objects containing the details of the discount. If product reference (product_code, product_uid, or hsn_code) is present in the object, then the discount will be on the item. If not, the discount will be on the invoice.
            /// </summary>
            public List<RazorPayDiscounts> discounts { get; set; }
            /// <summary>
            /// Optional : Amount used from the customer's wallet for this transaction.
            /// </summary>
            public Decimal used_wallet_amount { get; set; }

            public List<RazorPayAdditionalcharges> additional_charges { get; set; }

        }
        /// <summary>
        /// Create a Bill
        /// </summary>
        public class RazorPayBills
        {
            /// <summary>
            /// Unique id of the bill generated.
            /// </summary>
            public String id { get; set; }
            /// <summary>
            /// UNIX timestamp of the date when the bill was generated.
            /// </summary>
            public Int32? created_at { get; set; }

            /// <summary>
            /// The link to the receipt.
            /// </summary>
            public String receipt_url { get; set; }
            /// <summary>
            /// The type of business. Possible values : ecommerce | retail
            /// </summary>
            public String business_type { get; set; }
            /// <summary>
            /// The category the business falls under. Possible values: events | food_and_beverages | retail_and_consumer_goods | other_services
            /// </summary>
            public String business_category { get; set; }
            /// <summary>
            /// Details of the customer. Required if receipt mode is digital or digital_and_print
            /// </summary>
            public RazorPayCustomer customer { get; set; }
            /// <summary>
            /// This is an array of objects containing details of the employees associated with the receipt.
            /// </summary>
            public List<RazorPayEmployee> employee { get; set; } 
            /// <summary>
            /// Customer loyalty details.
            /// </summary>
            public RazorPayLoyalty loyalty { get; set; }
            /// <summary>
            /// Associated store code for the receipt. 
            /// Required if you have a multi-store setup where you have a single integration and have multiple stores under you.
            /// </summary>
            public String store_code { get; set; }
            /// <summary>
            /// Optional : An array of strings representing relevant tags associated with the invoice.
            /// </summary>
            public String[] tags { get; set; }
            /// <summary>
            /// Mandatory : UNIX timestamp of the date and time when the receipt was generated.
            /// </summary>
            public Int32? receipt_timestamp { get; set; }

            /// <summary>
            /// Mandatory : Unique receipt number generated for the bill.
            /// </summary>
            public String receipt_number { get; set; }
            /// <summary>
            /// Mandatory : The type of receipt. Possible values: tax_invoice | sales_invoice | sales_return_invoice | proforma_invoice | credit_invoice | purchase_invoice | debit_invoice | order_confirmation
            /// </summary>
            public String receipt_type { get; set; }
            /// <summary>
            /// Mandatory : Indicates the delivery type of the receipt. Possible values: digital | print | digital_and_print
            /// </summary>
            public String receipt_delivery { get; set; }
            /// <summary>
            /// OPtional : Bar code generated after the transaction. This will be displayed on the digital bill only.
            /// </summary>
            public String bar_code_number { get; set; }
            /// <summary>
            /// Optional : QR code generated after the transaction. This will be displayed on the digital bill only.
            /// </summary>
            public String qr_code_number { get; set; }
            /// <summary>
            /// Mandatory : POS number of the machine that generated the bill. This is applicable if business_type is retail.
            /// </summary>
            public String billing_pos_number { get; set; }
            /// <summary>
            /// Optional : The type of POS machine. This is applicable if business_type is retail. Possible values: traditional_pos | kiosk_pos
            /// </summary>
            public String pos_category { get; set; }
            /// <summary>
            /// Optional : Incremental order number of the generated bill.
            /// </summary>
            public String order_number { get; set; }
            /// <summary>
            /// Optional : Order service type of the generated bill. This is applicable if business_category is food_and_beverages. Possible values: dine_in | take_away
            /// </summary>
            public String order_service_type { get; set; }
            /// <summary>
            /// Optional : Order delivery status. This is applicable if business_type is ecommerce.
            /// </summary>
            public String delivery_status_url { get; set; }
            ///// <summary>
            ///// ASK- USERNAME??
            ///// </summary>
            //public String cashier_name { get; set; }
            ///// <summary>
            ///// ASK- USERCODE??
            ///// </summary>
            //public String cashier_code { get; set; }
            /// <summary>
            /// This is an array of objects containing the product data of the bill. Required if receipt_type is not credit_invoice or debit_invoice.
            /// </summary>
            public List<RazorpayLineItems> line_items { get; set; }
            /// <summary>
            /// Mandatory : Details of the receipt.
            /// </summary>
            public RazorPayReceipts receipt_summary { get; set; }
            /// <summary>
            /// This is an array of objects containing the details of the taxes applied. Required if receipt_type is tax_inovice, purchase_invoice or sales_invoice.
            /// </summary>
            public List<RazorPayTaxes> taxes { get; set; }
            /// <summary>
            /// Mandatory : This is an array of objects containing the details of the payment.
            /// </summary>
            public List<RazorPayPayments> payments { get; set; }
            /// <summary>
            /// Optional : Details of the event booking. Required if business_category is events.
            /// </summary>
            public RazorPayEvents Event { get; set; }
            /// <summary>
            /// Optional : This object contains IRN ( Invoice Reference Number ) related details. If irn is present, qr_code and irn_number are required.
            /// </summary>
            public RazorPayIRN irn { get; set; }

        }



        public class RazorpayBillMe
        {
            //public RazorPayBills Bills = new RazorPayBills();
            public DataTable SendBill(String cPath, String BILLID, DataTable dtMst, DataTable dtDet, DataTable dtPayment,  String cCredentialName, String cCredentialPassword, String cAPIAddress,out String cJSON)
            {
                cJSON = "";
                DataTable dt = new DataTable("RazorPaySendBill");
                dt.Columns.Add("err_msg", typeof(System.String));
                dt.Rows.Add();
                APIBaseClass clsCommon = new APIBaseClass();
                try
                {
                    string jsonData = string.Empty;
                    RazorPayBills ObjBill = new RazorPayBills();

                    string strURL = cAPIAddress + "/bills";
                    List<RazorpayLineItems> lineItems = new List<RazorpayLineItems>();
                    List<RazorPayPayments> payments = new List<RazorPayPayments>();
                    List<RazorPayDiscounts> ListDiscount = new List<RazorPayDiscounts>();
                    List<RazorPayEmployee> ListEmp = new List<RazorPayEmployee>();
                    foreach (DataRow drow in dtMst.Rows)
                    {
                        //ObjBill.id = Convert.ToString(drow[""]);
                        //ObjBill.receipt_url = Convert.ToString(drow[""]);
                        ObjBill.business_type = "retail";
                        ObjBill.business_category = "retail_and_consumer_goods";
                        ObjBill.store_code = Convert.ToString(drow["location_code"]);
                        //ObjBill.tags = new String[] { "ROHIT","RAMAN" };
                        DateTime cmdt = clsCommon.ConvertDateTime(drow["cm_dt"]);
                        DateTime cmtime = clsCommon.ConvertDateTime(drow["cm_time"]);
                        var epoch = ((new DateTime(cmdt.Year, cmdt.Month, cmdt.Day, cmtime.Hour, cmtime.Minute, cmtime.Second, DateTimeKind.Utc) - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds)-19800;
                        ObjBill.created_at = (Int32)epoch;
                        ObjBill.receipt_timestamp = (Int32)epoch;
                        ObjBill.receipt_number = Convert.ToString(drow["CM_NO"]);
                        ObjBill.receipt_type = "sales_invoice";//Convert.ToString(drow[""]);
                        ObjBill.receipt_delivery = "print";// Convert.ToString(drow[""]);
                        //ObjBill.bar_code_number = Convert.ToString(drow["product_code"]);
                        //ObjBill.qr_code_number = Convert.ToString(drow[""]);
                        ObjBill.billing_pos_number = Convert.ToString(drow["location_code"]);
                        ObjBill.pos_category = "traditional_pos";// Convert.ToString(drow[""]);
                        //ObjBill.order_number = Convert.ToString(drow[""]);
                        //ObjBill.order_service_type = Convert.ToString(drow[""]);
                        //ObjBill.delivery_status_url = Convert.ToString(drow[""]);
                        //ObjBill.cashier_name = Convert.ToString(drow[""]);
                        //ObjBill.cashier_code = Convert.ToString(drow[""]);

                        if (clsCommon.ConvertDecimal(drow["DISCOUNT_PERCENTAGE"]) != 0)
                        {
                            RazorPayDiscounts itemdisc = new RazorPayDiscounts();
                            itemdisc.name = "BillDiscount";
                            itemdisc.percent = clsCommon.ConvertDecimal(drow["DISCOUNT_PERCENTAGE"]);
                            itemdisc.amount = clsCommon.ConvertDecimal(drow["discount_amount"])*100;
                            ListDiscount.Add(itemdisc);
                        }

                        RazorPayAddress address = new RazorPayAddress();
                       

                        RazorPayCustomer cust = new RazorPayCustomer();
                        if (!String.IsNullOrEmpty(Convert.ToString(drow["user_customer_code"])))
                        {
                            address.address_line_1 = Convert.ToString(drow["address0"]);
                            address.address_line_2 = Convert.ToString(drow["address1"]);
                            address.city = Convert.ToString(drow["city"]);
                            address.country = Convert.ToString(drow["COUNTRY_NAME"]);
                            address.landmark = Convert.ToString(drow["address2"]);
                            address.pin_code = Convert.ToString(drow["pincode"]);
                            address.province = Convert.ToString(drow["state"]);

                            //cust.age = clsCommon.ConvertInt(drow[""]);
                            if (clsCommon.ConvertDateTime(drow["dt_anniversary"]) > new DateTime(1900, 1, 1))
                            {
                                DateTime anniversary_date = clsCommon.ConvertDateTime(drow["dt_anniversary"]);
                                epoch = (new DateTime(anniversary_date.Year, anniversary_date.Month, anniversary_date.Day, 0, 0, 0, DateTimeKind.Utc) - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds - 19800;
                                cust.anniversary_date = (Int32)epoch;
                            }
                            //cust.company_name = Convert.ToString(drow[""]);
                            cust.contact = Convert.ToString(drow["mobile"]);
                            cust.customer_id = Convert.ToString(drow["user_customer_code"]);
                            if (clsCommon.ConvertDateTime(drow["dt_birth"]) > new DateTime(1900, 1, 1))
                            {
                                DateTime date_of_birth = clsCommon.ConvertDateTime(drow["dt_birth"]);
                                epoch = (new DateTime(date_of_birth.Year, date_of_birth.Month, date_of_birth.Day, 0, 0, 0, DateTimeKind.Utc) - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds - 19800;
                                cust.date_of_birth = (Int32)epoch;
                            }
                            cust.email = Convert.ToString(drow["email"]);
                            //cust.gender = "male";// Convert.ToString(drow[""]);
                            cust.gstin = Convert.ToString(drow["cus_gst_no"]);
                            //cust.marital_status = "married";// Convert.ToString(drow[""]);
                            cust.name = Convert.ToString(drow["customer_fname"]);
                            //cust.profession = Convert.ToString(drow[""]);
                            //cust.spouse_name = Convert.ToString(drow[""]);
                            //if (!String.IsNullOrEmpty(Convert.ToString(drow["EINV_IRN_NO"])))
                            {
                                cust.billing_address = address;
                            }
                            //cust.shipping_address = address;

                            ObjBill.customer = cust;
                        }


                        RazorPayIRN irn = new RazorPayIRN();
                        if (!String.IsNullOrEmpty(Convert.ToString(drow["EINV_IRN_NO"])))
                        {
                            irn.acknowledgement_number = Convert.ToString(drow["ACH_NO"]);
                            DateTime ACH_DT = clsCommon.ConvertDateTime(drow["ACH_DT"]);
                            epoch = (new DateTime(ACH_DT.Year, ACH_DT.Month, ACH_DT.Day, ACH_DT.Hour, ACH_DT.Minute, ACH_DT.Second, DateTimeKind.Utc) - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds-19800;
                            irn.acknowledgement_date = (Int32)epoch;
                            irn.irn_number = Convert.ToString(dtMst.Rows[0]["EINV_IRN_NO"]);
                            irn.qr_code = Convert.ToString(dtMst.Rows[0]["IRN_QR_CODE"]);
                            ObjBill.irn = irn;
                        }
                    }
                    foreach (DataRow drow in dtDet.Rows)
                    {
                        RazorpayLineItems li = new RazorpayLineItems();
                        li.brand = Convert.ToString(drow["brand"]);
                        li.colour = Convert.ToString(drow["colour"]);
                        li.description = Convert.ToString(drow["description"]);
                        //li.discount = clsCommon.ConvertDecimal(drow[""]);
                        //li.discount_description = Convert.ToString(drow[""]);
                        //li.financier_data.name = Convert.ToString(drow[""]);
                        //li.financier_data.reference = Convert.ToString(drow[""]);
                        li.hsn_code = Convert.ToString(drow["hsn_code"]);
                        //li.image_url = Convert.ToString(drow[""]);
                        li.name = Convert.ToString(drow["name"]);
                        li.product_code = Convert.ToString(drow["product_code"]);
                        //li.product_uid = Convert.ToString(drow["product_uid"]);
                        li.quantity = clsCommon.ConvertDecimal(drow["quantity"]);
                        li.size = Convert.ToString(drow["size"]);
                        //li.sub_items = null;
                        //li.tags = (new String[] { "" });
                        //li.taxes.name = Convert.ToString(drow[""]);
                        //li.taxes.amount = clsCommon.ConvertDecimal(drow[""]);
                        //li.taxes.percentage = clsCommon.ConvertDecimal(drow[""]);
                        li.total_amount = clsCommon.ConvertDecimal(drow["total_amount"])*100;
                        li.unit = "pc";// Convert.ToString(drow["unit"]);
                        li.unit_amount = clsCommon.ConvertDecimal(drow["unit_amount"])*100;
                        li.employee_id= Convert.ToString(drow["emp_code"]);
                        List<RazorPayTaxes> listtax = new List<RazorPayTaxes>();
                        RazorPayTaxes litax = new RazorPayTaxes();
                        if (clsCommon.ConvertDecimal(drow["taxes_name_CGST"]) != 0)
                        {
                            litax = new RazorPayTaxes();
                            litax.name = "CGST";
                            litax.percentage= (clsCommon.ConvertDecimal(drow["TAX_PERCENT"])/2);
                            litax.amount = clsCommon.ConvertDecimal(drow["taxes_name_CGST"])*100;
                            listtax.Add(litax);
                        }
                        if (clsCommon.ConvertDecimal(drow["taxes_name_SGST"]) != 0)
                        {
                            litax = new RazorPayTaxes();
                            litax.name = "SGST";
                            litax.percentage = (clsCommon.ConvertDecimal(drow["TAX_PERCENT"]) / 2);
                            litax.amount = clsCommon.ConvertDecimal(drow["taxes_name_CGST"])*100;
                            listtax.Add(litax);
                        }
                        if (clsCommon.ConvertDecimal(drow["taxes_name_IGST"]) != 0)
                        {
                            litax = new RazorPayTaxes();
                            litax.name = "CGST";
                            litax.percentage = (clsCommon.ConvertDecimal(drow["TAX_PERCENT"]));
                            litax.amount = clsCommon.ConvertDecimal(drow["taxes_name_CGST"])*100;
                            listtax.Add(litax);
                        }
                        li.taxes = listtax;
                        if (clsCommon.ConvertDecimal(drow["discount_percent"]) != 0)
                        {
                            RazorPayDiscounts itemdisc = new RazorPayDiscounts();
                            itemdisc.name = "ItemDiscount";
                            itemdisc.product_code = li.product_code;
                            itemdisc.percent = clsCommon.ConvertDecimal(drow["discount_percent"]);
                            itemdisc.amount = clsCommon.ConvertDecimal(drow["discount"])*100;
                            itemdisc.hsn_code = li.hsn_code;
                            ListDiscount.Add(itemdisc);

                            RazorPayAdditionalcharges itemAddCharges = new RazorPayAdditionalcharges();
                            itemAddCharges.description= "ItemDiscount";
                            itemAddCharges.percent = clsCommon.ConvertDecimal(drow["discount_percent"]);
                            itemAddCharges.amount = clsCommon.ConvertDecimal(drow["discount"]) * 100;
                            li.additional_charges = itemAddCharges;
                        }
                       
                        lineItems.Add(li);
                    }
                    foreach (DataRow drow in dtPayment.Rows)
                    {
                        RazorPayPayments li = new RazorPayPayments();
                        li.amount = clsCommon.ConvertDecimal(drow["amount"])*100;
                        li.currency = "INR";// Convert.ToString(drow[""]);
                        li.method = Convert.ToString(drow["paymode_name"]);
                        li.payment_reference_id = Convert.ToString(drow["ref_no"]);
                        //li.financier_data.name = Convert.ToString(drow[""]);
                        //li.financier_data.reference = Convert.ToString(drow[""]);
                        payments.Add(li);
                    }
                    List<RazorPayTaxes> listtaxAll = new List<RazorPayTaxes>();
                    RazorPayTaxes litaxAll = new RazorPayTaxes();
                    if (dtDet.Rows.Count > 0)
                    {
                        DataTable dtTax = dtDet.DefaultView.ToTable(true, new string[] { "TAX_PERCENT" });
                        foreach (DataRow drow in dtTax.Rows)
                        {
                            Decimal dtaxes_name_CGST = clsCommon.ConvertDecimal(dtDet.Compute("SUM(taxes_name_CGST)", "TAX_PERCENT=" + clsCommon.ConvertDecimal(drow["TAX_PERCENT"])));
                            Decimal dtaxes_name_SGST = clsCommon.ConvertDecimal(dtDet.Compute("SUM(taxes_name_SGST)", "TAX_PERCENT=" + clsCommon.ConvertDecimal(drow["TAX_PERCENT"])));
                            Decimal dtaxes_name_IGST = clsCommon.ConvertDecimal(dtDet.Compute("SUM(taxes_name_IGST)", "TAX_PERCENT=" + clsCommon.ConvertDecimal(drow["TAX_PERCENT"])));
                            if (clsCommon.ConvertDecimal(dtaxes_name_CGST) != 0)
                            {
                                litaxAll = new RazorPayTaxes();
                                litaxAll.name = "CGST";
                                litaxAll.percentage = (clsCommon.ConvertDecimal(drow["TAX_PERCENT"]) / 2);
                                litaxAll.amount = clsCommon.ConvertDecimal(dtaxes_name_CGST)*100;
                                listtaxAll.Add(litaxAll);
                            }
                            if (clsCommon.ConvertDecimal(dtaxes_name_SGST) != 0)
                            {
                                litaxAll = new RazorPayTaxes();
                                litaxAll.name = "SGST";
                                litaxAll.percentage = (clsCommon.ConvertDecimal(drow["TAX_PERCENT"]) / 2);
                                litaxAll.amount = clsCommon.ConvertDecimal(dtaxes_name_CGST)*100;
                                listtaxAll.Add(litaxAll);
                            }
                            if (clsCommon.ConvertDecimal(dtaxes_name_IGST) != 0)
                            {
                                litaxAll = new RazorPayTaxes();
                                litaxAll.name = "CGST";
                                litaxAll.percentage = (clsCommon.ConvertDecimal(drow["TAX_PERCENT"]));
                                litaxAll.amount = clsCommon.ConvertDecimal(dtaxes_name_CGST)*100;
                                listtaxAll.Add(litaxAll);
                            }
                            ObjBill.taxes = listtaxAll;
                        }
                        
                        RazorPayReceipts rcpt = new RazorPayReceipts();
                        List<RazorPayAdditionalcharges> listatdcharges = new List<RazorPayAdditionalcharges>();
                        RazorPayAdditionalcharges atdcharges = new RazorPayAdditionalcharges();
                        if (clsCommon.ConvertDecimal(dtMst.Rows[0]["atd_charges"])!=0)
                        {
                            atdcharges = new RazorPayAdditionalcharges();
                            atdcharges.description = "OtherCharges";
                            atdcharges.percent = 0;
                            atdcharges.amount = clsCommon.ConvertDecimal(dtMst.Rows[0]["atd_charges"]) * 100;

                            listatdcharges.Add(atdcharges);
                        }
                        if (clsCommon.ConvertDecimal(dtMst.Rows[0]["discount_amount"]) != 0)
                        {
                            atdcharges = new RazorPayAdditionalcharges();
                            atdcharges.description = "BillLevelDiscount";
                            atdcharges.percent = clsCommon.ConvertDecimal(dtMst.Rows[0]["discount_percentage"]);
                            atdcharges.amount = clsCommon.ConvertDecimal(dtMst.Rows[0]["discount_amount"]) * 100;

                            listatdcharges.Add(atdcharges);
                        }
                        if (clsCommon.ConvertDecimal(dtMst.Rows[0]["round_off"]) != 0)
                        {
                            atdcharges = new RazorPayAdditionalcharges();
                            atdcharges.description = "RoundOff";
                            atdcharges.percent = 0;
                            atdcharges.amount = clsCommon.ConvertDecimal(dtMst.Rows[0]["round_off"]) * 100;

                            listatdcharges.Add(atdcharges);
                        }
                        if (listatdcharges.Count>0)
                        rcpt.additional_charges = listatdcharges;

                        rcpt.total_quantity = clsCommon.ConvertDecimal(dtDet.Compute("SUM(quantity)", ""));
                        rcpt.total_tax_amount =( clsCommon.ConvertDecimal(dtDet.Compute("SUM(taxes_name_CGST)", ""))+clsCommon.ConvertDecimal(dtDet.Compute("SUM(taxes_name_SGST)", ""))+clsCommon.ConvertDecimal(dtDet.Compute("SUM(taxes_name_IGST)", "")))*100;
                        rcpt.total_tax_percent = clsCommon.ConvertDecimal(dtDet.Compute("SUM(TAX_PERCENT)", ""));
                        //rcpt.total_discount_percent = clsCommon.ConvertDecimal(dtDet.Compute("SUM(discount_percent)", ""));
                        rcpt.total_discount_amount = (clsCommon.ConvertDecimal(dtDet.Compute("SUM(discount)", "")) + clsCommon.ConvertDecimal(dtMst.Compute("SUM(discount_amount)", ""))) * 100;
                        rcpt.change_amount = clsCommon.ConvertDecimal(dtMst.Compute("SUM(payback)", ""))*100;
                        rcpt.currency = "INR";
                        rcpt.net_payable_amount = clsCommon.ConvertDecimal(dtMst.Compute("SUM(NET_AMOUNT)", ""))*100;
                        rcpt.payment_status = "paid";
                        rcpt.roundup_amount = clsCommon.ConvertDecimal(dtMst.Compute("SUM(round_off)", ""))*100;
                        rcpt.sub_total_amount = clsCommon.ConvertDecimal(dtDet.Compute("SUM(sub_total_amount)", ""))*100;
                        rcpt.total_discount_percent = clsCommon.ConvertDecimal(rcpt.total_discount_amount / clsCommon.ConvertDecimal(dtDet.Compute("SUM(sub_total_amount)", "")));
                        if (ListDiscount.Count > 0)
                            rcpt.discounts = ListDiscount;
                        ObjBill.receipt_summary = rcpt;

                        DataTable dtEmp = dtDet.DefaultView.ToTable(true, new string[] {"empcode", "emp" });
                        foreach (DataRow drow in dtEmp.Rows)
                        {
                            if (String.IsNullOrEmpty(Convert.ToString(drow["emp"]))) continue;
                            RazorPayEmployee emp = new RazorPayEmployee();
                            emp.id = Convert.ToString(drow["empcode"]);
                            emp.name = Convert.ToString(drow["emp"]);
                            emp.role = "Salesman";
                            ListEmp.Add(emp);
                        }
                        dtEmp = dtDet.DefaultView.ToTable(true, new string[] {"empcode1", "emp1" });
                        foreach (DataRow drow in dtEmp.Rows)
                        {
                            if (String.IsNullOrEmpty(Convert.ToString(drow["emp1"]))) continue;
                            RazorPayEmployee emp = new RazorPayEmployee();
                            emp.id = Convert.ToString(drow["empcode1"]);
                            emp.name = Convert.ToString(drow["emp1"]);
                            emp.role = "Salesman";
                            ListEmp.Add(emp);
                        }
                        dtEmp = dtDet.DefaultView.ToTable(true, new string[] { "empcode2", "emp2" });
                        foreach (DataRow drow in dtEmp.Rows)
                        {
                            if (String.IsNullOrEmpty(Convert.ToString(drow["emp2"]))) continue;
                            RazorPayEmployee emp = new RazorPayEmployee();
                            emp.id = Convert.ToString(drow["empcode1"]);
                            emp.name = Convert.ToString(drow["emp1"]);
                            emp.role = "Salesman";
                            ListEmp.Add(emp);
                        }
                    }
                    if(ListEmp.Count>0)
                    ObjBill.employee = ListEmp;
                    //ObjBill.loyalty = null;
                    ObjBill.payments = payments;
                    //ObjBill.Event = null;
                    
                    ObjBill.line_items = lineItems;
                    Newtonsoft.Json.JsonSerializer serializer = new Newtonsoft.Json.JsonSerializer();// (typeof(List<RootObject>));

                    //Newtonsoft.Json.JsonWriter jw
                    using (StreamWriter sw = new StreamWriter(cPath + "\\" + BILLID + "_RAZORPAYBILLS_json_WNULL.txt", false, Encoding.UTF8))
                    {//using(MemoryStream ms = new MemoryStream())
                        using (Newtonsoft.Json.JsonWriter writer = new Newtonsoft.Json.JsonTextWriter(sw))
                        {
                            serializer.Serialize(writer, ObjBill);
                            // {"ExpiryDate":new Date(1230375600000),"Price":0}
                        }
                    }

                    JsonSerializerSettings jss=new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() };
                    jss.NullValueHandling = NullValueHandling.Ignore;
                    string data = Newtonsoft.Json.JsonConvert.SerializeObject(ObjBill,jss);
                    cJSON = data;
                    File.WriteAllText(cPath + "\\" + BILLID + "_RAZORPAYBILLS_DATA_WONULL.txt", data);

                    //try
                    //{
                    //    ServicePointManager.SecurityProtocol = (SecurityProtocolType)3072;
                    //    cAPIAddress = cAPIAddress.TrimEnd('/') + "/bills";
                    //    HttpWebRequest request = (HttpWebRequest)WebRequest.Create(cAPIAddress);// "https://api-web.ext.dev.razorpay.in/v1/bills");
                    //    request.Method = "POST";
                    //    request.ContentType = "application/json";
                    //    string userName = cCredentialName;// "rzp_live_PhkeB7jKfjSMWe";
                    //    string password = cCredentialPassword;// "kbtGVQ5zlbCiQeKnv78E70Wz";
                    //    var basicAuthBytes = Encoding.GetEncoding("ISO-8859-1").GetBytes($"{userName}:{password}");
                    //    var authHeaderValue = $"Basic {System.Convert.ToBase64String(basicAuthBytes)}";
                    //    request.Headers["Authorization"] = authHeaderValue;
                    //    using (StreamWriter writer = new StreamWriter(request.GetRequestStream()))
                    //    {
                    //        writer.Write(cJSON);
                    //    }

                    //    using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                    //    {
                    //        if (response.StatusCode == HttpStatusCode.OK)
                    //        {
                    //            using (StreamReader reader = new StreamReader(response.GetResponseStream()))
                    //            {
                    //                string responseText = reader.ReadToEnd();
                    //                cJSON = responseText;
                    //                dt.Rows[0]["err_msg"] = ex.Message;
                    //                //Use the response, accordingly
                    //            }
                    //        }
                    //        else
                    //        {
                    //            //Error
                    //        }
                    //    }
                    //}
                    //catch (WebException ex)
                    //{
                    //    //Log exception

                    //    if (ex.Response != null)
                    //    {
                    //        using (StreamReader reader = new StreamReader(ex.Response.GetResponseStream()))
                    //        {
                    //            string errorResponse = reader.ReadToEnd();
                    //            cJSON = errorResponse;
                    //            dt.Rows[0]["err_msg"] = ex.Message;
                    //            //Log the error response
                    //        }

                    //    }
                    //}
                    //catch (Exception ex)
                    //{
                    //    cJSON = ex.Message;
                    //    dt.Rows[0]["err_msg"] = ex.Message;
                    //}

                }
                catch (Exception ex)
                {
                    cJSON = ex.Message;
                    dt.Rows[0]["err_msg"] = ex.Message;
                }
                return dt;
            }

            public DataTable UploadBill(String cPath, String RAZORPAYBILLID, String cCredentialName, String cCredentialPassword, String cAPIAddress, String cJSON,out String cResult)
            {
                cResult = "";
                DataTable dt = new DataTable("RazorPayUploadBill");
                dt.Columns.Add("err_msg", typeof(System.String));
                dt.Columns.Add("refno", typeof(System.String));
                dt.Rows.Add();
                APIBaseClass clsCommon = new APIBaseClass();
                try
                {

                    try
                    {
                        ServicePointManager.SecurityProtocol = (SecurityProtocolType)3072;
                        cAPIAddress = cAPIAddress.TrimEnd('/') + "/bills" + (!String.IsNullOrEmpty(RAZORPAYBILLID) ? "/"+ RAZORPAYBILLID : "");
                        HttpWebRequest request = (HttpWebRequest)WebRequest.Create(cAPIAddress);// "https://api-web.ext.dev.razorpay.in/v1/bills");
                        request.Method = (!String.IsNullOrEmpty(RAZORPAYBILLID) ? "PATCH" : "POST");
                        request.ContentType = "application/json";
                        string userName = cCredentialName;// "rzp_live_PhkeB7jKfjSMWe";
                        string password = cCredentialPassword;// "kbtGVQ5zlbCiQeKnv78E70Wz";
                        var basicAuthBytes = Encoding.GetEncoding("ISO-8859-1").GetBytes($"{userName}:{password}");
                        var authHeaderValue = $"Basic {System.Convert.ToBase64String(basicAuthBytes)}";
                        request.Headers["Authorization"] = authHeaderValue;
                        using (StreamWriter writer = new StreamWriter(request.GetRequestStream()))
                        {
                            writer.Write(cJSON);
                            writer.Flush();
                        }

                        using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                        {
                            if (response.StatusCode == HttpStatusCode.OK)
                            {
                                using (StreamReader reader = new StreamReader(response.GetResponseStream()))
                                {
                                    string responseText = reader.ReadToEnd();
                                    cResult = responseText;
                                    dt.Rows[0]["err_msg"] = cResult;
                                    RazorPayBills cls = new RazorPayBills();
                                    try
                                    {
                                        cls = Newtonsoft.Json.JsonConvert.DeserializeObject<RazorPayBills>(responseText);
                                        dt.Rows[0]["refno"] = cls.id;
                                    }
                                    catch (Exception ex)
                                    {

                                    }
                                    //Use the response, accordingly
                                }
                            }
                            else
                            {
                                //Error
                            }
                        }
                    }
                    catch (WebException ex)
                    {
                        //Log exception

                        if (ex.Response != null)
                        {
                            using (StreamReader reader = new StreamReader(ex.Response.GetResponseStream()))
                            {
                                string errorResponse = reader.ReadToEnd();
                                cResult = errorResponse;
                                dt.Rows[0]["err_msg"] = cResult;
                                //Log the error response
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        cResult = ex.Message;
                        dt.Rows[0]["err_msg"] = cResult;
                    }

                }
                catch (Exception ex)
                {
                    cResult = ex.Message;
                    dt.Rows[0]["err_msg"] = ex.Message;
                }
                return dt;
            }

            public DataTable DeleteBill(String cPath, String RAZORPAYBILLID, String cCredentialName, String cCredentialPassword, String cAPIAddress, out String cResult)
            {
                cResult = "";
                DataTable dt = new DataTable("RazorPayUploadBill");
                dt.Columns.Add("err_msg", typeof(System.String));
                dt.Columns.Add("refno", typeof(System.String));
                dt.Rows.Add();
                APIBaseClass clsCommon = new APIBaseClass();
                try
                {

                    try
                    {
                        ServicePointManager.SecurityProtocol = (SecurityProtocolType)3072;
                        cAPIAddress = cAPIAddress.TrimEnd('/') + "/bills" + (!String.IsNullOrEmpty(RAZORPAYBILLID) ? "/" + RAZORPAYBILLID : "");
                        HttpWebRequest request = (HttpWebRequest)WebRequest.Create(cAPIAddress);// "https://api-web.ext.dev.razorpay.in/v1/bills");
                        request.Method = "DELETE";
                        request.ContentType = "application/json";
                        string userName = cCredentialName;// "rzp_live_PhkeB7jKfjSMWe";
                        string password = cCredentialPassword;// "kbtGVQ5zlbCiQeKnv78E70Wz";
                        var basicAuthBytes = Encoding.GetEncoding("ISO-8859-1").GetBytes($"{userName}:{password}");
                        var authHeaderValue = $"Basic {System.Convert.ToBase64String(basicAuthBytes)}";
                        request.Headers["Authorization"] = authHeaderValue;
                        
                        using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                        {
                            if (response.StatusCode == HttpStatusCode.OK)
                            {
                                using (StreamReader reader = new StreamReader(response.GetResponseStream()))
                                {
                                    string responseText = reader.ReadToEnd();
                                    cResult = responseText;
                                    dt.Rows[0]["err_msg"] = cResult;
                                    //RazorPayDeleteError cls = new RazorPayDeleteError();
                                    //try
                                    //{
                                    //    cls = Newtonsoft.Json.JsonConvert.DeserializeObject<RazorPayDeleteError>(responseText);
                                    dt.Rows[0]["refno"] = RAZORPAYBILLID;
                                    //}
                                    //catch (Exception)
                                    //{

                                    //}
                                    //Use the response, accordingly
                                }
                            }
                            else
                            {
                                //Error
                            }
                        }
                    }
                    catch (WebException ex)
                    {
                        //Log exception

                        if (ex.Response != null)
                        {
                            using (StreamReader reader = new StreamReader(ex.Response.GetResponseStream()))
                            {
                                string errorResponse = reader.ReadToEnd();
                                cResult = errorResponse;
                                dt.Rows[0]["err_msg"] = cResult;
                                //Log the error response
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        cResult = ex.Message;
                        dt.Rows[0]["err_msg"] = cResult;
                    }

                }
                catch (Exception ex)
                {
                    cResult = ex.Message;
                    dt.Rows[0]["err_msg"] = ex.Message;
                }
                return dt;
            }
        }
        //public class SAVE_SKU_BILL_DETAILS
        //{
        //    private string GenerateXML_SaveSKUBillDetails(DataTable MD)
        //    {
        //        StringBuilder stringBuilder = new StringBuilder();
        //        DataRow row1 = MD.Rows[0];

        //        foreach (DataRow row2 in (InternalDataCollectionBase)MD.Rows)
        //        {

        //        }

        //        return stringBuilder.ToString();
        //    }

        //    private DataTable GetDetailsFromAPI_SaveSKUBillDetails(string cReturnedStr)
        //    {
        //        DataTable saveSkuBillDetails = new DataTable("PointsAccrualEOSS");
        //        saveSkuBillDetails.Columns.Add("err_msg", typeof(string));
        //        saveSkuBillDetails.Rows.Add();
        //        if (string.IsNullOrEmpty(cReturnedStr))
        //        {
        //            saveSkuBillDetails.Rows[0]["err_msg"] = (object)"String not return by API";
        //            return saveSkuBillDetails;
        //        }
        //        try
        //        {
        //            string str1 = Convert.ToString(cReturnedStr);
        //            string[] separator1 = new string[1] { "{" };
        //            foreach (string str2 in str1.Split(separator1, StringSplitOptions.RemoveEmptyEntries))
        //            {
        //                string[] separator2 = new string[1] { "," };
        //                foreach (string str3 in str2.Split(separator2, StringSplitOptions.RemoveEmptyEntries))
        //                {
        //                    string[] strArray = str3.Replace("\"", "").Replace("}", "").Split(new string[1]
        //                    {
        //      ":"
        //                    }, StringSplitOptions.RemoveEmptyEntries);
        //                    if (strArray.Length != 0)
        //                        saveSkuBillDetails.Columns.Add(strArray[0], typeof(string));
        //                    if (strArray.Length > 1)
        //                        saveSkuBillDetails.Rows[0][strArray[0]] = (object)strArray[1];
        //                }
        //            }
        //            if (saveSkuBillDetails.Columns.Contains("ReturnCode"))
        //            {
        //                if (Convert.ToString(saveSkuBillDetails.Rows[0]["ReturnCode"]) != "0")
        //                    saveSkuBillDetails.Rows[0]["err_msg"] = (object)Convert.ToString(saveSkuBillDetails.Rows[0]["ReturnMessage"]);
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            saveSkuBillDetails.Rows[0]["err_msg"] = (object)("GetDetailsFromAPI_SaveSKUBillDetails : " + ex.Message);
        //        }
        //        return saveSkuBillDetails;
        //    }

        //    public DataTable SaveSKUBillDetails(string cPath, DataTable MD, string cCredentialName, string cCredentialPassword, string cAPIAddress)
        //    {
        //        DataTable dataTable = new DataTable(nameof(SaveSKUBillDetails));
        //        dataTable.Columns.Add("err_msg", typeof(string));
        //        dataTable.Rows.Add();
        //        try
        //        {
        //            string saveSkuBillDetails = this.GenerateXML_SaveSKUBillDetails(MD);
        //            File.WriteAllText(cPath + "\\_SaveSKUBillDetails.txt", Convert.ToString(saveSkuBillDetails));
        //            string cReturnedStr = new RestAPIRestClient(cAPIAddress, HttpVerb.POST, saveSkuBillDetails).MakeRequestThirdParty("/SaveSKUBillDetails", saveSkuBillDetails, cCredentialName, cCredentialPassword);
        //            File.WriteAllText(cPath + "\\_SaveSKUBillDetails_Returned.txt", Convert.ToString(cReturnedStr));
        //            dataTable = this.GetDetailsFromAPI_SaveSKUBillDetails(cReturnedStr);
        //        }
        //        catch (Exception ex)
        //        {
        //            dataTable.Rows[0]["err_msg"] = (object)ex.Message;
        //        }
        //        return dataTable;
        //    }
        //}
    }
}
