using Newtonsoft.Json;

namespace RestWizappService.Models
{
    public class PosArtAttr
    {
        public string article_code { get; set; }
        public string attr1_key_name { get; set; }
        public string attr2_key_name { get; set; }
        public string attr3_key_name { get; set; }
        public string attr4_key_name { get; set; }
        public string attr5_key_name { get; set; }
        public string attr6_key_name { get; set; }
        public string attr7_key_name { get; set; }
        public string attr8_key_name { get; set; }
        public string attr9_key_name { get; set; }
        public string attr10_key_name { get; set; }
        public string attr11_key_name { get; set; }
        public string attr12_key_name { get; set; }
        public string attr13_key_name { get; set; }
        public string attr14_key_name { get; set; }
        public string attr15_key_name { get; set; }
        public string attr16_key_name { get; set; }
        public string attr17_key_name { get; set; }
        public string attr18_key_name { get; set; }
        public string attr19_key_name { get; set; }
        public string attr20_key_name { get; set; }
        public string attr21_key_name { get; set; }
        public string attr22_key_name { get; set; }
        public string attr23_key_name { get; set; }
        public string attr24_key_name { get; set; }
        public string attr25_key_name { get; set; }

    }

    public class PosAttrSynch
    {
        public List<PosArtAttr> PosArtNames { get; set; }
    }
    public class PosSku
    {
        public string product_code { get; set; }
        public string article_no { get; set; }
        public string section_name { get; set; }
        public string sub_section_name { get; set; }
        public string para1_name { get; set; }
        public string para2_name { get; set; }
        public string para3_name { get; set; }
        public string para4_name { get; set; }
        public string para5_name { get; set; }
        public string para6_name { get; set; }
        public string para7_name { get; set; }
        public string ac_name { get; set; }
        public string supplier_alias { get; set; }
        public string sn_hsn_code { get; set; }
        public string mrp { get; set; }
        public string purchase_bill_dt { get; set; }
        public string purchase_receipt_dt { get; set; }
        public string purchase_bill_no { get; set; }


    }

    public class PosSkuSynch
    {
        public string attrdata { get; set; }
        public List<PosSku> PosSku { get; set; }
    }

}
