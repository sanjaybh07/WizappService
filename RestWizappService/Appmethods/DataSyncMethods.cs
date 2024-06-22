using Newtonsoft.Json;
using RestWizappService.Appmethods;
using RestWizappService.Models;
using Swashbuckle.AspNetCore.SwaggerGen;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;

namespace RestWizappService.Appmethods
{
    public class DataSyncMethods
    {

        commonMethods globalMethods = new commonMethods();

        public string GetActiveAttrCols(SqlConnection conn,ref string cErr)
        {

            string cAttrCols = "";

            try
            {

                string cExpr = "declare @cConfigCOls varchar(1000) select @cConfigCOls=coalesce(@cConfigCOls+',','')+column_name from " +
                        " config_attr (NOLOCK) where table_caption<>'' select @cConfigCOls artColsNew";
                SqlCommand cmd = new SqlCommand(cExpr, conn);

                object result = cmd.ExecuteScalar();

                if (result != null)
                    cAttrCols = (string)result;
            }

            catch (Exception ex)
            {
                int errLine = globalMethods.GetErrorLineNo(ex);
                cErr="Error in getting active attribute List at step#"+errLine.ToString()+" "+ex.Message.ToString();
            }

            return cAttrCols;   
        }
        public dynamic GetArticleAttrDiffData(string cLocId, object Body, ref string cErr)
        {
            dynamic result = new ExpandoObject();
            bool bTempTableCreated = false;
            SqlConnection conn = new SqlConnection();
            SqlCommand cmd = new SqlCommand();
            DataSet dSetMismatches = new DataSet();
           

            try
            {
                string serializedObject = Newtonsoft.Json.JsonConvert.SerializeObject(Body, Newtonsoft.Json.Formatting.Indented);

                DataSet Dd = (DataSet)Newtonsoft.Json.JsonConvert.DeserializeObject(serializedObject, (typeof(DataSet)));

                DataTable tAttrMst = Dd.Tables["PosArtNames"];

                conn = globalMethods.GetSqlConnection(ref cErr);
                if (!String.IsNullOrEmpty(cErr))
                    goto lblLast;

                conn.Open();
                
                string cAttrCols = GetActiveAttrCols(conn,ref cErr);
                if (!string.IsNullOrEmpty(cErr))
                    goto lblLast;

                string cExpr = "SELECT article_code,"+cAttrCols+" INTO #tAttrmstNew FROM art_names (NOLOCK) WHERE 1=2";
                cmd=new SqlCommand(cExpr,conn);

                cmd.ExecuteNonQuery();

                bTempTableCreated = true;
                List<SqlBulkCopyColumnMapping> columnMappings = new List<SqlBulkCopyColumnMapping>();
                SqlBulkCopyOptions bcpOptions = SqlBulkCopyOptions.KeepNulls;

                using (SqlBulkCopy bcp = new SqlBulkCopy(conn))
                {
                    bcp.BatchSize = 5000;
                    bcp.BulkCopyTimeout = 1000;
                    bcp.DestinationTableName = "#tAttrmst";


                    cErr = globalMethods.addBulkCopyColMappings(cmd, tAttrMst, "#tAttrmst", ref columnMappings);
                    if (!string.IsNullOrEmpty(cErr))
                        goto lblLast;

                    foreach (var columnMapping in columnMappings)
                    {
                        bcp.ColumnMappings.Add(columnMapping);
                    }

                    bcp.WriteToServer(tAttrMst);
                }

                cExpr = "declare @cAttrJoin varchar(1000) select @cAttrJoin=coalesce(@cAttrJoin+' AND ','')+'af.'+" +
                    "replace(column_name,'key_name','key_code')+'='+table_name+'.'+replace(column_name,'key_name','key_code')" +
                    " from config_attr (NOLOCK) where table_caption<>'' select @cAttrJoin ";
                cmd.CommandText= cExpr;

                string cAttrJoin=cmd.ExecuteScalar().ToString();

                cExpr = "declare @cAttrWc varchar(1000) " +
                    "select @cAttrWc=coalesce(@cAttrWc+' AND ','')+'a.'+column_name+'<>'+table_name+'.'+column_name" +
                    " from config_attr (NOLOCK) where table_caption<>'' select @cAttrWc ";

                cmd.CommandText= cExpr;
                string cAttrCompExpr=cmd.ExecuteScalar().ToString();

                cExpr = "declare @cAttrRetCols varchar(1000) select @cAttrRetCols=coalesce(@cAttrRetCols+',','')+table_name+'.'+" +
                    "column_name+' ho_'+column_name from config_attr (NOLOCK) where table_caption<>'' select @cAttrRetCols ";
                cmd.CommandText=cExpr;
                string cAttrColsComp=cmd.ExecuteScalar().ToString();    

                cExpr = "SELECT "+cAttrColsComp+",a.* FROM #tAttrmst a " +
                    " JOIN article_fix_attr af (NOLOCK) ON af.article_code=a.article_code " +cAttrJoin+
                    " WHERE "+ cAttrCompExpr;

                cmd.CommandText = cExpr;

                SqlDataAdapter sda = new SqlDataAdapter(cmd);

                DataTable tMismatches = new DataTable();
                sda.Fill(tMismatches);

                if (tMismatches.Rows.Count == 0)
                    goto lblLast;

                //Use this query to check only how many records gone  into Temp table for debugging purpose
                //cmd.CommandText = "select * from #tskumst";
                //DataTable tTempSku = new DataTable();
                //sda=new SqlDataAdapter(cmd);
                //sda.Fill(tTempSku);

                cExpr = cExpr.Replace("a.*", "a.* INTO #tMismatches");
                cExpr = cExpr + " AND 1=2";

                cmd.CommandText = cExpr;

                cmd.ExecuteNonQuery();

                globalMethods.InsertBcpTemp(tMismatches, "#tMismatches", conn);

                int nAttrLoop = 1;
                object searchRow;
                string cAttrname="", cAttrTableName = "", cAttrCodeColumn = "", cAttrNameColumn = "";
                while (nAttrLoop <= 25)
                {
                    cAttrname = "attr" + nAttrLoop.ToString();
                    cAttrTableName = cAttrname + "_mst";
                    cAttrCodeColumn = cAttrname + "_key_code";
                    cAttrNameColumn = cAttrname + "_key_name";

                    if (cAttrColsComp.Contains(cAttrNameColumn))
                    {
                        searchRow = tMismatches.Select("isnull(ho_" + cAttrNameColumn + ",'')<>isnull(" + cAttrNameColumn + ",'')", "").FirstOrDefault();
                        if (searchRow != null)
                        {
                            cmd.CommandText = " SELECT DISTINCT " + cAttrCodeColumn + ",a.ho_" + cAttrNameColumn + " " + cAttrNameColumn + " FROM #tMismatches a " +
                                " JOIN " + cAttrTableName + " b (NOLOCK) ON a.ho_" + cAttrNameColumn + "= b." + cAttrNameColumn + " WHERE " +
                                "isnull(a.ho_" + cAttrNameColumn + ",'')<>isnull(a." + cAttrNameColumn + ",'')";
                            sda = new SqlDataAdapter(cmd);
                            dSetMismatches.Tables.Add(cAttrTableName);
                            sda.Fill(dSetMismatches.Tables[cAttrTableName]);
                        }
                    }

                    nAttrLoop++;
                }


            }

            catch (Exception ex)
            {
                int errLine = globalMethods.GetErrorLineNo(ex);
                cErr = "Error in GetArticleAttrDiffData at Line#" + errLine.ToString() + " " + ex.Message.ToString();
            }

        lblLast:
            if (bTempTableCreated)
            {
                cmd.CommandText = "DROP TABLE #tAttrmst";
                cmd.ExecuteNonQuery();
            }

            if (conn.State == ConnectionState.Open)
                conn.Close();

            if (string.IsNullOrEmpty(cErr))
            {
                if (dSetMismatches.Tables.Count > 0)
                    result = dSetMismatches;
                else
                    result.Message = "";

            }

            return result;
        }
        public dynamic GetSkuDiffData(string cLocId, object Body,ref string cErr)
        {
            dynamic result = new ExpandoObject();
            bool bTempTableCreated = false;
            SqlConnection conn = new SqlConnection();
            SqlCommand cmd = new SqlCommand();
            DataSet dSetMismatches = new DataSet();
            string cAttrCols = "";
            try
            {
                var settings = new JsonSerializerSettings
                {
                    FloatFormatHandling = (FloatFormatHandling)2,
                    Formatting = Formatting.Indented,
                };

                string serializedObject = Newtonsoft.Json.JsonConvert.SerializeObject(Body, settings);

                DataSet Dd = (DataSet)Newtonsoft.Json.JsonConvert.DeserializeObject(serializedObject, (typeof(DataSet)));

                DataTable tSkuMst = Dd.Tables["PosSku"];
                //PosSkuSynch Entrymst = Newtonsoft.Json.JsonConvert.DeserializeObject<PosSkuSynch>(serializedObject);

                //List<PosSkuSynch> entryMstList = new List<PosSkuSynch> { Entrymst };

                //commonMethods globalMethods = new commonMethods();

                //DataTable tMst = globalMethods.CreateDataTablewithNull<PosSkuSynch>(entryMstList);



                conn = globalMethods.GetSqlConnection(ref cErr);
                if (!String.IsNullOrEmpty(cErr))
                    goto lblLast;

                conn.Open();
                //string cExpr = "SELECT product_code,section_name,sub_section_name,article_no,ac_name,para1_name,para2_name,para3_name," +
                //    "para4_name,para5_name,para6_name,para7_name,convert(numeric(20,2),0) mrp,PURCHASE_BILL_NO,PURCHASE_BILL_DT,purchase_receipt_Dt,supplier_alias,sn_hsn_code " +
                //    " INTO #tSkumst FROM sku_names (NOLOCK) WHERE 1=2";

                string cExpr = "CREATE TABLE #tSkuMst ( product_code VARCHAR(500),section_name VARCHAR(255),sub_section_name VARCHAR(500)," +
                    "article_no VARCHAR(500),ac_name VARCHAR(500),para1_name VARCHAR(500),para2_name VARCHAR(500),para3_name VARCHAR(500)," +
                    "para4_name VARCHAR(500),para5_name VARCHAR(500),para6_name VARCHAR(500),para7_name VARCHAR(500),mrp DECIMAL(20, 2)," +
                    "PURCHASE_BILL_NO VARCHAR(500),PURCHASE_BILL_DT DATETIME,purchase_receipt_Dt DATETIME,supplier_alias VARCHAR(500)," +
                    "sn_hsn_code VARCHAR(500))";
                cmd = new SqlCommand(cExpr, conn);
                cmd.ExecuteNonQuery();

                bTempTableCreated = true;
                List<SqlBulkCopyColumnMapping> columnMappings = new List<SqlBulkCopyColumnMapping>();
                SqlBulkCopyOptions bcpOptions = SqlBulkCopyOptions.KeepNulls;

                string cColName = ""; ;
                foreach (DataColumn dc in tSkuMst.Columns)
                {
                    cColName = dc.ColumnName;
                }
                using (SqlBulkCopy bcp = new SqlBulkCopy(conn))
                {
                    bcp.BatchSize = 5000;
                    bcp.BulkCopyTimeout = 1000;
                    bcp.DestinationTableName = "#tSkuMst";


                    cErr = globalMethods.addBulkCopyColMappings(cmd, tSkuMst, "#tSkuMst", ref columnMappings);
                    if (!string.IsNullOrEmpty(cErr))
                        goto lblLast;

                    foreach (var columnMapping in columnMappings)
                    {
                        bcp.ColumnMappings.Add(columnMapping);
                    }

                    bcp.WriteToServer(tSkuMst);
                }


                //Use this query to check only how many records gone  into Temp table for debugging purpose
                cmd.CommandText = "select * from #tskumst";
                DataTable tTempSku = new DataTable();
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                sda.Fill(tTempSku);

                cmd.CommandText = "SELECT upd_purinfo FROM location (NOLOCK) WHERE dept_id='" + cLocId + "'";
                bool bUpdPurInfo = Convert.ToBoolean(cmd.ExecuteScalar());

                // Left join of para7 is put because of foreign key constraint not created by Dinkar for this column and this column is nullable
                cExpr = "SELECT sm.section_name ho_section_name,sd.sub_section_name ho_sub_section_name,art.article_no ho_article_no," +
                    "lm.ac_name ho_ac_name,p1.para1_name ho_para1_name,p2.para2_name ho_para2_name,p3.para3_name ho_para3_name,p4.para4_name ho_para4_name," +
                    "p5.para5_name ho_para5_name,p6.para6_name ho_para6_name,p7.para7_name ho_para7_name,sku.mrp ho_mrp,sku.inv_no ho_PURCHASE_BILL_NO," +
                    "sku.INV_DT ho_PURCHASE_BILL_DT,sku.receipt_dt ho_purchase_receipt_Dt,lm.alias ho_supplier_alias,sku.hsn_code ho_hsn_code,a.* " +
                    " FROM #tSkumst a " +
                    " JOIN sku (NOLOCK) ON sku.product_code=a.product_code JOIN article art (NOLOCK) ON art.article_code=sku.article_code" +
                    " JOIN sectiond sd(NOLOCK) ON sd.sub_section_code=art.sub_section_code JOIN sectionm sm (NOLOCK) ON sm.section_code=sd.section_code" +
                    " JOIN para1 p1 (NOLOCK) ON p1.para1_code=sku.para1_code JOIN para2 p2 (NOLOCK) ON p2.para2_code=sku.para2_code" +
                    " JOIN para3 p3 (NOLOCK) ON p3.para3_code=sku.para3_code JOIN para4 p4 (NOLOCK) ON p4.para4_code=sku.para4_code" +
                    " JOIN para5 p5 (NOLOCK) ON p5.para5_code=sku.para5_code JOIN para6 p6 (NOLOCK) ON p6.para6_code=sku.para6_code" +
                    " LEFT JOIN para7 p7 (NOLOCK) ON p7.para7_code=sku.para7_code JOIN lm01106 lm (NOLOCK) ON lm.ac_code=sku.ac_code " +
                    " WHERE a.section_name<>sm.section_name OR sd.sub_section_name<>a.sub_section_name OR art.article_no<>a.article_no OR" +
                    " p1.para1_name<>a.para1_name OR p2.para2_name<>a.para2_name OR p3.para3_name<>a.para3_name OR" +
                    " p4.para4_name<>a.para4_name OR p5.para5_name<>a.para5_name OR p6.para6_name<>a.para6_name OR isnull(p7.para7_name,'')<>isnull(a.para7_name,'') OR" +
                    " sku.mrp<>a.mrp " + (bUpdPurInfo ? " OR lm.ac_name<>a.ac_name OR sku.inv_no<>a.PURCHASE_BILL_NO OR convert(date,sku.INV_DT)<>a.PURCHASE_BILL_DT OR " +
                    "convert(date,sku.receipt_dt)<>a.purchase_receipt_Dt OR ISNULL(lm.alias,'')<>a.supplier_alias" : "");

                cmd.CommandText = cExpr;

                sda = new SqlDataAdapter(cmd);

                DataTable tMismatches = new DataTable();
                sda.Fill(tMismatches);


                cAttrCols = GetActiveAttrCols(conn, ref cErr);
                if (!string.IsNullOrEmpty(cErr))
                    goto lblLast;


                if (!string.IsNullOrEmpty(cAttrCols))
                {
                    DataTable attrCols= new DataTable("attrCols");
                    attrCols.Columns.Add("attrList",typeof(string));

                    dSetMismatches.Tables.Add("attrCols");

                    DataRow drNew=attrCols.NewRow();
                    drNew["attrList"] = cAttrCols;
                    attrCols.Rows.Add(drNew);
                                     
                }


                if (tMismatches.Rows.Count == 0)
                    goto lblLast;


                cExpr = cExpr.Replace("a.*", "a.* INTO #tMismatches");
                cExpr = cExpr + " AND 1=2";

                cmd.CommandText = cExpr;

                cmd.ExecuteNonQuery();

                cmd.CommandText = "select * from #tMismatches";
                tTempSku = new DataTable();
                sda = new SqlDataAdapter(cmd);
                sda.Fill(tTempSku);

                globalMethods.InsertBcpTemp(tMismatches, "#tMismatches", conn);

                cmd.CommandText = "select * from #tMismatches";
                tTempSku = new DataTable();
                sda = new SqlDataAdapter(cmd);
                sda.Fill(tTempSku);

                bool bSectionMismatch = false, bSubSectionMismatch = false, bArticleMismatch = false;

                DataRow searchRow = tMismatches.Select("ho_section_name<>section_name", "").FirstOrDefault();
                if (searchRow != null)
                {
                    cmd.CommandText = " SELECT DISTINCT section_code,ho_section_name section_name FROM #tMismatches a " +
                            " JOIN sectionm b (NOLOCK) ON a.ho_section_name=b.section_name" +
                            " WHERE ho_section_name<>a.section_name OR ho_sub_section_name<>a.sub_section_name OR ho_article_no<>a.article_no";
                    sda = new SqlDataAdapter(cmd);
                    dSetMismatches.Tables.Add("sectionm");
                    sda.Fill(dSetMismatches.Tables["sectionm"]);

                    bSectionMismatch = true;
                }

                searchRow = tMismatches.Select("ho_sub_section_name<>sub_section_name", "").FirstOrDefault();
                if (searchRow != null || bSectionMismatch)
                {
                    cmd.CommandText = " SELECT DISTINCT sub_section_code,ho_sub_section_name sub_section_name,b.section_code FROM #tMismatches a " +
                            " JOIN sectionm b (NOLOCK) ON b.section_name=a.ho_section_name " +
                            " JOIN sectiond c (NOLOCK) ON a.ho_sub_section_name=c.sub_section_name AND c.section_code=b.section_code" +
                            "  WHERE ho_sub_section_name<>a.sub_section_name or ho_section_name<>a.section_name OR ho_article_no<>a.article_no";
                    sda = new SqlDataAdapter(cmd);
                    dSetMismatches.Tables.Add("sectiond");
                    sda.Fill(dSetMismatches.Tables["sectiond"]);
                    if (searchRow != null)
                        bSubSectionMismatch = true;
                }

                searchRow = tMismatches.Select("ho_article_no<>article_no", "").FirstOrDefault();
                if (searchRow != null)
                    bArticleMismatch = true;

                searchRow = tMismatches.Select("isnull(ho_hsn_code,'')<>isnull(sn_hsn_code,'')", "").FirstOrDefault();
                if (searchRow != null || bArticleMismatch)
                {
                    cmd.CommandText = " SELECT DISTINCT ho_hsn_code hsn_code,RETAILSALE_TAX_METHOD FROM #tMismatches a" +
                                      " JOIN hsn_mst b (NOLOCK) ON b.hsn_code=a.ho_hsn_code WHERE isnull(ho_hsn_code,'')<>isnull(a.sn_hsn_code,'') " +
                                      " OR ho_article_no<>a.article_no";
                    sda = new SqlDataAdapter(cmd);
                    dSetMismatches.Tables.Add("hsn_mst");
                    sda.Fill(dSetMismatches.Tables["hsn_mst"]);
                }


                if (bArticleMismatch || bSubSectionMismatch || bSectionMismatch)
                {
                    cmd.CommandText = "SELECT DISTINCT article_code,ho_article_no article_no,sub_section_code,uom_code,b.hsn_code FROM #tMismatches a " +
                            " JOIN article b (NOLOCK) ON ho_article_no=b.article_no WHERE ho_article_no<>a.article_no OR ho_sub_section_name<>a.sub_section_name " +
                            " or ho_section_name<>a.section_name";

                    sda = new SqlDataAdapter(cmd);
                    dSetMismatches.Tables.Add("article");
                    sda.Fill(dSetMismatches.Tables["article"]);
                }



                int nParaLoop = 1;
                string cParaTableName = "", cParaCodeColumn = "", cParaNameColumn = "";
                while (nParaLoop <= 7)
                {
                    cParaTableName = "para" + nParaLoop.ToString();
                    cParaCodeColumn = cParaTableName + "_code";
                    cParaNameColumn = cParaTableName + "_name";

                    searchRow = tMismatches.Select("isnull(ho_" + cParaNameColumn + ",'')<>isnull(" + cParaNameColumn + ",'')", "").FirstOrDefault();
                    if (searchRow != null)
                    {
                        cmd.CommandText = " SELECT DISTINCT " + cParaCodeColumn + ",a.ho_" + cParaNameColumn + " " + cParaNameColumn + " FROM #tMismatches a " +
                            " JOIN " + cParaTableName + " b (NOLOCK) ON a.ho_" + cParaNameColumn + "= b." + cParaNameColumn + " WHERE " +
                            "isnull(a.ho_" + cParaNameColumn + ",'')<>isnull(a." + cParaNameColumn + ",'')";
                        sda = new SqlDataAdapter(cmd);
                        dSetMismatches.Tables.Add(cParaTableName);
                        sda.Fill(dSetMismatches.Tables[cParaTableName]);
                    }
                    nParaLoop++;
                }


                if (bUpdPurInfo)
                {
                    searchRow = tMismatches.Select("ho_ac_name<>ac_name OR ho_supplier_alias<>supplier_alias", "").FirstOrDefault();
                    if (searchRow != null)
                    {
                        cmd.CommandText = " SELECT DISTINCT ac_code,ho_ac_name ac_name,b.alias,b.head_code FROM #tMismatches a JOIN lm01106 b (NOLOCK) ON " +
                                " a.ho_ac_name=b.ac_name WHERE ho_ac_name<>a.ac_name or ho_supplier_alias<>supplier_alias";
                        sda = new SqlDataAdapter(cmd);
                        dSetMismatches.Tables.Add("lm01106");
                        sda.Fill(dSetMismatches.Tables["lm01106"]);
                    }
                }

                cmd.CommandText = "SELECT DISTINCT a.product_code,article_code,para1_code,para2_code,para3_code,para4_code," +
                       " para5_code,para6_code,para7_code,sku.hsn_code,sku.mrp" + (bUpdPurInfo ? ",inv_no,INV_DT,receipt_dt,ac_code" : "") + 
                       " FROM #tMismatches a " +
                       " JOIN sku (NOLOCK) ON sku.product_code=a.product_code";
                sda = new SqlDataAdapter(cmd);
                dSetMismatches.Tables.Add("sku");
                sda.Fill(dSetMismatches.Tables["sku"]);

            }

            catch (Exception ex)
            {
                int errLine = globalMethods.GetErrorLineNo(ex);
                cErr = "Error in SynchSkuPos at Line#" + errLine.ToString() + " " + ex.Message.ToString();
            }

        lblLast:
            if (bTempTableCreated)
            {
                cmd.CommandText = "DROP TABLE #tSkumst";
                cmd.ExecuteNonQuery();
            }

            if (conn.State == ConnectionState.Open)
                conn.Close();

            if (string.IsNullOrEmpty(cErr))
            {
                if (dSetMismatches.Tables.Count> 0)
                  result = dSetMismatches;
                else
                  result.Message = "";

            }

            return result;
        }
    }
}
