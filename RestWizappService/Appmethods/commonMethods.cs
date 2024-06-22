using RestWizappService.Models;
using System.Data.SqlClient;
using System.Data;
using System.Dynamic;
using System.Text;
using System.Reflection;

namespace RestWizappService.Appmethods
{
    public class commonMethods
    {

        public DataTable CreateDataTablewithNull<T>(List<T> items)
        {
            DataTable dataTable = new DataTable(typeof(T).Name);
            //Get all the properties
            PropertyInfo[] Props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (PropertyInfo prop in Props)
            {
                //Setting column names as Property names
                dataTable.Columns.Add(prop.Name);
                dataTable.Columns[prop.Name].AllowDBNull = true;
            }
            foreach (T item in items)
            {
                DataRow dr = dataTable.NewRow();
                for (int i = 0; i < Props.Length; i++)
                {
                    //inserting property values to datatable rows
                    dr[i] = Props[i].GetValue(item, null);
                }

                dataTable.Rows.Add(dr);
            }
            //put a breakpoint here and check datatable
            return dataTable;
        }
        public DataTable CreateDataTableWithDataType<T>(List<T> items)
        {
            DataTable dataTable = new DataTable(typeof(T).Name);
            //Get all the properties
            PropertyInfo[] Props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (PropertyInfo prop in Props)
            {
                //Setting column names as Property names
                dataTable.Columns.Add(prop.Name, prop.PropertyType);
                dataTable.Columns[prop.Name].AllowDBNull = true;
            }
            foreach (T item in items)
            {

                var values = new object[Props.Length];
                for (int i = 0; i < Props.Length; i++)
                {
                    //inserting property values to datatable rows
                    values[i] = Props[i].GetValue(item, null);
                }

                dataTable.Rows.Add(values);
            }
            //put a breakpoint here and check datatable
            return dataTable;
        }
        public string Encrypt(string cString)
        {
            cString = cString.Trim();

            StringBuilder cEncStr = new StringBuilder();
            Int16 i;
            foreach (char c in cString)
            {
                i = (Int16)c;
                cEncStr.Append((char)(i > 250 ? 250 : 250 - i));
            }
            return cEncStr.ToString();
        }
        public int GetErrorLineNo(Exception ex)
        {
            var lineNumber = 0;
            const string lineSearch = ":line ";
            var index = ex.StackTrace.LastIndexOf(lineSearch);
            if (index != -1)
            {
                var lineNumberText = ex.StackTrace.Substring(index + lineSearch.Length);
                if (int.TryParse(lineNumberText, out lineNumber))
                {
                }
            }
            return lineNumber;
        }
        public object GetUsersList(SqlConnection con)
        {
            dynamic result = new ExpandoObject();
            try
            {


                String cExpr = "";

                cExpr = $" Select user_Code userCode,userName,role_id roleCode,isnull(inactive,0) as inactive,email loginId," +
                    $" convert(varchar(20),format(refreshTokenValidity,'dd-MMM-yyyy')) refreshTokenValidity,(CASE WHEN ISNULL(API_ACCESS,0)=0 THEN '' else '1' END) apiAccess " +
                    $"from users ";

                SqlCommand cmd = new SqlCommand(cExpr, con);
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                DataSet dset = new DataSet();
                sda.Fill(dset, "TDATA");

                result.Users = dset.Tables["TDATA"];

                return result;


            }
            catch (Exception ex)
            {
                result.errMsg = ex.Message;
                return result;
            }


        }
        public string addBulkCopyColMappings(SqlCommand cmd, DataTable cSourceTable, string cSqlTableName, ref List<SqlBulkCopyColumnMapping> columnMappings)
        {
            try
            {

                bool columnExists;

                cmd.CommandText = "select * from " + cSqlTableName + " (NOLOCK) WHERE 1=2";
                SqlDataAdapter sda = new SqlDataAdapter(cmd);

                DataTable dtCursor = new DataTable();
                sda.Fill(dtCursor);

                foreach (DataColumn dc in dtCursor.Columns)
                {
                    columnExists = cSourceTable.Columns
                    .Cast<DataColumn>()
                    .Any(column => string.Equals(column.ColumnName, dc.ColumnName, StringComparison.OrdinalIgnoreCase));

                    if (columnExists)
                        columnMappings.Add(new SqlBulkCopyColumnMapping(dc.ColumnName, dc.ColumnName));
                }
            }

            catch (Exception ex)
            {
                int errLineNo = GetErrorLineNo(ex);
                return "Error in addBulkCopyColMappings at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();
            }

            return "";
        }
        public string InsertBcpTemp(DataTable tSourceTable, string targetSqlTable, SqlConnection conn)
        {
            string cErr = "";

            try
            {
                using (SqlBulkCopy sbc = new SqlBulkCopy(conn))
                {

                    sbc.BatchSize = 5000;
                    sbc.BulkCopyTimeout = 1000;
                    sbc.DestinationTableName = targetSqlTable;

                    sbc.WriteToServer(tSourceTable);
                }
            }

            catch (Exception ex)
            {
                cErr = "Error in InsertBcpTemp method for Table:" + targetSqlTable + " " + ex.Message.ToString();
            }

            return cErr;
        }
        public string InsertBcp(DataTable tSourceTable, string targetSqlTable, SqlBulkCopyOptions bcpOptions, List<SqlBulkCopyColumnMapping> columnMappings,
        SqlConnection conn, SqlTransaction sqlTran, SqlCommand cmd, List<string> skipColumns = null)
        {
            string cErr = "";

            if (skipColumns == null) skipColumns = new List<string>();

            try
            {
                using (SqlBulkCopy sbc = new SqlBulkCopy(conn, bcpOptions, sqlTran))
                {

                    sbc.BatchSize = 100;
                    sbc.BulkCopyTimeout = 1000;
                    sbc.DestinationTableName = targetSqlTable;

                    columnMappings.Clear();
                    cErr = addBulkCopyColMappings(cmd, tSourceTable, targetSqlTable, ref columnMappings);
                    if (!string.IsNullOrEmpty(cErr))
                        return cErr;

                    foreach (var columnMapping in columnMappings)
                    {
                        if (!skipColumns.Contains(columnMapping.ToString()))
                            sbc.ColumnMappings.Add(columnMapping);
                    }

                    sbc.WriteToServer(tSourceTable);
                }
            }

            catch (Exception ex)
            {
                cErr = "Error in Insertbcp method for Table:" + targetSqlTable + " " + ex.Message.ToString();
            }

            return cErr;
        }
        public SqlConnection GetSqlConnection(ref String cError)
        {

            SqlConnection sqlCon = new SqlConnection();

            if (!String.IsNullOrEmpty(AppConfigModel.apiRejectedMsg))
            {
                cError = AppConfigModel.apiRejectedMsg;
                goto lblLast;
            }


            try
            {
                string GroupCode = (string.IsNullOrEmpty(AppConfigModel.tokenGroupCode)?AppConfigModel.apiGroupCode:AppConfigModel.tokenGroupCode);

                string cConStr = AppConfigModel.globalConnConfig["ConnectionStrings:CON_" + GroupCode];

                if (string.IsNullOrEmpty(cConStr))
                {
                    cError = "Invalid Connection String For Group Code " + GroupCode;
                    goto lblLast;
                }
                else
                {
                    sqlCon = new SqlConnection(cConStr);

                    sqlCon.Open();
                    if (sqlCon.State != ConnectionState.Open)
                    {
                        cError = "Unable To Connect Client Database";
                        goto lblLast;
                    }

                    AppConfigModel.DefaultConnectionString = cConStr;
                    return sqlCon;

                }



            }
            catch (Exception ex)
            {
                cError = ex.Message;
            }
            finally
            {
                sqlCon.Close();

            }

        lblLast:
            return sqlCon;
        }
    }
}
