using Microsoft.AspNetCore.Hosting;
using System.Dynamic;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.FileSystemGlobbing.Internal;
using static System.Reflection.Metadata.BlobBuilder;
using System.Data;
using System.Security.Policy;
using System.Runtime.ConstrainedExecution;
using System.Data.SqlClient;
using RestWizappService.Models;
using RestWizappService.Appmethods;
using RestWizappService.Models;
using System.Text.RegularExpressions;

namespace RestWizappService.App_methods
{
    public class CloudMethods
    {

        commonMethods globalMethods;
        public CloudMethods()
        {
            globalMethods = new commonMethods();
        }


        public List<string> GetDbNamesFromBlobs(List<BlobItem> blobs, string databaseName)
        {
            // Define the patterns to match filenames
            string pattern1 = @"_diff_part.*\.bak";
            string pattern2 = @"_part.*\.bak";

            List<string> dbNames = blobs
                .Where(blobItem =>
                    blobItem.Name.Length >= 4 && // Ensure the name is at least 4 characters long
                    (blobItem.Name.ToLower().StartsWith(databaseName.ToLower()) || databaseName == "") && // Check if the name starts with the database name or databaseName is empty
                    (Regex.IsMatch(blobItem.Name, pattern1, RegexOptions.IgnoreCase) ||
                     Regex.IsMatch(blobItem.Name, pattern2, RegexOptions.IgnoreCase)) // Check for pattern match
                )
                .Select(blobItem =>
                {
                    string blobName = blobItem.Name.ToUpper();

                    // Remove the right side pattern match
                    if (Regex.IsMatch(blobItem.Name, pattern1, RegexOptions.IgnoreCase))
                    {
                        // Find the index of the match for pattern1 and trim it
                        int index = Regex.Match(blobItem.Name, pattern1, RegexOptions.IgnoreCase).Index;
                        return blobName.Substring(0, index);
                    }
                    else if (Regex.IsMatch(blobItem.Name, pattern2, RegexOptions.IgnoreCase))
                    {
                        // Find the index of the match for pattern2 and trim it
                        int index = Regex.Match(blobItem.Name, pattern2, RegexOptions.IgnoreCase).Index;
                        return blobName.Substring(0, index);
                    }

                    return blobName; // If no pattern matches, return the original name
                })
                .Distinct() // Ensure unique names
                .ToList();

            return dbNames;
        }


        public string CreateAzureCredential(SqlCommand cmd,string credName)
        {
            string cErr = "";
            try
            {
                cmd.CommandText = "SELECT name  FROM sys.credentials (NOLOCK) WHERE name = '" + credName + "'";
                object objExists = cmd.ExecuteScalar();

                if (objExists == null)
                {
                    cmd.CommandText = "CREATE CREDENTIAL ["+credName+"] WITH IDENTITY = 'SHARED ACCESS SIGNATURE'," +
                        "SECRET = '" + AppConfigModel.blobSasToken + "'";
                    cmd.ExecuteNonQuery();
                }
            }

            catch (Exception ex)
            {
                int errLineNo = globalMethods.GetErrorLineNo(ex);
                cErr = "Error in CreateAzureCredential at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();
            }

            return cErr;
        }
        private bool IsBlobNameValid(string blobName)
        {
            // Define a set of invalid characters
            char[] invalidChars = { '\\', '/', '?', '#', '%', '+' };

            // Check if the blob name contains any invalid characters
            foreach (char invalidChar in invalidChars)
            {
                if (blobName.Contains(invalidChar))
                    return false;
            }
            return true;
        }

        public string PopulateHeadersInfo(Int16 fileMode, SqlCommand cmd, SqlDataAdapter sda,
           ref DataTable dtHeaders, string cPartFilesUrl, string cFileName)
        {
            string cErr = "";
            try
            {
                dtHeaders.Rows.Clear();

                cmd.CommandText = "RESTORE HEADERONLY FROM " + cPartFilesUrl;
                sda = new SqlDataAdapter(cmd);
                sda.Fill(dtHeaders);

                //DataTable dtCheckHeaders = new DataTable();
                cmd.CommandText =  "INSERT INTO #BackupFiles (fileMode,filename,databaseBackupLsn,backupFinishDate,backuptype,firstlsn,lastlsn,CheckpointLSN)" +
                " SELECT " + fileMode.ToString() + ",'" + cFileName + "' fileName,"+dtHeaders.Rows[0]["databaseBackupLsn"] +
                " databaseBackupLsn,'" + Convert.ToDateTime(dtHeaders.Rows[0]["backupFinishDate"]).ToString("yyyy-MM-dd HH:mm:ss") + "' backupFinishDate," +
                dtHeaders.Rows[0]["BackupType"] + " BackupType," + dtHeaders.Rows[0]["FirstLSN"] + " FirstLSN," +
                dtHeaders.Rows[0]["LastLSN"] + " LastLSN,'" + dtHeaders.Rows[0]["CheckpointLSN"] +"' CheckpointLSN ";
                //sda = new SqlDataAdapter(cmd);
                //sda.Fill(dtCheckHeaders);


                cmd.ExecuteNonQuery();
            }

            catch (Exception ex)
            {
                int errLineNo = globalMethods.GetErrorLineNo(ex);
                cErr = "Error in PopulateHeadersInfo at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();
            }

            return cErr;

        }


        public string GetLsnInfoForDb(ref DataTable dtBackupInfo, ref DataTable dtHeaders, ref DataTable dtFileInfo, string dbName, string containerName,
           SqlCommand cmd, SqlDataAdapter sda, List<BlobItem> fullBackupParts, List<BlobItem> diffBackupParts, List<BlobItem> tranLogBackupFiles)
        {

            BlobItem lastModifiedPart = null;
            BlobItem diffBackupFile = null;
            string cErr = "", cFileName = "";

            try
            {
                // Process full backup parts if no single full backup file was found
                // Determine the most recent part file
                lastModifiedPart = fullBackupParts
                    .OrderByDescending(part => part.Name)
                    .FirstOrDefault();

                string cPartFile = "";
                foreach (BlobItem item in fullBackupParts)
                {
                    cPartFile = item.Name;
                }
                BlobItem mostRecentFullBackup = null, mostRecentDiffBackup = null;
                string cPartFilesUrl = "";
                string blobServiceEndpoint = AppConfigModel.blobServiceEndpoint;

                var partFile = fullBackupParts.FirstOrDefault();
                cFileName = lastModifiedPart.Name;
                cPartFilesUrl = cPartFilesUrl + (cPartFilesUrl == "" ? "" : ",") +
                    "URL = '" + blobServiceEndpoint + containerName + "/" + cFileName + "'";

                mostRecentFullBackup = partFile;


                cErr = PopulateHeadersInfo(1, cmd, sda, ref dtHeaders, cPartFilesUrl, cFileName);
                if (!string.IsNullOrEmpty(cErr))
                    goto lblLast;


                DateTimeOffset? lastFullBackupDate = null, lastDiffBackupDate = null;
                lastFullBackupDate = mostRecentFullBackup.Properties.LastModified;

                if (diffBackupParts.Any() && mostRecentFullBackup != null)
                {
                    lastModifiedPart = diffBackupParts.Where(r => r.Properties.LastModified > lastFullBackupDate)
                    .OrderByDescending(part => part.Properties.LastModified)
                    .FirstOrDefault();

                    cPartFilesUrl = "";
                    if (lastModifiedPart != null)
                    {
                        cFileName = lastModifiedPart.Name;
                        cPartFilesUrl = cPartFilesUrl + (cPartFilesUrl == "" ? "" : ",") +
                        "URL = '" + blobServiceEndpoint + containerName + "/" + cFileName + "'";

                        mostRecentDiffBackup = lastModifiedPart;
                        lastDiffBackupDate = mostRecentDiffBackup.Properties.LastModified;

                    }


                    if (!string.IsNullOrEmpty(cPartFilesUrl))
                    {
                        cErr = PopulateHeadersInfo(2, cmd, sda, ref dtHeaders, cPartFilesUrl, cFileName);
                        if (!string.IsNullOrEmpty(cErr))
                            goto lblLast;
                    }

                }


                if (mostRecentFullBackup != null)
                {
                    DateTimeOffset? lastDataBackupDate = (lastDiffBackupDate != null && lastDiffBackupDate > lastFullBackupDate
                        ? lastDiffBackupDate : lastFullBackupDate);

                    foreach (var tranLogFile in tranLogBackupFiles)
                    {
                        // Check if the .trn file's LastModified date is more recent than the last full backup
                        if (tranLogFile.Properties.LastModified > lastFullBackupDate && lastFullBackupDate > lastFullBackupDate)
                        {
                            cFileName = tranLogFile.Name;
                            cPartFilesUrl = "URL = '" + blobServiceEndpoint + containerName + "/" + cFileName + "'";
                            cErr = PopulateHeadersInfo(4, cmd, sda, ref dtHeaders, cPartFilesUrl, cFileName);
                            if (!string.IsNullOrEmpty(cErr))
                                goto lblLast;
                        }

                        if (tranLogFile.Properties.LastModified > lastDataBackupDate)
                        {
                            cFileName = tranLogFile.Name;
                            cPartFilesUrl = "URL = '" + blobServiceEndpoint + containerName + "/" + cFileName + "'";
                            cErr = PopulateHeadersInfo(3, cmd, sda, ref dtHeaders, cPartFilesUrl, cFileName);
                            if (!string.IsNullOrEmpty(cErr))
                                goto lblLast;
                        }
                    }

                }

                cmd.CommandText = "SELECT * FROM #BackupFiles";
                sda = new SqlDataAdapter(cmd);
                DataTable dtFileDetails = new DataTable();
                sda.Fill(dtFileDetails);

                cmd.CommandText = @" with MainBackup as (select firstlsn from #BackupFiles where backuptype=1),
                 BackupData AS (SELECT filename,(CASE WHEN backuptype=1 then firstlsn else  databasebackuplsn end) databasebackuplsn,
                 BackupFinishDate,BackupType,FirstLSN,LastLSN,
                 ROW_NUMBER() OVER (ORDER BY BackupFinishDate) AS RowNum
                 FROM #BackupFiles ),validbackups as
                 (SELECT bd1.databasebackuplsn, bd1.BackupType,bd1.RowNum, bd1.filename,BD1.BackupFinishDate AS CurrentBackupDate,BD1.FirstLSN ,BD1.LastLSN ,
                 (CASE WHEN bd1.BackupType=2 THEN (CASE WHEN bd1.databasebackuplsn<>bd3.firstlsn  then 'Backup is invalid as it does not belong to Last Full backup'
                       WHEN BD1.LastLSN = BD2.FirstLSN or bd2.firstlsn is null THEN 'LSN Sequence Valid' ELSE 'LSN Sequence Invalid' END)
                 else (case when bd1.databasebackuplsn=bd3.firstlsn then 'Backup is Valid' ELSE 'Backup is invalid as it does not belong to Last Full backup' END) END) AS LSNStatus 
                 FROM BackupData BD1
                 LEFT JOIN  BackupData BD2 ON BD1.RowNum = BD2.RowNum - 1 and bd2.BackupType=bd1.BackupType
                 join MainBackup bd3 on 1=1 )
                 SELECT a.* ,isnull(b.invalidFileName,'') brokernFileName
                FROM ValidBackups a LEFT JOIN 
                (SELECT min(rowNum) rowNum,min(fileName) invalidFileName   FROM ValidBackups 
                    WHERE LSNStatus LIKE '%invalid%') b ON 1=1
                where a.rowNum<b.rowNum OR b.rowNum IS NULL

                ORDER BY RowNum;";

                //WHERE RowNum < (
                //    -- Get the row number of the first invalid backup
                //    SELECT ISNULL(MIN(RowNum), (SELECT MAX(RowNum) FROM ValidBackups)) 
                //    FROM ValidBackups 
                //    WHERE LSNStatus LIKE '%invalid%'

                sda = new SqlDataAdapter(cmd);
                DataTable dtValidInfo = new DataTable();
                sda.Fill(dtValidInfo);

                DataRow dr = dtBackupInfo.NewRow();
                dr["dbName"] = dbName;
                dr["fullBackupFile"] = dtValidInfo.Select("BackupType=1", "").FirstOrDefault()["fileName"].ToString();
                dr["lastFullBackupTime"] = Convert.ToDateTime(dtValidInfo.Select("BackupType=1", "").FirstOrDefault()["CurrentBackupDate"]);

                DataRow drDiffBackup = dtValidInfo.Select("BackupType=5", "CurrentBackupDate desc").FirstOrDefault();

                if (drDiffBackup != null)
                {
                    dr["diffBackupFile"] = drDiffBackup["fileName"].ToString();
                    dr["lastDiffBackupTime"] = Convert.ToDateTime(drDiffBackup["CurrentBackupDate"]);
                }

                DataRow drLogBackup = dtValidInfo.Select("Backuptype=2", "CurrentBackupDate desc").FirstOrDefault();

                if (drLogBackup != null)
                {
                    dr["lastTranLogBackupFile"] = drLogBackup["filename"].ToString();
                    dr["lastLogBackupTime"] = Convert.ToDateTime(drLogBackup["CurrentBackupDate"]);
                }

                dtBackupInfo.Rows.Add(dr);

            }

            catch (Exception ex)
            {
                int errLineNo = globalMethods.GetErrorLineNo(ex);
                cErr = "Error in GetLsnInfoForDb at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();
            }

        lblLast:

            return cErr;
        }

        public async Task<(dynamic, string)> GetVerifiedBackupsList(string containerName, string databaseName)
        {
            string cErr = "";
            dynamic result = new ExpandoObject();
            DataTable dtBackupInfo = new DataTable();
            try
            {
                string serverConnStr = AppConfigModel.globalConnConfig["ConnectionStrings:CON_testserver"];
                SqlConnection conn = new SqlConnection(serverConnStr);
                conn.Open();
                if (conn.State != ConnectionState.Open)
                {
                    cErr = "Unable To Connect Sql Server...";
                    goto lblLast;
                }

                SqlCommand cmd = new SqlCommand("", conn);
                SqlDataAdapter sda = new SqlDataAdapter(cmd);

                string blobServiceEndpoint = AppConfigModel.blobServiceEndpoint;

                string sasToken = AppConfigModel.blobSasToken;
                // Combine the Blob Service Endpoint,Container name and SAS token
                string sasUri = $"{blobServiceEndpoint}{containerName}?{sasToken}";

                // Create BlobContainerClient using SAS token
                BlobContainerClient _blobContainerClient = new BlobContainerClient(new Uri(sasUri));
                List<(string dbName, string BlobName, DateTimeOffset? LastModified, Int64? fileSize)> matchingBlobs =
                    new List<(string, string, DateTimeOffset?, Int64?)>();
                
                var blobs= _blobContainerClient.GetBlobs().ToList();

                List<string> dbNames = GetDbNamesFromBlobs(blobs, databaseName);


                List<BlobItem> tranLogBackupFiles = new List<BlobItem>();
                List<BlobItem> fullBackupParts = new List<BlobItem>();
                List<BlobItem> diffBackupParts = new List<BlobItem>();


                DataTable dtHeaders = new DataTable();
                DataTable dtFileInfo = new DataTable();
                dtFileInfo.Columns.Add("fileName", typeof(string));
                dtFileInfo.Columns.Add("lastUpdate", typeof(DateTime));

                dtBackupInfo.Columns.Add("dbName", typeof(string));
                dtBackupInfo.Columns.Add("fullBackupFile", typeof(string));
                dtBackupInfo.Columns.Add("lastFullBackupTime", typeof(DateTime));
                dtBackupInfo.Columns.Add("diffBackupFile", typeof(string));
                dtBackupInfo.Columns.Add("lastDiffBackupTime", typeof(DateTime));
                dtBackupInfo.Columns.Add("lastTranLogBackupFile", typeof(string));
                dtBackupInfo.Columns.Add("lastLogBackupTime", typeof(DateTime));


                cmd.CommandText = @"CREATE TABLE #BackupFiles (fileMode int, fileName varchar(400),databasebackuplsn numeric(25,0),backupFinishDate datetime,
                       BackupType INT,
                       FirstLSN NUMERIC(25, 0),
                       LastLSN NUMERIC(25, 0),
                       CheckpointLSN NUMERIC(25, 0)
                   )";

                cmd.ExecuteNonQuery();

                foreach (var dbName in dbNames)
                {
                    //if (dbName.Substring(dbName.Length - 6, 6).ToLower() == "_image" ||
                    //    dbName.Substring(dbName.Length - 4, 4).ToLower() == "_pmt")
                    //    continue;

                    fullBackupParts.Clear();
                    diffBackupParts.Clear();
                    tranLogBackupFiles.Clear();

                    // Get a flat listing of blobs with metadata
                    await foreach (BlobItem blobItem in _blobContainerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None))
                    {
                        if (blobItem.Name.StartsWith(dbName, StringComparison.OrdinalIgnoreCase)
                        && (dbName.EndsWith("_pmt", StringComparison.OrdinalIgnoreCase) ||
                            !blobItem.Name.StartsWith(dbName + "_pmt", StringComparison.OrdinalIgnoreCase))
                        && (dbName.EndsWith("_image", StringComparison.OrdinalIgnoreCase) ||
                            !blobItem.Name.StartsWith(dbName + "_image", StringComparison.OrdinalIgnoreCase)))
                        {
                            if (blobItem.Name.EndsWith(".bak", StringComparison.OrdinalIgnoreCase))
                            {
                                // Group part files for full backup
                                if (blobItem.Name.Contains("_diff_part", StringComparison.OrdinalIgnoreCase))
                                {
                                    diffBackupParts.Add(blobItem);
                                }
                                else if (blobItem.Name.Contains("_part", StringComparison.OrdinalIgnoreCase) && !blobItem.Name.Contains("diff", StringComparison.OrdinalIgnoreCase))
                                {
                                    fullBackupParts.Add(blobItem);
                                }
                            }
                            else if (blobItem.Name.EndsWith(".trn", StringComparison.OrdinalIgnoreCase))
                            {
                                // Collect differential backup files
                                tranLogBackupFiles.Add(blobItem);
                            }

                            matchingBlobs.Add((dbName, blobItem.Name, blobItem.Properties.LastModified,
                                 blobItem.Properties.ContentLength));
                        }

                    }


                    cmd.CommandText = "truncate table #BackupFiles";
                    cmd.ExecuteNonQuery();


                    if (fullBackupParts.Any())
                    {
                        cErr = GetLsnInfoForDb(ref dtBackupInfo, ref dtHeaders, ref dtFileInfo, dbName, containerName, cmd, sda, fullBackupParts, diffBackupParts,
                            tranLogBackupFiles);
                        if (!string.IsNullOrEmpty(cErr))
                            goto lblLast;
                    }

                }





                cErr = "";
                result = dtBackupInfo;
            }

            catch (Exception ex)
            {
                int errLineNo = globalMethods.GetErrorLineNo(ex);
                cErr = "Error in GetVerifiedBackupsList at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();
            }

        lblLast:

            return (result, cErr);
        }
        public async Task<string> RestoreDbFiles(string containerName, string restoreDbName, string restoreServerName, string restorePath,bool restoreAsTest=true)
        {
            string cErr = "";
            try
            {

                string blobServiceEndpoint = "https://csg100320029a649e64.blob.core.windows.net/";
                //string sasToken = "sv=2022-11-02&ss=bfqt&srt=sc&sp=rwdlacupiytfx&se=2024-09-12T12:34:23Z&st=2024-09-11T12:34:23Z&spr=https,http&sig=xUUsQNKCogA2rQnj47SSmczq9qbaVV0QKpxJ%2Bm9bbpc%3D";

                string sasToken = AppConfigModel.blobSasToken;
                // Combine the Blob Service Endpoint,Container name and SAS token
                string sasUri = $"{blobServiceEndpoint}{containerName}?{sasToken}";

                // Create BlobContainerClient using SAS token
                BlobContainerClient _blobContainerClient = new BlobContainerClient(new Uri(sasUri));
                //BlobContainerClient _blobContainerClient = new BlobContainerClient(blobServiceEndpoint, containerName);
                List<(string BlobName, DateTimeOffset? LastModified, Int64? fileSize)> matchingBlobs =
                    new List<(string, DateTimeOffset?, Int64?)>();

                List<string> RestoreCommands = new List<string> { };

                string cSearch = "", cPartNo = "";
                DataRow dRow;
                Int32 startIndex, endIndex;

                DateTimeOffset? lastModified;
                BlobItem fullBackupFile = null;
                BlobItem mostRecentDataBackup = null;
                BlobItem lastModifiedPart = null;
                BlobItem diffBackupFile = null;
                List<BlobItem> tranLogBackupFiles = new List<BlobItem>();
                List<BlobItem> fullBackupParts = new List<BlobItem>();
                List<BlobItem> diffBackupParts = new List<BlobItem>();
                string blobName = "";
                // Get a flat listing of blobs with metadata
                await foreach (BlobItem blobItem in _blobContainerClient.GetBlobsAsync(traits: BlobTraits.None))
                {

                    //blobName = blobItem.Name;
                    //if (!IsBlobNameValid(blobName))
                    //    continue;

                    // Manually filter blob names with a case-insensitive check
                    if (blobItem.Name.StartsWith(restoreDbName, StringComparison.OrdinalIgnoreCase)
                    && (restoreDbName.EndsWith("_pmt", StringComparison.OrdinalIgnoreCase) ||
                        !blobItem.Name.StartsWith(restoreDbName + "_pmt", StringComparison.OrdinalIgnoreCase))
                    && (restoreDbName.EndsWith("_image", StringComparison.OrdinalIgnoreCase) ||
                        !blobItem.Name.StartsWith(restoreDbName + "_image", StringComparison.OrdinalIgnoreCase)))
                    {
                        if (blobItem.Name.EndsWith(".bak", StringComparison.OrdinalIgnoreCase))
                        {
                            // Group part files for full backup
                            if (blobItem.Name.Contains("_diff_part", StringComparison.OrdinalIgnoreCase))
                            {
                                diffBackupParts.Add(blobItem);
                            }
                            else if (blobItem.Name.Contains("_part", StringComparison.OrdinalIgnoreCase) && !blobItem.Name.Contains("diff", StringComparison.OrdinalIgnoreCase))
                            {
                                fullBackupParts.Add(blobItem);
                            }
                        }
                        else if (blobItem.Name.EndsWith(".trn", StringComparison.OrdinalIgnoreCase))
                        {
                            // Collect differential backup files
                            tranLogBackupFiles.Add(blobItem);
                        }

                        matchingBlobs.Add((blobItem.Name, blobItem.Properties.LastModified,
                             blobItem.Properties.ContentLength));
                    }


                    //matchingBlobs.Add((blobItem.Name, blobItem.Properties.LastModified,
                    //     blobItem.Properties.ContentLength));
                }


                string cExpr = "", cFullBackupExpr = "", cDiffBackupExpr = "", cFilesListExpr = "";
                // Process full backup parts if no single full backup file was found
                // Determine the most recent part file
                lastModifiedPart = fullBackupParts
                    .OrderByDescending(part => part.Properties.LastModified)
                    .FirstOrDefault();

                string cPartFilesUrl = "";
                foreach (var partFile in fullBackupParts)
                {
                    if (string.IsNullOrEmpty(cFilesListExpr))
                        cFilesListExpr = "RESTORE FILELISTONLY FROM " +
                        "URL = '" + blobServiceEndpoint + containerName + "/" + partFile.Name + "'";

                    cPartFilesUrl = cPartFilesUrl + (cPartFilesUrl == "" ? "" : ",") +
                        "URL = '" + blobServiceEndpoint + containerName + "/" + partFile.Name + "'";

                    mostRecentDataBackup = partFile;
                }

                string cTargetDbName = restoreDbName + (restoreAsTest ? "_test" : "");

                cFullBackupExpr = "RESTORE DATABASE " + cTargetDbName +
                " FROM " + cPartFilesUrl +" WITH NORECOVERY";
                

                if (diffBackupParts.Any())
                {
                    cPartFilesUrl = "";
                    foreach (var partFile in diffBackupParts)
                    {
                        cPartFilesUrl = cPartFilesUrl + (cPartFilesUrl == "" ? "" : ",") +
                        "URL = '" + blobServiceEndpoint + containerName + "/" + partFile.Name + "'";

                        mostRecentDataBackup = partFile;
                    }

                    cDiffBackupExpr= "RESTORE DATABASE " + cTargetDbName +
                    " FROM " + cPartFilesUrl + " WITH NORECOVERY";

                    
                }

                string serverConnStr = AppConfigModel.globalConnConfig["ConnectionStrings:CON_" + restoreServerName];
                SqlConnection conn = new SqlConnection(serverConnStr);

                conn.Open();
                if (conn.State != ConnectionState.Open)
                {
                    cErr = "Unable To Connect Target Server Database";
                    goto lblLast;
                }

                SqlCommand cmd = new SqlCommand("", conn);

                //string credName = blobServiceEndpoint + " / " + containerName;
                //cErr = CreateAzureCredential(cmd,credName);
                //if (!string.IsNullOrEmpty(cErr))
                //    goto lblLast;

                cmd.CommandText = cFilesListExpr;
                DataTable dtFilesList = new DataTable();
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                sda.Fill(dtFilesList);

                string cLogicalDataFileName = dtFilesList.Select("type='D'").FirstOrDefault()["LogicalName"].ToString();
                string cLogicalLogFileName = dtFilesList.Select("type='L'").FirstOrDefault()["LogicalName"].ToString();

                cmd.CommandText = "SELECT TOP 1 name FROM sys.databases WHERE name='" + restoreDbName + "'";
                object objDbName = cmd.ExecuteScalar();

                string cDataFilePath = restorePath + "\\" + restoreDbName + ".mdf",
                        cLogFilePath = restorePath + "\\" + restoreDbName + "_log.ldf";
                if (objDbName != null)
                {
                    //cmd.CommandText = "select a.physical_name from sys.master_files a (NOLOCK) " +
                    //    "join sys.databases b (NOLOCK) on a.database_id=b.database_id where b.name='" + restoreDbName + "'" +
                    //    " AND type=0  ";
                    //cDataFilePath = cmd.ExecuteScalar().ToString();

                    //cmd.CommandText = "select a.physical_name from sys.master_files a (NOLOCK) " +
                    //    " join sys.databases b (NOLOCK) on a.database_id=b.database_id where b.name='" + restoreDbName + "'" +
                    //    " AND type=1";
                    //cLogFilePath = cmd.ExecuteScalar().ToString();

                    cErr = "Database " + restoreDbName + " already exists on target Server..Cannot restore again";
                    goto lblLast;
                }


                cFullBackupExpr = cFullBackupExpr + ",Replace, move '" + cLogicalDataFileName + "' to '" + cDataFilePath + "'," +
                " move '" + cLogicalLogFileName + "' to '" + cLogFilePath + "'";
                cmd.CommandText = cFullBackupExpr;

                RestoreCommands.Add(cFullBackupExpr);

                if (diffBackupParts.Any())
                    RestoreCommands.Add(cDiffBackupExpr);

                    //cmd.ExecuteNonQuery();

                // Process differential backup files if the most recent full backup file is found
                if (mostRecentDataBackup != null)
                {
                    DateTimeOffset? lastDataBackupDate = mostRecentDataBackup.Properties.LastModified;

                  

                    foreach (var tranLogFile in tranLogBackupFiles)
                    {
                        // Check if the .trn file's LastModified date is more recent than the last full backup
                        if (tranLogFile.Properties.LastModified > lastDataBackupDate)
                        {
                            cExpr = "RESTORE LOG " + cTargetDbName +
                             " FROM URL = '" + blobServiceEndpoint + containerName + "/" + tranLogFile.Name + "'" +
                             " WITH NORECOVERY";
                            cmd.CommandText = cExpr;
                            RestoreCommands.Add(cExpr);
                            //cmd.ExecuteNonQuery();
                        }
                    }
                }

                cmd.CommandTimeout = 600;

                foreach(string restoreCmd in RestoreCommands)
                {
                    cmd.CommandText=restoreCmd;
                    cmd.ExecuteNonQuery();
                }

                cErr = "";
                //result = matchingBlobs;
            }

            catch (Exception ex)
            {
                int errLineNo = globalMethods.GetErrorLineNo(ex);
                cErr = "Error in GetAvailableDbs at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();
            }

        lblLast:

            return cErr;
        }

        public dynamic GetContainers(ref string cErr)
        {
            dynamic result = new ExpandoObject();
            try
            {
                string blobServiceEndpoint = "https://csg100320029a649e64.blob.core.windows.net/";
                //"/subscriptions/004ce696-b3c1-41ef-bccb-6794135c3d8e/resourceGroups/cloud-shell-storage-centralindia/providers/Microsoft.Storage/storageAccounts/csg100320029a649e64/blobServices/default";
                string sasToken = AppConfigModel.blobSasToken;

                // Combine the Blob Service Endpoint and SAS token
                //string sasUri = $"{blobServiceEndpoint}?{sasToken}";
                string blobServiceUri = $"{blobServiceEndpoint}?{sasToken}";


                // Create BlobServiceClient using the SAS token
                BlobServiceClient blobServiceClient = new BlobServiceClient(new Uri(blobServiceUri));

                // List containers in the blob service
                var containersList = blobServiceClient.GetBlobContainers();

                List<string> containers = containersList
                   .Select(container => container.Name).ToList();

                result = containers;
            }

            catch (Exception ex)
            {
                int errLineNo = globalMethods.GetErrorLineNo(ex);
                cErr = "Error in GetContainers at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();
            }

            return result;
        }

        public dynamic GetAvailableDbs(string containerName, ref string cErr)
        {
            dynamic result = new ExpandoObject();
            try
            {
                string blobServiceEndpoint = "https://csg100320029a649e64.blob.core.windows.net/";
                string sasToken = AppConfigModel.blobSasToken;

                // Combine the Blob Service Endpoint and SAS token
                string sasUri = $"{blobServiceEndpoint}{containerName}?{sasToken}";

                // Create BlobContainerClient using SAS token
                BlobContainerClient containerClient = new BlobContainerClient(new Uri(sasUri));
                List<BlobItem> blobs = containerClient.GetBlobs().ToList();



                List<string> dbNames = GetDbNamesFromBlobs(blobs,"");


                result = dbNames;
            }

            catch (Exception ex)
            {
                int errLineNo = globalMethods.GetErrorLineNo(ex);
                cErr = "Error in GetAvailableDbs at Line#" + errLineNo.ToString() + ":" + ex.Message.ToString();
            }

            return result;
        }
    }
}
