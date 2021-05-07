import context from './context.js'
import transport from './transport.js'
import db from './db.js'
import fs from './fs.js'
import util from './util.js'

let clientSchemaList = [];
let clientSchemaMap = {};
let clientSchemaSubList = [];
let clientSchemaSubMap = {};

let clientFolderList = [];
let clientFolderMap = {};
let clientFolderSubList = [];
let clientFolderSubMap = {};

let schemaNameIdMap = {}; // schemaName->schemaId
let pvcAdminDb;
let syncSummary = {};

let syncClientId = -1;
let syncServerId = 0;
let transactionId = 0;

context.onSyncStateChange("READY");

/**
* Start a synchronization session.
* @param syncDirection Sync direction.
* Valid values are REFRESH_ONLY, CHECK_IN_ONLY and
* TWO_WAY. If null, defaults to TWO_WAY.
* @param syncSchemas List of sync schema names to sync.
* To sync all, use a null or empty syncSchemaNames.
* @returns A promise of SyncSummary object
* @throws An error if there is already an active sync session
*/
async function sync(syncDirection, syncSchemas, syncFolders) {

    try {
        if (context.syncState["syncing"]) {
            throw new Error("There is already an active sync session. Will not start a new one.");
        }
        context.syncState["syncing"] = true;


        pvcAdminDb = await db.getDbConn(context.settings.adminDbName);
        await transport.init(pvcAdminDb);

        await init(syncDirection, syncSchemas, syncFolders);
        await send();
        await receive();

        let hasDefChanges = false;
        if (syncSummary.hasDefChanges) {
            context.log("Will sync again since last sync only refreshed def changes.");
            hasDefChanges = true;
            await init(syncDirection, syncSchemas, syncFolders);
            await send();
            await receive();
        }
        if (syncSummary.hasDefChanges) {
            context.log("Will sync again since last sync only refreshed def changes.");
            hasDefChanges = true;
            await init(syncDirection, syncSchemas, syncFolders);
            await send();
            await receive();
        }
        syncSummary.hasDefChanges = hasDefChanges;
        syncSummary.hasDataChanges = syncSummary.refreshDIU_done.reduce((sum, item) => sum + item) > 0;

        await db.closeAllDbConns(false);
    } catch (e) {
        syncSummary.syncException = e;
        syncSummary.syncErrorMessages += e;
        await db.closeAllDbConns(true);
    } finally {

        context.syncState["syncing"] = false;

        syncSummary.syncEndTime = new Date().getTime();
        syncSummary.syncDuration = (syncSummary.syncEndTime - syncSummary.syncBeginTime) / 1000.00 + " seconds";

        context.log("syncSummary=" + JSON.stringify(syncSummary));

        if (syncSummary.syncException) {
            if (context.syncState != "FAILED") {
                context.onSyncStateChange("FAILED", syncSummary);
            }
            context.log("Sync completed with error: " + JSON.stringify(syncSummary.syncException));
        } else {
            context.onSyncStateChange("SUCCEEDED", syncSummary);
            context.log("Sync completed successfully");
        }
    }
    return syncSummary;
}

async function init(syncDirection, syncSchemas, syncFolders) {
    context.log("begin agent init");

    await queryClientSchemasState();
    await queryClientFoldersState();

    // reset syncSummary
    for (let key in syncSummary) {
        delete syncSummary[key]
    }

    context.log("syncServerUrl=" + context.settings.syncServerUrl);
    context.log("syncUserName=" + context.settings.syncUserName);
    context.log("syncDeviceName=" + context.settings.syncDeviceName);

    let propertiesSql = "SELECT NAME, VALUE FROM pvc$sync_client_properties";
    let propertiesRs = await pvcAdminDb.executeSql(propertiesSql, []);
    for (let i = 0; i < propertiesRs.rows.length; i++) {
        // Each row is a standard JavaScript array indexed by
        // column names.
        let clientPropertiesRow = propertiesRs.rows.item(i);
        if (clientPropertiesRow["NAME"] == "pervasync.client.id") {
            syncClientId =
                Number(clientPropertiesRow["VALUE"]);
        }
        if (clientPropertiesRow["NAME"] == "pervasync.server.id") {
            syncServerId =
                Number(clientPropertiesRow["VALUE"]);
        }
        if (clientPropertiesRow["NAME"] == "pervasync.transaction.id") {
            transactionId =
                Number(clientPropertiesRow["VALUE"]);
        }
    }

    context.log("syncClientId=" + syncClientId);
    context.log("syncServerId=" + syncServerId);
    context.log("transactionId=" + transactionId);

    if (!syncDirection) {
        syncDirection = "TWO_WAY";
    }

    // sync summary
    syncSummary.syncBeginTime = new Date().getTime();
    syncSummary.checkInDIU_requested = [0, 0, 0];
    syncSummary.checkInDIU_done = [0, 0, 0];
    syncSummary.refreshDIU_requested = [0, 0, 0];
    syncSummary.refreshDIU_done = [0, 0, 0];
    syncSummary.hasDefChanges = false;
    syncSummary.hasDataChanges = false;
    syncSummary.errorCode = -1;
    syncSummary.checkInStatus = "NOT_AVAILABLE";
    syncSummary.checkInSchemaNames = [];
    syncSummary.refreshSchemaNames = [];
    syncSummary.checkInFolderNames = [];
    syncSummary.refreshFolderNames = [];

    syncSummary.refreshStatus = "NOT_AVAILABLE";
    syncSummary.serverSnapshotAge = -1;

    syncSummary.user = context.settings.syncUserName;
    syncSummary.device = context.settings.syncDeviceName;
    syncSummary.syncDirection = syncDirection;
    syncSummary.syncErrorMessages = "";
    syncSummary.syncErrorStacktraces = "";

    //context.log("Determine sync schemas and folders");
    if (!syncSchemas || syncSchemas.length == 0) {
        syncSummary.syncSchemaNames = [];
        for (let schemaId in clientSchemaMap) {
            syncSummary.syncSchemaNames.push(clientSchemaMap[schemaId].name);
        }
    } else {
        syncSummary.syncSchemaNames = syncSchemas;
    }
    context.log("syncSummary.syncSchemaNames: " + syncSummary.syncSchemaNames.join());

    if (!syncFolders || syncFolders.length == 0) {
        syncSummary.syncFolderNames = [];
        for (let folderId in clientFolderMap) {
            syncSummary.syncFolderNames.push(clientFolderMap[folderId].name);
        }
    } else {
        syncSummary.syncFolderNames = syncFolders;
    }
    context.log("syncSummary.syncFolderNames: " + syncSummary.syncFolderNames.join());
    context.log("end agent init");
}

/**
 * get Schema, SchemaSub, Objects.
 */
async function queryClientSchemasState() {
    context.log("calling queryClientSchemasState ...");
    clientSchemaList = [];
    clientSchemaMap = {};
    clientSchemaSubList = [];
    clientSchemaSubMap = {};

    // Schemas
    let schemasSql =
        "SELECT SYNC_SCHEMA_ID,SYNC_SCHEMA_NAME,CLIENT_DB_SCHEMA," +
        "DEF_CN,SUB_CN,DATA_CN,SYNC_CLIENT_ID,SERVER_DB_TYPE FROM pvc$sync_schemas";
    let schemasRs = await pvcAdminDb.executeSql(schemasSql, []);
    //context.log("schemasRs.rows.length=" + schemasRs.rows.length);
    for (let i = 0; i < schemasRs.rows.length; i++) {
        // Each row is a standard JavaScript array indexed by
        // column names.
        let row = schemasRs.rows.item(i);
        //context.log("row=" + JSON.stringify(row));
        let syncSchema = {};
        let syncSchemaSub = {};
        syncSchema.id = row['SYNC_SCHEMA_ID'];
        syncSchemaSub.syncSchemaId = syncSchema.id;
        syncSchema.name = row['SYNC_SCHEMA_NAME'];
        syncSchema.clientDbSchema = row['CLIENT_DB_SCHEMA'];
        syncSchema.defCn = row['DEF_CN'];
        syncSchemaSub.defCn = syncSchema.defCn;
        syncSchema.subCn = row['SUB_CN'];
        syncSchemaSub.subCn = syncSchema.subCn;
        syncSchemaSub.dataCn = row['DATA_CN'];
        syncSchemaSub.syncClientId = row['SYNC_CLIENT_ID'];
        syncSchema.serverDbType = row['SERVER_DB_TYPE'];
        syncSchema.tableMap = {};
        let tableList = [];
        syncSchemaSub.tableSubMap = {};
        let newTablesList = [];
        syncSchema.tableList = tableList;
        if (!syncSchema.tableList) {
            syncSchema.tableList = [];
        }
        syncSchemaSub.newTables = newTablesList;

        //context.log("syncSchema=\n" + JSON.stringify(syncSchema));

        // post DB retrieval processing
        //context.log("post DB retrieval processing");

        // insert into collections
        schemaNameIdMap[syncSchema.name] = syncSchema.id;
        clientSchemaList.push(syncSchema);
        clientSchemaMap[syncSchema.id] = syncSchema;
        clientSchemaSubList.push(syncSchemaSub);
        //context.log("Pushed to clientSchemaSubList, syncSchemaSub.syncSchemaId=" + syncSchemaSub.syncSchemaId);
        clientSchemaSubMap[syncSchemaSub.syncSchemaId] = syncSchemaSub;


        // tables: tableMap and tableSubMap
        let sql1 =
            "SELECT ID,DEF_CN,NAME,RANK," +
            "ALLOW_CHECK_IN,ALLOW_REFRESH,HAS_PK,CHECK_IN_SUPER_USERS,IS_NEW,SUBSETTING_MODE,SUBSETTING_QUERY" +
            " FROM pvc$sync_tables WHERE SYNC_SCHEMA_ID=? ORDER BY RANK ASC";
        let tablesRs = await pvcAdminDb.executeSql(sql1, [syncSchema.id]);
        for (let j = 0; j < tablesRs.rows.length; j++) {
            // Each row is a standard JavaScript array indexed by
            // column names.
            let row = tablesRs.rows.item(j);

            let syncTable = {};
            let tableSub = {};
            syncTable.lobColCount = 0;
            syncTable.id = row['ID'];
            tableSub.tableId = syncTable.id;
            syncTable.defCn = row['DEF_CN'];
            syncTable.name = row['NAME'];
            syncTable.rank = row['RANK'];
            syncTable.allowCheckIn =
                "Y" == (row['ALLOW_CHECK_IN']);
            syncTable.allowRefresh =
                "Y" == (row['ALLOW_REFRESH']);
            syncTable.hasPk =
                "Y" == (row['HAS_PK']);
            let strCheckInSuperUsers = row['CHECK_IN_SUPER_USERS'];
            syncTable.checkInSuperUsers = strCheckInSuperUsers.split(",");
            let isNew = ("Y" == row['IS_NEW']);
            if (isNew) {
                newTablesList.push(syncTable.id);
            }
            syncTable.subsettingMode = (row['SUBSETTING_MODE']);

            // subsettingQuery
            syncTable.subsettingQuery = row['SUBSETTING_QUERY'];
            tableList.push(syncTable);
            syncSchema.tableMap[syncTable.id] = syncTable;
            syncSchemaSub.tableSubMap[tableSub.tableId] = tableSub;
            syncTable.pkList = [];

            // columns
            let cols = "";
            let colQs = "";
            let colsEqQs = ""; // col1=?,col2=?...
            let lobCols = ""; // lob cols
            let lobColsEqQs = ""; // col1=?,col2=?...
            let pks = "";
            let pkQs = "";
            let pkEqQs = "";
            let tCols = "";
            let pkJoinMT = "";
            let column;
            let colList = [];
            let pkColList = [];
            let regColList = [];
            let lobColList = [];

            let sql2 =
                "SELECT NAME,DEVICE_COL_DEF,JDBC_TYPE,NATIVE_TYPE,COLUMN_SIZE,SCALE,NULLABLE," +
                "PK_SEQ,ORDINAL_POSITION,DEFAULT_VALUE" +
                " FROM pvc$sync_table_columns WHERE " +
                " SYNC_TABLE_ID=? ORDER BY ORDINAL_POSITION ASC";
            let colsRs = await pvcAdminDb.executeSql(sql2, [syncTable.id]);
            for (let k = 0; k < colsRs.rows.length; k++) {
                // Each row is a standard JavaScript array indexed by
                // column names.
                let row = colsRs.rows.item(k);
                column = {};
                column.columnName = row['NAME'];
                column.deviceColDef = row['DEVICE_COL_DEF'];
                column.dataType = row['JDBC_TYPE'];
                column.typeName = row['NATIVE_TYPE'];
                column.columnSize = row['COLUMN_SIZE'];
                column.decimalDigits = row['SCALE'];
                column.nullable =
                    ("Y" == (row['NULLABLE']));
                column.pkSeq = row['PK_SEQ'];
                column.ordinalPosition = row['ORDINAL_POSITION'];
                column.defaultValue = row['DEFAULT_VALUE'];
                if (column.pkSeq > 0) {
                    pkColList.push(column);
                } else if (!db.isBlob(syncSchema.serverDbType, column) &&
                    !db.isClob(syncSchema.serverDbType, column)) {
                    regColList.push(column);
                } else {
                    lobColList.push(column);
                }
                colList.push(column);
            }

            // sort pk list
            pkColList.sort(function (o1, o2) {
                return o1.pkSeq - o2.pkSeq
            });

            // pk columns
            for (let pki in pkColList) {
                column = pkColList[pki];
                if (pks.length > 0) {
                    pks += ",";
                    pkQs += ",";
                    pkEqQs += " AND ";
                    pkJoinMT += " AND ";
                }
                syncTable.pkList.push(column.columnName);
                let columnName = db.quote(column.columnName);
                pks += columnName;
                pkQs += "?";
                pkEqQs += columnName + "=?";
                pkJoinMT +=
                    "m." + columnName + "=t." + columnName;
            }

            // pk and reg cols
            pkColList = pkColList.concat(regColList);
            let i = 0;
            for (i in pkColList) {
                column = pkColList[i];
                if (cols.length > 0) {
                    cols += ",";
                    colsEqQs += ",";
                    tCols += ",";
                    colQs += ",";
                }
                let columnName = db.quote(column.columnName);
                cols += columnName;
                colsEqQs += columnName + "=";
                tCols += "t." + columnName;
                colQs += "?";
                colsEqQs += "?";
            }

            // lob cols
            for (i in lobColList) {
                column = lobColList[i];
                if (cols.length > 0) {
                    cols += ",";
                    colsEqQs += ",";
                    if (!context.settings.separateLobQuery) {
                        tCols += ",";
                    }
                    colQs += ",";
                }
                let columnName = db.quote(column.columnName);
                cols += columnName;
                colsEqQs += columnName + "=";
                if (!context.settings.separateLobQuery) {
                    tCols += "t." + columnName;
                }
                if (lobCols.length > 0) {
                    lobCols += ",";
                    lobColsEqQs += ",";
                }
                lobCols += columnName;
                lobColsEqQs += columnName + "=?";
                let emptyLob = "''";
                colQs += emptyLob;
                colsEqQs += emptyLob;
                syncTable.lobColCount = Number(syncTable.lobColCount) + 1;
            }

            syncTable.columns = colList;
            if (!syncTable.columns) {
                syncTable.columns = [];
            }
            pkColList = pkColList.concat(lobColList);
            syncTable.columnsPkRegLob = pkColList;
            if (!syncTable.columnsPkRegLob) {
                syncTable.columnsPkRegLob = [];
            }
            //context.log("syncTable.columnsPkRegLob=" + JSON.stringify(syncTable.columnsPkRegLob));

            syncTable.pks = pks;

            // db conn is made with clientDbSchema
            //let dqSchema = context.settings.DQ + clientDbSchema + context.settings.DQ;
            let dqSchema = context.settings.DQ + "main" + context.settings.DQ;

            let dqTableName =
                context.settings.DQ + syncTable.name + context.settings.DQ;
            let dqTableMateName =
                context.settings.DQ + syncTable.name + "$m" + context.settings.DQ;

            // refresh DMLs for Table and syncTable mate
            // delete
            syncTable.sqlDelete =
                "DELETE FROM " + dqSchema + "." + dqTableName +
                " WHERE " + pkEqQs;
            //" WHERE (" + pks + ") IN (SELECT " + pkQs +
            //")";
            syncTable.sqlDeleteM =
                "DELETE FROM " + dqSchema + "." + dqTableMateName +
                " WHERE " + pkEqQs;
            //" WHERE (" + pks + ") IN (SELECT " + pkQs +
            //")";
            // insert
            syncTable.sqlInsert =
                "INSERT OR REPLACE INTO " + dqSchema + "." + dqTableName +
                " (" + cols + ") VALUES (" + colQs + ")";
            syncTable.sqlInsertM =
                "INSERT OR REPLACE INTO " + dqSchema + "." + dqTableMateName +
                "(VERSION$$," + pks + ") VALUES (?," + pkQs + ")";
            // update
            syncTable.sqlUpdate =
                "UPDATE " + dqSchema + "." + dqTableName +
                " SET " + colsEqQs +
                " WHERE " + pkEqQs;
            //" WHERE (" + pks +
            //") IN (SELECT " + pkQs + ")";
            syncTable.sqlUpdateM =
                "UPDATE " + dqSchema + "." + dqTableMateName +
                " SET VERSION$$=?" +
                " WHERE " + pkEqQs;
            //" WHERE (" + pks +
            //") IN (SELECT " + pkQs + ")";

            // select lob locators
            if (lobCols.length > 0) {
                syncTable.sqlQueryLob =
                    "SELECT " + lobCols + " FROM " + dqSchema +
                    "." + dqTableName +
                    " WHERE " + pkEqQs;
                //" WHERE (" +
                //syncTable.pks + ") IN (SELECT " + pkQs +
                //")";

                // sqlUpdateLob
                syncTable.sqlUpdateLob =
                    "UPDATE " + dqSchema + "." + dqTableName +
                    " SET " + lobColsEqQs +
                    " WHERE " + pkEqQs;
                // " WHERE (" + pks +
                //") IN (SELECT " + pkQs + ")";
            }

            // sql queries
            // insert, update
            syncTable.sqlQueryI =
                "SELECT m.VERSION$$," + tCols + " FROM " +
                dqSchema + "." + dqTableName + " t, " + dqSchema +
                "." + dqTableMateName + " m WHERE m.DML$$=? AND TXN$$=? AND " +
                pkJoinMT;
            syncTable.sqlQueryU = syncTable.sqlQueryI;
            //delete
            syncTable.sqlQueryD =
                "SELECT VERSION$$," + pks + " FROM " + dqSchema +
                "." + dqTableMateName + " WHERE DML$$='D' AND TXN$$=? ";

            //context.log("syncTable=\n" + JSON.stringify(syncTable));
        }
    }
}

async function queryClientFoldersState() {
    context.log("calling queryClientFoldersState ...");

    clientFolderList = [];
    clientFolderMap = {};
    clientFolderSubList = [];
    clientFolderSubMap = {};

    // Folders
    let foldersSql =
        "SELECT ID,SYNC_FOLDER_NAME,SERVER_FOLDER_PATH,CLIENT_FOLDER_PATH,"
        + "RECURSIVE,FILE_PATH_STARTS_WITH,FILE_NAME_ENDS_WITH,"
        + "ALLOW_CHECK_IN,ALLOW_REFRESH,CHECK_IN_SUPER_USERS,DEF_CN,"
        + "SUB_CN,FILE_CN,SYNC_CLIENT_ID,"
        + "NO_INIT_SYNC_NETWORKS,NO_SYNC_NETWORKS"
        + " FROM pvc$sync_folders";
    let foldersRs = await pvcAdminDb.executeSql(foldersSql, []);
    for (let i = 0; i < foldersRs.rows.length; i++) {
        // Each row is a standard JavaScript array indexed by
        // column names.
        let syncFolderRow = foldersRs.rows.item(i);

        let syncFolder = {};
        let syncFolderSub = {};

        syncFolder.id = syncFolderRow["ID"];
        syncFolderSub.syncFolderId = syncFolder.id;
        syncFolder.name = syncFolderRow["SYNC_FOLDER_NAME"];
        syncFolder.serverFolderPath = syncFolderRow["SERVER_FOLDER_PATH"];
        syncFolder.clientFolderPath = syncFolderRow["CLIENT_FOLDER_PATH"];
        syncFolder.recursive = ("Y" == syncFolderRow["RECURSIVE"]);
        syncFolder.filePathStartsWith = syncFolderRow["FILE_PATH_STARTS_WITH"];
        syncFolder.fileNameEndsWith = syncFolderRow["FILE_NAME_ENDS_WITH"];
        syncFolder.allowCheckIn = ("Y" == syncFolderRow["ALLOW_CHECK_IN"]);
        syncFolder.allowRefresh = ("Y" == syncFolderRow["ALLOW_REFRESH"]);
        let strCheckInSuperUsers = syncFolderRow["CHECK_IN_SUPER_USERS"];
        syncFolder.checkInSuperUsers = strCheckInSuperUsers.split(",");
        syncFolder.defCn = syncFolderRow["DEF_CN"];
        syncFolderSub.defCn = syncFolder.defCn;
        syncFolder.subCn = syncFolderRow["SUB_CN"];
        syncFolderSub.subCn = syncFolder.subCn;
        syncFolderSub.fileCn = syncFolderRow["FILE_CN"];
        syncFolderSub.syncClientId = syncFolderRow["SYNC_CLIENT_ID"];
        syncFolder.noInitSyncNetworks = syncFolderRow["NO_INIT_SYNC_NETWORKS"];
        syncFolder.noSyncNetworks = syncFolderRow["NO_SYNC_NETWORKS"];

        syncFolder.fileList = [];
        syncFolder.fileMap = {};

        //context.log("syncFolder=\n" + JSON.stringify(syncFolder));

        let filesSql =
            "SELECT FILE_NAME,IS_DIRECTORY,LENGTH,LAST_MODIFIED,"
            + "FILE_CN,FILE_CT FROM pvc$sync_files "
            + " WHERE SYNC_FOLDER_ID=? ORDER BY FILE_CN DESC";
        let filesRs = await pvcAdminDb.executeSql(filesSql, [syncFolder.id]);
        for (let j = 0; j < filesRs.rows.length; j++) {
            // Each row is a standard JavaScript array indexed by
            // column names.
            let syncFileRow = filesRs.rows.item(j);

            //context.log("syncFileName=" + syncFileRow["FILE_NAME"]);
            let syncFile = {};
            syncFile.syncFolderIdFileName = syncFileRow["SYNC_FOLDER_ID__FILE_NAME"];
            syncFile.syncFolderId = syncFolder.id;
            syncFile.fileName = syncFileRow["FILE_NAME"];
            syncFile.isDirectory =
                "Y" == (syncFileRow["IS_DIRECTORY"]);
            syncFile.length = syncFileRow["LENGTH"];
            syncFile.lastModified = syncFileRow["LAST_MODIFIED"];
            syncFile.fileCn = syncFileRow["FILE_CN"];
            syncFile.fileCt = syncFileRow["FILE_CT"];

            //context.log("syncFile=\n" + JSON.stringify(syncFile));

            syncFolder.fileList.push(syncFile);
            syncFolder.fileMap[syncFile.fileName] = syncFile;
        }

        // post DB retrieval processing
        //context.log("post DB retrieval processing for folder files");

        // insert into collections
        clientFolderList.push(syncFolder);
        clientFolderMap[syncFolder.id] = syncFolder;
        clientFolderSubList.push(syncFolderSub);
        //context.log("Pushed to clientFolderSubList, syncFolderSub.syncFolderId=" + syncFolderSub.syncFolderId);
        clientFolderSubMap[syncFolderSub.syncFolderId] = syncFolderSub;

        //context.log("end initSyncFolder for " + syncFolder.name);
    }
}

function getPath(folderName) {
    let path = null;
    for (let syncFolder of clientFolderList) {
        if (syncFolder.name == folderName) {
            path = context.settings.filesPath + syncFolder.clientFolderPath + "__" + context.settings.accountId;
            break;
        }
    }
    return path;
}

async function setSyncClientProperty(name, value) {
    let propertiesSql = "INSERT OR REPLACE INTO pvc$sync_client_properties (NAME, VALUE) VALUES(?, ?)";
    await pvcAdminDb.executeSql(propertiesSql, [name, value]);
    //context.log("setSyncClientProperty count=" + propertiesRs.rowsAffected);
}
async function send() {

    // sync start
    context.onSyncStateChange("COMPOSING");

    // sync request
    let syncRequest = {};
    //context.log("populating clientProperties");
    syncRequest.clientVersion = context.settings.VERSION;
    syncRequest.user = context.settings.syncUserName;
    syncRequest.device = context.settings.syncDeviceName;
    syncRequest.password = context.settings.syncUserPassword;
    syncRequest.serverId = syncServerId;
    syncRequest.clientId = syncClientId;
    // sync options
    syncRequest.syncDirection = syncSummary.syncDirection;
    syncRequest.syncSchemaNames = syncSummary.syncSchemaNames;
    syncRequest.syncFolderNames = syncSummary.syncFolderNames;

    //
    // Upload phase
    //
    context.log("Upload phase");
    syncSummary.uploadBeginTime = new Date().getTime();
    syncSummary.sessionId = syncSummary.uploadBeginTime;

    await transport.openOutputStream(context.settings.syncUserName + "-" + context.settings.syncDeviceName + "-"
        + syncSummary.sessionId);

    let cmd = {};
    cmd.name = "SYNC_REQUEST";
    //context.log(cmd.name);
    cmd.value = syncRequest;
    await transport.writeCommand(cmd);

    cmd = {};
    cmd.name = "SCHEMA_SUB_STATE";
    //context.log(cmd.name);
    cmd.value = clientSchemaSubList;
    await transport.writeCommand(cmd);

    cmd = {};
    cmd.name = "FOLDER_SUB_STATE";
    //context.log(cmd.name);
    cmd.value = clientFolderSubList;
    await transport.writeCommand(cmd);

    //
    // CHECK_IN_DATA
    //
    if (syncSummary.syncDirection == "REFRESH_ONLY") {
        context.log("syncDirection=REFRESH_ONLY. Check in skipped.");
    } else {
        context.log("Checking in client transactions");
        await checkInData();
        await checkInFiles();
    }

    cmd = {};
    cmd.name = "END_SYNC_REQUEST";
    cmd.value = null;
    await transport.writeCommand(cmd);
    //context.log(cmd.name);

    await transport.closeOutputStream(receive);
}

/**
 * 
 * @param {*} payload String or hex encoded string if isBinary
 * @param {*} isBinary 
 */
async function sendLob(payload, isBinary) {
    //context.log("sendLob, isBinary=" + isBinary);
    if (!payload) {
        //context.log("sendLob, payload empty" );
        let syncLob = {};
        syncLob.isBinary = isBinary;
        syncLob.isNull = true;
        syncLob.totalLength = 0;
        let cmd = {};
        cmd.name = "LOB";
        cmd.value = syncLob;
        await transport.writeCommand(cmd);
    } else {
        //context.log("sendLob, payload.length=" + payload.length );
        let offset = 0;
        while (offset < payload.length) {
            let syncLob = {};
            syncLob.isBinary = isBinary;
            syncLob.isNull = false;
            syncLob.totalLength = payload.length;
            if (syncLob.isBinary) {
                syncLob.totalLength = payload.length / 2;
            }
            let chunkSize = context.settings.lobBufferSize;
            if ((payload.length - offset) < context.settings.lobBufferSize) {
                chunkSize = payload.length - offset;
            }
            syncLob.txtPayload = payload.substr(offset, chunkSize);
            offset += chunkSize;
            let cmd = {};
            cmd.name = "LOB";
            cmd.value = syncLob;
            await transport.writeCommand(cmd);
        }
    }

}

/**
 * send CheckIns for each  pervasync schemaName
 */
async function checkInData() {
    context.log("checkInData() start");

    let cmd = {};
    cmd.name = "CHECK_IN_DATA";
    cmd.value = null;
    await transport.writeCommand(cmd);

    for (let iSchema in clientSchemaSubList) {

        // get schema
        let clientSchemaSub = clientSchemaSubList[iSchema];
        let syncSchema =
            clientSchemaMap[clientSchemaSub.syncSchemaId];
        //context.log("Doing pre check in transaction id assignment for pervasync schema " + syncSchema.name);

        // determine if it's on sync list
        let isOnSyncList = false;
        for (let j in syncSummary.syncSchemaNames) {
            let name = syncSummary.syncSchemaNames[j];
            if (name.toUpperCase() == syncSchema.name.toUpperCase()) {
                isOnSyncList = true;
                break;
            }
        }
        if (!isOnSyncList) {
            context.log("Will skip schema " + syncSchema.name +
                " since it's not on sync list.");
            continue;
        }

        // schema db
        let schemaDb = await db.getDbConn(syncSchema.clientDbSchema);

        if (!syncSchema.tableList) {
            syncSchema.tableList = [];
        }
        // Table iterator
        for (let n in syncSchema.tableList) {
            let syncTable = syncSchema.tableList[n];
            let isSuperUser = false;
            if (syncTable.checkInSuperUsers) {
                for (let l in syncTable.checkInSuperUsers) {
                    if (syncTable.checkInSuperUsers[l].toUpperCase() == context.settings.syncUserName.toUpperCase()) {
                        isSuperUser = true;
                        break;
                    }
                }
            }
            if (!syncTable.allowCheckIn && !isSuperUser) {
                continue;
            }

            let sqlUpdateM = "UPDATE " + context.settings.DQ + "main" +
                context.settings.DQ + "." + context.settings.DQ + syncTable.name +
                "$m" + context.settings.DQ +
                " SET TXN$$=? WHERE DML$$ IS NOT NULL";
            context.log("sqlUpdateM: " + sqlUpdateM);
            await schemaDb.executeSql(sqlUpdateM, [transactionId]);
        }

        cmd = {};
        cmd.name = "SCHEMA";
        cmd.value = clientSchemaSub;
        transport.writeCommand(cmd);

        // Table iterator
        let tableList = syncSchema.tableList;
        let dmlType = null;//[] = { "U",   "I", "D"};
        for (let dml1 = 1; dml1 >= 0; dml1--) {
            if (dml1 == 0) {
                tableList.reverse();
            }

            for (let k = 0; k < tableList.length; k++) {
                let syncTable = tableList[k];

                for (let dml2 = 0; dml2 < dml1 + 1; dml2++) {

                    if (dml1 == 0) {
                        dmlType = "D";
                    } else if (dml2 == 0) {
                        dmlType = "U";
                    } else if (dml2 == 1) {
                        dmlType = "I";
                    }

                    console.log("dmlType=" + dmlType + ", syncTable.name=" + syncTable.name);

                    let sqlQuery;
                    let dmlCmd = null;
                    switch (dmlType) {
                        case "D":
                            dmlCmd = "DELETE";
                            break;
                        case "I":
                            dmlCmd = "INSERT";
                            break;
                        case "U":
                            dmlCmd = "UPDATE";
                            break;
                    }

                    let queryRs;
                    if (dmlType == "D") { // delete
                        sqlQuery = syncTable.sqlQueryD;
                        context.log("syncTable.sqlQueryD: " + syncTable.sqlQueryD);
                        queryRs = await schemaDb.executeSql(sqlQuery, [transactionId]);
                    } else if (dmlType == "I") { // Insert
                        sqlQuery = syncTable.sqlQueryI;
                        context.log("syncTable.sqlQueryI: " + syncTable.sqlQueryI);
                        queryRs = await schemaDb.executeSql(sqlQuery, ["I", transactionId]);
                    } else if (dmlType == "U") { // update
                        sqlQuery = syncTable.sqlQueryU;
                        context.log("syncTable.sqlQueryU: " + syncTable.sqlQueryU);
                        queryRs = await schemaDb.executeSql(sqlQuery, ["U", transactionId]);
                    }

                    if (queryRs.rows.length > 0) {
                        let count = 0;
                        cmd = {};
                        cmd.name = dmlCmd;
                        cmd.value = syncTable.id;
                        transport.writeCommand(cmd);
                        for (let i = 0; i < queryRs.rows.length; i++) {
                            // Each row is a standard JavaScript array indexed by
                            // column names.
                            let row = queryRs.rows.item(i);
                            //context.log("row=" + JSON.stringify(row));
                            let pkVals = [];
                            count++;
                            if (dmlType == "D") {
                                syncSummary.checkInDIU_requested[0] += 1;
                            } else if (dmlType == "I") {
                                syncSummary.checkInDIU_requested[1] += 1;
                            } else {
                                syncSummary.checkInDIU_requested[2] += 1;
                            }
                            cmd.name = "ROW";
                            let colValList = [];
                            //let splitted;
                            if (dmlType == "D") { // delete
                                colValList.push(String(row['VERSION$$'])); // version col
                                for (let m = 0; m < syncTable.pkList.length; m++) {
                                    let obj = null;
                                    obj = row[syncTable.columnsPkRegLob[m].columnName];
                                    if (obj != null) {
                                        colValList.push(String(obj)); // cast to String
                                    } else {
                                        colValList.push(null);
                                    }
                                }
                            } else { // insert or update
                                colValList.push(String(row['VERSION$$'])); // version col
                                for (let m = 0; m < syncTable.columnsPkRegLob.length - syncTable.lobColCount; m++) {
                                    let column = syncTable.columnsPkRegLob[m];
                                    let obj = null;
                                    obj = row[column.columnName];
                                    //context.log("column.columnName=" + column.columnName);
                                    //context.log("obj=" + obj);
                                    if (obj != null) {
                                        colValList.push(String(obj)); // cast to String
                                    } else {
                                        colValList.push(null);
                                    }
                                    if (m < syncTable.pkList.length) {
                                        pkVals.push(obj);
                                    }
                                }
                            }
                            cmd = {};
                            cmd.name = "ROW";
                            cmd.value = colValList;
                            transport.writeCommand(cmd);

                            // lob payloads
                            //context.log("syncTable.lobColCount=" + syncTable.lobColCount);
                            if ((dmlType == "I" || dmlType == "U") && syncTable.lobColCount > 0) { // insert/update and there are  lob cols
                                for (let m = 0; m < syncTable.lobColCount; m++) {
                                    let column =
                                        syncTable.columnsPkRegLob[m + syncTable.columnsPkRegLob.length - syncTable.lobColCount];
                                    let isBinary = (db.isBlob(syncSchema.serverDbType, column));
                                    let payload = row[column.columnName];
                                    //context.log("lob payload column.columnName=" + column.columnName + ", payload=" + payload);
                                    //context.log("lob payload isBinary=" + isBinary);
                                    await sendLob(payload, isBinary);
                                }
                            }
                        }

                        // TABLE DML (INSERT UPDATE DELETE) "END"
                        let end_cmd =
                            "END_" + dmlCmd;
                        cmd = {};
                        cmd.name = end_cmd;
                        cmd.value = null;
                        transport.writeCommand(cmd);

                        context.log(syncTable.name + ", " + dmlCmd + ", " +
                            count);
                    }
                }
            }
        }
        tableList.reverse();

        // END_SCHEMA
        cmd = {};
        cmd.name = "END_SCHEMA";
        cmd.value = null;
        transport.writeCommand(cmd);
    }
    // END_CHECK_IN_DATA
    cmd = {};
    cmd.name = "END_CHECK_IN_DATA";
    cmd.value = null;
    transport.writeCommand(cmd);
}

/**
    * scanFolder called by checkInFiles() to process folder files
    *
    * @param syncFolder SyncFolder
    * @param strFolder directory path relative to syncFolder path; no starting
    * or ending file separators
    * @throws Throwable
    */
async function scanFolder(syncFolder, strFolder) {
    //context.log("scanFolder, strFolder=" + strFolder);
    let syncFolderPath = getPath(syncFolder.name);
    //context.log("scanFolder, syncFolderPath=" + syncFolderPath);
    let folderPath = syncFolderPath;
    if (strFolder) {
        folderPath += "/" + strFolder;
    }
    //context.log("scanFolder, folderPath=" + folderPath);

    let folderExists = await fs.exists(folderPath);
    if (!folderExists) {
        context.log("scanFolder, will skip as folder not exists");
        return;
    }
    let isDir = await fs.isDir(folderPath);
    if (!isDir) {
        //context.log("scanFolder, will skip as not a folder");
        return;
    }

    let fileEntryArray = await fs.ls(folderPath);
    for (let fileEntry of fileEntryArray) {
        let fileName = fileEntry.name;
        //context.log("scanFolder, fileName=" + fileName);
        let syncFilePath = fileName;
        if (strFolder) {
            syncFilePath = strFolder + "/" + fileName;
        }
        let syncFile = syncFolder.fileMap[syncFilePath];
        let fullPath = syncFolderPath + "/" + syncFilePath;
        //context.log("scanFolder, fullPath=" + fullPath);

        let isDir = await fs.isDir(fullPath);
        let stat = await fs.stat(fullPath);

        if (!isDir && stat.size == 0) {
            context.log("Ignoring empty file: " + fullPath);
            if (syncFile) {
                syncFile.exists = true; // so that file won't be marked as delete
            }
            continue;
        }

        // non-empty directory
        if (isDir) {
            let fileEntryArray = await fs.ls(fullPath);
            if (fileEntryArray.length > 0) {
                if (syncFolder.recursive) {
                    await scanFolder(syncFolder, syncFilePath);
                }
                /*if (syncFile != null) {
                    context.log("scanFolder, set dir syncFile.exists = true" );
                    syncFile.exists = true;
                }*/
            }
        }

        // file and empty dir
        if (syncFile == null) {
            context.log("Found new file. File name: " + syncFilePath);
            // insert
            syncFile = {};
            syncFile.fileName = syncFilePath;
            syncFile.isDirectory = isDir;
            syncFile.length = stat.size;
            syncFile.lastModified = stat.lastModified;
            syncFile.fileCt = "I";
            syncFile.fileCn = -1;
            syncFile.exists = true;
            syncFolder.fileMap[syncFile.fileName] = syncFile;
            syncFolder.fileList.push(syncFile);

            let sqlInsert = "INSERT OR REPLACE INTO pvc$sync_files ( "
                + "SYNC_FOLDER_ID,FILE_NAME,IS_DIRECTORY,LENGTH,LAST_MODIFIED,"
                + // 'S'--Server Synced, 'I','U','D'-- Client
                // changes
                "FILE_CN,FILE_CT,TXN$$,ADDED) VALUES(?,?,?,?,?,?,'I',?,"
                + context.settings.SYSDATE + ")";
            await pvcAdminDb.executeSql(sqlInsert, [syncFolder.id, syncFile.fileName, (isDir ? "Y" : "N"), syncFile.length,
            syncFile.lastModified, syncFile.fileCn, transactionId]);
        } else {
            // mark existing file so that we can identify deleted files
            // (syncFile.exists = false)
            //context.log("scanFolder, set syncFile.exists = true for syncFilePath: " + syncFilePath);
            syncFile.exists = true;

            if (stat.lastModified / 1000 != syncFile.lastModified / 1000 || // accuracy: second
                stat.size != syncFile.length) {

                context.log("File updated. File name: " + syncFilePath);
                //context.log("stat.lastModified: " + stat.lastModified);
                //context.log("syncFile.lastModified: " + syncFile.lastModified);
                //context.log("stat.size: " + stat.size);
                //context.log("syncFile.length: " + syncFile.length);
                // update
                syncFile.length = stat.size;
                syncFile.lastModified = stat.lastModified;
                /*if ("S" == syncFile.fileCt
                    || "D" == syncFile.fileCt) {
                    syncFile.fileCt = "U";
                }*/
                syncFile.fileCt = "U";

                let sqlUpdate = "UPDATE pvc$sync_files SET "
                    + " IS_DIRECTORY=?,LENGTH=?,LAST_MODIFIED=?,"
                    + // 'S'--Server Synced, 'I','U','D'--
                    // Client changes
                    // "FILE_CT=DECODE(FILE_CT,'S','U','D','U',FILE_CT)"
                    // +
                    "FILE_CT='U',TXN$$=?"
                    + " WHERE SYNC_FOLDER_ID=? AND FILE_NAME=?";
                await pvcAdminDb.executeSql(sqlUpdate, [(isDir ? "Y" : "N"), syncFile.length, syncFile.lastModified,
                    transactionId, syncFolder.id, syncFile.fileName]);
            }
        }
    }
}

/**
     * send CheckIns for each pervasync folder
     */
async function checkInFiles() {
    if (clientFolderSubList.length == 0) {
        return;
    }

    context.log("Doing file check in ...");

    let cmd = {};
    cmd.name = "CHECK_IN_FILES";
    cmd.value = null;
    await transport.writeCommand(cmd);

    // check in for each folder
    for (let clientFolderSub of clientFolderSubList) {
        let syncFolder = clientFolderMap[clientFolderSub.syncFolderId];

        // determine if it's in sync list 
        let isOnSyncList = false;
        for (let name of syncSummary.syncFolderNames) {
            //context.log("name=" + name);
            if (name.toUpperCase() == syncFolder.name.toUpperCase()) {
                isOnSyncList = true;
                break;
            }
        }

        if (!isOnSyncList) {
            context.log("Skipping folder " + syncFolder.name
                + " since it's not on sync list.");
            continue;
        }


        context.log("Doing folder " + syncFolder.name);

        // No checkin until folder is first refreshed
        if (syncFolder.allowRefresh && clientFolderSub.fileCn < 0) {
            context.log("No checkin until folder is first refreshed");
            continue;
        }

        if (!syncFolder.allowCheckIn) {
            continue;
        }

        cmd = {};
        cmd.name = "FOLDER";
        cmd.value = clientFolderSub;
        transport.writeCommand(cmd);

        // process folder files

        let folderPath = getPath(syncFolder.name);

        let folderExistes = await fs.exists(folderPath);
        if (!folderExistes) {
            await fs.mkdirs(folderPath);
        }

        // Set file exists flag
        for (let syncFile of syncFolder.fileList) {
            syncFile.exists = false;
        }

        // scanSubFolder
        await scanFolder(syncFolder, "");

        // Update deleted files in DB
        for (let syncFile of syncFolder.fileList) {
            if (!syncFile.exists) {
                context.log("deleted file, syncFile.fileName=" + syncFile.fileName);
                syncFile.fileCt = "D";

                let sqlDelete = "UPDATE pvc$sync_files SET"
                    + " FILE_CT='D', TXN$$=? WHERE SYNC_FOLDER_ID=? AND FILE_NAME=?";
                await pvcAdminDb.executeSql(sqlDelete, [transactionId, syncFolder.id, syncFile.fileName]);
            }
        }

        let prefixes = syncFolder.filePathStartsWith.split(",");
        let suffixes = syncFolder.fileNameEndsWith.split(",");

        // check in folder files
        for (let syncFile of syncFolder.fileList) {
            if ("S" == syncFile.fileCt || syncFile.fileCt == null) {
                continue;
            }
            // only send file changes that satisfy the match criteria
            let prefixMatch = false;
            let suffixMatch = false;
            context.log("syncFile.fileName=" + syncFile.fileName);
            if (prefixes.length < 1) {
                prefixMatch = true;
            } else {
                prefixMatch = false;
                for (let prefix of prefixes) {
                    if (syncFile.fileName.startsWith(prefix)) {
                        prefixMatch = true;
                        context.log("matches with prefixe " + prefix);
                        break;
                    }
                }
            }

            if (suffixes.length < 1) {
                suffixMatch = true;
            } else {
                suffixMatch = false;
                for (let suffix of suffixes) {
                    if (syncFile.fileName.endsWith(suffix)) {
                        suffixMatch = true;
                        context.log("matches with suffix " + suffix);
                        break;
                    }
                }
            }

            if (prefixMatch && suffixMatch) {
                cmd = {};
                cmd.name = "FILE";
                cmd.value = syncFile;
                transport.writeCommand(cmd);

                if (!syncFile.isDirectory) {
                    if ("D" == syncFile.fileCt) {
                        syncSummary.checkInDIU_requested[0] += 1;
                        context.log("Sending delete for "
                            + syncFile.fileName);
                    } else if ("I" == syncFile.fileCt) {
                        syncSummary.checkInDIU_requested[1] += 1;
                        context.log("Sending insert for "
                            + syncFile.fileName);
                    } else {
                        syncSummary.checkInDIU_requested[2] += 1;
                        context.log("Sending update for "
                            + syncFile.fileName);
                    }
                }

                if ("D" == syncFile.fileCt || syncFile.isDirectory) {
                    continue;
                }

                // file contents
                let syncFolderPath = getPath(syncFolder.name);
                let syncFilerPath = syncFolderPath + "/" + syncFile.fileName;
                //context.log("syncFilerPath=" + syncFilerPath);
                //let stat = await fs.stat(syncFilerPath);
                //context.log("stat.size= " + stat.size);
                //context.log("stat.lastModified= " + stat.lastModified);

                let bytes = await fs.readBytes(syncFilerPath);
                //context.log("fs.readBytes returned bytes lenght: " + bytes.length);
                let payload = util.bytes2hex(bytes);
                //context.log("util.bytes2hex returned payload lenght: " + payload.length);
                await sendLob(payload, true);
            }
        }

        // END_FOLDER
        cmd = {};
        cmd.name = "END_FOLDER";
        cmd.value = null;
        transport.writeCommand(cmd);
    }

    // END_CHECK_IN_DATA
    cmd = {};
    cmd.name = "END_CHECK_IN_FILES";
    cmd.value = null;
    transport.writeCommand(cmd);
}


async function receive() {

    context.onSyncStateChange("PROCESSING");
    syncSummary.downloadBeginTime = new Date().getTime();

    let uploadDurationInSeconds =
        (syncSummary.downloadBeginTime -
            syncSummary.uploadBeginTime) / 1000.0;
    context.log("Upload time (seconds): " +
        uploadDurationInSeconds);

    // Reading from server
    await transport.openInputStream();

    // receive SYNC_RESPONSE
    let cmd = await transport.readCommand();
    if ("SYNC_RESPONSE" != cmd.name) {
        throw Error("Expecting SYNC_RESPONSE, got " + cmd.name);
    }

    await receiveServerResponse(cmd);

    // receive END_SYNC_RESPONSE REFRESH_SCHEMA_DEF REFRESH_DATA
    syncSummary.refreshStatus = "IN_PROGRESS";
    for (; ;) {
        cmd = await transport.readCommand();
        if ("END_SYNC_RESPONSE" == cmd.name) {
            context.log("Receiving server response end (END_SYNC_RESPONSE)");
            break;
        } else {
            if ("REFRESH_SCHEMA_DEF" != cmd.name &&
                "REFRESH_FOLDER_DEF" != cmd.name &&
                "REFRESH_DATA" != cmd.name &&
                "REFRESH_FILES" != cmd.name &&
                "SYNC_SUMMARY" != cmd.name) {
                throw Error("Expecting SYNC_RESPONSE END_SYNC_RESPONSE, SYNC_SUMMARY, " +
                    "REFRESH_SCHEMA_DEF, REFRESH_FOLDER_DEF, REFRESH_FILES or REFRESH_DATA, got " + cmd.name);
            }

            if ("REFRESH_SCHEMA_DEF" == cmd.name) {
                context.log("Receiving schema definitions (REFRESH_SCHEMA_DEF)");
                await receiveRefreshSchemaDef(cmd);
            } else if ("REFRESH_FOLDER_DEF" == cmd.name) {
                context.log("Receiving folder definitions (REFRESH_FOLDER_DEF)");
                await receiveRefreshFolderDef(cmd);
            } else if ("REFRESH_DATA" == cmd.name) {
                context.log("Receiving schema data (REFRESH_DATA)");
                await receiveRefreshData(cmd);
            } else if ("REFRESH_FILES" == cmd.name) {
                context.log("Receiving folder files (REFRESH_FILES)");
                await receiveRefreshFiles(cmd);
            } else if ("SYNC_SUMMARY" == cmd.name) {
                context.log("Receiving server message (SYNC_SUMMARY)");
                await receiveSyncSummary(cmd);
            }
        }
    }
    syncSummary.refreshStatus = "SUCCESS";
    await transport.closeInputStream();
}

/**
 * receive SYNC_RESPONSE command.
 */
async function receiveServerResponse(cmd) {
    context.log("Begin receiving server response (SYNC_RESPONSE)");
    let syncResponse = cmd.value;
    if (syncResponse.serverId < 0) {
        context.log("syncResponse.clientId=" + syncResponse.clientId);
        context.log("syncResponse.serverId=" + syncResponse.serverId);
        throw "Received invalid server response. Try sync again next time.";
    }

    // ClientProperties
    let serverVersion = syncResponse.serverVersion;
    context.log("serverVersion=" + serverVersion);

    // success list
    if (!syncResponse.successSchemaNames) {
        syncResponse.successSchemaNames = [];
    }
    let count = 0;
    let resultSet;
    for (let schemaName of syncResponse.successSchemaNames) {
        //context.log("schemaName=" + schemaName);
        let syncSchema;
        for (let j = 0; j < clientSchemaList.length; j++) {
            let schema = clientSchemaList[j];
            //context.log("schema=" + schema.name);
            if (schemaName.toUpperCase() == schema.name.toUpperCase()) {
                syncSchema = schema;
                break;
            }
        }

        context.log("Doing post check in cleanup for sync schema " + syncSchema.name);
        let schemaDb = await db.getDbConn(syncSchema.clientDbSchema);

        if (!syncSchema.tableList) {
            syncSchema.tableList = [];
        }
        // Table iterator
        for (let k = 0; k < syncSchema.tableList.length; k++) {
            let syncTable = syncSchema.tableList[k];
            let isSuperUser = false;
            if (syncTable.checkInSuperUsers) {
                for (let l = 0; l < syncTable.checkInSuperUsers.length; l++) {
                    if (syncTable.checkInSuperUsers[l].toUpperCase() == context.settings.syncUserName.toUpperCase()) {
                        isSuperUser = true;
                        break;
                    }
                }
            }
            if (!syncTable.allowCheckIn && !isSuperUser) {
                continue;
            }

            let sqlDeleteM =
                "DELETE FROM " + context.settings.DQ + "main" + context.settings.DQ
                + "." + context.settings.DQ + syncTable.name + "$m"
                + context.settings.DQ
                + " WHERE DML$$='D' AND TXN$$=?";
            resultSet = await schemaDb.executeSql(sqlDeleteM, [transactionId]);
            count += resultSet.rowsAffected;
            let sqlUpdateM =
                // "UPDATE " + context.settings.DQ + syncSchema.clientDbSchema
                "UPDATE "
                + context.settings.DQ
                + "main"
                + context.settings.DQ
                + "."
                + context.settings.DQ
                + syncTable.name
                + "$m"
                + context.settings.DQ
                + " SET VERSION$$=VERSION$$+1, DML$$=NULL WHERE (DML$$='I' OR DML$$='U') AND TXN$$=?";
            resultSet = await schemaDb.executeSql(sqlUpdateM, [transactionId]);
            count += resultSet.rowsAffected;
        }
    }

    // folders

    if (!syncResponse.successFolderNames) {
        syncResponse.successFolderNames = [];
    }

    for (let folderName of syncResponse.successFolderNames) {

        //context.log("folderName=" + folderName);
        let syncFolder = null;
        for (let folder of clientFolderList) {
            if (folderName.toUpperCase() == folder.name.toUpperCase()) {
                syncFolder = folder;
                break;
            }
        }

        context.log("Doing post check in cleanup for  pervasync folder "
            + syncFolder.name);
        context.log("syncFolder.id=" + syncFolder.id);
        context.log("transactionId=" + transactionId);

        // loop backwards for search and delete
        let i = syncFolder.fileList.length;
        while (i--) {
            let syncFile = syncFolder.fileList[i];
            if (syncFile.fileCt == "D") {
                delete syncFolder.fileMap[syncFile.fileName];
                syncFolder.fileList.splice(i, 1);
            } else if (syncFile.fileCt == "I" || syncFile.fileCt == "U") {
                syncFile.fileCt = "S";
            }
        }

        let sqlDelete = "DELETE FROM pvc$sync_files "
            + " WHERE SYNC_FOLDER_ID=? AND FILE_CT='D'"; // AND TXN$$=?
        resultSet = await pvcAdminDb.executeSql(sqlDelete, [syncFolder.id]);
        count += resultSet.rowsAffected;
        let sqlUpdate = "UPDATE pvc$sync_files SET "
            + "FILE_CT='S'"
            + " WHERE SYNC_FOLDER_ID=? AND (FILE_CT='I' OR FILE_CT='U')";// AND TXN$$=?";
        resultSet = await pvcAdminDb.executeSql(sqlUpdate, [syncFolder.id]);
        count += resultSet.rowsAffected;
    }

    if (count > 0) {
        transactionId++;
        context.log("save transactionId to DB: " + transactionId);
        await setSyncClientProperty("pervasync.transaction.id", "" + transactionId);
    }

    let newSyncDeviceName = syncResponse.device;
    let newSyncClientId = syncResponse.clientId;
    let newSyncServerId = syncResponse.serverId;

    context.log("newSyncClientId=" + newSyncClientId);
    context.log("newSyncDeviceName=" + newSyncDeviceName);
    context.log("newSyncServerId=" + newSyncServerId);

    if (newSyncClientId != syncClientId) {
        context.log("save newSyncClientId to DB: " + newSyncClientId);
        await setSyncClientProperty("pervasync.client.id", "" + newSyncClientId);
        context.log("SYNC_CLIENT_ID has changed. Old syncClientId = " +
            syncClientId + ", newSyncClientId = " +
            newSyncClientId);
        syncClientId = newSyncClientId;
    }
    if (newSyncDeviceName != context.settings.syncDeviceName) {
        await setSyncClientProperty("pervasync.device.name", "" + newSyncDeviceName);
        context.log("syncDeviceName has changed. old syncDeviceName = " +
            context.settings.syncDeviceName + ", newSyncDeviceName = " +
            newSyncDeviceName);
        context.settings.syncDeviceName = newSyncDeviceName;
    }
    if (newSyncServerId != syncServerId) {
        context.log("save newSyncServerId to DB: " + newSyncServerId);
        await setSyncClientProperty("pervasync.server.id", "" + newSyncServerId);
        context.log("syncServerId has changed. old syncServerId = " +
            syncServerId + ", newSyncServerId = " +
            newSyncServerId);
        syncServerId = newSyncServerId;
    }
    context.log("End receiving server response (SYNC_RESPONSE)");
}

async function receiveSyncSummary(cmd) {
    let serverSyncSummary = cmd.value;

    //context.log("serverSyncSummary:\r\n" + JSON.stringify(serverSyncSummary, null, 4));
    //context.log("merging server syncSummary with client syncSummary");

    if (syncSummary.checkInStatus != "FAILURE") {
        syncSummary.checkInStatus = serverSyncSummary.checkInStatus;
    }
    if (serverSyncSummary.refreshStatus == "FAILURE") {
        syncSummary.refreshStatus = "FAILURE";
    }

    syncSummary.checkInSchemaNames = serverSyncSummary.checkInSchemaNames;
    syncSummary.checkInFolderNames = serverSyncSummary.checkInFolderNames;
    if (serverSyncSummary.checkInDIU_done) {
        syncSummary.checkInDIU_done = serverSyncSummary.checkInDIU_done;
    }
    if (serverSyncSummary.syncErrorMessages && serverSyncSummary.errorCode > 0
        && serverSyncSummary.errorCode != 2059) { // PVS_CHECK_IN_SKIPPED = 2059
        syncSummary.errorCode = serverSyncSummary.errorCode;
        let serverException = "PVC_SYNC_SERVER_REPORTED_ERROR:" + serverSyncSummary.syncErrorMessages;
        syncSummary.syncException = serverException;

        if (!syncSummary.syncErrorMessages) {
            syncSummary.syncErrorMessages =
                serverSyncSummary.syncErrorMessages;
        } else {
            syncSummary.syncErrorMessages += "\r\n" +
                serverSyncSummary.syncErrorMessages;
        }

        if (serverSyncSummary.syncErrorStacktraces) {
            if (!syncSummary.syncErrorStacktraces) {
                syncSummary.syncErrorStacktraces =
                    serverSyncSummary.syncErrorStacktraces;
            } else {
                syncSummary.syncErrorStacktraces += "\r\n" +
                    serverSyncSummary.syncErrorStacktraces;
            }
        }
    }

    syncSummary.serverSnapshotAge = serverSyncSummary.serverSnapshotAge;
}

// To disable DML logging, name: dml_log_status, value: 'DISABLED'
async function setSchemaProperty(schemaDb, name, value) {
    let sql = "INSERT OR REPLACE INTO pvc$schema_properties(NAME,VALUE) VALUES(?,?)";
    await schemaDb.executeSql(sql, [name, value]);
}

/**
 * create pvc$schema_properties
 */
async function createClientDbSchema(schemaDb) {

    let sqlCreate = "CREATE TABLE IF NOT EXISTS pvc$schema_properties(\r\n" +
        "    NAME " + context.settings.VARCHAR + "(128) PRIMARY KEY,\r\n" +
        "    VALUE " + context.settings.VARCHAR + "(255)\r\n" +
        ") ";
    context.log("sqlCreate=" + sqlCreate);
    await schemaDb.executeSql(sqlCreate, []);
    await setSchemaProperty(schemaDb, "dml_log_status", "ENABLED");
}

/**
 * Create  pervasync table, including table mate and triggers
 */
async function createSyncTable(schemaDb, syncTable, serverSchema, dropOnly) {
    let i;

    let clientDbSchema = serverSchema.clientDbSchema;

    // db conn is made with serverSchema.clientDbSchema
    clientDbSchema = "main";

    let dqSchema = context.settings.DQ + clientDbSchema + context.settings.DQ;
    let dqTableName = context.settings.DQ + syncTable.name + context.settings.DQ;
    let dqTableMateName =
        context.settings.DQ + syncTable.name + "$m" + context.settings.DQ;
    let dqITriggerName =
        context.settings.DQ + syncTable.name + "$i" + context.settings.DQ;
    let dqUTriggerName =
        context.settings.DQ + syncTable.name + "$u" + context.settings.DQ;
    let dqDTriggerName =
        context.settings.DQ + syncTable.name + "$d" + context.settings.DQ;

    // drop Table and syncTable mate
    let sqlDropTable = "DROP TABLE IF EXISTS " + dqSchema + "." + dqTableName;
    let sqlDropTableMate =
        "DROP TABLE IF EXISTS " + dqSchema + "." + dqTableMateName;

    try {
        // syncSummary.defChanges += "\r\n" + sqlDropTable;
        context.log("sqlDropTable=" + sqlDropTable);
        await schemaDb.executeSql(sqlDropTable, []);
    } catch (e1) {
        context.log("Ignored error: " + e1);
    }
    try {
        // syncSummary.defChanges += "\r\n" + sqlDropTableMate;
        context.log("sqlDropTableMate=" + sqlDropTableMate);
        await schemaDb.executeSql(sqlDropTableMate, []);
    } catch (e1) {
        context.log("Ignored error: " + JSON.stringify(e1));
    }

    if (dropOnly) {
        return;
    }

    // create Table and syncTable mate
    let sqlCreateTable =
        "CREATE TABLE IF NOT EXISTS " + dqSchema + "." + dqTableName + "(\r\n";
    let sqlCreateTableMate =
        "CREATE TABLE IF NOT EXISTS " + dqSchema + "." + dqTableMateName + "(\r\n";
    let isFirstCol = true;
    if (context.settings.add_idPkColumn) {
        sqlCreateTable += "_id INTEGER PRIMARY KEY";
        isFirstCol = false;
    }
    let pkMap = {};
    for (i in syncTable.columns) {
        let column = syncTable.columns[i];
        let columnName = db.quote(column.columnName);

        if (column.pkSeq >= 0) {
            pkMap[column.pkSeq] = columnName;
            sqlCreateTableMate +=
                columnName + " " + column.deviceColDef + ",\r\n";
        }

        if (context.settings.add_idPkColumn
            && "_id" == columnName.toLowerCase()) {
            continue;
        }

        sqlCreateTable += (isFirstCol ? "" : ",\n") +
            columnName + " " + column.deviceColDef;
    }
    console.log("sqlCreateTable1=" + sqlCreateTable);
    let pk = "";
    let newPk = "";
    let pkEqualsNewPk = "";
    let pkEqualsOldPk = "";
    for (i = 0; ; i++) {
        let name = pkMap[i];
        if (i > 0 && !name) {
            break;
        }
        if (name) {
            if (pk.length > 0) {
                pk += ",";
                newPk += ",";
                pkEqualsNewPk += " AND ";
                pkEqualsOldPk += " AND ";
            }
            pk += name;
            newPk += " " + context.settings.NEW + "." + name;
            pkEqualsNewPk += name + "= " + context.settings.NEW + "." + name;
            pkEqualsOldPk += name + "= " + context.settings.OLD + "." + name;
        }
    }

    let pkOrUnique = "PRIMARY KEY";
    if (context.settings.add_idPkColumn) {
        pkOrUnique = "UNIQUE";
    }

    sqlCreateTable += (syncTable.hasPk
        && !(context.settings.add_idPkColumn && "_id"
            == pk) ? ",\n" + pkOrUnique + "("
            + pk + ")\n" : "\n")
        + ") " + context.settings.syncClientDbTableOptions;
    console.log("sqlCreateTable2=" + sqlCreateTable);
    sqlCreateTableMate += "VERSION$$ " + context.settings.NUMERIC + "(20) DEFAULT -1,\r\n" +
        "DML$$ CHAR(1), TXN$$ " + context.settings.NUMERIC + "(20)" +
        (syncTable.hasPk ? ",\n" + "PRIMARY KEY" + "(" + pk
            + ")\n" : "") +
        ") " + context.settings.syncClientDbTableOptions;

    let dqSchemaDot = "";
    let sqlTableMateI1 =
        "CREATE INDEX IF NOT EXISTS " + dqSchemaDot + context.settings.DQ + syncTable.name + "$mi1" + context.settings.DQ +
        " ON " + dqTableMateName + "(DML$$) " + context.settings.NOLOGGING;

    let sqlTableMateI2 =
        "CREATE INDEX IF NOT EXISTS " + dqSchemaDot + context.settings.DQ + syncTable.name + "$mi2" + context.settings.DQ +
        " ON " + dqTableMateName + "(TXN$$) " + context.settings.NOLOGGING;

    // syncSummary.defChanges += "\r\n" + sqlCreateTable;
    context.log("sqlCreateTable=" + sqlCreateTable);
    await schemaDb.executeSql(sqlCreateTable, []);

    // syncSummary.defChanges += "\r\n" + sqlCreateTableMate;
    context.log("sqlCreateTableMate=" + sqlCreateTableMate);
    await schemaDb.executeSql(sqlCreateTableMate, []);

    // syncSummary.defChanges += "\r\n" + sqlTableMateI1;
    context.log("sqlTableMateI1=" + sqlTableMateI1);
    await schemaDb.executeSql(sqlTableMateI1, []);

    // syncSummary.defChanges += "\r\n" + sqlTableMateI2;
    context.log("sqlTableMateI2=" + sqlTableMateI2);
    await schemaDb.executeSql(sqlTableMateI2, []);

    // Drop triggers
    let sqlList = [];
    sqlList.push("DROP TRIGGER IF EXISTS " + dqSchema + "." + dqITriggerName);
    sqlList.push("DROP TRIGGER IF EXISTS " + dqSchema + "." + dqDTriggerName);
    sqlList.push("DROP TRIGGER IF EXISTS " + dqSchema + "." + dqUTriggerName);
    for (i in sqlList) {
        let sql = sqlList[i];
        try {
            context.log("sql=" + sql);
            await schemaDb.executeSql(sql, []);
        } catch (e1) {
            context.log("Ignored error: " + e1);
        }
    }

    // create triggers
    let logEnabled = "'ENABLED' IN (SELECT VALUE FROM pvc$schema_properties WHERE NAME='dml_log_status')";

    // insert (VERSION$$ default to -1)
    let sqlCreateInsertTrigger =
        "CREATE TRIGGER " + dqSchema + "." + dqITriggerName +
        " BEFORE INSERT ON " + dqSchema + "." + dqTableName +
        " FOR EACH ROW" + " WHEN (" + logEnabled + ") BEGIN " +
        " INSERT OR IGNORE INTO " + dqTableMateName + "(" +
        pk + ",DML$$) VALUES(" + newPk + ",'I');" +
        " UPDATE " + dqTableMateName + " SET DML$$='U',TXN$$=NULL" + "  WHERE " +
        pkEqualsNewPk + " AND VERSION$$>-1;" +
        " END";
    context.log("sqlCreateInsertTrigger=" + sqlCreateInsertTrigger);
    await schemaDb.executeSql(sqlCreateInsertTrigger, []);

    // delete
    let sqlCreateDeleteTrigger =
        "CREATE TRIGGER " + dqSchema + "." + dqDTriggerName +
        " BEFORE DELETE ON " + dqSchema + "." + dqTableName +
        " FOR EACH ROW" + " WHEN (" + logEnabled + ") BEGIN " +
        " DELETE FROM " + dqTableMateName +
        "  WHERE " + pkEqualsOldPk + " AND VERSION$$=-1;" +
        " UPDATE " + dqTableMateName + " SET DML$$='D',TXN$$=NULL" + "  WHERE " +
        pkEqualsOldPk + ";" + " END;";
    context.log("sqlCreateDeleteTrigger=" + sqlCreateDeleteTrigger);
    await schemaDb.executeSql(sqlCreateDeleteTrigger, []);

    // update
    let sqlCreateUpdateTrigger =
        "CREATE TRIGGER " + dqSchema + "." + dqUTriggerName +
        " BEFORE UPDATE ON " + dqSchema + "." + dqTableName +
        " FOR EACH ROW" + " WHEN (" + logEnabled + ") BEGIN " +

        // update -- delete part
        " DELETE FROM " + dqTableMateName +
        "  WHERE " + pkEqualsOldPk + " AND VERSION$$=-1;" +
        " UPDATE " + dqTableMateName + " SET DML$$='D',TXN$$=NULL" + "  WHERE " +
        pkEqualsOldPk + ";" +


        // update -- insert or update new PK part
        " INSERT OR IGNORE INTO " +
        dqTableMateName + "(" + pk +
        ", DML$$) VALUES(" + newPk + ",'I');" +
        " UPDATE " + dqTableMateName + " SET DML$$='U',TXN$$=NULL" + "  WHERE " +
        pkEqualsNewPk + " AND VERSION$$>-1;" +
        " END;";
    context.log("sqlCreateUpdateTrigger=" + sqlCreateUpdateTrigger);
    await schemaDb.executeSql(sqlCreateUpdateTrigger, []);
}

async function receiveRefreshSchemaDef(cmd) {
    context.log("Entering receiveRefreshSchemaDef()");
    let serverSchemas = cmd.value;
    if (!serverSchemas) {
        context.log("serverSchemas == null in receiveRefreshSchemaDef");
        return;
    }
    syncSummary.hasDefChanges = true;

    let sql = null;
    //try {
    if (syncSummary.syncDirection == "CHECK_IN_ONLY") {
        throw ("PVC_REFRESH_NOT_ALLOWED");
    }
    // for each  sync schema, drop deleted tables; do this before alter and create
    for (let i in serverSchemas) {
        let serverSchema = serverSchemas[i];
        context.log("receiveRefreshSchemaDef drop deleted tables, serverSchema.name=" + serverSchema.name);
        let schemaDb = await db.getDbConn(serverSchema.clientDbSchema);

        serverSchema.tableMap = {};
        if (!serverSchema.tableList) {
            serverSchema.tableList = [];
        }
        for (let j in serverSchema.tableList) {
            let syncTable = serverSchema.tableList[j];
            serverSchema.tableMap[syncTable.id] = syncTable;
        }

        let clientSchema = clientSchemaMap[serverSchema.id];
        let clientSchemaSub = clientSchemaSubMap[serverSchema.id];

        context.log("calling createClientDbSchema");
        await createClientDbSchema(schemaDb);

        // use main for clientDbSchema
        //let clientDbSchema = serverSchema.clientDbSchema;
        let clientDbSchema = "main";

        // delete clientDbSchema objects
        // for each table
        if ("D" == serverSchema.defCt) {
            serverSchema.tableMap = clientSchema.tableMap;
        }
        for (let k in serverSchema.tableMap) {
            let syncTable = serverSchema.tableMap[k];
            context.log("receiveRefreshSchemaDef syncTable.name=" + syncTable.name);
            if ("D" != serverSchema.defCt &&
                "D" != syncTable.defCt) {
                continue;
            }
            let tableSub;
            if (clientSchemaSub) {
                tableSub =
                    clientSchemaSub.tableSubMap[syncTable.id];
            }
            if (!tableSub) {
                continue;
            } else {
                // dropTable
                let tableMate = syncTable.name + "$m";
                let sqlDropTable =
                    "DROP TABLE IF EXISTS " + context.settings.DQ + clientDbSchema +
                    context.settings.DQ + "." + context.settings.DQ + syncTable.name +
                    context.settings.DQ;
                let sqlDropTableMate =
                    "DROP TABLE IF EXISTS " + context.settings.DQ + clientDbSchema +
                    context.settings.DQ + "." + context.settings.DQ + tableMate +
                    context.settings.DQ;

                try {
                    context.log("sqlDropTable=" + sqlDropTable);
                    await schemaDb.executeSql(sqlDropTable, []);
                } catch (e1) {
                    context.log("Ignored error: " + e1);
                }
                try {
                    context.log("sqlDropTableMate=" + sqlDropTableMate);
                    await schemaDb.executeSql(sqlDropTableMate, []);
                } catch (e1) {
                    context.log("Ignored error: " + e1);
                }
            }
        }
    }

    // for each  pervasync schema, create or alter tables, create sequences, exec sqls
    for (let i in serverSchemas) {
        let serverSchema = serverSchemas[i];
        let serverDbType = serverSchema.serverDbType;
        context.log("receiveRefreshSchemaDef create or alter tables, serverSchema.name=" + serverSchema.name);

        let schemaDb = await db.getDbConn(serverSchema.clientDbSchema);

        if ("D" == serverSchema.defCt) {
            continue;
        }

        // create or alter clientDbSchema objects
        // for each table
        let tableList = serverSchema.tableList;
        tableList = tableList.reverse();

        // Drop tables; from child to parent
        for (let j in tableList) {
            let syncTable = tableList[j];
            context.log("receiveRefreshSchemaDef createSyncTable droponly syncTable.name=" + syncTable.name);
            // drop SyncTable
            await createSyncTable(schemaDb, syncTable, serverSchema, true);
        }

        // Create or alter tables; from parent to child
        tableList = tableList.reverse();
        for (let k in tableList) {
            let syncTable = tableList[k];

            // update column.deviceColDef
            if (syncTable.columns != null) {
                for (let column_i in syncTable.columns) {
                    let column = syncTable.columns[column_i];
                    let deviceColDef = null;
                    deviceColDef =
                        db.getDeviceColDef(serverDbType, column);
                    column.deviceColDef = deviceColDef;
                }
            }

            if ("D" == syncTable.defCt) {
                continue;
            } else {
                // createSyncTable
                context.log("receiveRefreshSchemaDef createSyncTable syncTable.name=" + syncTable.name);
                await createSyncTable(schemaDb, syncTable, serverSchema, false);
            }
        }

        // exec sqls
        if (!serverSchema.sqlList) {
            serverSchema.sqlList = [];
        }
        for (let l in serverSchema.sqlList) {
            let syncSql = serverSchema.sqlList[l];
            if ("D" == syncSql.defCt ||
                "SQLITE" != syncSql.clientDbType &&
                "ALL" != syncSql.clientDbType ||
                !syncSql.sqlText) {
                continue;
            }
            try {
                sql = syncSql.sqlText;
                context.log(sql);
                // syncSummary.defChanges += "\r\n" + sql;
                await schemaDb.executeSql(sql, []);
            } catch (sqle) {
                let errMsg =
                    "Got exception, " + sqle + " when executing SQL(" +
                    sql + ")";
                if (syncSql.ignoreExecError) {
                    context.log("Ignoring: " + errMsg);
                } else {
                    context.log(errMsg);
                    throw sqle;
                }
            }
        }
    }

    // Update syncClientAdminUser metadata tables, DMLs

    for (let i in serverSchemas) {
        let serverSchema = serverSchemas[i];
        context.log("receiveRefreshSchemaDef update metadata tables, serverSchema.name=" + serverSchema.name);

        let clientSchemaSub =
            clientSchemaSubMap[serverSchema.id];
        context.log("serverSchema=" + serverSchema.name);

        // for each table, insert or delete metadata
        let sqlDeleteTable =
            "DELETE FROM pvc$sync_tables" + " WHERE SYNC_SCHEMA_ID=? AND NAME=?";
        let sqlInsertTable =
            "INSERT INTO pvc$sync_tables(" + " ID,SYNC_SCHEMA_ID," +
            " NAME,RANK,DEF_CN,DEF_CT," +
            "ALLOW_CHECK_IN,ALLOW_REFRESH,HAS_PK,CHECK_IN_SUPER_USERS," +
            "IS_NEW,SUBSETTING_MODE,SUBSETTING_QUERY,ADDED" +
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?," + context.settings.SYSDATE + ")";
        let sqlDeleteTableCols =
            "DELETE FROM pvc$sync_table_columns" + " WHERE SYNC_TABLE_ID=?";
        let sqlInsertColumn =
            "INSERT INTO pvc$sync_table_columns(" +
            " SYNC_TABLE_ID," +
            " NAME,DEVICE_COL_DEF,JDBC_TYPE,NATIVE_TYPE,COLUMN_SIZE,SCALE," +
            " NULLABLE,PK_SEQ,ORDINAL_POSITION,DEFAULT_VALUE" +
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?)";

        for (let j in serverSchema.tableMap) {
            let syncTable = serverSchema.tableMap[j];

            context.log("syncTable.id=" + syncTable.id);
            context.log("serverSchema.id=" + serverSchema.id);
            context.log("syncTable.name=" + syncTable.name);

            // delete syncTableCols
            context.log(sqlDeleteTableCols);
            await pvcAdminDb.executeSql(sqlDeleteTableCols, [syncTable.id]);
            // delete syncTable
            context.log(sqlDeleteTable);
            await pvcAdminDb.executeSql(sqlDeleteTable, [serverSchema.id, syncTable.name]);

            if ("D" == serverSchema.defCt ||
                "D" == syncTable.defCt) {
                continue;
            }

            // insert syncTable
            context.log(sqlInsertTable);
            await pvcAdminDb.executeSql(sqlInsertTable, [syncTable.id, serverSchema.id, syncTable.name,
            syncTable.rank, syncTable.defCn, syncTable.defCt, syncTable.allowCheckIn ? "Y" : "N",
            syncTable.allowRefresh ? "Y" : "N", syncTable.hasPk ? "Y" : "N",
            syncTable.checkInSuperUsers ? syncTable.checkInSuperUsers.join(",") : "",
            "I" == syncTable.defCt ? "Y" : "N", syncTable.subsettingMode, syncTable.subsettingQuery]);

            // insert syncTable columns
            for (let k in syncTable.columns) {
                let column = syncTable.columns[k];
                context.log(sqlInsertColumn);
                await pvcAdminDb.executeSql(sqlInsertColumn, [syncTable.id, db.quote(column.columnName), column.deviceColDef,
                column.dataType, column.typeName, column.columnSize, column.decimalDigits,
                column.nullable ? "Y" : "N", column.pkSeq, column.ordinalPosition, column.defaultValue]);
            }
        }

        // update  pervasync schema metadata
        let sqlDeleteSyncSchema =
            "DELETE FROM pvc$sync_schemas" + " WHERE  SYNC_SCHEMA_ID=?";
        let sqlUpdateSyncSchema =
            "UPDATE pvc$sync_schemas" + " SET SYNC_SCHEMA_NAME=?," +
            " SERVER_DB_SCHEMA=?,CLIENT_DB_SCHEMA=?,DEF_CN=?," +
            " SUB_CN=?,SYNC_CLIENT_ID=?,SERVER_DB_TYPE=?,ADDED=" + context.settings.SYSDATE + "" +
            " WHERE SYNC_SCHEMA_ID=?";
        let sqlInsertSyncSchema =
            "INSERT INTO pvc$sync_schemas(" + " SYNC_SCHEMA_ID,SYNC_SCHEMA_NAME," +
            " SERVER_DB_SCHEMA,CLIENT_DB_SCHEMA,DEF_CN,SUB_CN,DATA_CN,SYNC_CLIENT_ID,SERVER_DB_TYPE,ADDED" +
            ") VALUES(?,?,?,?,?,?,-1,?,?," + context.settings.SYSDATE + ")";

        if ("D" == serverSchema.defCt) {
            context.log("sqlDeleteSyncSchema=" + sqlDeleteSyncSchema);
            await pvcAdminDb.executeSql(sqlDeleteSyncSchema, [serverSchema.id]);
        } else if (clientSchemaSub != null) { // update
            context.log("sqlUpdateSyncSchema=" + sqlUpdateSyncSchema);
            await pvcAdminDb.executeSql(sqlUpdateSyncSchema, [serverSchema.name, serverSchema.serverDbSchema,
            serverSchema.clientDbSchema, serverSchema.defCn, serverSchema.subCn,
                syncClientId, serverSchema.serverDbType, serverSchema.id]);
        } else { // insert
            context.log("sqlInsertSyncSchema=" + sqlInsertSyncSchema);
            await pvcAdminDb.executeSql(sqlInsertSyncSchema, [serverSchema.id, serverSchema.name, serverSchema.serverDbSchema,
            serverSchema.clientDbSchema, serverSchema.defCn, serverSchema.subCn, syncClientId, serverSchema.serverDbType]);
        }
    }

    //} catch (e) {
    //    context.log("Got exception in receiveRefreshSchemaDef,  " + JSON.stringify(e));
    //}
    context.log("Leaving receiveRefreshSchemaDef()");
}

async function receiveRefreshFolderDef(cmd) {
    context.log("begin receiveRefreshFolderDef()");
    let syncFolders = cmd.value;
    if (!syncFolders) {
        context.log("syncFolders == null in receiveRefreshFolderDef");
        return;
    }
    syncSummary.hasDefChanges = true;

    // try {
    if (syncSummary.syncDirection == "CHECK_IN_ONLY") {
        throw new Error("PVC_REFRESH_NOT_ALLOWED");
    }

    // Update DB

    for (let syncFolder of syncFolders) {

        context.log("Update metadata, syncFolder.name=" + syncFolder.name + ", syncFolder.id=" + syncFolder.id);

        // Will remove folder that has path or filter changes.
        let clientFolder = clientFolderMap[syncFolder.id];
        let clientFolderSub = clientFolderSubMap[syncFolder.id];
        if ("D" != syncFolder.defCt
            && clientFolderSub
            && clientFolder
            && (syncFolder.serverFolderPath != clientFolder.serverFolderPath
                || syncFolder.clientFolderPath != clientFolder.clientFolderPath
                || syncFolder.filePathStartsWith != clientFolder.filePathStartsWith
                || syncFolder.fileNameEndsWith != clientFolder.fileNameEndsWith)) {
            syncFolder.defCt = "D";
            context.log("Will remove folder that has def changes. Folder name: "
                + clientFolder.name);
        }

        // update pervasync folder metadata
        let sqlDeleteSyncFolder = "DELETE FROM pvc$sync_folders"
            + " WHERE ID=?";
        let sqlDeleteFolderFiles = "DELETE FROM pvc$sync_files "
            + " WHERE SYNC_FOLDER_ID=?";
        let sqlInsertSynFolder = "INSERT OR REPLACE INTO pvc$sync_folders("
            + "ID,SYNC_FOLDER_NAME,SERVER_FOLDER_PATH,CLIENT_FOLDER_PATH,"
            + "RECURSIVE,FILE_PATH_STARTS_WITH,FILE_NAME_ENDS_WITH,"
            + "ALLOW_CHECK_IN,ALLOW_REFRESH,CHECK_IN_SUPER_USERS,DEF_CN,"
            + "SUB_CN,FILE_CN,SYNC_CLIENT_ID,"
            + "NO_INIT_SYNC_NETWORKS,NO_SYNC_NETWORKS," + "ADDED"
            + ") VALUES(?,?,?,?," + "?,?,?,?,?,?,?,?,?,?,?,?,"
            + context.settings.SYSDATE + ")";

        // update  pervasync Folder metadata

        if ("D" == syncFolder.defCt) {
            // delete from DB
            context.log("delete from DB, syncFolder=" + syncFolder.name);
            await pvcAdminDb.executeSql(sqlDeleteFolderFiles, [syncFolder.id]);
            await pvcAdminDb.executeSql(sqlDeleteSyncFolder, [syncFolder.id]);

            // delete from collections
            delete clientFolderMap[syncFolder.id];
            delete clientFolderSubMap[syncFolder.id];
            let j = clientFolderList.length;
            while (j--) {
                if (clientFolderList[j].id == syncFolder.id) {
                    clientFolderList.splice(j, 1);
                }
            }
            j = clientFolderSubList.length;
            while (j--) {
                if (clientFolderSubList[j].syncFolderId == syncFolder.id) {
                    clientFolderSubList.splice(j, 1);
                }
            }

        } else {
            // insert/update
            context.log("insert/update DB, syncFolder=" + syncFolder.name);
            await pvcAdminDb.executeSql(sqlInsertSynFolder,
                [syncFolder.id, syncFolder.name, syncFolder.serverFolderPath, syncFolder.clientFolderPath,
                (syncFolder.recursive ? "Y" : "N"), syncFolder.filePathStartsWith, syncFolder.fileNameEndsWith,
                (syncFolder.allowCheckIn ? "Y" : "N"), (syncFolder.allowRefresh ? "Y" : "N"), syncFolder.checkInSuperUsers.join(","),
                syncFolder.defCn, syncFolder.subCn, (clientFolderSub ? clientFolderSub.fileCn : -1), syncClientId,
                syncFolder.noInitSyncNetworks, syncFolder.noSyncNetworks
                ]
            );
        }
    }

    context.log("end receiveRefreshFolderDef()");
}

/**
 * Refresh data
 */
async function receiveRefreshData(cmd) {
    context.log("Begin receiveRefreshData");
    if (syncSummary.syncDirection == "CHECK_IN_ONLY") {
        throw Error("PVC_REFRESH_NOT_ALLOWED");
    }

    // receive END_REFRESH_DATA SYNC_SUMMARY SCHEMA
    for (; ;) {
        cmd = await transport.readCommand();
        if ("END_REFRESH_DATA" == cmd.name) {
            context.log("Receiving (END_REFRESH_DATA)");
            break;
        } else if ("SYNC_SUMMARY" != cmd.name &&
            "SCHEMA" != cmd.name) {
            throw Error("Expecting SYNC_SUMMARY, SCHEMA, or END_REFRESH_DATA, got " + cmd.name);
        }

        if ("SYNC_SUMMARY" == cmd.name) {
            context.log("Receiving server SYNC_SUMMARY");
            await receiveSyncSummary(cmd);
            return;
        }

        //("SCHEMA" == cmd))
        let serverSchemaSub = cmd.value;
        let clientSchemaSub =
            clientSchemaSubMap[serverSchemaSub.syncSchemaId];
        let clientSchema = clientSchemaMap[serverSchemaSub.syncSchemaId];

        if (!clientSchema) {
            throw Error("Missing schema def info " +
                "when refreshing schema data. syncSchemaId=" +
                serverSchemaSub.syncSchemaId);
        }

        let schemaDb = await db.getDbConn(clientSchema.clientDbSchema);

        context.log("disable Dml Log");
        await setSchemaProperty(schemaDb, "dml_log_status", "DISABLED");

        // receiveSchema
        let receiveSuccess = await receiveSchema(schemaDb, clientSchema);

        context.log("re-enable Dml Log");
        await setSchemaProperty(schemaDb, "dml_log_status", "ENABLED");
        if (!receiveSuccess) {
            throw ("Failed to receive schema " + clientSchema.name);
        }

        // Update syncClientAdminUser metadata
        context.log("Updating DATA_CN for schema " +
            serverSchemaSub.syncSchemaId + " to " +
            serverSchemaSub.dataCn);
        let sqlUpdateSyncSchema =
            "UPDATE pvc$sync_schemas" + " SET DATA_CN=?" +
            " WHERE SYNC_SCHEMA_ID=?";
        await pvcAdminDb.executeSql(sqlUpdateSyncSchema, [serverSchemaSub.dataCn, serverSchemaSub.syncSchemaId]);

        // update IS_NEW

        if (clientSchemaSub.newTables &&
            clientSchemaSub.newTables.length > 0) {
            let sqlUpdateTable =
                "UPDATE pvc$sync_tables" + " SET IS_NEW='N'" +
                " WHERE ID=?";
            for (let i in clientSchemaSub.newTables) {
                let tableId = clientSchemaSub.newTables[i];
                await pvcAdminDb.executeSql(sqlUpdateTable, [tableId]);
            }
            clientSchemaSub.newTables = [];
        }

        syncSummary.refreshSchemaNames.push(clientSchema.name);
    }

    context.log("End receiveRefreshData");
}

/**
 * receiveSchema, called by receiveRefreshData
 */
async function receiveSchema(schemaDb, clientSchema) {

    // receive END_SCHEMA ERROR dmls(INSERT, DELETE)
    for (; ;) {
        let cmd = await transport.readCommand();
        if ("END_SCHEMA" == cmd.name) {
            context.log("Receiving (END_SCHEMA)");
            break;
        } else if ("SYNC_SUMMARY" != cmd.name &&
            "DELETE" != cmd.name &&
            "INSERT" != cmd.name) {
            throw Error("Expecting END_SCHEMA, SYNC_SUMMARY, DELETE, INSERT, got " + cmd.name);
        }

        if ("SYNC_SUMMARY" == cmd.name) {
            context.log("Receiving server SYNC_SUMMARY");
            await receiveSyncSummary(cmd);
            return false;
        }

        let dmlType = cmd.name;
        let tableId = cmd.value;
        let syncTable = clientSchema.tableMap[tableId];
        //TableSub tableSub = (TableSub) clientSchemaSub.tableSubMap.get(tableId);
        context.log("syncTable.name = " + syncTable.name);
        let receiveSuccess = await receiveDml(schemaDb, clientSchema, dmlType, syncTable);
        if (!receiveSuccess) {
            return false;
        }
    }
    return true;
}

/**
 * receiveDml, called by receiveSchema
 */
async function receiveDml(schemaDb, clientSchema, dmlType, syncTable) {
    // update Table and table mate
    let i;
    let count = 0;

    // receive END_DML ERROR ROW
    for (; ;) {
        count += 1;
        //context.log("Reading cmd");
        let cmd = await transport.readCommand();
        //context.log("Processing cmd.name " + cmd.name);
        if (("END_" + dmlType) == cmd.name) {
            //context.log("Receiving END_" + dmlType);
            break;
        } else if ("SYNC_SUMMARY" != cmd.name &&
            "ROW" != cmd.name) {
            //context.log("Expecting END_" + dmlType +
            //    ", SYNC_SUMMARY or ROW, got " + cmd.name);
            throw ("Expecting END_" + dmlType +
                ", SYNC_SUMMARY or ROW, got " + cmd.name);
        }

        if ("SYNC_SUMMARY" == cmd.name) {
            //context.log("Receiving server SYNC_SUMMARY");
            await receiveSyncSummary();
            return false;
        }
        //else if ("ROW" == cmd) {
        let colValList = cmd.value;
        if ("DELETE" == dmlType) {

            syncSummary.refreshDIU_requested[0] += 1;
            // no version for server sent delete
            //context.log("syncTable.sqlDelete=" + syncTable.sqlDelete);
            await schemaDb.executeSql(syncTable.sqlDelete, colValList);
            await schemaDb.executeSql(syncTable.sqlDeleteM, colValList);
            syncSummary.refreshDIU_done[0] += 1;

        } else if ("INSERT" == dmlType) {

            syncSummary.refreshDIU_requested[1] += 1;

            let versionArr = colValList.slice(0, 1);
            let pkArr = colValList.slice(1, syncTable.pkList.length + 1);
            let colArr = colValList.slice(1, syncTable.columnsPkRegLob.length - syncTable.lobColCount + 1);

            //context.log("syncTable.columnsPkRegLob.length: " + syncTable.columnsPkRegLob.length);
            //context.log("syncTable.lobColCount: " + syncTable.lobColCount);
            //context.log("colValList: " + colValList);
            //context.log("versionArr: " + versionArr);
            //context.log("colArr.concat(pkArr): " + colArr.concat(pkArr));
            //schemaConn.execute(syncTable.sqlUpdate, colArr.concat(pkArr));
            //context.log("syncTable.sqlInsert=" + syncTable.sqlInsert);
            await schemaDb.executeSql(syncTable.sqlInsert, colArr);
            syncSummary.refreshDIU_done[2] += 1;

            await schemaDb.executeSql(syncTable.sqlInsertM, versionArr.concat(pkArr));

            // lob payloads
            if (syncTable.lobColCount > 0) { // insert/update and there are lob cols, MySql
                let lobStrArr = [];
                for (i = 0; i < syncTable.lobColCount; i++) {
                    //let column =
                    //syncTable.columnsPkRegLob[i + syncTable.columnsPkRegLob.length - syncTable.lobColCount];
                    //context.log("Rceiving LOB col " + column.columnName);
                    // receiveLob
                    let lobStr = await receiveLob();
                    lobStrArr.push(lobStr);
                }
                //context.log("syncTable.sqlUpdateLob=" + syncTable.sqlUpdateLob);
                await schemaDb.executeSql(syncTable.sqlUpdateLob, lobStrArr.concat(pkArr));
            }

        } else {
            //context.log("PVC_WRONG_DML_TYPE: " + dmlType);
            throw ("PVC_WRONG_DML_TYPE: " + dmlType);
        }

        //context.log("end processing cmd.name " + cmd.name);
    }
    context.log("receiveDml count = " + count);

    //context.log("return true");
    return true;
}

async function receiveLob() {
    //context.log("begin receiveLob");
    let lobStr = "";
    let nWriteTotal = 0;
    for (; ;) {
        let cmd = await transport.readCommand();
        let syncLob = cmd.value;
        if (syncLob.isNull || syncLob.totalLength == 0) {
            break;
        }

        lobStr += syncLob.txtPayload;
        nWriteTotal += syncLob.isBinary ? syncLob.txtPayload.length / 2 : syncLob.txtPayload.length;

        if (syncLob.isNull || nWriteTotal >= syncLob.totalLength) {
            if (nWriteTotal != syncLob.totalLength) {
                throw Error("nWriteTotal != syncLob.totalLength, nWriteTotal=" +
                    nWriteTotal +
                    ", syncLob.totalLength=" +
                    syncLob.totalLength);
            }
            /*context.log("nWriteTotal=" +
                nWriteTotal +
                ", syncLob.totalLength=" +
                syncLob.totalLength);*/
            break;
        }

    }
    //context.log("end receiveLob");
    return lobStr;
}
/**
 * Refresh data
 */
async function receiveRefreshFiles(cmd) {
    context.log("Begin receiveRefreshFiles");
    if (syncSummary.syncDirection == "CHECK_IN_ONLY") {
        throw Error("PVC_REFRESH_NOT_ALLOWED");
    }

    // receive END_REFRESH_FILES SYNC_SUMMARY FOLDER
    for (; ;) {
        cmd = await transport.readCommand();
        if ("END_REFRESH_FILES" == cmd.name) {
            context.log("Receiving (END_REFRESH_FILES)");
            break;
        } else if ("SYNC_SUMMARY" != cmd.name &&
            "FOLDER" != cmd.name) {
            throw Error("Expecting SYNC_SUMMARY, FOLDER, or END_REFRESH_FILES, got " + cmd.name);
        }

        if ("SYNC_SUMMARY" == cmd.name) {
            context.log("Receiving server SYNC_SUMMARY");
            await receiveSyncSummary(cmd);
            return;
        }

        //("FOLDER" == cmd))
        let serverFolderSub = cmd.value;
        let clientFolderSub =
            clientFolderSubMap[serverFolderSub.syncFolderId];
        let clientFolder = clientFolderMap[serverFolderSub.syncFolderId];

        if (!clientFolder) {
            throw Error("Missing Folder def info " +
                "when refreshing Folder data. syncFolderId=" +
                serverFolderSub.syncFolderId);
        }

        // receiveFolder
        await receiveFolder(clientFolder);

        // Update syncClientAdminUser metadata

        let sqlUpdateSynFolder = "UPDATE pvc$sync_folders"
            + " SET FILE_CN=?" + " WHERE ID=?";
        await pvcAdminDb.executeSql(sqlUpdateSynFolder, [serverFolderSub.fileCn, serverFolderSub.syncFolderId]);
        clientFolderSub.fileCn = serverFolderSub.fileCn;
        syncSummary.refreshFolderNames.push(clientFolder.name);
    }

    context.log("End receiveRefreshFiles");
}

/**
 * receiveFolder, called by receiveRefreshData
 */
async function receiveFolder(syncFolder) {

    let folderPath = getPath(syncFolder.name);

    // receive END_FOLDER FILE
    for (; ;) { // for each FILE
        let cmd = await transport.readCommand();
        if ("END_FOLDER" == cmd.name) {
            context.log("Receiving (END_FOLDER)");
            break;
        } else if ("SYNC_SUMMARY" != cmd.name &&
            "FILE" != cmd.name) {
            throw Error("Expecting FILE, SYNC_SUMMARY or END_FOLDER, got " + cmd.name);
        }

        if ("SYNC_SUMMARY" == cmd.name) {
            context.log("Receiving server SYNC_SUMMARY");
            await receiveSyncSummary(cmd);
            return false;
        }

        let syncFile = cmd.value;

        let updateMetaData = false;
        let writeFile = false;
        let filePath = folderPath + "/" + syncFile.fileName;
        let isDir = syncFile.isDirectory;
        let exists = await fs.exists(filePath);

        if (isDir) {
            // directory
            if ("D" == syncFile.fileCt && exists) {
                let fileEntryArray = await fs.ls(filePath);
                if (fileEntryArray.length == 0) {
                    context.log("Deleting " + filePath);
                    await fs.rm(filePath);
                }

            } else if (("I" == syncFile.fileCt || "U" == syncFile.fileCt) && !exists) {
                await fs.mkdirs(filePath);
            }
        } else {
            // files

            if ("D" == syncFile.fileCt) {
                syncSummary.refreshDIU_requested[0] += 1;
                context.log("delete of file " + syncFile.fileName
                    + " requested");
            } else if ("I" == syncFile.fileCt) {
                syncSummary.refreshDIU_requested[1] += 1;
                context.log("insert of file " + syncFile.fileName
                    + " requested");
            } else if ("U" == syncFile.fileCt) {
                syncSummary.refreshDIU_requested[2] += 1;
                context.log("update of file " + syncFile.fileName
                    + " requested");
            }

            if ("I" == syncFile.fileCt
                || "U" == syncFile.fileCt) {
                if (!exists) {
                    syncSummary.refreshDIU_done[1] += 1;
                } else {
                    if (isDir) {
                        await fs.rm(filePath);
                    }

                    syncSummary.refreshDIU_done[2] += 1;
                }

                // create file
                let parentPath = fs.parent(filePath);
                context.log("parentPath=" + parentPath);
                let parentExists = await fs.exists(parentPath);
                if (!parentExists) {
                    context.log("calling parent.mkdirs");
                    await fs.mkdirs(parentPath);
                } else {
                    context.log("parent exists");
                }

                context.log("Creating file " + filePath);
                if (!exists) {
                    await fs.createFile(filePath);
                }

                writeFile = true;
                updateMetaData = true;
                if ("SOFTWARE_UPDATE_SYNC_FOLDER" == syncFolder.name) {
                    writeFile = false;
                }
            } else if ("D" == syncFile.fileCt) {
                // delete file
                if (!exists) {
                    syncSummary.refreshDIU_done[0] += 0;
                } else {
                    context.log("Deleting " + filePath);
                    await fs.rm(filePath);
                    syncSummary.refreshDIU_done[0] += 1;
                }
            } else {
                throw new Error("PVC_WRONG_FILE_CHANGE_TYPE:" + syncFile.fileCt);
            }

            // file content

            if ("D" != syncFile.fileCt && !isDir) {
                let lobStr = await receiveLob();
                if (writeFile && lobStr) {
                    let bytes = util.hex2bytes(lobStr);
                    await fs.writeBytes(filePath, bytes);
                    let stat = await fs.stat(filePath);
                    context.log("syncFile.length=" + syncFile.length
                        + ", stat.size="
                        + stat.size);
                    syncFile.lastModified = stat.lastModified;
                    syncFile.length = stat.size;
                }
            } // file content

            if (updateMetaData) {

                let i = syncFolder.fileList.length;
                let found = false;
                while (i--) {
                    if (syncFile.fileName == syncFolder.fileList[i].fileName) {
                        if (syncFile.fileCt == "D") {
                            delete syncFolder.fileMap[syncFile.fileName];
                            syncFolder.fileList.splice(i, 1);
                        } else {
                            syncFile.fileCt = "S";
                            syncFolder.fileList.splice(i, 1, syncFile);
                            syncFolder.fileMap[syncFile.fileName] = syncFile;
                        }
                        found = true;
                        break;
                    }
                }
                if (!found && syncFile.fileCt != "D") {
                    syncFile.fileCt = "S";
                    syncFolder.fileList.push(syncFile);
                    syncFolder.fileMap[syncFile.fileName] = syncFile;
                }

                // update file meta data
                if ("D" == syncFile.fileCt) {
                    // delete
                    let sqlDelete = "DELETE FROM pvc$sync_files "
                        + " WHERE SYNC_FOLDER_ID=? AND FILE_NAME=?";
                    await pvcAdminDb.executeSql(sqlDelete,
                        [syncFolder.id, syncFile.fileName]);

                } else {
                    // update
                    let sqlInsert = "INSERT OR REPLACE INTO pvc$sync_files ( "
                        + "SYNC_FOLDER_ID,FILE_NAME,IS_DIRECTORY,LENGTH,LAST_MODIFIED,"
                        + "FILE_CN,FILE_CT,ADDED) VALUES(?,?,?,?,?,?,'S',"
                        + context.settings.SYSDATE + ")";
                    await pvcAdminDb.executeSql(sqlInsert,
                        [syncFolder.id, syncFile.fileName, (isDir ? "Y" : "N"), syncFile.length, syncFile.lastModified, syncFile.fileCn]);
                }
            }
        }
    } // for each FILE
    return true;
}

export default {
    sync
}