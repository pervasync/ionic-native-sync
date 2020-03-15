import context from './context.js'
import setup from "./setup.js";
import fs from "./fs.js";
import db from "./db.js";
import agent from "./agent.js";

async function config(settings, reset) {

  try {
    context.log("pervasync index.js, settings=" + JSON.stringify(settings));
    context.settings = Object.assign(context.settings, settings);

    settings = context.settings;
    if (!settings.syncServerUrl || !settings.syncUserName || !settings.syncUserPassword) {
      throw new Error("syncServerUrl, syncUserName and syncUserPassword are required in settings.");
    }

    await fs.init();
    if (!settings.filesPath) {
      settings.filesPath = await fs.getFilesPath();
    }
    await db.init();
    await setup.setup(reset);
  } catch (error) {
    context.log("Got error in config: " + JSON.stringify(error));
    throw error;
  }
}

async function sync() {

  if (context.syncState.configured) {
    let syncSummary = await agent.sync();
    // PVS_WRONG_SYNC_SERVER_ID = 2025
    if (syncSummary.errorCode == 2025) {
      context.log("Will reset as syncSummary.errorCode == PVS_WRONG_SYNC_SERVER_ID");
      await setup.setup(true);
      syncSummary = await agent.sync();
    }
    return syncSummary;
  } else {
    throw new Error("Call Pervasync config before sync");
  }
}

async function openSchemaDb(schemaName) {
  if (context.syncState.configured) {
    let schemasSql =
      "SELECT CLIENT_DB_SCHEMA FROM pvc$sync_schemas WHERE SYNC_SCHEMA_NAME=?";
    try {
      let pvcAdminDb = await db.getDbConn(context.settings.adminDbName);
      let schemasRs = await pvcAdminDb.executeSql(schemasSql, [schemaName]);
      if (schemasRs.rows.length > 0) {
        let clientDbSchema = schemasRs.rows.item(0)['CLIENT_DB_SCHEMA'];
        db.sqliteDatabaseConfig.name = clientDbSchema + "__" + context.settings.accountId + ".db";
        let schemaDb = await db.sqlite.create(db.sqliteDatabaseConfig);
        return schemaDb;
      } else {
        throw new Error("Schema not found: " + schemaName);
      }
    } finally {
      await db.closeDbConn(context.settings.adminDbName);
    }
  } else {
    throw new Error("Call Pervasync config before openSchemaDb");
  }
}

async function getPath(folderName) {
  //context.log("getPath, folderName=" + folderName);
  if (context.syncState.configured) {
    let foldersSql =
      "SELECT CLIENT_FOLDER_PATH FROM pvc$sync_folders WHERE SYNC_FOLDER_NAME=?";
    try {
      let pvcAdminDb = await db.getDbConn(context.settings.adminDbName);
      let foldersRs = await pvcAdminDb.executeSql(foldersSql, [folderName]);
      if (foldersRs.rows.length > 0) {
        let clientFolderPath = foldersRs.rows.item(0)['CLIENT_FOLDER_PATH'];
        //context.log("clientFolderPath=" + clientFolderPath);
        let path = context.settings.filesPath + clientFolderPath + "__" + context.settings.accountId;
        return path;
      } else {
        return null;
      }
    } finally {
      await db.closeDbConn(context.settings.adminDbName);
    }
  } else {
    throw new Error("Call Pervasync config before getPath");
  }
}

export default {
  config,
  sync,
  openSchemaDb,
  getPath
}
