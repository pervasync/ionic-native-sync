import context from './context.js'
import fs from './fs.js'
import db from './db.js'

async function setup(reset) {
  context.syncState.configured = false;
  context.log("begin pervasync setup.js, setup(), context.settings.filesPath=" + context.settings.filesPath);

  if (reset) {
    context.log("will reset");

    try {
      await fs.rmrf(context.settings.filesPath);
    } catch (error) {
      context.log("Ignored error in rm filesPath: " + JSON.stringify(error));
    }

    await db.deleteDb(context.settings.adminDbName);

  }

  let exists = await fs.exists(context.settings.filesPath);
  //context.log("settings.filesPath(pervasync) exists=" + exists);
  if (!exists) {
    await fs.mkdir(context.settings.filesPath);
  }

  //context.log("sqlList");
  let sqlList = [];

  //context.log("sync_client_properties");
  sqlList.push("CREATE TABLE IF NOT EXISTS pvc$sync_client_properties(\n" +
    "    NAME " + context.settings.VARCHAR + "(128) PRIMARY KEY,\n" +
    "    VALUE " + context.settings.VARCHAR + "(255)\n" +
    ") " + context.settings.syncClientDbTableOptions);

  //context.log("sync_schemas");
  sqlList.push("CREATE TABLE IF NOT EXISTS pvc$sync_schemas(\n" +
    "    SYNC_SCHEMA_ID " + context.settings.NUMERIC + "(20),\n" +
    "    SYNC_CLIENT_ID " + context.settings.NUMERIC + "(20),\n" +
    "    SYNC_SCHEMA_NAME " + context.settings.VARCHAR + "(128),\n" +
    "    SERVER_DB_TYPE " + context.settings.VARCHAR + "(128),\n" +
    "    SERVER_DB_SCHEMA " + context.settings.VARCHAR + "(128),\n" +
    "    CLIENT_DB_SCHEMA " + context.settings.VARCHAR + "(128),\n" +
    "    DEF_CN " + context.settings.NUMERIC + "(20),\n" +
    "    SUB_CN " + context.settings.NUMERIC + "(20),\n" +
    "    DATA_CN " + context.settings.NUMERIC + "(20),\n" +
    "    NO_INIT_SYNC_NETWORKS " + context.settings.VARCHAR + "(256),\n" +
    "    NO_SYNC_NETWORKS " + context.settings.VARCHAR + "(256),\n" +
    "    ADDED " + context.settings.DATETIME + ",\n" +
    "    PRIMARY KEY(SYNC_SCHEMA_ID)\n" +
    ") " + context.settings.syncClientDbTableOptions);

  //context.log("sync_tables");
  sqlList.push("CREATE TABLE IF NOT EXISTS pvc$sync_tables(\n" +
    "    ID " + context.settings.NUMERIC + "(20) PRIMARY KEY,\n" +
    "    SYNC_SCHEMA_ID " + context.settings.NUMERIC + "(20),\n" +
    //+"    SYNC_TABLE_SUB_ID " + context.settings.NUMERIC + "(20),\n"
    "    NAME " + context.settings.VARCHAR + "(128),\n" +
    "    RANK " + context.settings.NUMERIC + "(20),\n" +
    "    DEF_CN " + context.settings.NUMERIC + "(20),\n" +
    "    DEF_CT " + context.settings.VARCHAR + "(1),\n" +
    "    SUBSETTING_MODE " + context.settings.VARCHAR + "(128),\n" +
    "    SUBSETTING_QUERY " + context.settings.CLOB + ",\n" +
    "    IS_NEW " + context.settings.VARCHAR + "(1),\n" +
    "    ALLOW_CHECK_IN " + context.settings.VARCHAR + "(1),\n" +
    "    ALLOW_REFRESH " + context.settings.VARCHAR + "(1),\n" +
    "    HAS_PK " + context.settings.VARCHAR + "(1),\n" +
    "    CHECK_IN_SUPER_USERS " + context.settings.VARCHAR + "(4000),\n" +
    "    ADDED " + context.settings.DATETIME + ",\n" +
    "    CONSTRAINT pvc$sync_tables_uk UNIQUE (SYNC_SCHEMA_ID,NAME)\n" +
    ") " + context.settings.syncClientDbTableOptions);

  //context.log("sync_table_columns");
  sqlList.push("CREATE TABLE IF NOT EXISTS pvc$sync_table_columns(\n" +
    "    SYNC_TABLE_ID " + context.settings.NUMERIC + "(20),\n" +
    "    NAME " + context.settings.VARCHAR + "(128),\n" +
    "    DEVICE_COL_DEF " + context.settings.VARCHAR + "(128),\n" +
    "    JDBC_TYPE " + context.settings.NUMERIC + "(20),\n" +
    "    NATIVE_TYPE " + context.settings.VARCHAR + "(128),\n" +
    "    COLUMN_SIZE " + context.settings.NUMERIC + "(20),\n" +
    "    SCALE " + context.settings.NUMERIC + "(20),\n" +
    "    NULLABLE " + context.settings.VARCHAR + "(1), \n" +
    "    PK_SEQ " + context.settings.NUMERIC + "(20),\n" +
    "    ORDINAL_POSITION " + context.settings.NUMERIC + "(20),\n" +
    "    DEFAULT_VALUE " + context.settings.VARCHAR + "(1024), \n" +
    "    ADDED " + context.settings.DATETIME + ",\n" +
    "    FOREIGN KEY(SYNC_TABLE_ID) REFERENCES pvc$sync_tables(ID) ON DELETE CASCADE,\n" +
    "    PRIMARY KEY(SYNC_TABLE_ID, NAME)\n" +
    ") " + context.settings.syncClientDbTableOptions);

  //context.log("sequences");
  sqlList.push("CREATE TABLE IF NOT EXISTS pvc$sequences(\n" +
    "    SEQ_SCHEMA " + context.settings.VARCHAR + "(128),\n" +
    "    SEQ_NAME " + context.settings.VARCHAR + "(128),\n" +
    "    START_VALUE " + context.settings.NUMERIC + "(20),\n" +
    "    MAX_VALUE " + context.settings.NUMERIC + "(20),\n" +
    "    CURRENT_VALUE " + context.settings.NUMERIC + "(20),\n" +
    "    CONSTRAINT pvc$sequences_pk PRIMARY KEY (SEQ_SCHEMA,SEQ_NAME)\n" +
    ") " + context.settings.syncClientDbTableOptions);

  //context.log("sync_folders");
  sqlList.push("CREATE TABLE IF NOT EXISTS pvc$sync_folders(\n" +
    "    ID " + context.settings.NUMERIC + "(20) PRIMARY KEY,\n" +
    "    SYNC_CLIENT_ID " + context.settings.NUMERIC + "(20),\n" +
    "    SYNC_FOLDER_NAME " + context.settings.VARCHAR + "(128),\n" +
    "    SERVER_FOLDER_PATH " + context.settings.VARCHAR + "(4000),\n" +
    "    CLIENT_FOLDER_PATH " + context.settings.VARCHAR + "(4000),\n" +
    "    RECURSIVE " + context.settings.VARCHAR + "(1),\n" +
    "    FILE_PATH_STARTS_WITH " + context.settings.VARCHAR + "(4000),\n" +
    "    FILE_NAME_ENDS_WITH " + context.settings.VARCHAR + "(4000),\n" +
    "    ALLOW_CHECK_IN " + context.settings.VARCHAR + "(1),\n" +
    "    ALLOW_REFRESH " + context.settings.VARCHAR + "(1),\n" +
    "    CHECK_IN_SUPER_USERS " + context.settings.VARCHAR + "(4000),\n" +
    "    DEF_CN " + context.settings.NUMERIC + "(20),\n" +
    "    SUB_CN " + context.settings.NUMERIC + "(20),\n" +
    "    FILE_CN " + context.settings.NUMERIC + "(20),\n" +
    "    NO_INIT_SYNC_NETWORKS " + context.settings.VARCHAR + "(256),\n" +
    "    NO_SYNC_NETWORKS " + context.settings.VARCHAR + "(256),\n" +
    "    ADDED " + context.settings.DATETIME + ",\n" +
    "    CONSTRAINT pvc$sync_schemas_unique UNIQUE (SYNC_FOLDER_NAME)\n" +
    ") " + context.settings.syncClientDbTableOptions);

  //context.log("sync_files");
  sqlList.push("CREATE TABLE IF NOT EXISTS pvc$sync_files(\n" +
    "    SYNC_FOLDER_ID " + context.settings.NUMERIC +
    "      REFERENCES pvc$sync_folders(ID),\n" +
    "    FILE_NAME " + context.settings.VARCHAR + "(128),\n" +
    "    IS_DIRECTORY " + context.settings.VARCHAR + "(1),\n" +
    "    LENGTH " + context.settings.NUMERIC + "(20),\n" +
    "    LAST_MODIFIED " + context.settings.NUMERIC + "(20),\n" +
    "    FILE_CN " + context.settings.NUMERIC + "(20),\n" +
    "    FILE_CT " + context.settings.VARCHAR + "(1),\n" +
    "    TXN$$ " + context.settings.NUMERIC + "(20),\n" +
    "    ADDED " + context.settings.DATETIME + ",\n" +
    "    PRIMARY KEY (SYNC_FOLDER_ID,FILE_NAME)\n" +
    ") " + context.settings.syncClientDbTableOptions);

  //context.log("sync_files");
  sqlList.push("CREATE INDEX IF NOT EXISTS pvc$sync_files$i1 ON pvc$sync_files (SYNC_FOLDER_ID, TXN$$) " +
    context.settings.NOLOGGING);

  //context.log("sync_history");
  sqlList.push("CREATE TABLE IF NOT EXISTS pvc$sync_history(\n" +
    "    ID " + context.settings.BIGINT_AUTO_INCREMENT + " PRIMARY KEY,\n" +
    "    USER_NAME " + context.settings.VARCHAR + "(128),\n" +
    "    DEVICE_NAME " + context.settings.VARCHAR + "(128),\n" +
    "    BEGIN_TIME " + context.settings.DATETIME + " NOT NULL,\n" +
    "    SYNC_TYPE " + context.settings.VARCHAR + "(128),\n" +
    "    SYNC_DIRECTION " + context.settings.VARCHAR + "(128),\n" +
    "    DURATION " + context.settings.NUMERIC + "(20),\n" +
    "    CHECK_IN_STATUS " + context.settings.VARCHAR + "(128),\n" +
    "    CHECK_IN_DELETES " + context.settings.NUMERIC + "(20),\n" +
    "    CHECK_IN_INSERTS " + context.settings.NUMERIC + "(20),\n" +
    "    CHECK_IN_UPDATES " + context.settings.NUMERIC + "(20),\n" +
    "    REFRESH_STATUS " + context.settings.VARCHAR + "(128),\n" +
    "    REFRESH_DELETES " + context.settings.NUMERIC + "(20),\n" +
    "    REFRESH_INSERTS " + context.settings.NUMERIC + "(20),\n" +
    "    REFRESH_UPDATES " + context.settings.NUMERIC + "(20),\n" +
    "    HAS_DEF_CHANGES CHAR(1),\n" +
    "    ERROR_CODE " + context.settings.NUMERIC + "(20),\n" +
    "    MESSAGES " + context.settings.CLOB + "\n" +
    ") " + context.settings.syncClientDbTableOptions);

  //context.log("sync_history");
  sqlList.push("CREATE INDEX IF NOT EXISTS pvc$sync_history$i1 ON pvc$sync_history (BEGIN_TIME) " +
    context.settings.NOLOGGING);

  //context.log("lob_locators");
  sqlList.push("CREATE TABLE IF NOT EXISTS pvc$lob_locators(\n" +
    "    ID " + context.settings.BIGINT_AUTO_INCREMENT + " PRIMARY KEY,\n" +
    "    COMMAND " + context.settings.VARCHAR + "(128),\n" +
    "    LOB_LOCATOR " + context.settings.CLOB + ",\n" +
    "    ADDED " + context.settings.DATETIME + "\n" +
    ") " + context.settings.syncClientDbTableOptions);

  //context.log("payload_out");
  sqlList.push("CREATE TABLE IF NOT EXISTS pvc$payload_out(\n" +
    "    ID " + context.settings.BIGINT_AUTO_INCREMENT + " PRIMARY KEY,\n" +
    "    PAYLOAD " + context.settings.CLOB + ",\n" +
    "    ADDED " + context.settings.DATETIME + "\n" +
    ") " + context.settings.syncClientDbTableOptions);

  //context.log("payload_in");
  sqlList.push("CREATE TABLE IF NOT EXISTS pvc$payload_in(\n" +
    "    ID " + context.settings.BIGINT_AUTO_INCREMENT + " PRIMARY KEY,\n" +
    "    PAYLOAD " + context.settings.CLOB + ",\n" +
    "    ADDED " + context.settings.DATETIME + "\n" +
    ") " + context.settings.syncClientDbTableOptions);

  // Creating tables
  let adminDbConn = await db.getDbConn(context.settings.adminDbName);
  for (let sql of sqlList) {
    context.log("executeSql: \n" + sql);
    await adminDbConn.executeSql(sql, []);
  }
  await db.closeDbConn(context.settings.adminDbName);
  context.syncState.configured = true;

  context.log("end pervasync setup.js, setup()");
}

export default {
  setup
}
