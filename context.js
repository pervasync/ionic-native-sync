// Sync Log
function log(message) {
  console.log("PERVASYNC_LOG:: " + message);
}

// Sync state 

var syncState = {
  "configured": false,
  "syncing": false
};

function onSyncStateChange(state, syncSummary) {
  syncState['state'] = state;
  syncState['syncSummary'] = syncSummary;
  //log("sync state: " + state);
  if (typeof settings.onSyncStateChange == 'function') {
    try {
      //log("calling settings.onSyncStateChange");
      let progress = 0;
      if ('READY' == state) {
        progress = 0;
      } else if ('COMPOSING' == state) {
        progress = 0 / 4;
      } else if ('SENDING' == state) {
        progress = 1 / 4;
      } else if ('RECEIVING' == state) {
        progress = 2 / 4;
      } else if ('PROCESSING' == state) {
        progress = 3 / 4;
      } else if ('SUCCEEDED' == state || 'FAILED' == state) {
        progress = 1;
      }
      syncState['progress'] = progress;
      settings.onSyncStateChange(state, progress, syncSummary);
    } catch (e) {
      log("Ignored onSyncStateChange error: " + JSON.stringify(e));
    }
  }
}

// Sync settings
var settings = {
  VERSION: "9.0.0",
  VARCHAR: "VARCHAR",
  NUMERIC: "NUMERIC",
  DATETIME: "DATETIME",
  BLOB: "BLOB",
  CLOB: "CLOB",
  BIGINT_AUTO_INCREMENT: "INTEGER",
  BIGINT: "BIGINT",
  NOLOGGING: "",
  SYSDATE: "DATETIME('now')",
  DAY: "DAY",
  EMPTY_BLOB: "NULL",
  EMPTY_CLOB: "NULL",
  DQ: "",
  DML_ROW_NOT_FOUND: "SQL%NOTFOUND",
  NEW: "NEW",
  OLD: "OLD",
  TEMPORARY: "TEMPORARY",
  MORE: "31{\"name\":\"MORE\",\"valueLength\":0}",
  syncClientDbTableOptions: "",
  add_idPkColumn: true,

  accountId: 0,
  filesPath: null,
  dbLocation: 'default',
  encryptionKey: null,
  syncServerUrl: null, // required
  syncUserName: null, // required
  syncDeviceName: "DEFAULT",
  syncUserPassword: null, // required
  adminDbName: "pvcadmin",
  maxMessageSize: 2000000,
  lobBufferSize: 400000
}

export default {
  settings,
  syncState,
  onSyncStateChange,
  log
}
