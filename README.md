# ionic-native-sync

Two way, incremental sync between Ionic Native SQLite databases and MySQL, Oracle, MS SQL Server and PostgreSQL databases

## Features

* Direct DB synchronization between on device SQLite databases and server side MySQL, Oracle, MS SQL Server and PostgreSQL databases
* Each sync user could subscribe to a subset of server side data
* Files can also be syned

## Demo

Check out [ionic-native-sync-demo](https://github.com/pervasync/ionic-native-sync-demo).

## Setup

This library is available on npm, install it with: `npm install --save ionic-native-sync`.

## Usage

1. Import ionic-native-sync as INSync:

```javascript
import INSync from "ionic-native-sync";
```

2. Configure INSync:

```javascript
export class HomePage {

  syncState = {};
  constructor(private platform: Platform, private alertController: AlertController,
    private changeDetectorRef: ChangeDetectorRef) {

    // "bind" the sync methods with "this" so that 
    // the methods could access instance properties like "syncState"
    this.doSync = this.doSync.bind(this);
    this.onSyncStateChange = this.onSyncStateChange.bind(this);

    // Prepare "settings" which contains sync server connection info. 
    let settings = {
      "syncServerUrl": "http://192.168.0.6:8080/pervasync/server",
      "syncUserName": "user_1",
      "syncUserPassword": "welcome1",
      "onSyncStateChange": this.onSyncStateChange
    };

    // Pass the settings to ionic-native-sync via "INSync.config(settings)"
    // first thing after app is started (platform ready)
    this.platform.ready().then(async (readySource) => {
      console.log('Platform ready from', readySource);
      console.log("Calling INSync.config");
      await INSync.config(settings);
    });
  }
```

3. Start a sync session:

```javascript
  /**
   * Method to start a sync session. Invoke in an action listener
   */
  async doSync() {
    console.log("Calling INSync.sync");
    await INSync.sync();
  }

  /**
   * Sync status change listener. Register the listener in "settings" arg of "INSync.config(settings)"
   * @param state Possible values: READY, COMPOSING, SENDING, RECEIVING, PROCESSING, SUCCEEDED, FAILED
   * @param progress Sync progress with a value between 0 and 1
   * @param syncSummary A JSON object contains detailed sync info. Sample: 
   * {"syncBeginTime":1583825680054,"checkInDIU_requested":[0,0,0],"checkInDIU_done":[0,0,0],
   * "refreshDIU_requested":[0,2028,0],"refreshDIU_done":[0,3,2025],
   * "hasDefChanges":true,"hasDataChanges":true,"errorCode":-1,"checkInStatus":"SUCCESS",
   * "checkInSchemaNames":["schema1","schema2"],"refreshSchemaNames":["schema1","schema2"],
   * "checkInFolderNames":[],"refreshFolderNames":["folder1"],"refreshStatus":"SUCCESS",
   * "serverSnapshotAge":8151664,"user":"user_1","device":"DEFAULT","syncDirection":"TWO_WAY",
   * "syncErrorMessages":"","syncErrorStacktraces":"","syncSchemaNames":["schema1","schema2"],
   * "syncFolderNames":["folder1"],"uploadBeginTime":1583825680060,"sessionId":1583825680060,
   * "downloadBeginTime":1583825689143,"syncEndTime":1583825727533,"syncDuration":"47.479 seconds"}
   */
  async onSyncStateChange(state, progress, syncSummary) {
    console.log("onSyncStateChange, state=" + state + ", progress=" + progress);
    if (syncSummary) {
      console.log("onSyncStateChange, syncSummary=" + JSON.stringify(syncSummary));
    }

    this.syncState['state'] = state;
    this.syncState['progress'] = progress;
    this.syncState['summary'] = JSON.stringify(syncSummary);
    this.changeDetectorRef.detectChanges();

    if ('SUCCEEDED' == state || 'FAILED' == state) {
      const alert = await this.alertController.create({
        header: 'Sync Completed',
        subHeader: 'Result: ' + state,
        message: 'Sync Summary: ' + JSON.stringify(syncSummary),
        buttons: ['OK']
      });

      alert.present();
    }
  }
```

4. Get a handle to the synced SQLite database and synced folder path:

```javascript
  async insertRecord() {
    // "schema1" is the published sync schema name 
    // "INSync.openSchemaDb" will return it's local DB
    let message = '';
    let schemaDb = null;
    let sql = '';
    let rs = null;
    try {
      schemaDb = await INSync.openSchemaDb("schema1");

      //test INSERT
      sql = "BEGIN TRANSACTION";
      await schemaDb.executeSql(sql, []);
      sql = "INSERT INTO executives(ID, NAME, TITLE) VALUES (?, ?, ?)";
      await schemaDb.executeSql(sql, [9999, 'Pervasync', 'Ionic Native']);
      sql = "INSERT INTO employees(ID, NAME, TITLE) VALUES (?, ?, ?)";
      await schemaDb.executeSql(sql, [9999, 'Pervasync', 'Ionic Native']);
      sql = "COMMIT TRANSACTION";
      await schemaDb.executeSql(sql, []);

      // test SELECT
      sql = "SELECT ID, NAME, TITLE FROM executives WHERE ID=9999";
      rs = await schemaDb.executeSql(sql, []);
      if (rs.rows.length > 0) {
        let name = rs.rows.item(0)['NAME'];
        console.log("Executive name: " + name);
      }

      message = "Insert record succeeded.";
    } catch (error) {
      message = "Insert record failed with error: " + JSON.stringify(error);
    } finally {
      if (schemaDb) {
        schemaDb.close();
      }
      const alert = await this.alertController.create({
        header: 'Done',
        message: message,
        buttons: ['OK']
      });

      alert.present();
    }
  }

  // method to duplicate an existing file
  async copyFile() {
    // "folder1" is the published sync folder name 
    // "INSync.getPath" will return its local path
    let message = '';
    try {
      let path = await INSync.getPath("folder1");
      console.log("copyFile, path=" + path);
      await File.copyFile(path, "Shared/ladies_t.jpg", path, "Shared/ladies_t_from_ionic.jpg");
      message = "Copy file succeeded.";
    } catch (error) {
      message = "Copy file failed with error: " + JSON.stringify(error);
    } finally {
      const alert = await this.alertController.create({
        header: 'Done',
        message: message,
        buttons: ['OK']
      });

      alert.present();
    }
  }
```

## Complete Example

Check out [ionic-native-sync-demo](https://github.com/pervasync/ionic-native-sync-demo)


