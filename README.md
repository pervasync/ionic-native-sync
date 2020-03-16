# ionic-native-sync

Two way, incremental sync between Ionic Native SQLite databases and MySQL, Oracle, MS SQL Server and PostgreSQL databases

## Features

* Direct DB synchronization between on device SQLite databases and server side MySQL, Oracle, MS SQL Server and PostgreSQL databases
* Each sync user could subscribe to a subset of server side data
* Files can also be syned

## Setup

For end-to-end testing, you need to first setup a Pervasync server and publish your central database tables for sync. See [Pervasync documentation](https://docs.google.com/document/u/1/d/1Oioo0MxSArRgBdZ0wmLND-1AdzVLyolNd-yWw59tIC8/pub) for instructions.

Create an Ionic app if you don't already have one. Change directory to app root.

    ````
    ionic start ionic-native-sync-demo blank --type=angular
    cd ionic-native-sync-demo
    ````

Add the SQLite, file and HTTP cordova plugins.

    ````
    ionic cordova plugin add cordova-plugin-file
    ionic cordova plugin add cordova-sqlite-storage
    ionic cordova plugin add cordova-plugin-advanced-http
    ````

Install this sync package together with SQLite, file npm packages

    ````
    npm install --save ionic-native-sync
    npm install --save @ionic-native/file
    npm install  --save @ionic-native/sqlite
    ````

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

4. Interact with the synced SQLite database and synced folders:

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

## Test On Simulators

### Android

To test on Android, if your sync server is setup with plain HTTP instead of HTTPS, you will need to add your server to the domain list that have cleartextTrafficPermitted set to "true" by editing `<app root>/resources/android/xml/network_security_config.xml`. For example:
    ```
    <network-security-config>
        <domain-config cleartextTrafficPermitted="true">
            <domain includeSubdomains="true">localhost</domain>
            <domain includeSubdomains="true">192.168.0.6</domain>        
        </domain-config>
    </network-security-config>
    ```

Build and install for Android

    ```
    cd ionic-native-sync-demo
    ionic cordova build android
    adb install platforms/android/app/build/outputs/apk/debug/app-debug.apk
    ```
You could use [Android device file explorer](https://developer.android.com/studio/debug/device-file-explorer) to check the SQLite databases and folders synced.

### iOS

Build and install for iOS

    ```
    ionic cordova build ios
    ionic cordova emulate ios --livereload --consolelogs 
    ```

To find the simulator locations of the synced SQLite databases and file folders, 

    ```
    cd ~/Library/Developer/CoreSimulator/Devices/ 
    find . -name pvcadmin__0.db
    ./6328A886-853F-40E0-BC65-A5FA1DFB1E03/data/Containers/Data/Application/57B0DD1F-E23B-47CC-8739-8920A5314320/Library/LocalDatabase/pvcadmin__0.db
    ```

The databases would be in `Library/LocalDatabase` and the sync folders would be in `Library/NoCloud`.

## Complete Example

Check out [ionic-native-sync-demo](https://github.com/pervasync/ionic-native-sync-demo)


