import context from './context.js'
import { HTTP } from '@ionic-native/http'

//
// transport, write to and read from temp DB table
//
let pvcAdminDb;
let sessionId, messageId, messageSize;
let payloadOutId, payloadInId;
let requestTextAry, requestText, responseText, responseOffset;

async function init(pvcAdminDb_in) {
    pvcAdminDb = pvcAdminDb_in;
}

// transport.openOutputStream
async function openOutputStream(sessionId_in) {
    sessionId = sessionId_in; // new session
    messageId = -1;
    payloadOutId = 0;
    payloadInId = 0;
    requestText = "";
    requestTextAry = [];
    messageSize = 0;
    //context.log("truncating TABLE pvc$payload_out");
    await pvcAdminDb.executeSql("DELETE FROM pvc$payload_out", []);
    //context.log("truncating TABLE pvc$payload_in");
    await pvcAdminDb.executeSql("DELETE FROM pvc$payload_in", []);
}

// transport.closeOutputStream
async function closeOutputStream() {//agentReceive_in) {
    //agentReceive = agentReceive_in;
    if (requestTextAry.length > 0) {
        //context.log("save requestText to DB in closeOutputStream");
        let sql = "INSERT INTO pvc$payload_out (ID, PAYLOAD) VALUES(?,?)";
        await pvcAdminDb.executeSql(sql, [payloadOutId, requestTextAry.join("")]);
        requestTextAry = [];
        messageSize = 0;
    }

    // reset IDs before httpSend
    payloadOutId = 0;
    payloadInId = 0;
    messageId = -1;
    await httpSend();
}

// transport.openInputStream
async function openInputStream() {
    payloadInId = 0;
    responseText = "";
}

// transport.closeInputStream
async function closeInputStream() {
}

// writeCommand
async function writeCommand(cmd) {
    //context.log("writing " + cmd.name);
    let cmdJsonLength, strCmdJsonLength, cmdJson, cmdValueJson;

    if (cmd.value) {
        cmdValueJson = JSON.stringify(cmd.value);
        if (cmdValueJson) {
            cmd.valueLength = cmdValueJson.length;
        }
    }

    let tempValue = cmd.value;
    cmd.value = null;
    cmdJson = JSON.stringify(cmd);
    cmd.value = tempValue;

    cmdJsonLength = cmdJson.length;

    if (cmdJsonLength > 0 && cmdJsonLength < 10) {
        strCmdJsonLength = "0" + cmdJsonLength;
    } else if (cmdJsonLength < 100) {
        strCmdJsonLength = "" + cmdJsonLength;
    } else {
        throw Error("cmdJsonLength not within 1 to 99: " +
            cmdJsonLength);
    }

    let lengthToWrite = strCmdJsonLength.length + cmdJson.length;
    if (cmd.valueLength > 0) {
        lengthToWrite += cmdValueJson.length;
    }

    if (lengthToWrite > context.settings.maxMessageSize) {
        throw Error("message size limit reached with a single command");
    }

    if (cmd.name != "MORE" &&
        (messageSize + lengthToWrite) > context.settings.maxMessageSize) {

        // message size limit reached, send MORE to server
        let clientMore = {};
        clientMore.name = "MORE";
        await writeCommand(clientMore);

        //context.log("save requestText to DB in writeCommand");
        var sql = "INSERT INTO pvc$payload_out (ID, PAYLOAD) VALUES(?,?)";
        await pvcAdminDb.executeSql(sql, [payloadOutId, requestTextAry.join("")]);

        requestTextAry = [];
        messageSize = 0;
        payloadOutId = Number(payloadOutId) + 1;

        // write the original sync command
        await writeCommand(cmd);

    } else {
        messageSize += lengthToWrite;

        //context.log("Writing:" + strCmdJsonLength + " " + cmdJson);
        requestTextAry.push(strCmdJsonLength);
        requestTextAry.push(cmdJson);
        if (cmd.valueLength > 0) {
            //context.log("cmdValueJson.length:" + cmdValueJson.length);
            requestTextAry.push(cmdValueJson);
        }
    }
}

// readCommand
async function readCommand() {
    //context.log("begin readCommand");
    let cmd, cmdJsonLength = 0;
    let cmdJson, cmdValueJson, charArray;

    // retrieve responseText from DB
    if (!responseText || responseOffset >= responseText.length) {
        //context.log("retrieve responseText from DB, payloadInId=" + payloadInId);
        let sql = "SELECT PAYLOAD FROM pvc$payload_in where ID=?";
        let data = await pvcAdminDb.executeSql(sql, [payloadInId]);
        if (data.rows.length == 1) {
            responseText = data.rows.item(0)["PAYLOAD"];
            responseOffset = 0;
            payloadInId = Number(payloadInId) + 1;
            //context.log("Retrieved responseText");
        } else {
            context.log("Retrieved responseText was empty");
        }
    }

    if (!responseText) {
        throw Error("No responseText to read");
    }

    charArray = responseText.substr(responseOffset, 2);
    responseOffset += 2;
    cmdJsonLength = parseInt(new String(charArray));
    //context.log("cmdJsonLength: " + cmdJsonLength);

    charArray = responseText.substr(responseOffset, cmdJsonLength);
    responseOffset += cmdJsonLength;
    cmdJson = charArray;
    //context.log("cmdJson: " + cmdJson);
    try {
        cmd = JSON.parse(cmdJson);
    } catch (e1) {
        context.log("Failed to parse cmdJson: " + cmdJson);
        throw e1;
    }
    //context.log("JSON.stringify(cmd)=" + JSON.stringify(cmd));
    //context.log("cmd.name=" + cmd.name);
    //context.log("cmd.valueLength=" + cmd.valueLength);

    if (cmd.valueLength > 0) {
        charArray = responseText.substr(responseOffset, cmd.valueLength);
        responseOffset += cmd.valueLength;
        cmdValueJson = charArray;
        //context.log("cmdValueJson=" + cmdValueJson);
        try {
            cmd.value = JSON.parse(cmdValueJson);
        } catch (e1) {
            context.log("Failed to parse cmdValueJson: " + cmdValueJson);
            throw e1;
        }
    }

    // received MORE from server
    if (cmd.name == "MORE") {
        //context.log("received MORE");
        cmd = await readCommand();
    }

    //context.log("end readCommand. cmd.name=" + cmd.name);
    return cmd;
}

// send client commands that were cached in temp DB table
// in one or more http requests; server responses are saved
// temp DB table
var serverMore = false;
async function httpSend() {

    context.log("httpSend begin");
    messageId += 1;
    requestText = "";
    responseText = "";
    responseOffset = 0;

    // retrieve requestText from DB
    //context.log("retrieve requestText from DB");
    let sql = "SELECT PAYLOAD FROM pvc$payload_out where ID=?";
    let data = await pvcAdminDb.executeSql(sql, [payloadOutId]);
    if (data.rows.length == 1) {
        requestText = data.rows.item(0)["PAYLOAD"];
    }
    payloadOutId = Number(payloadOutId) + 1;

    if (!requestText && serverMore) {
        requestText = context.settings.MORE;
        context.log("Will send client morePayload");
    }

    if (!requestText) {
        throw Error("No more request to send");
    }

    context.onSyncStateChange("SENDING");

    let headers = {
        "Content-Type": "application/octet-stream",
        "transport-serialization": "Json",
        "session-type": "SYNC",
        "max-message-size": "" + context.settings.maxMessageSize,
        "If-Modified-Since": "Sat, 1 Jan 2005 00:00:00 GMT",
    };
    if (sessionId) {
        headers["session-id"] = sessionId;
        headers["message-id"] = "" + messageId;
    } else {
        throw new Error("Invalid sessionId: " + sessionId);
    }

    context.log("Calling fetch for message #" + messageId
        + ". headers=" + JSON.stringify(headers));
    //+ ". body=" + requestText);


    let response = "";
    try {
        //HTTP.setDataSerializer("utf8");
        response = await HTTP.sendRequest(context.settings.syncServerUrl, {
            method: "post",
            headers: headers,
            serializer: "utf8",
            data: requestText
        });
    } catch (fetchError) {
        context.log("fetchError=" + JSON.stringify(fetchError));
        throw fetchError;
    }

    //context.log("response=" + JSON.stringify(response));
    context.onSyncStateChange("RECEIVING");
    if (response.status != 200) {
        throw Error("Got non-OK response: " + response.status);
    }
    let responseHeaders = response.headers;
    //context.log("responseHeaders=" + JSON.stringify(responseHeaders));

    sessionId = responseHeaders["session-id"];
    //context.log("Response sessionId: " + sessionId);
    messageId = Number(responseHeaders["message-id"]);
    //context.log("Response messageId: " + messageId);

    responseText = response.data;

    context.log("Recieved responseText, length: " + responseText.length);
    //if (responseText.length < 2000) {
    //context.log("responseText: " + responseText);
    //}

    // save responseText to DB
    //context.log("save responseText to DB");
    sql = "INSERT INTO pvc$payload_in (ID, PAYLOAD) VALUES(?,?)";
    await pvcAdminDb.executeSql(sql, [payloadInId, responseText]);
    payloadInId = Number(payloadInId) + 1;

    // http transport done?
    if (responseText.length >= context.settings.MORE.length &&
        context.settings.MORE ==
        responseText.substr(responseText.length - context.settings.MORE.length,
            context.settings.MORE.length)) {
        context.log("Server has more to send");
        serverMore = true;
        return await httpSend();
    } else {
        context.log("http transport done");
        serverMore = false;
        //agentReceive();
    }
}

export default {
    init,
    openOutputStream,
    closeOutputStream,
    openInputStream,
    closeInputStream,
    writeCommand,
    readCommand
}



