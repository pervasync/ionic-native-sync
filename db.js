import context from './context.js'
import { SQLite } from '@ionic-native/sqlite';

var sqlite = SQLite;
var sqliteDatabaseConfig = {
    location: 'default',
    androidDatabaseProvider: 'system'
};
var dbConnMap = {};

async function init() {
    if (context.settings.encryptionKey) {
        sqliteDatabaseConfig.key = context.settings.encryptionKey;
    }
    if (context.settings.dbLocation) {
        sqliteDatabaseConfig.location = context.settings.dbLocation;
    }
}

async function getDbConn(dbName) {
    //context.log("getDbConn for " + dbName);
    let dbConn = dbConnMap[dbName];
    if (!dbConn) {
        dbConn = await openDbConn(dbName);
        dbConnMap[dbName] = dbConn;
    }
    return dbConn;
}

async function openDbConn(dbName) {
    //context.log("openDbConn for " + dbName);
    let dbConn = null;
    sqliteDatabaseConfig.name = dbName + "__" + context.settings.accountId + ".db";
    dbConn = await sqlite.create(sqliteDatabaseConfig);
    if (!dbConn) {
        throw Error("openDbConn failed for dbName: " + dbName);
    }
    //context.log("opened dbConn");
    //context.log("BEGIN TRANSACTION " + dbName);
    await dbConn.executeSql("BEGIN TRANSACTION", []);
    return dbConn;
}

async function closeDbConn(dbName, rollback) {
    //context.log("closeDbConn for " + dbName);
    let dbConn = dbConnMap[dbName];
    if (dbConn) {
        try {
            if (rollback) {
                //context.log("ROLLBACK TRANSACTION " + dbName);
                await dbConn.executeSql("ROLLBACK TRANSACTION", []);
            } else {
                //context.log("COMMIT TRANSACTION " + dbName);
                await dbConn.executeSql("COMMIT TRANSACTION", []);
            }
            await dbConn.close();
            dbConnMap[dbName] = null;
        } catch (err) {
            context.log("Ignored error in closeDbConn: " + JSON.stringify(err));
        }
    }
}

async function closeAllDbConns(rollback) {
    //context.log("closeAllDbConns, rollback=" + rollback);

    for (let dbName in dbConnMap) {
        await closeDbConn(dbName, rollback);
    }

}

async function deleteDb(dbName) {
    //context.log("deleteDb for " + dbName);
    try {
        sqliteDatabaseConfig.name = dbName + "__" + context.settings.accountId + ".db";
        await sqlite.deleteDatabase(sqliteDatabaseConfig);
    } catch (error) {
        context.log("Ignored error in deleteDatabase: " + JSON.stringify(error));
    }

}

let getDeviceColDef = function (serverDbType, column) {

    let colDef = null;
    let colType = column.typeName;
    //context.log("colType: " + colType);
    if (!colType) {
        context.log("empty colType");
        colType = "";
    }
    colType = colType.toUpperCase();

    let splitted = colType.split(/[ (]/, 2);
    colType = splitted[0];

    if (serverDbType == "ORADB") {
        colDef = oracleToSqlite(column, colType);
    } else if (serverDbType == "MYSQL") {
        colDef = mysqlToSqlite(column, colType);
    } else if (serverDbType == "MSSQL") {
        colDef = mssqlToSqlite(column, colType);
    } else if (serverDbType == "POSTGRESQL") {
        colDef = postgresqlToSqlite(column, colType);
    } else {
        throw new Error("serverDbType not supported: " + serverDbType);
    }

    column.deviceColDef = colDef;

    return colDef;
}

let mysqlToSqlite = function (column, colType) {
    let colDef = null;
    let precision = column.columnSize;
    let scale = column.decimalDigits;
    let defaultValue = column.defaultValue;

    if ("TINYINT" == colType) { // 1 bytes
        colDef = colType;
    } else if ("SMALLINT" == colType) { // 2 bytes
        colDef = colType;
    } else if ("MEDIUMINT" == colType) { // 3 bytes
        colDef = colType;
    } else if ("INT" == colType ||
        "INTEGER" == colType) { // 4 bytes
        colDef = "INT"; // avoid INTEGER PRIMARY KEY column becoming rowid
    } else if ("BIGINT" == colType) { // 8 bytes
        colDef = colType;
    } else if ("DOUBLE" == colType ||
        "DOUBLE PRECISION" == colType ||
        "REAL" == colType) { // 8 bytes
        colDef = colType;
        if (scale > 0) {
            colDef += "(" + precision;
            colDef += "," + scale;
            colDef += ")";
        }
    } else if ("FLOAT" == colType) {
        colDef = "FLOAT";
        if (precision > 0) {
            colDef += "(" + precision + ")";
        }
    } else if ("DECIMAL" == colType || "DEC" == colType ||
        "NUMERIC" == colType) { //
        colDef = colType + "(" + precision + "," + scale + ")";
    } else if ("BIT" == colType) { //
        if (precision < 1) {
            colDef = "TINYINT(1)";
        } else {
            colDef = "TINYINT(1)";
            //colDef = "INT" + "(" + precision + ")";
        }
    } else if ("DATE" == colType || "DATETIME" == colType ||
        "TIME" == colType || "TIMESTAMP" == colType) {
        colDef = colType;
        //if (defaultValue != null && defaultValue.toUpperCase().indexOf("CURRENT_TIMESTAMP") < 0) {
        //    defaultValue = "'" + defaultValue + "'";
        //}
        defaultValue = null;
    } else if ("YEAR" == colType) { //
        colDef = colType;
    } else if (isBlob("MYSQL", column)) {
        colDef = colType;
        if (defaultValue != null) {
            defaultValue = "'" + defaultValue + "'";
        }
    } else if (isClob("MYSQL", column)) {
        colDef = colType;
        if (defaultValue != null) {
            defaultValue = "'" + defaultValue + "'";
        }
    } else if ("CHAR" == colType) { //
        colDef = "CHAR(" + precision + ")";
        if (defaultValue != null) {
            defaultValue = "'" + defaultValue + "'";
        }
    } else if ("VARCHAR" == colType) { //
        colDef = "VARCHAR(" + precision + ")";
        if (defaultValue != null) {
            defaultValue = "'" + defaultValue + "'";
        }
    } else if (colType.indexOf("SET") > -1 || colType.indexOf("ENUM") > -1) { //
        colDef = "VARCHAR(128)";
        if (defaultValue != null) {
            defaultValue = "'" + defaultValue + "'";
        }
    }else if("VARBINARY" == colType){
        colDef = colType + "(" + precision + ")";
        /*if (defaultValue != null) {
            if(defaultValue.toUpperCase().startsWith("0X")){
                defaultValue = "x'" + defaultValue.substring(2) + "'";
            }else if(!defaultValue.toUpperCase().startsWith("X'")){
                defaultValue = null;
            }
        }*/
        defaultValue = null;
    } else {
        colDef = colType + "(" + precision + ")";
        if (defaultValue != null) {
            defaultValue = "'" + defaultValue + "'";
        }
    }

    if (defaultValue != null) {
        colDef += " DEFAULT " + defaultValue;
    }

    if (!column.nullable) {
        colDef += " NOT NULL";
    }

    return colDef;
}


let oracleToSqlite = function (column, colType) {
    let colDef = null;
    let precision = column.columnSize;
    let scale = column.decimalDigits;
    let defaultValue = column.defaultValue;

    if ("NUMBER" == colType || "DECIMAL" == colType ||
        "NUMERIC" == colType) {
        if (precision <= 0) {
            colDef = colType;
        } else {
            colDef = colType + "(" + precision + "," + scale + ")";
        }
    } else if ("DATE" == colType || "TIMESTAMP" == colType) {
        colDef = colType;
        defaultValue = null;
    } else if ("VARCHAR2" == colType || "NVARCHAR2" == colType ||
        "VARCHAR" == colType ||
        "CHAR VARYING" == colType ||
        "CHARACTER VARYING" == colType ||
        "NVARCHAR" == colType ||
        "NCHAR VARYING" == colType ||
        "NATIONAL CHAR VARYING" == colType ||
        "NATIONAL CHARACTER VARYING" == colType) {
        if (precision <= 0) {
            colDef = colType;
        } else {
            colDef = colType + "(" + precision + ")";
        }
    } else if ("CHAR" == colType || "NCHAR" == colType ||
        "CHARACTER" == colType ||
        "NATIONAL CHAR" == colType ||
        "NATIONAL CHARACTER" == colType) {
        if (precision <= 0) {
            colDef = colType;
        } else {
            colDef = colType + "(" + precision + ")";
        }
    } else if (isBlob("ORACLE", column)) {
        colDef = "BLOB";
    } else if (isClob("ORACLE", column)) {
        colDef = colType;
    } else if ("RAW" == colType) {
        colDef = colType + "(" + precision + ")";
    } else if ("INTEGER" == colType) {
        colDef = "INT"; // avoid INTEGER PRIMARY KEY column becoming rowid
    } else if ("INT" == colType ||
        "SMALLINT" == colType ||
        "DOUBLE PRECISION" == colType ||
        "REAL" == colType) {
        colDef = colType;
    } else if ("FLOAT" == colType) {
        colDef = "FLOAT";
        if (precision > 0) {
            colDef += "(" + precision + ")";
        }
    } else {
        colDef = colType;
        if (precision > 0) {
            colDef += "(" + precision + ")";
        }
    }

    if (defaultValue != null) {
        colDef += " DEFAULT " + defaultValue;
    }

    if (!column.nullable) {
        colDef += " NOT NULL";
    }

    return colDef;
}

let mssqlToSqlite = function (column, colType) {
    let colDef = null;
    let precision = column.columnSize;
    let scale = column.decimalDigits;
    let defaultValue = column.defaultValue;

    // strip parenthesis off "(x)" or "((x))"
    if (defaultValue != null) {
        defaultValue = defaultValue.trim();
        if (defaultValue.startsWith("((") && defaultValue.endsWith("))")) {
            defaultValue = defaultValue.substring(2, defaultValue.length - 2);
        } else if (defaultValue.startsWith("(") && defaultValue.endsWith(")")) {
            defaultValue = defaultValue.substring(1, defaultValue.length - 1);
        }
        if (defaultValue.toUpperCase().startsWith("N'") && defaultValue.endsWith("'")) {
            defaultValue = defaultValue.substring(1);
        }
    }

    if (isBlob("MSSQL", column)) {
        colDef = "BLOB";
    } else if (isClob("MSSQL", column)) {
        colDef = colType;
    } else if (colType.endsWith("IDENTITY")) { // identity
        colDef = colType.substring(0, colType.length() - "IDENTITY".length());
    } else if ("BIT" == (colType)) { // bit
        colDef = colType;
    } else if ("TINYINT" == (colType)) { // 1 bytes
        colDef = colType;
    } else if ("SMALLINT" == (colType)) { // 2 bytes
        colDef = colType;
    } else if ("MEDIUMINT" == (colType)) { // 3 bytes, non exist for MSSql
        colDef = colType;
    } else if ("INT" == (colType)
        || "INTEGER" == (colType)) { // 4 bytes
        colDef = "INT"; // avoid INTEGER PRIMARY KEY column becoming rowid
    } else if ("BIGINT" == (colType)) { // 8 bytes
        colDef = colType;
    } else if ("REAL" == (colType)) { // Java Float
        colDef = colType;
        if (scale > 0) {
            colDef += "(" + precision;
            colDef += "," + scale;
            colDef += ")";
        }
    } else if ("FLOAT" == (colType)) {// Java Double
        colDef = colType;
        if (precision > 0) {
            colDef += "(" + precision + ")";
        }
    } else if ("DECIMAL" == (colType) || "MONEY" == (colType)
        || "SMALLMONEY" == (colType)
        || "NUMERIC" == (colType)) { //
        colDef = colType;
        if ("DECIMAL".equals(colType) || "NUMERIC".equals(colType)) {
            colDef += "(" + precision + "," + scale + ")";
        }
        if ("MONEY".equals(colType)) {
            colDef = "DECIMAL" + "(" + precision + "," + scale + ")";
        }
    } else if ("DATE" == (colType) || "DATETIME" == (colType)
        || "TIME" == (colType) || "DATETIME2" == (colType)) {
        colDef = colType;
    } else if ("CHAR" == (colType)) {
        colDef = "CHAR" + "(" + precision + ")";
    } else if ("NCHAR" == (colType)) {
        colDef = "CHAR" + "(" + precision + ")";
    } else if ("VARCHAR" == (colType) || "NVARCHAR" == (colType)) { // 
        colDef = "VARCHAR" + "(" + precision + ")";
    } else if ("BINARY" == (colType) || "VARBINARY" == (colType)
        || "TIMESTAMP" == (colType) || "ROWVERSION" == (colType)) { // 
        colDef = colType;
        if ("BINARY".equals(colType) || "VARBINARY".equals(colType)) {
            colDef += "(" + precision + ")";
        }
        if ("TIMESTAMP".equals(colType) || "ROWVERSION".equals(colType)) {
            colDef = "VARBINARY" + "(" + precision + ")";
        }
        defaultValue = null;
    } else if ("UNIQUEIDENTIFIER" == (colType)) {// uniqueidentifier
        colDef = "VARCHAR(72)";
        defaultValue = null;
    } else if ("SQL_VARIANT" == (colType) || "TABLE" == (colType) || "HIERARCHYID" == (colType)) {
        colDef = "VARBINARY(4000)";
        defaultValue = null;
    } else {
        colDef = colType;
        if (precision > 0) {
            colDef += "(" + precision + ")";
        }
    }

    if (defaultValue != null) {
        colDef += " DEFAULT " + defaultValue;
    }

    if (!column.nullable) {
        colDef += " NOT NULL";
    }

    return colDef;
}

let postgresqlToSqlite = function (column, colType) {
    let colDef = null;
    let precision = column.columnSize;
    let scale = column.decimalDigits;
    let defaultValue = column.defaultValue;

    if ("BOOLEAN" == (colType) || "BOOL" == (colType)) { // 1 byte
        colDef = colType;
    } else if ("SMALLINT" == (colType) || "INT2" == (colType)) { // 2 bytes
        colDef = colType;
    } else if ("INTEGER" == (colType) || "INT4" == (colType)) { // 4 bytes
        colDef = "INT"; // avoid INTEGER PRIMARY KEY column becoming rowid
    } else if ("BIGINT" == (colType) || "INT8" == (colType)) { // 8 bytes
        colDef = colType;
    } else if ("SERIAL" == (colType)) { // 4 bytes
        colDef = "INT"; // avoid INTEGER PRIMARY KEY column becoming rowid
        defaultValue = null; // default to implicit sequence value
    } else if ("BIGSERIAL" == (colType)) { // 8 bytes
        colDef = "INT"; // avoid INTEGER PRIMARY KEY column becoming rowid
        defaultValue = null; // default to implicit sequence value
    } else if ("REAL" == (colType) || "FLOAT4" == (colType)) { // 8 bytes
        colDef = colType;
    } else if ("DOUBLE PRECISION" == (colType) || "FLOAT8" == (colType)) {
        colType = "DOUBLE PRECISION";
    } else if ("DECIMAL" == (colType)
        || "NUMERIC" == (colType)) { //
        colDef = colType + "(" + precision + "," + scale + ")";
    } else if ("MONEY" == (colType)) {
        colDef = "DECIMAL";
    } else if ("BIT" == (colType) || "BIT VARYING" == (colType)) { //
        colDef = "VARBINARY";
        defaultValue = null;
    } else if ("DATE" == (colType)
        || "TIME" == (colType) || "TIMESTAMP" == (colType)) {
        colDef = colType;
        defaultValue = null;
    } else if (colType.startsWith("INTERVAL")) { // 
        colDef = colType;
    } else if ("BYTEA" == (colType)) {
        colDef = "VARBINARY";
        defaultValue = null;
    } else if ("TEXT" == (colType) || "XML" == (colType)) { // 
        colDef = "TEXT";
    } else if (isBlob("POSTGRESQL", column)) {
        colDef = "BLOB";
        defaultValue = null;
    } else if (isClob("POSTGRESQL", column)) {
        colDef = "CLOB";
        defaultValue = null;
    } else if ("CHAR" == (colType) || "CHARACTER" == (colType) || "BPCHAR" == (colType)) { // 
        colDef = "CHAR" + "(" + precision + ")";
    } else if ("VARCHAR" == (colType) || "CHARACTER VARYING" == (colType)) { // 
        colDef = colType + "(" + precision + ")";
    } else if (colType.startsWith("ENUM")) { //
        colDef = "VARCHAR(128)";
    } else {
        colDef = colType + "(" + precision + ")";
    }

    if (defaultValue != null) {
        colDef += " DEFAULT " + defaultValue;
    }

    if (!column.nullable) {
        colDef += " NOT NULL";
    }

    return colDef;
}

let isBlob = function (serverDbType, column) {
    let colType = column.typeName.toUpperCase();
    let splitted = colType.split(/[ (]/, 2);
    colType = splitted[0];
    if (serverDbType == "MYSQL") {
        if ("TINYBLOB" == colType ||
            "MEDIUMBLOB" == colType ||
            "BLOB" == colType || "LONGBLOB" == colType) {
            //context.log("isBlob() returns true for colType " + colType);
            return true;
        }
    } else if (serverDbType == "ORADB") {
        if ("BLOB" == colType || "BFILE" == colType) {
            //context.log("isBlob() returns true for colType " + colType);
            return true;
        }
    } else if (serverDbType == "MSSQL") {
        if ("LONGVARBINARY" == column.dataType) {
            return true;
        } else if ("IMAGE" == colType || "VARBINARY" == colType && column.columnSize > 8000) {
            return true;
        }
    } else if (serverDbType == "POSTGRESQL") {
        if ("OID" == colType) {
            return true;
        }
    }

    return false;
}

let isClob = function (serverDbType, column) {
    let colType = column.typeName.toUpperCase();
    let splitted = colType.split(/[ (]/, 2);
    colType = splitted[0];

    if (serverDbType == "MYSQL") {
        if ("TINYTEXT" == colType || "MEDIUMTEXT" == colType ||
            "TEXT" == colType || "LONGTEXT" == colType) {
            //context.log("isClob() returns true for colType " + colType);
            return true;
        }
    } else if (serverDbType == "ORADB") {
        if ("CLOB" == colType || "NCLOB" == colType) {
            //context.log("isClob() returns true for colType " + colType);
            return true;
        }
    } else if (serverDbType == "MSSQL") {
        if ("LONGVARCHAR" == column.dataType || "LONGNVARCHAR" == column.dataType
            || "SQLXML" == column.dataType) {
            return true;
        } else if ("TEXT" == colType || "NTEXT" == colType || "XML" == colType) {
            return true;
        } else if (("VARCHAR" == colType || "NVARCHAR" == colType) && column.columnSize > 8000) {
            return true;
        }
    } else if (serverDbType == "POSTGRESQL") {
        /* No types support CLOB API in POSTGRESQL. Well maybe an OID column which is handled by BLOB API
        }*/
    }

    return false;
}

let KEY_WORDS = {};
KEY_WORDS["ABORT"] = "Y";
KEY_WORDS["ACTION"] = "Y";
KEY_WORDS["ADD"] = "Y";
KEY_WORDS["AFTER"] = "Y";
KEY_WORDS["ALL"] = "Y";
KEY_WORDS["ALTER"] = "Y";
KEY_WORDS["ANALYZE"] = "Y";
KEY_WORDS["AND"] = "Y";
KEY_WORDS["AS"] = "Y";
KEY_WORDS["ASC"] = "Y";
KEY_WORDS["ATTACH"] = "Y";
KEY_WORDS["AUTOINCREMENT"] = "Y";
KEY_WORDS["BEFORE"] = "Y";
KEY_WORDS["BEGIN"] = "Y";
KEY_WORDS["BETWEEN"] = "Y";
KEY_WORDS["BY"] = "Y";
KEY_WORDS["CASCADE"] = "Y";
KEY_WORDS["CASE"] = "Y";
KEY_WORDS["CAST"] = "Y";
KEY_WORDS["CHECK"] = "Y";
KEY_WORDS["COLLATE"] = "Y";
KEY_WORDS["COLUMN"] = "Y";
KEY_WORDS["COMMIT"] = "Y";
KEY_WORDS["CONFLICT"] = "Y";
KEY_WORDS["CONSTRAINT"] = "Y";
KEY_WORDS["CREATE"] = "Y";
KEY_WORDS["CROSS"] = "Y";
KEY_WORDS["CURRENT_DATE"] = "Y";
KEY_WORDS["CURRENT_TIME"] = "Y";
KEY_WORDS["CURRENT_TIMESTAMP"] = "Y";
KEY_WORDS["DATABASE"] = "Y";
KEY_WORDS["DEFAULT"] = "Y";
KEY_WORDS["DEFERRABLE"] = "Y";
KEY_WORDS["DEFERRED"] = "Y";
KEY_WORDS["DELETE"] = "Y";
KEY_WORDS["DESC"] = "Y";
KEY_WORDS["DETACH"] = "Y";
KEY_WORDS["DISTINCT"] = "Y";
KEY_WORDS["DROP"] = "Y";
KEY_WORDS["EACH"] = "Y";
KEY_WORDS["ELSE"] = "Y";
KEY_WORDS["END"] = "Y";
KEY_WORDS["ESCAPE"] = "Y";
KEY_WORDS["EXCEPT"] = "Y";
KEY_WORDS["EXCLUSIVE"] = "Y";
KEY_WORDS["EXISTS"] = "Y";
KEY_WORDS["EXPLAIN"] = "Y";
KEY_WORDS["FAIL"] = "Y";
KEY_WORDS["FOR"] = "Y";
KEY_WORDS["FOREIGN"] = "Y";
KEY_WORDS["FROM"] = "Y";
KEY_WORDS["FULL"] = "Y";
KEY_WORDS["GLOB"] = "Y";
KEY_WORDS["GROUP"] = "Y";
KEY_WORDS["HAVING"] = "Y";
KEY_WORDS["IF"] = "Y";
KEY_WORDS["IGNORE"] = "Y";
KEY_WORDS["IMMEDIATE"] = "Y";
KEY_WORDS["IN"] = "Y";
KEY_WORDS["INDEX"] = "Y";
KEY_WORDS["INDEXED"] = "Y";
KEY_WORDS["INITIALLY"] = "Y";
KEY_WORDS["INNER"] = "Y";
KEY_WORDS["INSERT"] = "Y";
KEY_WORDS["INSTEAD"] = "Y";
KEY_WORDS["INTERSECT"] = "Y";
KEY_WORDS["INTO"] = "Y";
KEY_WORDS["IS"] = "Y";
KEY_WORDS["ISNULL"] = "Y";
KEY_WORDS["JOIN"] = "Y";
KEY_WORDS["KEY"] = "Y";
KEY_WORDS["LEFT"] = "Y";
KEY_WORDS["LIKE"] = "Y";
KEY_WORDS["LIMIT"] = "Y";
KEY_WORDS["MATCH"] = "Y";
KEY_WORDS["NATURAL"] = "Y";
KEY_WORDS["NO"] = "Y";
KEY_WORDS["NOT"] = "Y";
KEY_WORDS["NOTNULL"] = "Y";
KEY_WORDS["NULL"] = "Y";
KEY_WORDS["OF"] = "Y";
KEY_WORDS["OFFSET"] = "Y";
KEY_WORDS["ON"] = "Y";
KEY_WORDS["OR"] = "Y";
KEY_WORDS["ORDER"] = "Y";
KEY_WORDS["OUTER"] = "Y";
KEY_WORDS["PLAN"] = "Y";
KEY_WORDS["PRAGMA"] = "Y";
KEY_WORDS["PRIMARY"] = "Y";
KEY_WORDS["QUERY"] = "Y";
KEY_WORDS["RAISE"] = "Y";
KEY_WORDS["RECURSIVE"] = "Y";
KEY_WORDS["REFERENCES"] = "Y";
KEY_WORDS["REGEXP"] = "Y";
KEY_WORDS["REINDEX"] = "Y";
KEY_WORDS["RELEASE"] = "Y";
KEY_WORDS["RENAME"] = "Y";
KEY_WORDS["REPLACE"] = "Y";
KEY_WORDS["RESTRICT"] = "Y";
KEY_WORDS["RIGHT"] = "Y";
KEY_WORDS["ROLLBACK"] = "Y";
KEY_WORDS["ROW"] = "Y";
KEY_WORDS["SAVEPOINT"] = "Y";
KEY_WORDS["SELECT"] = "Y";
KEY_WORDS["SET"] = "Y";
KEY_WORDS["TABLE"] = "Y";
KEY_WORDS["TEMP"] = "Y";
KEY_WORDS["TEMPORARY"] = "Y";
KEY_WORDS["THEN"] = "Y";
KEY_WORDS["TO"] = "Y";
KEY_WORDS["TRANSACTION"] = "Y";
KEY_WORDS["TRIGGER"] = "Y";
KEY_WORDS["UNION"] = "Y";
KEY_WORDS["UNIQUE"] = "Y";
KEY_WORDS["UPDATE"] = "Y";
KEY_WORDS["USING"] = "Y";
KEY_WORDS["VACUUM"] = "Y";
KEY_WORDS["VALUES"] = "Y";
KEY_WORDS["VIEW"] = "Y";
KEY_WORDS["VIRTUAL"] = "Y";
KEY_WORDS["WHEN"] = "Y";
KEY_WORDS["WHERE"] = "Y";
KEY_WORDS["WITH"] = "Y";
KEY_WORDS["WITHOUT"] = "Y";

function quote(str) {

    if (str == null || KEY_WORDS[str.toUpperCase()] != "Y") {
        return str;
    }

    // remove quotes
    if (str.startsWith("[") && str.endsWith("]") ||
        str.startsWith("`") && str.endsWith("`") ||
        str.startsWith("\"") && str.endsWith("\"")) {
        str = str.substring(1, str.length() - 1);
    }

    // add quotes
    str = "\"" + str + "\"";

    return str;
}

export default {
    init,
    getDbConn,
    closeDbConn,
    closeAllDbConns,
    deleteDb,
    sqlite,
    sqliteDatabaseConfig,
    getDeviceColDef,
    isBlob,
    isClob,
    quote
}