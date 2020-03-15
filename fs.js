import { File } from '@ionic-native/file'
import context from './context.js'
import util from './util.js'

var file = File;

async function getFilesPath() {
    let filesPath;// = file.dataDirectory; //+ (file.dataDirectory.endsWith("/") ? "pvcfiles" : "/pvcfiles");
    filesPath = file.dataDirectory; 
    context.log("fs getFilesPath, filesPath=" + filesPath);
    return filesPath;
}

async function init() {
}

async function rm(path) {
    //context.log("fs rm, path=" + path);
    let pathParts = util.splitPath(path);

    if (await isFile(path)) {
        return file.removeFile(pathParts[0], pathParts[1]);
    } else if (await isDir(path)) {
        return file.removeDir(pathParts[0], pathParts[1]);
    }
}

async function rmrf(path) {
    //context.log("fs rmrf, path=" + path);
    let pathParts = util.splitPath(path);

    let isDir = await isDir(path);
    //context.log("isDir=" + isDir);
    if (isDir) {
        let files = await file.listDir(pathParts[0], pathParts[1]);
        for (let i = 0; i < files.length; i++) {
            let file = files[i];
            let filePath = path + "/" + file.name;
            //context.log("filePath=" + filePath);
            await rmrf(filePath);
        }
    }

    return await rm(path);
}

async function createFile(path) {
    let pathParts = util.splitPath(path);
    return await file.createFile(pathParts[0], pathParts[1], true); // replace=true
}

async function writeString(path, str) {
    let pathParts = util.splitPath(path);
    return await file.writeExistingFile(pathParts[0], pathParts[1], str);
}
async function readString(path) {
    let pathParts = util.splitPath(path);
    let str = await file.readAsText(pathParts[0], pathParts[1]);
    return str;
}

/**
 * 
 * @param {*} path 
 * @param {*} bytes Uint8Array
 */
async function writeBytes(path, bytes) {
    //context.log("begin writeBytes, path=" + path);

    let options = { replace: true };
    let pathParts = util.splitPath(path);
    await file.writeFile(pathParts[0], pathParts[1], bytes.buffer, options); // Uint8Array to ArrayBuffer
    return;
}

/**
 * 
 * @param {*} path 
 * @returns Uint8Array
 */
async function readBytes(path) {
    let pathParts = util.splitPath(path);

    let arrayBuffer = await file.readAsArrayBuffer(pathParts[0], pathParts[1]);
    //context.log("arrayBuffer.byteLength=" + arrayBuffer.byteLength);
    let bytes = new Uint8Array(arrayBuffer);
    return bytes;
}

async function mkdir(path) {
    let pathParts = util.splitPath(path);
    return await file.createDir(pathParts[0], pathParts[1], false);
}

async function mkdirs(path) {
    let parentPath = parent(path);
    let parentExist = await exists(parentPath);
    if (!parentExist) {
        await mkdirs(parentPath);
    }
    return await mkdir(path);
}

function parent(path) {
    let pathParts = util.splitPath(path);
    return pathParts[0];
    //return path.substring(0, path.lastIndexOf("/"));
}
async function ls(path) {
    //context.log("ls, path=" + path);
    let pathParts = util.splitPath(path);
    //context.log("ls, calling listDir");
    let files = null;
    files = await file.listDir(pathParts[0], pathParts[1]);
    return files;
}

async function mv(from, to) {
    let fromParts = util.splitPath(from);
    let toParts = util.splitPath(to);

    return await file.moveFile(fromParts[0], fromParts[1], toParts[0], toParts[1]);
}

async function cp(src, dest) {
    let srcParts = util.splitPath(src);
    let destParts = util.splitPath(dest);

    return await file.copyFile(srcParts[0], srcParts[1], destParts[0], destParts[1]);
}

async function exists(path) {
    return await isDir(path) || await isFile(path);
}

async function isDir(path) {
    let pathParts = util.splitPath(path);

    //context.log("file.isDir, path=" + path + ", pathParts[0]=" + pathParts[0] + ", pathParts[1]=" + pathParts[1]);
    try {
        let isDir = await file.checkDir(pathParts[0], pathParts[1]);
        return isDir;
    } catch (error) {
        //context.log("Return false for error in file.isDir: " + JSON.stringify(error));
        return false;
    }
}

async function isFile(path) {
    let pathParts = util.splitPath(path);

    //context.log("file.isFile, path=" + path + ", pathParts[0]=" + pathParts[0] + ", pathParts[1]=" + pathParts[1]);
    try {
        let isFile = await file.checkFile(pathParts[0], pathParts[1]);
        return isFile;
    } catch (error) {
        //context.log("Return false for error in file.isFile: " + JSON.stringify(error));
        return false;
    }
}

const getEntryMetadata = entry => new Promise((resolve, reject) => {
    entry.getMetadata(metadata => {
        //context.log("getEntryMetadata metadata=" + JSON.stringify(metadata));
        if (metadata) {
            resolve(metadata);
        } else {
            reject("Failed to get entry metadata, entry=" + JSON.stringify(entry));
        }
    });
});

async function stat(path) {
    //context.log("begin stat, path=" + path);
    let entry = await file.resolveLocalFilesystemUrl(path);
    //context.log("entry=" + JSON.stringify(entry));
    let metadata = await getEntryMetadata(entry);
    //context.log("stat metadata=" + JSON.stringify(metadata));
    metadata.lastModified = new Date(metadata.modificationTime).getTime();
    //context.log("stat metadata=" + JSON.stringify(metadata));
    return metadata;
}

export default {
    init,
    getFilesPath,
    rm,
    rmrf,
    createFile,
    writeString,
    writeBytes,
    readString,
    readBytes,
    mkdir,
    mkdirs,
    ls,
    mv,
    cp,
    exists,
    isDir,
    isFile,
    stat,
    parent
}