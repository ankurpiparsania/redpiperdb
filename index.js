// index.js
const { platform, arch } = process;

let nativeBinding = null;

if (platform === 'win32' && arch === 'x64') {
    nativeBinding = require('./piper-redb.win32-x64-msvc.node');
} else if (platform === 'darwin' && arch === 'x64') {
    nativeBinding = require('./piper-redb.darwin-x64.node');
} else if (platform === 'darwin' && arch === 'arm64') {
    nativeBinding = require('./piper-redb.darwin-arm64.node');
} else if (platform === 'linux' && arch === 'x64') {
    nativeBinding = require('./piper-redb.linux-x64-gnu.node');
} else {
    throw new Error(`Unsupported OS: ${platform}, architecture: ${arch}`);
}

module.exports = nativeBinding;