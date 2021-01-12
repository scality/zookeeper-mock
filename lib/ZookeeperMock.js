const assert = require('assert');
const async = require('async');
const EventEmitter = require('events');
const zookeeper = require('node-zookeeper-client');
const crypto = require('crypto');

/**
 * This mock object is to overwrite the zkClient for e.g. test race
 * conditions. It is not complete so far but could be extended.
 * @class
 */
class ZookeeperMockClient {
    constructor(mock, basePath) {
        this._mock = mock;
        this._basePath = basePath;
        this.state = zookeeper.State.DISCONNECTED;
        this._emitter = new EventEmitter();
        this._id = crypto.randomBytes(10).toString('hex');
    }

    connect() {
        this.state = zookeeper.State.CONNECTED;
        return process.nextTick(() => this._emitter.emit('connected'));
    }

    on(event, cb) {
        this._emitter.on(event, cb);
    }

    once(event, cb) {
        this._emitter.once(event, cb);
    }

    close() {
        this._mock._iterate(true, (level, parent, name, child) => {
            if (child._id === this._id &&
                (child.mode === zookeeper.CreateMode.EPHEMERAL ||
                 child.mode === zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL)) {
                // eslint-disable-next-line
                delete parent.children[name];
                parent.emitter.emit('watcher', {
                    type: zookeeper.Event.NODE_CHILDREN_CHANGED,
                    name: 'NODE_CHILDREN_CHANGED',
                    path: this._getPath(parent)
                });
            }
        });
        this.state = zookeeper.State.DISCONNECTED;
    }

    _zeroPad(path, n, width) {
        const _n = `${n}`;
        return path + (_n.length >= width ? _n :
            new Array(width - _n.length + 1).join('0') + _n);
    }

    create(path, data, acls, mode, callback) {
        // eslint-disable-next-line
        path = this._basePath + path;
        const optionalArgs = [data, acls, mode, callback];
        // eslint-disable-next-line
        data = acls = mode = callback = undefined;
        optionalArgs.forEach(arg => {
            if (Array.isArray(arg)) {
                // eslint-disable-next-line
                acls = arg;
            } else if (typeof arg === 'number') {
                // eslint-disable-next-line
                mode = arg;
            } else if (Buffer.isBuffer(arg)) {
                // eslint-disable-next-line
                data = arg;
            } else if (typeof arg === 'function') {
                // eslint-disable-next-line
                callback = arg;
            }
        });
        assert(
            typeof callback === 'function',
            'callback must be a function.'
        );
        // eslint-disable-next-line
        acls = Array.isArray(acls) ? acls : zookeeper.ACL.OPEN_ACL_UNSAFE;
        // eslint-disable-next-line
        mode = typeof mode === 'number' ? mode : zookeeper.CreateMode.PERSISTENT;
        assert(
            data === null || data === undefined || Buffer.isBuffer(data),
            'data must be a valid buffer, null or undefined.'
        );
        assert(acls.length > 0, 'acls must be a non-empty array.');
        this._mock._log('CREATE', path,
                        data ? data.toString() : undefined, acls, mode);
        this._validatePath(path);
        return this._create(path, data, acls, mode, callback);
    }

    _create(path, data, acls, mode, callback) {
        // console.log('_CREATE', path,
        // data ? data.toString() : undefined , mode);
        let _path = path;
        let _eexist = false;
        let name;
        const { parent, baseName, err } = this._getZNode(path, false);
        if (err) {
            return process.nextTick(() => callback(err));
        }
        name = baseName;
        if (!(mode === zookeeper.CreateMode.PERSISTENT_SEQUENTIAL ||
              mode === zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL)) {
            if (Object.prototype.hasOwnProperty.call(parent.children, name)) {
                _eexist = true;
            }
        }
        if (_eexist) {
            return callback(
                new zookeeper.Exception(
                    zookeeper.Exception.NODE_EXISTS,
                    'NODE_EXISTS',
                    path,
                    this._create));
        }
        let isEphemeral = false;
        let isSequential = false;
        if (mode === zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL ||
            mode === zookeeper.CreateMode.EPHEMERAL) {
            isEphemeral = true;
        }
        if (mode === zookeeper.CreateMode.PERSISTENT_SEQUENTIAL ||
            mode === zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL) {
            _path = this._zeroPad(path, parent.counter, 10);
            name = this._zeroPad(name, parent.counter, 10);
            isSequential = true;
            parent.counter++;
        }
        parent.children[name] = {};
        parent.children[name].name = name;
        parent.children[name]._id = this._id;
        parent.children[name].isEphemeral = isEphemeral;
        parent.children[name].isSequential = isSequential;
        parent.children[name].data = data;
        parent.children[name].acls = acls;
        parent.children[name].mode = mode;
        parent.children[name].parent = parent;
        // ephemeral nodes cannot have children
        if (!isEphemeral) {
            parent.children[name].children = {};
        }
        parent.children[name].emitter = new EventEmitter();
        parent.children[name].counter = 0;
        parent.emitter.emit('watcher', {
            type: zookeeper.Event.NODE_CHILDREN_CHANGED,
            name: 'NODE_CHILDREN_CHANGED',
            path: _path
        });
        return process.nextTick(() => callback(null, _path));
    }

    // returns { parent, baseName, err }
    _getZNode(path, shallExist = true) {
        // console.log('_getZNode', path, shallExist);
        let parent = this._mock._zkState;
        let next = undefined;
        let baseName = '';
        let key;
        const arr = path.split('/');
        for (key = 1; key < arr.length; key++) {
            const name = arr[key];
            if (parent.isEphemeral) {
                return {
                    parent: null, baseName: null,
                    err: new zookeeper.Exception(
                        zookeeper.Exception.NO_CHILDREN_FOR_EPHEMERALS,
                        'NO_CHILDREN_FOR_EPHEMERALS',
                        path,
                        this._getZNode)
                };
            }
            next = parent.children[name];
            const isLast = (key === arr.length - 1);
            if ((!next && !isLast) ||
                (!next && isLast && shallExist)) {
                // console.log('result NO_NODE', isLast, shallExist);
                return {
                    parent: null, baseName: null,
                    err: new zookeeper.Exception(
                        zookeeper.Exception.NO_NODE,
                        'NO_NODE',
                        path,
                        this._getZNode)
                };
            }
            if (isLast) {
                baseName = name;
                break;
            }
            parent = next;
            baseName = name;
        }
        // console.log('result', parent, baseName);
        return { parent, baseName, err: null };
    }

    _getPath(znode) {
        const arr = [];
        for (let z = znode; z !== undefined; z = z.parent) {
            if (z.name) {
                arr.push(z.name);
            }
        }
        return `/${arr.reverse().join('/')}`;
    }

    setData(path, data, version, callback) {
        // eslint-disable-next-line
        path = this._basePath + path;
        if (!callback) {
            // eslint-disable-next-line
            callback = version;
            // eslint-disable-next-line
            version = -1;
        }
        this._mock._log('SETDATA', path, data.toString(), version);
        this._validatePath(path);
        const { parent, baseName, err } = this._getZNode(path);
        if (err) {
            return process.nextTick(() => callback(err));
        }
        parent.children[baseName].data = data;
        // trigger watcher
        parent.children[baseName].emitter.emit('watcher', {
            type: zookeeper.Event.NODE_DATA_CHANGED,
            name: 'NODE_DATA_CHANGED',
            path
        });
        return process.nextTick(() => callback(null, {}));
    }

    getData(path, watcher, callback) {
        // eslint-disable-next-line
        path = this._basePath + path;
        if (!callback) {
            // eslint-disable-next-line
            callback = watcher;
            // eslint-disable-next-line
            watcher = undefined;
        }
        this._mock._log('GETDATA', path);
        this._validatePath(path);
        const { parent, baseName, err } = this._getZNode(path);
        if (err) {
            return process.nextTick(() => callback(err));
        }
        // take care of races
        const emitter = parent.children[baseName].emitter;
        const data = parent.children[baseName].data;
        if (watcher) {
            emitter.once(
                'watcher',
                event => watcher(event));
        }
        return process.nextTick(
            () => callback(null, data, {}));
    }

    getChildren(path, watcher, callback) {
        // eslint-disable-next-line
        path = this._basePath + path;
        if (!callback) {
            // eslint-disable-next-line
            callback = watcher;
            // eslint-disable-next-line
            watcher = undefined;
        }
        this._mock._log('GETCHILDREN', path);
        this._validatePath(path);
        const children = [];
        const { parent, baseName, err } = this._getZNode(path);
        if (err) {
            return process.nextTick(() => callback(err));
        }
        if (watcher) {
            parent.children[baseName].emitter.once(
                'watcher',
                event => watcher(event));
        }
        Object.entries(parent.children[baseName].children).forEach(kv => {
            children.push(kv[0]);
        });
        return process.nextTick(() => callback(null, children, {}));
    }

    _validatePath(path) {
        assert(
            path && typeof path === 'string',
            'Node path must be a non-empty string.'
        );
        assert(path[0] === '/', 'Node path must start with / character.');
        // Shortcut, no need to check more since the path is the root.
        if (path.length === 1) {
            return;
        }
        assert(
            path[path.length - 1] !== '/',
            'Node path must not end with / character.'
        );
        assert(
            !/\/\//.test(path),
            'Node path must not contain empty node name.'
        );
        assert(
            !/\/\.(\.)?(\/|$)/.test(path),
            'Node path must not contain relative path(s).'
        );
    }

    mkdirp(path, data, acls, mode, callback) {
        const optionalArgs = [data, acls, mode, callback];
        // eslint-disable-next-line
        data = acls = mode = callback = undefined;
        optionalArgs.forEach(arg => {
            if (Array.isArray(arg)) {
                // eslint-disable-next-line
                acls = arg;
            } else if (typeof arg === 'number') {
                // eslint-disable-next-line
                mode = arg;
            } else if (Buffer.isBuffer(arg)) {
                // eslint-disable-next-line
                data = arg;
            } else if (typeof arg === 'function') {
                // eslint-disable-next-line
                callback = arg;
            }
        });
        assert(
            typeof callback === 'function',
            'callback must be a function.'
        );
        // eslint-disable-next-line
        acls = Array.isArray(acls) ? acls : zookeeper.ACL.OPEN_ACL_UNSAFE;
        // eslint-disable-next-line
        mode = typeof mode === 'number' ? mode : zookeeper.CreateMode.PERSISTENT;
        assert(
            data === null || data === undefined || Buffer.isBuffer(data),
            'data must be a valid buffer, null or undefined.'
        );
        assert(acls.length > 0, 'acls must be a non-empty array.');
        this._mock._log('MKDIRP', path);
        this._validatePath(path);
        let currentPath = '';
        // Remove the empty string
        const nodes = path.split('/').slice(1);
        let i = 0;
        async.eachSeries(nodes, (node, next) => {
            currentPath = `${currentPath}/${node}`;
            const isLast = (i === nodes.length - 1);
            this._create(
                currentPath,
                isLast ? data : null,
                acls,
                isLast ? mode : zookeeper.CreateMode.PERSISTENT,
                (err, path) => {
                    i++;
                    // Skip node exist error.
                    if (err && err.getCode() ===
                        zookeeper.Exception.NODE_EXISTS && !isLast) {
                        next(null);
                        return;
                    }
                    if (isLast &&
                        (mode === zookeeper.CreateMode.PERSISTENT_SEQUENTIAL ||
                         mode === zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL)) {
                        currentPath = path;
                    }
                    next(err);
                });
        }, err => (process.nextTick(() => callback(err, currentPath))));
    }

    exists(path, watcher, callback) {
        // eslint-disable-next-line
        path = this._basePath + path;
        if (!callback) {
            // eslint-disable-next-line
            callback = watcher;
            // eslint-disable-next-line
            watcher = undefined;
        }
        this._mock._log('EXISTS', path);
        this._validatePath(path);
        const { parent, baseName, err } = this._getZNode(path);
        if (err) {
            if (err.name === 'NO_NODE') {
                return process.nextTick(
                    () => callback(null, null));
            }
            return process.nextTick(() => callback(err));
        }
        if (parent && !parent.children[baseName]) {
            return process.nextTick(() => callback(null, null));
        }
        // TODO: should actually return 'Stat' object if the node was found
        return process.nextTick(
            () => callback(null, true));
    }

    _traverseAndRemove(path, arr, idx, node) {
        const name = arr[idx];
        // console.log('name', name, node);
        if (!node.children[name]) {
            return new zookeeper.Exception(
                zookeeper.Exception.NO_NODE,
                'NO_NODE',
                path,
                this._traverseAndRemove);
        }
        if (idx === (arr.length - 1)) {
            // check children, if node has children, dont remove
            const nodeChildren = node.children[name].children;
            if (nodeChildren !== undefined) {
                const nodeChildrenCount = Object.keys(nodeChildren).length;
                if (nodeChildrenCount > 0) {
                    return new zookeeper.Exception(
                        zookeeper.Exception.NOT_EMPTY,
                        'NOT_EMPTY',
                        path,
                        this._traverseAndRemove);
                }
            }
            // trigger watchers
            node.children[name].emitter.emit('watcher', {
                type: zookeeper.Event.NODE_DELETED,
                name: 'NODE_DELETED',
                path
            });
            node.emitter.emit('watcher', {
                type: zookeeper.Event.NODE_CHILDREN_CHANGED,
                name: 'NODE_CHILDREN_CHANGED',
                path: this._getPath(node)
            });
            // remove node
            // eslint-disable-next-line
            delete node.children[name];
            return true;
        } else {
            // eslint-disable-next-line
            idx++;
            return this._traverseAndRemove(
                path, arr, idx, node.children[name]);
        }
    }

    _remove(path) {
        const arr = path.split('/');
        const result = this._traverseAndRemove(
            path, arr, 1, this._mock._zkState);
        return result instanceof Error ?
            { err: result } : { err: null, removed: result };
    }

    remove(path, version, callback) {
        // eslint-disable-next-line
        path = this._basePath + path;
        if (!callback) {
            // eslint-disable-next-line
            callback = version;
            // eslint-disable-next-line
            version = -1;
        }
        this._mock._log('REMOVE', path, version);
        this._validatePath(path);
        const { err, removed } = this._remove(path);
        if (err) {
            return process.nextTick(() => callback(err));
        }
        return process.nextTick(() => callback(null, removed));
    }
}

class ZookeeperMock {
    constructor(config) {
        this._doLog = false;
        if (config !== undefined) {
            if (config.doLog) {
                this._doLog = config.doLog;
            }
        }
        this._zkState = {};
        this._resetState();
    }

    // reset the state between the tests
    _resetState() {
        this._zkState = {
            isRoot: true,
            children: {},
            emitter: new EventEmitter(),
            isSequential: false,
            counter: 0,
        };
    }

    _basePath(connectionString) {
        let basePath = '';
        if (connectionString !== undefined) {
            const idx = connectionString.indexOf('/');
            if (idx !== -1) {
                basePath =
                    connectionString.substring(idx);
            }
        }
        return basePath;
    }

    createClient(connectionString) {
        this._log('CREATECLIENT', connectionString);
        return new ZookeeperMockClient(this, this._basePath(connectionString));
    }

    // cb(level, parent, name, child)
    __iterate(reverse, level, parent, cb) {
        if (!parent.children) {
            return undefined;
        }
        const keys = Object.keys(parent.children);
        let i;
        for (i = 0; i < keys.length; i++) {
            const name = keys[i];
            const child = parent.children[keys[i]];
            if (!reverse) {
                cb(level, parent, name, child);
            }
            this.__iterate(reverse, level + 1, child, cb);
            if (reverse) {
                cb(level, parent, name, child);
            }
        }
        return undefined;
    }

    // iterate over all the children
    _iterate(reverse, cb) {
        return this.__iterate(reverse, 0, this._zkState, cb);
    }

    // for testing
    _dump() {
        return this._iterate(false, (level, parent, name, child) => {
            const padStr = new Array(level + 1).join(' ');
            // eslint-disable-next-line
            console.log(padStr, name,
                        child.data ? child.data.toString() : undefined);
        });
    }

    _log(...args) {
        if (this._doLog) {
            // eslint-disable-next-line
            console.log(args);
        }
    }
}

module.exports = ZookeeperMock;
