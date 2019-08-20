const assert = require('assert');
const async = require('async');
const EventEmitter = require('events');
const zookeeper = require('node-zookeeper-client');

let initialized = false;

// shared state between instances
let _zkState = {};

// reset the state between the tests
function resetState() {
    _zkState = {
        children: {},
        emitter: new EventEmitter(),
        isSequential: false,
        counter: 0,
    };
}

/**
 * This mock object is to overwrite the zkClient for e.g. test race
 * conditions. It is not complete so far but could be extended.
 * @class
 */
class ZookeeperMock {
    constructor() {
        if (!initialized) {
            resetState();
            initialized = true;
        }
        this.state = zookeeper.State.DISCONNECTED;
    }

    _resetState() {
        resetState();
    }

    connect() {
        this.state = zookeeper.State.CONNECTED;
    }

    close() {
        this._iterate(true, (level, parent, name, child) => {
            if (child.mode ===
                zookeeper.CreateMode.EPHEMERAL) {
                // eslint-disable-next-line
                delete parent.children[name];
                parent.emitter.emit('watcher', {
                    type: zookeeper.Event.NODE_CHILDREN_CHANGED,
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
        // console.log('CREATE', path, data, acls, mode);
        let cur = _zkState;
        let prev = {};
        let prevName = null;
        let _path = path;
        let _eexist = false;
        path.split('/').forEach((name, idx, array) => {
            const isLast = (idx === array.length - 1);
            // console.log('NAME', name, isLast);
            if (!Object.prototype.hasOwnProperty.call(cur.children, name)) {
                if (isLast && mode ===
                    zookeeper.CreateMode.PERSISTENT_SEQUENTIAL) {
                    cur.children[name] = {};
                    cur.children[name].isSequential = true;
                } else {
                    cur.children[name] = {};
                    cur.children[name].children = {};
                    cur.children[name].emitter = new EventEmitter();
                    cur.children[name].isSequential = false;
                    cur.children[name].counter = 0;
                }
            } else {
                if (isLast) {
                    if (mode !== zookeeper.CreateMode.PERSISTENT_SEQUENTIAL) {
                        _eexist = true;
                    }
                }
            }
            prev = cur;
            prevName = name;
            cur = cur.children[name];
        });
        if (_eexist) {
            return callback(
                new zookeeper.Exception(
                    zookeeper.Exception.NODE_EXISTS,
                    'NODE_EXISTS',
                    path,
                    this.create));
        }
        // console.log('PREV', prev, 'NAME', prevName);
        if (mode === zookeeper.CreateMode.PERSISTENT_SEQUENTIAL) {
            _path = this._zeroPad(path, prev.counter, 10);
            prevName = this._zeroPad(prevName, prev.counter, 10);
            prev.children[prevName] = {};
            prev.counter++;
        }
        // console.log('_PATH', _path);
        prev.children[prevName].data = data;
        prev.children[prevName].acls = acls;
        prev.children[prevName].mode = mode;
        prev.children[prevName].parent = prev;
        prev.children[prevName].name = prevName;
        prev.emitter.emit('watcher', {
            type: zookeeper.Event.NODE_CHILDREN_CHANGED,
            path: _path
        });
        return process.nextTick(() => callback(null, _path));
    }

    _getZNode(path) {
        let next = _zkState;
        let prev = {};
        let prevName = null;
        let key;
        const arr = path.split('/');
        for (key = 0; key < arr.length; key++) {
            const name = arr[key];
            if (!next) {
                return { prev: null, prevName: null,
                         err: new zookeeper.Exception(
                             zookeeper.Exception.NO_NODE,
                             'NO_NODE',
                             path,
                             this._getZNode) };
            }
            prev = next;
            prevName = name;
            next = next.children[name];
        }
        return { prev, prevName, err: null };
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
        if (!callback) {
            // eslint-disable-next-line
            callback = version;
            // eslint-disable-next-line
            version = -1;
        }
        const { prev, prevName, err } = this._getZNode(path);
        if (err) {
            return process.nextTick(() => callback(err));
        }
        prev.children[prevName].data = data;
        return process.nextTick(() => callback(null, {}));
    }

    getData(path, watcher, callback) {
        if (!callback) {
            // eslint-disable-next-line
            callback = watcher;
            // eslint-disable-next-line
            watcher = undefined;
        }
        const { prev, prevName, err } = this._getZNode(path);
        if (err) {
            return process.nextTick(() => callback(err));
        }
        return process.nextTick(
            () => callback(null, prev.children[prevName].data, {}));
    }

    getChildren(path, watcher, callback) {
        if (!callback) {
            // eslint-disable-next-line
            callback = watcher;
            // eslint-disable-next-line
            watcher = undefined;
        }
        const children = [];
        const { prev, prevName, err } = this._getZNode(path);
        if (err) {
            return process.nextTick(() => callback(err));
        }
        if (watcher) {
            prev.children[prevName].emitter.on(
                'watcher',
                event => watcher(event));
        }
        Object.entries(prev.children[prevName].children).forEach(kv => {
            if (!kv[1].isSequential) {
                children.push(kv[0]);
            }
        });
        return process.nextTick(() => callback(null, children, {}));
    }

    removeRecur(path, callback) {
        const { prev, prevName, err } = this._getZNode(path);
        if (err) {
            return process.nextTick(() => callback(err));
        }
        delete prev[prevName];
        return process.nextTick(() => callback());
    }

    validatePath(path) {
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
        let currentPath = '';
        this.validatePath(path);
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
        // Remove the empty string
        const nodes = path.split('/').slice(1);
        async.eachSeries(nodes, (node, next) => {
            currentPath = `${currentPath}/${node}`;
            this.create(currentPath, data, acls, mode, error => {
                // Skip node exist error.
                if (error && error.getCode() ===
                    zookeeper.Exception.NODE_EXISTS) {
                    next(null);
                    return;
                }
                next(error);
            });
        }, err => {
            callback(err, currentPath);
        });
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
            return undefined;
        }
        return undefined;
    }

    // iterate over all the children
    _iterate(reverse, cb) {
        return this.__iterate(reverse, 0, _zkState, cb);
    }

    // for testing
    _dump() {
        return this._iterate(false, (level, parent, name, child) => {
            const padStr = new Array(level + 1).join(' ');
            // eslint-disable-next-line
            console.log(padStr, name, child.data);
        });
    }
}

module.exports = ZookeeperMock;
