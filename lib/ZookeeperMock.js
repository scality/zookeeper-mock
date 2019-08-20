const assert = require('assert');
const async = require('async');
const EventEmitter = require('events');
const zookeeper = require('node-zookeeper-client');

/**
 * This mock object is to overwrite the zkClient to e.g. simulate race
 * conditions.  It is not complete so far but could be extended.
 * @class
 */
class ZookeeperMock {
    constructor() {
        this._zkState = {
            children: {},
            emitter: new EventEmitter(),
            isSequential: false,
            counter: 0,
        };
    }

    zeroPad(path, n, width) {
        const _n = `${n}`;
        return path + (_n.length >= width ? _n :
                       new Array(width - _n.length + 1).join('0') + _n);
    }

    create(path, data, acls, mode, callback) {
        // console.log('CREATE', path, data, acls, mode);
        // XXX check data is a buffer
        let cur = this._zkState;
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
                new zookeeper.Exception(zookeeper.Exception.NODE_EXISTS,
                                        'NODE_EXISTS',
                                        path,
                                        this.create));
        }
        // console.log('PREV', prev, 'NAME', prevName);
        if (mode === zookeeper.CreateMode.PERSISTENT_SEQUENTIAL) {
            _path = this.zeroPad(path, prev.counter, 10);
            prevName = this.zeroPad(prevName, prev.counter, 10);
            prev.children[prevName] = {};
            prev.counter++;
        }
        // console.log('_PATH', _path);
        prev.children[prevName].data = data;
        prev.children[prevName].acls = acls;
        prev.children[prevName].mode = mode;
        prev.emitter.emit('NODE_CHILDREN_CHANGED', {});
        return process.nextTick(() => callback(null, _path));
    }

    _getZNode(path) {
        let next = this._zkState;
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
            prev.children[prevName].emitter.once(
                'NODE_CHILDREN_CHANGED',
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

    // internal for testing
    _dump(pad, obj) {
        const keys = Object.keys(obj);
        let i;
        for (i = 0; i < keys.length; i++) {
            const name = keys[i];
            const child = obj[keys[i]];
            const padStr = new Array(pad + 1).join(' ');
            // eslint-disable-next-line
            console.log(padStr, name, child.data);
            if (child.children) {
                return this._dump(pad + 1, child.children);
            }
        }
        return undefined;
    }

    // for testing
    dump() {
        return this._dump(0, this._zkState.children);
    }
}

module.exports = ZookeeperMock;
