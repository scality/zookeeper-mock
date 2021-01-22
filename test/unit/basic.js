const assert = require('assert');
const zookeeper = require('node-zookeeper-client');

const ZookeeperMock = require('../../lib/ZookeeperMock');

describe('zookeeper mock', () => {
    const zk = new ZookeeperMock({
        doLog: false,
        maxRandomDelay: 0
    });

    afterEach(() => {
        zk._resetState();
    });

    it('connect event', done => {
        const zkc = zk.createClient();
        zkc.once('connected', () => (done()));
        zkc.connect();
    });

    it('shall extract basePath correctly', done => {
        assert.strictEqual(zk._basePath(), '');
        assert.strictEqual(zk._basePath('localhost:2181/foo'), '/foo');
        assert.strictEqual(zk._basePath('localhost:2181'), '');
        return done();
    });

    it('_getZNode, _getPath', done => {
        const zkc = zk.createClient();
        const path1 = '/foo/bar/qux';
        const path2 = `${path1}/quz`;
        const data = new Buffer('xxx');
        zkc.mkdirp(path1, data, err => {
            assert.ifError(err);
            const znode = zkc._getZNode(path2, false);
            assert.ifError(znode.err);
            const path = zkc._getPath(znode.parent);
            assert(path === path1);
            return done();
        });
    });

    it('ephemeral nodes cannot have child', done => {
        const path1 = '/xxx';
        const data1 = new Buffer('{prop1: "foo", prop2: "bar"}');
        const path2 = '/xxx/yyy';
        const data2 = new Buffer('{prop1: "foo", prop2: "bar"}');
        const zkc = zk.createClient();
        zkc.on('connected', () => {
            zkc.mkdirp(
                path1, data1, {},
                zookeeper.CreateMode.EPHEMERAL,
                err => {
                    assert.ifError(err);
                    zkc.mkdirp(
                        path2, data2, {},
                        zookeeper.CreateMode.EPHEMERAL,
                        err => {
                            assert(err);
                            assert.equal(
                                err.code,
                                zookeeper.Exception.NO_CHILDREN_FOR_EPHEMERALS);
                            return done();
                        });
                });
        });
        zkc.connect();
    });

    it('ephemeral nodes deleted after close', done => {
        const path1 = '/xxx';
        const data1 = new Buffer('{prop1: "foo", prop2: "bar"}');
        const path2 = '/xxx/yyy';
        const data2 = new Buffer('{prop1: "foo", prop2: "bar"}');
        const zkc = zk.createClient();
        zkc.on('connected', () => {
            zkc.mkdirp(
                path1, data1, {},
                zookeeper.CreateMode.PERSISTENT,
                err => {
                    assert.ifError(err);
                    zkc.mkdirp(
                        path2, data2, {},
                        zookeeper.CreateMode.EPHEMERAL,
                        err => {
                            assert.ifError(err);
                            const znode1 = zkc._getZNode(path2, true);
                            assert.ifError(znode1.err);
                            zkc.close();
                            const znode2 = zkc._getZNode(path2, true);
                            assert(znode2.err);
                            assert.equal(znode2.err.code,
                                         zookeeper.Exception.NO_NODE);
                            return done();
                        });
                });
        });
        zkc.connect();
    });

    it('ephemeral nodes deleted per conn after close', done => {
        const path1 = '/xxx';
        const data1 = new Buffer('{prop1: "foo", prop2: "bar"}');
        const path2 = '/yyy';
        const data2 = new Buffer('{prop1: "foo", prop2: "bar"}');
        const zkc1 = zk.createClient();
        const zkc2 = zk.createClient();
        zkc1.on('connected', () => {
            zkc1.mkdirp(
                path1, data1, {},
                zookeeper.CreateMode.EPHEMERAL,
                err => {
                    assert.ifError(err);
                    zkc2.on('connected', () => {
                        zkc2.mkdirp(
                            path2, data2, {},
                            zookeeper.CreateMode.EPHEMERAL,
                            err => {
                                assert.ifError(err);
                                let znode1;
                                let znode2;
                                znode1 = zkc1._getZNode(path1, true);
                                assert.ifError(znode1.err);
                                znode2 = zkc2._getZNode(path2, true);
                                assert.ifError(znode2.err);
                                zkc1.close();
                                znode1 = zkc2._getZNode(path1, true);
                                assert(znode1.err);
                                znode2 = zkc2._getZNode(path2, true);
                                assert.ifError(znode2.err);
                                return done();
                            });
                    });
                    zkc2.connect();
                });
        });
        zkc1.connect();
    });

    it('basic create/get/set/getchildren', done => {
        const zkc = zk.createClient();
        const topLevelPath = '/foo/bar';
        const path1 = `${topLevelPath}/qux`;
        const path2 = `${topLevelPath}/quxx`;
        const data1 = new Buffer('{ quz: 42, quxx: 43 }');
        const data2 = new Buffer('{ quz: 42, quxx: 44 }');
        const data3 = new Buffer('{ quz: 42, quxx: 45 }');
        zkc.mkdirp(path1, data1, {}, {}, err => {
            assert.ifError(err);
            zkc.getData(path1, (err, data) => {
                assert.deepStrictEqual(data, data1);
                zkc.setData(path1, data2, err => {
                    assert.ifError(err);
                    zkc.getData(path1, (err, data) => {
                        assert.deepStrictEqual(data, data2);
                        zkc.mkdirp(path2, data3, {}, {}, err => {
                            assert.ifError(err);
                            zkc.getChildren(topLevelPath, (err, children) => {
                                assert.deepStrictEqual(children,
                                    ['qux', 'quxx']);
                                return done();
                            });
                        });
                    });
                });
            });
        });
    });

    it('eexist', done => {
        const zkc = zk.createClient();
        const topLevelPath = '/foo/bar';
        const path1 = `${topLevelPath}/qux`;
        const data1 = new Buffer('{ quz: 42, quxx: 43 }');
        zkc.mkdirp(path1, data1, {}, {}, err => {
            assert.ifError(err);
            zkc.mkdirp(path1, data1, {}, {}, err => {
                assert(err !== null);
                assert(err.getCode() === zookeeper.Exception.NODE_EXISTS);
                return done();
            });
        });
    });

    it('getData on non-existent path', done => {
        const zkc = zk.createClient();
        const path1 = '/foo';
        zkc.getData(path1, err => {
            assert(err && err.name === 'NO_NODE');
            return done();
        });
    });

    it('mkdirp', done => {
        const zkc = zk.createClient();
        const path1 = '/foo/bar/qux/quz';
        const data = new Buffer('xxx');
        zkc.mkdirp(path1, data, err => {
            assert.ifError(err);
            return done();
        });
    });

    it('exists', done => {
        const zkc = zk.createClient();
        const path1 = '/a1';
        const path2 = '/a2';
        const data1 = new Buffer('42');
        zkc.mkdirp(path1, data1, {}, {}, err => {
            assert.ifError(err);
            zkc.remove(path1, (err, stat) => {
                assert.ifError(err);
                assert(stat);
                zkc.exists(path2, (err, stat) => {
                    assert.ifError(err);
                    assert.strictEqual(stat, null);
                    return done();
                });
            });
        });
    });

    it('remove', done => {
        const zkc = zk.createClient();
        const topLevelPath = '/a1';
        const path1 = `${topLevelPath}/a1`;
        const path2 = '/a2';
        const data1 = new Buffer('42');
        zkc.mkdirp(path1, data1, {}, {}, err => {
            assert.ifError(err);
            zkc.remove(topLevelPath, err => {
                assert(err);
                assert.strictEqual(err.code, zookeeper.Exception.NOT_EMPTY);
                zkc.remove(path1, (err, removed) => {
                    assert.ifError(err);
                    assert(removed);
                    zkc.remove(path2, err => {
                        assert(err);
                        assert.strictEqual(err.code,
                            zookeeper.Exception.NO_NODE);
                        return done();
                    });
                });
            });
        });
    });

    it('ephemeral sequential', done => {
        const zkc = zk.createClient();
        const topLevelPath = '/foo';
        const path1 = `${topLevelPath}/xxx`;
        const path2 = `${topLevelPath}/xxx`;
        const data1 = new Buffer('42');
        const data2 = new Buffer('43');
        zkc.mkdirp(path1, data1, {},
            zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL,
            err => {
                assert.ifError(err);
                zkc.mkdirp(path2, data2, {},
                    zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL,
                    err => {
                        assert.ifError(err);
                        zkc.getChildren(topLevelPath,
                            (err, children) => {
                                assert.ifError(err);
                                assert.deepStrictEqual(children,
                                    ['xxx0000000000',
                                        'xxx0000000001']);
                                return done();
                            });
                    });
            });
    });

    it('persistent sequential', done => {
        const zkc = zk.createClient();
        const topLevelPath = '/foo';
        const path1 = `${topLevelPath}/xxx`;
        const path2 = `${topLevelPath}/xxx`;
        const data1 = new Buffer('42');
        const data2 = new Buffer('43');
        zkc.mkdirp(path1, data1, {},
            zookeeper.CreateMode.PERSISTENT_SEQUENTIAL,
            err => {
                assert.ifError(err);
                zkc.mkdirp(path2, data2, {},
                    zookeeper.CreateMode.PERSISTENT_SEQUENTIAL,
                    err => {
                        assert.ifError(err);
                        zkc.getChildren(topLevelPath,
                            (err, children) => {
                                assert.ifError(err);
                                assert.deepStrictEqual(children,
                                    ['xxx0000000000',
                                        'xxx0000000001']);
                                return done();
                            });
                    });
            });
    });

    it('barrier_sequence', done => {
        // the goal here is to implement a barrier with a SEQUENCE
        // to wake up exactly one process
        const zkc = zk.createClient();
        const topLevelPath = '/workflow-engine';
        const path1 = `${topLevelPath}/xxx`;
        const path2 = `${topLevelPath}/xxx`;
        const path3 = `${topLevelPath}/xxx`;
        const data1 = new Buffer('42');
        const data2 = new Buffer('43');
        const data3 = new Buffer('44');
        zkc.mkdirp(
            path1, data1, {},
            zookeeper.CreateMode.PERSISTENT_SEQUENTIAL,
            err => {
                assert.ifError(err);
                // simulate a race
                process.nextTick(() => {
                    zkc.mkdirp(path2, data2, {},
                        zookeeper.CreateMode.PERSISTENT_SEQUENTIAL,
                        (err, path) => {
                            assert.ifError(err);
                            if (path === `${path1}0000000002`) {
                                // th1 firing
                                return done();
                            }
                            return undefined;
                        });
                });
                process.nextTick(() => {
                    zkc.mkdirp(path3, data3, {},
                        zookeeper.CreateMode.PERSISTENT_SEQUENTIAL,
                        (err, path) => {
                            assert.ifError(err);
                            if (path === `${path1}0000000002`) {
                                // th2 firing
                                return done();
                            }
                            return undefined;
                        });
                });
            });
    });

    it('watcher - NODE_CHILDREN_CHANGED', done => {
        const zkc1 = zk.createClient();
        const zkc2 = zk.createClient();
        const topLevelPath = '/workflow-engine-data-source';
        const path1 = `${topLevelPath}/xxx`;
        const data1 = new Buffer('{prop1: "foo", prop2: "bar"}');
        const data2 = new Buffer('');
        let eventCount = 0;
        zkc2.mkdirp(
            topLevelPath, data2, {},
            zookeeper.CreateMode.PERSISTENT,
            err => {
                assert.ifError(err);
                zkc2.getChildren(
                    topLevelPath,
                    event => {
                        assert(
                            event.type ===
                            zookeeper.Event.NODE_CHILDREN_CHANGED);
                        eventCount++;
                        if (eventCount === 1) {
                            zkc2.getChildren(
                                topLevelPath,
                                (err, children) => {
                                    assert.ifError(err);
                                    assert(children.length === 1);
                                    return done();
                                });
                        }
                    },
                    (err, children) => {
                        assert.ifError(err);
                        assert(children.length === 0);
                        zkc1.mkdirp(
                            path1, data1, {},
                            zookeeper.CreateMode.EPHEMERAL,
                            err => {
                                assert.ifError(err);
                            });
                    });
            });
    });

    it('watcher - NODE_DELETED', done => {
        const zkc = zk.createClient();
        const topLevelPath = '/a1';
        const path1 = `${topLevelPath}/a1`;
        const data1 = new Buffer('42');
        zkc.mkdirp(path1, data1, {}, {}, err => {
            assert.ifError(err);
            zkc.getData(path1, event => {
                assert(event);
                assert.strictEqual(event.type, zookeeper.Event.NODE_DELETED);
            }, err => {
                assert.ifError(err);
                zkc.remove(path1, (err, removed) => {
                    assert.ifError(err);
                    assert(removed);
                    return done();
                });
            });
        });
    });

    it('watcher - NODE_DATA_CHANGED', done => {
        const zkc = zk.createClient();
        const topLevelPath = '/a1';
        const path1 = `${topLevelPath}/a1`;
        const data1 = new Buffer('42');
        const data2 = new Buffer('43');
        zkc.mkdirp(path1, data1, {}, {}, err => {
            assert.ifError(err);
            zkc.getData(path1, event => {
                assert(event);
                assert.strictEqual(event.type,
                    zookeeper.Event.NODE_DATA_CHANGED);
            }, err => {
                assert.ifError(err);
                zkc.setData(path1, data2, err => {
                    assert.ifError(err);
                    return done();
                });
            });
        });
    });

    it('introduces delays', done => {
        const MAX_RANDOM_DELAY = 500;
        const MIN_RANDOM_DELAY = 400;
        const zk2 = new ZookeeperMock({
            doLog: false,
            maxRandomDelay: MAX_RANDOM_DELAY,
            minRandomDelay: MIN_RANDOM_DELAY
        });
        const zkc = zk2.createClient();
        const topLevelPath = '/a1';
        const path1 = `${topLevelPath}/a1`;
        const data1 = new Buffer('42');
        const before = new Date().getTime();
        zkc.mkdirp(path1, data1, {}, {}, err => {
            assert.ifError(err);
            const after = new Date().getTime();
            assert((after - before) > MIN_RANDOM_DELAY);
            return done();
        });
    });
});
