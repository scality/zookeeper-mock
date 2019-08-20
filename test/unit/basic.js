const assert = require('assert');
const zookeeper = require('node-zookeeper-client');

const ZookeeperMock = require('../../lib/ZookeeperMock');

describe('zookeeper mock', () => {
    afterEach(() => {
        const zkc = new ZookeeperMock();
        zkc._resetState();
    });

    it('basic create/get/set/getchildren', done => {
        const zkc = new ZookeeperMock();
        const topLevelPath = '/foo/bar';
        const path1 = `${topLevelPath}/qux`;
        const path2 = `${topLevelPath}/quxx`;
        const data1 = new Buffer('{ quz: 42, quxx: 43 }');
        const data2 = new Buffer('{ quz: 42, quxx: 44 }');
        const data3 = new Buffer('{ quz: 42, quxx: 45 }');
        zkc.create(path1, data1, {}, {}, err => {
            assert.ifError(err);
            zkc.getData(path1, (err, data) => {
                assert.deepStrictEqual(data, data1);
                zkc.setData(path1, data2, err => {
                    assert.ifError(err);
                    zkc.getData(path1, (err, data) => {
                        assert.deepStrictEqual(data, data2);
                        zkc.create(path2, data3, {}, {}, err => {
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
        const zkc = new ZookeeperMock();
        const topLevelPath = '/foo/bar';
        const path1 = `${topLevelPath}/qux`;
        const data1 = new Buffer('{ quz: 42, quxx: 43 }');
        zkc.create(path1, data1, {}, {}, err => {
            assert.ifError(err);
            zkc.create(path1, data1, {}, {}, err => {
                assert(err !== null);
                assert(err.getCode() === zookeeper.Exception.NODE_EXISTS);
                return done();
            });
        });
    });

    it('getData on non-existent path', done => {
        const zkc = new ZookeeperMock();
        const path1 = '/foo';
        zkc.getData(path1, err => {
            assert(err && err.name === 'NO_NODE');
            return done();
        });
    });

    it('mkdirp', done => {
        const zkc = new ZookeeperMock();
        const path1 = '/foo/bar/qux/quz';
        const data = new Buffer('xxx');
        zkc.mkdirp(path1, data, err => {
            assert.ifError(err);
            return done();
        });
    });

    it('_getZNode, _getPath', done => {
        const zkc = new ZookeeperMock();
        const path1 = '/foo/bar/qux';
        const path2 = `${path1}/quz`;
        const data = new Buffer('xxx');
        zkc.mkdirp(path1, data, err => {
            assert.ifError(err);
            const znode = zkc._getZNode(path2);
            assert.ifError(znode.err);
            const path = zkc._getPath(znode.prev);
            assert(path === path1);
            return done();
        });
    });

    it('_zeropad', () => {
        const zkc = new ZookeeperMock();
        assert.deepEqual(zkc._zeroPad('/foo', 42, 10), '/foo0000000042');
    });

    it('sequential', done => {
        const zkc = new ZookeeperMock();
        const topLevelPath = '/foo';
        const path1 = `${topLevelPath}/xxx`;
        const path2 = `${topLevelPath}/xxx`;
        const data1 = new Buffer('42');
        const data2 = new Buffer('43');
        zkc.create(path1, data1, {},
                   zookeeper.CreateMode.PERSISTENT_SEQUENTIAL,
                   err => {
                       assert.ifError(err);
                       zkc.create(path2, data2, {},
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
        const zkc = new ZookeeperMock();
        const topLevelPath = '/workflow-engine';
        const path1 = `${topLevelPath}/xxx`;
        const path2 = `${topLevelPath}/xxx`;
        const path3 = `${topLevelPath}/xxx`;
        const data1 = new Buffer('42');
        const data2 = new Buffer('43');
        const data3 = new Buffer('44');
        zkc.create(
            path1, data1, {},
            zookeeper.CreateMode.PERSISTENT_SEQUENTIAL,
            err => {
                assert.ifError(err);
                // simulate a race
                process.nextTick(() => {
                    zkc.create(path2, data2, {},
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
                    zkc.create(path3, data3, {},
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

    it('watcher NODE_CHILDREN_CHANGED', done => {
        const zkc1 = new ZookeeperMock();
        const zkc2 = new ZookeeperMock();
        const topLevelPath = '/workflow-engine-data-source';
        const path1 = `${topLevelPath}/xxx`;
        const data1 = new Buffer('{prop1: "foo", prop2: "bar"}');
        const data2 = new Buffer('');
        let eventCount = 0;
        zkc2.create(
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
                        if (eventCount === 2) {
                            zkc2.getChildren(
                                topLevelPath,
                                        (err, children) => {
                                            assert.ifError(err);
                                            assert(children.length === 0);
                                            return done();
                                        });
                        }
                    },
                    (err, children) => {
                        assert.ifError(err);
                        assert(children.length === 0);
                        zkc1.create(
                            path1, data1, {},
                            zookeeper.CreateMode.EPHEMERAL,
                            err => {
                                assert.ifError(err);
                                zkc1.close();
                            });
                    });
            });
    });
});
