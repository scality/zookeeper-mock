const assert = require('assert');
const zookeeper = require('node-zookeeper-client');

const ZookeeperMock = require('../../lib/ZookeeperMock');

describe('zookeeper mock', () => {
    it.only('basic create/get/set/getchildren', done => {
        const zkc = new ZookeeperMock();
        const subPath = '/foo/bar';
        const path1 = `${subPath}/qux`;
        const path2 = `${subPath}/quxx`;
        const data1 = {
            quz: 42,
            quxx: 43
        };
        const data2 = {
            quz: 42,
            quxx: 44
        };
        const data3 = {
            quz: 42,
            quxx: 45
        };
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
                            zkc.getChildren(subPath, (err, children) => {
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

    it.only('eexist', done => {
        const zkc = new ZookeeperMock();
        const subPath = '/foo/bar';
        const path1 = `${subPath}/qux`;
        const data1 = {
            quz: 42,
            quxx: 43
        };
        zkc.create(path1, data1, {}, {}, err => {
            assert.ifError(err);
            zkc.create(path1, data1, {}, {}, err => {
                assert(err !== null);
                assert(err.getCode() === zookeeper.Exception.NODE_EXISTS);
                return done();
            });
        });
    });

    it.only('zeropad', () => {
        const zkc = new ZookeeperMock();
        assert.deepEqual(zkc.zeroPad('/foo', 42, 10), '/foo0000000042');
    });

    it.only('sequential', done => {
        const zkc = new ZookeeperMock();
        const subPath = '/foo';
        const path1 = `${subPath}/xxx`;
        const path2 = `${subPath}/xxx`;
        const data1 = 42;
        const data2 = 43;
        zkc.create(path1, data1, {},
                   zookeeper.CreateMode.PERSISTENT_SEQUENTIAL,
                   err => {
                       assert.ifError(err);
                       zkc.create(path2, data2, {},
                                  zookeeper.CreateMode.PERSISTENT_SEQUENTIAL,
                                  err => {
                                      assert.ifError(err);
                                      zkc.getChildren(subPath,
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

    it.only('barrier', done => {
        // the goal here is to implement a barrier allowing processes
        // to continue after reaching a quota
        const zkc = new ZookeeperMock();
        const subPath = '/workflow-engine/xxx';
        const path1 = `${subPath}/foo`;
        const path2 = `${subPath}/bar`;
        const path3 = `${subPath}/qux`;
        const data1 = 42;
        const data2 = 43;
        const data3 = 44;
        zkc.create(path1, data1, {}, {}, err => {
            assert.ifError(err);
            zkc.getChildren(subPath, () => done(),
                            (err, children) => {
                                assert.ifError(err);
                                if (children.length < 2) {
                                    process.nextTick(() => {
                                        zkc.create(path2,
                                                   data2,
                                                   {},
                                                   {},
                                                   err => {
                                                       assert.ifError(err);
                                                   });
                                    });
                                }
                            });
            zkc.getChildren(subPath, () => {
                // do nothing to avoid double callback
            }, (err, children) => {
                assert.ifError(err);
                if (children.length < 2) {
                    process.nextTick(() => {
                        zkc.create(path3, data3, {}, {}, err => {
                            assert.ifError(err);
                        });
                    });
                }
            });
        });
    });

    it.only('barrier_signal', done => {
        // the goal here is to implement a barrier with a SEQUENCE
        // to wake up exactly one process
        const zkc = new ZookeeperMock();
        const subPath = '/workflow-engine';
        const path1 = `${subPath}/xxx`;
        const path2 = `${subPath}/xxx`;
        const path3 = `${subPath}/xxx`;
        const data1 = 42;
        const data2 = 43;
        const data3 = 44;
        zkc.create(path1, data1, {},
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
});
