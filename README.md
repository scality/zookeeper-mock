# zookeeper-mock
NodeJS Zookeeper Mock

This module can be used as a Zookeeper mock to simulate race
conditions in programs.

Examples of use:

```
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
```
