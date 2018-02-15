// const Fiber = require("fibers");
const { createJobClass } = require("./jobFactory");

const { Job, ...JobPrivate } = createJobClass({ isTest: true });

class DDP {
  call(name, params, cb = null) {
    if (cb === null || typeof cb !== "function") {
      switch (name) {
        case "root_true":
          return true;
        case "root_false":
          return false;
        case "root_param":
          return params[0];
        case "root_error":
          throw new Error("Method failed");
        default:
          throw new Error("Bad method in call");
      }
    } else {
      switch (name) {
        case "root_true":
          process.nextTick(() => cb(null, true));
          break;
        case "root_false":
          process.nextTick(() => cb(null, false));
          break;
        case "root_param":
          process.nextTick(() => cb(null, params[0]));
          break;
        case "root_error":
          process.nextTick(() => cb(new Error("Method failed")));
          break;
        default:
          process.nextTick(() => cb(new Error("Bad method in call")));
      }
    }
  }

//   connect() {
//     return process.nextTick(() => cb(null));
//   }

//   close() {
//     return process.nextTick(() => cb(null));
//   }

//   subscribe() {
//     return process.nextTick(() => cb(null));
//   }

//   observe() {
//     return process.nextTick(() => cb(null));
//   }
}

const makeDdpStub = (action) => (name, params, cb) => {
  const [err, res] = Array.from(action(name, params));
  if (cb) {
    return process.nextTick(() => cb(err, res));
  } else if (err) {
    throw err;
  }
  return res;
};

describe("Job", () => {
  it("has class constants", () => {
    expect(typeof Job.forever).toBe("number");
    expect(typeof Job.jobPriorities).toBe("object");
    expect(Object.keys(Job.jobPriorities)).toHaveLength(5);
    expect(Array.isArray(Job.jobRetryBackoffMethods)).toBe(true);
    expect(Job.jobRetryBackoffMethods).toHaveLength(2);
    expect(Array.isArray(Job.jobStatuses)).toBe(true);
    expect(Job.jobStatuses).toHaveLength(7);
    expect(Array.isArray(Job.jobLogLevels)).toBe(true);
    expect(Job.jobLogLevels).toHaveLength(4);
    expect(Array.isArray(Job.jobStatusCancellable)).toBe(true);
    expect(Job.jobStatusCancellable).toHaveLength(4);
    expect(Array.isArray(Job.jobStatusPausable)).toBe(true);
    expect(Job.jobStatusPausable).toHaveLength(2);
    expect(Array.isArray(Job.jobStatusRemovable)).toBe(true);
    expect(Job.jobStatusRemovable).toHaveLength(3);
    expect(Array.isArray(Job.jobStatusRestartable)).toBe(true);
    expect(Job.jobStatusRestartable).toHaveLength(2);
    expect(Array.isArray(Job.ddpPermissionLevels)).toBe(true);
    expect(Job.ddpPermissionLevels).toHaveLength(4);
    expect(Array.isArray(Job.ddpMethods)).toBe(true);
    expect(Job.ddpMethods).toHaveLength(18);
    expect(typeof Job.ddpMethodPermissions).toBe("object");
    expect(Object.keys(Job.ddpMethodPermissions)).toHaveLength(Job.ddpMethods.length);
  });

  it("has a _ddp_apply class variable that defaults as undefined outside of Meteor", () => {
    expect(Job._ddp_apply).toBeUndefined();
  });

  // it("has a processJobs method that is the JobQueue constructor", () => {
  //   expect(Job.processJobs).toEqual(Job.JobQueue);
  // });

  describe("setDDP", () => {
    const ddp = new DDP();

    describe("default setup", () => {
      it("throws if given a non-ddp object", () => {
        expect(() => Job.setDDP({})).toThrow(/Bad ddp object/);
      });

      it("properly sets the default _ddp_apply class variable", () => (
        new Promise((resolve) => {
          const spy = jest.spyOn(ddp, "call");
          Job.setDDP(ddp);

          Job._ddp_apply("test", [], () => {
            expect(spy).toHaveBeenCalledTimes(1);
            spy.mockClear();
            resolve();
          });
        })
      ));

      it("fails if subsequently called with a collection name", () => {
        expect(() => Job.setDDP(ddp, "test1")).toThrow(/Job.setDDP must specify/);
      });

      afterAll(() => {
        Job._ddp_apply = undefined; // eslint-disable-line camelcase
      });
    });

    describe("setup with collection name", () => {
      it("properly sets the default _ddp_apply class variable", () => (
        new Promise((resolve) => {
          const spy = jest.spyOn(ddp, "call");
          Job.setDDP(ddp, "test1");

          Job._ddp_apply.test1("test", [], () => {
            expect(spy).toHaveBeenCalledTimes(1);
            spy.mockClear();
            resolve();
          });
        })
      ));


      it("properly sets the _ddp_apply class variable when called with array", () => (
        new Promise((resolve) => {
          const spy = jest.spyOn(ddp, "call");
          Job.setDDP(ddp, ["test2", "test3"]);

          Job._ddp_apply.test2("test", [], () => {
            Job._ddp_apply.test3("test", [], () => {
              expect(spy).toHaveBeenCalledTimes(2);
              spy.mockClear();
              resolve();
            });
          });
        })
      ));

      it("fails if subsequently called without a collection name", () => {
        expect(() => Job.setDDP(ddp)).toThrow(/Job.setDDP must specify/);
      });

      afterAll(() => {
        Job._ddp_apply = undefined; // eslint-disable-line camelcase
      });
    });
  });

  // describe("Fiber support", () => {
  //   const ddp = new DDP();

  //   it("accepts a valid collection name and Fiber object and properly yields and runs", () => {
  //     const spy = jest.spyOn(ddp, "call");
  //     Job.setDDP(ddp, "test1", Fiber);

  //     const fib = Fiber(() => Job._ddp_apply.test1("test", []));
  //     fib.run();

  //     expect(spy).toHaveBeenCalledTimes(1);
  //     spy.mockClear();
  //   });

  //   // it("accepts a default collection name and valid Fiber object and properly yields and runs", () => {
  //   //   const spy = jest.spyOn(ddp, "call");
  //   //   Job.setDDP(ddp, Fiber);

  //   //   const fib = Fiber(() => Job._ddp_apply("test", []));
  //   //   fib.run();
  //   //   expect(spy).toHaveBeenCalledTimes(1);
  //   //   spy.mockClear();

  //   // });

  //   it("properly returns values from method calls", () => (
  //     new Promise((resolve) => {
  //       Job.setDDP(ddp, Fiber);

  //       const fib = Fiber(() => {
  //         expect(Job._ddp_apply("root_true", [])).toBeTruthy();
  //         expect(Job._ddp_apply("root_false", [])).toBeFalsy();
  //         // expect(Job._ddp_apply("root_param", [["a", 1, null]])).toEqual(["a", 1, null]);
  //         resolve();
  //       });

  //       fib.run();
  //     })
  //   ));

  //   it("properly propagates thrown errors within a Fiber", () => (
  //     new Promise((resolve) => {
  //       Job.setDDP(ddp, Fiber);

  //       const fib = Fiber(() => {
  //         expect(() => Job._ddp_apply("root_error", [])).toThrow(/Method failed/);
  //         expect(() => Job._ddp_apply("bad_method", [])).toThrow(/Bad method in call/);
  //         resolve();
  //       });

  //       fib.run();
  //     })
  //   ));

  //   afterEach(() => {
  //     Job._ddp_apply = undefined;
  //   });
  // });

  describe("private function", () => {
    // Note! These are internal helper functions, NOT part of the external API!
    describe("methodCall", () => {
      const ddp = new DDP();
      let spy;

      beforeAll(() => {
        spy = jest.spyOn(ddp, "call");
        Job.setDDP(ddp);
      });

      const { methodCall } = JobPrivate;

      it("should be a function", () => {
        expect(typeof methodCall).toBe("function");
      });

      it("should invoke the correct ddp method", () => (
        new Promise((resolve) => {
          methodCall("root", "true", [], (err, res) => {
            expect(spy).toHaveBeenCalledTimes(1);
            expect(spy.mock.calls[0][0]).toBe("root_true");
            expect(res).toBeTruthy();
            resolve();
          });
        })
      ));

      it("should pass the correct method parameters", () => (
        new Promise((resolve) => {
          const params = ["a", 1, [1, 2, 3], { foo: "bar" }];
          methodCall("root", "param", params, (err, res) => {
            expect(spy).toHaveBeenCalledTimes(1);
            expect(spy.mock.calls[0][0]).toEqual("root_param");
            expect(spy.mock.calls[0][1]).toEqual(params);
            expect(res).toBe("a");
            resolve();
          });
        })
      ));

      it("should invoke the after callback when provided", () => (
        new Promise((resolve) => {
          const after = jest.fn(() => true);
          return methodCall("root", "false", [], (err, res) => {
            expect(spy).toHaveBeenCalledTimes(1);
            expect(spy.mock.calls[0][0]).toEqual("root_false");
            expect(spy.mock.calls[0][1]).toEqual([]);
            expect(after).toHaveBeenCalledTimes(1);
            expect(res).toBe(true);
            resolve();
          }, after);
        })
      ));

      it("shouldn't invoke the after callback when error", () => (
        new Promise((resolve) => {
          const after = jest.fn(() => true);
          return methodCall("root", "error", [], (err, res) => {
            expect(spy).toHaveBeenCalledTimes(1);
            expect(spy.mock.calls[0][0]).toEqual("root_error");
            expect(spy.mock.calls[0][1]).toEqual([]);
            expect(after).toHaveBeenCalledTimes(0);
            expect(res).toBeUndefined();
            expect(() => { throw err; }).toThrow(/Method failed/);
            resolve();
          }, after);
        })
      ));

      it("should invoke the correct ddp method without callback", () => {
        const res = methodCall("root", "true", []);
        expect(spy).toHaveBeenCalledTimes(1);
        expect(spy.mock.calls[0][0]).toEqual("root_true");
        expect(res).toBe(true);
        expect(res).toBe(true);
      });

      it("should pass the correct method parameters without callback", () => {
        const params = ["a", 1, [1, 2, 3], { foo: "bar" }];
        const res = methodCall("root", "param", params);
        expect(spy).toHaveBeenCalledTimes(1);
        expect(spy.mock.calls[0][0]).toEqual("root_param");
        expect(spy.mock.calls[0][1]).toEqual(params);
        expect(res).toBe("a");
      });

      it("should invoke the after callback when provided without callback", () => {
        const after = jest.fn(() => true);
        const res = methodCall("root", "false", [], undefined, after);
        expect(spy).toHaveBeenCalledTimes(1);
        expect(spy.mock.calls[0][0]).toEqual("root_false");
        expect(spy.mock.calls[0][1]).toEqual([]);
        expect(after).toHaveBeenCalledTimes(1);
        expect(res).toBe(true);
      });

      it("should throw on error when invoked without callback", () => {
        const after = jest.fn(() => true);
        let res;
        expect(() => { res = methodCall("root", "error", [], undefined, after); }).toThrow(/Method failed/);
        expect(spy).toHaveBeenCalledTimes(1);
        expect(spy.mock.calls[0][0]).toEqual("root_error");
        expect(spy.mock.calls[0][1]).toEqual([]);
        expect(after).toHaveBeenCalledTimes(0);
        expect(res).toBeUndefined();
      });

      afterEach(() => ddp.call.mockClear());

      afterAll(() => {
        Job._ddp_apply = undefined; // eslint-disable-line camelcase
      });
    });

    describe("optionsHelp", () => {
      const { optionsHelp } = JobPrivate;
      const foo = {
        bar: "bat"
      };
      const gizmo = () => {};

      it("should return options and a callback when both are provided", () => {
        const res = optionsHelp([foo], gizmo);
        expect(res).toEqual([foo, gizmo]);
      });

      it("should handle a missing callback and return only options", () => {
        const res = optionsHelp([foo]);
        expect(res).toEqual([foo, undefined]);
      });

      it("should handle missing options and return empty options and the callback", () => {
        const res = optionsHelp([], gizmo);
        expect(res).toEqual([{}, gizmo]);
      });

      it("should handle when both options and callback are missing", () => {
        const res = optionsHelp([], undefined);
        expect(res).toEqual([{}, undefined]);
      });

      it("should throw an error when an invalid callback is provided", () => {
        expect(() => optionsHelp([foo], 5)).toThrow(/options not an object or bad callback/);
      });

      it("should throw an error when a non-array is passed for options", () => {
        expect(() => optionsHelp(foo, gizmo)).toThrow(/must be an Array with zero or one elements/);
      });

      it("should throw an error when a bad options array is passed", () => {
        expect(() => optionsHelp([foo, 5], gizmo)).toThrow(/must be an Array with zero or one elements/);
      });
    });


    describe("splitLongArray", () => {
      const { splitLongArray } = JobPrivate;
      const longArray = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

      it("should properly split an array", () => {
        const res = splitLongArray(longArray, 4);
        expect(res).toEqual([
          [0, 1, 2, 3],
          [4, 5, 6, 7],
          [8, 9, 10, 11]
        ]);
      });

      it("should handle remainders correctly", () => {
        const res = splitLongArray(longArray, 5);
        expect(res).toEqual([
          [0, 1, 2, 3, 4],
          [5, 6, 7, 8, 9],
          [10, 11]
        ]);
      });

      it("should handle an empty array", () => {
        const res = splitLongArray([], 5);
        expect(res).toEqual([]);
      });

      it("should handle a single element array", () => {
        const res = splitLongArray([0], 5);
        expect(res).toEqual([[0]]);
      });

      it("should throw if not given an array", () => {
        expect(() => splitLongArray({ foo: "bar" }, 5)).toThrow(/splitLongArray: bad params/);
      });

      it("should throw if given an out of range max value", () => {
        expect(() => splitLongArray(longArray, 0)).toThrow(/splitLongArray: bad params/);
      });

      it("should throw if given an invalid max value", () => {
        expect(() => splitLongArray(longArray, "cow")).toThrow(/splitLongArray: bad params/);
      });
    });

    describe("concatReduce", () => {
      const { concatReduce } = JobPrivate;

      it("should concat a to b", () => {
        expect(concatReduce([1], 2)).toEqual([1, 2]);
      });

      it("should work with non array for the first param", () => {
        expect(concatReduce(1, 2)).toEqual([1, 2]);
      });
    });

    describe("reduceCallbacks", () => {
      const { reduceCallbacks } = JobPrivate;

      it("should return undefined if given a falsy callback", () => {
        expect(reduceCallbacks(undefined, 5)).toBeUndefined();
      });

      it("should properly absorb the specified number of callbacks", () => {
        const spy = jest.fn();
        const cb = reduceCallbacks(spy, 3);
        cb(null, true);
        cb(null, false);
        cb(null, true);
        expect(spy).toHaveBeenCalledTimes(1);
        expect(spy).toHaveBeenCalledWith(null, true);
      });

      it("should properly reduce the callback results", () => {
        const spy = jest.fn();
        const cb = reduceCallbacks(spy, 3);
        cb(null, false);
        cb(null, false);
        cb(null, false);
        expect(spy).toHaveBeenCalledTimes(1);
        expect(spy).toHaveBeenCalledWith(null, false);
      });

      it("should properly reduce with a custom reduce function", () => {
        const { concatReduce } = JobPrivate;
        const spy = jest.fn();
        const cb = reduceCallbacks(spy, 3, concatReduce, []);
        cb(null, false);
        cb(null, true);
        cb(null, false);
        expect(spy).toHaveBeenCalledTimes(1);
        expect(spy).toHaveBeenCalledWith(null, [false, true, false]);
      });

      it("should throw if called too many times", () => {
        const spy = jest.fn();
        const cb = reduceCallbacks(spy, 2);
        cb(null, true);
        cb(null, true);
        expect(cb).toThrow(/reduceCallbacks callback invoked more than requested/);
      });

      it("should throw if given a non-function callback", () => {
        expect(() => reduceCallbacks(5)).toThrow(/Bad params given to reduceCallbacks/);
      });

      it("should throw if given an invalid number of callbacks to absorb", () => {
        expect(() => reduceCallbacks(() => {}, "cow")).toThrow(/Bad params given to reduceCallbacks/);
      });

      it("should throw if given an out of range number of callbacks to absorb", () => {
        expect(() => reduceCallbacks(() => {}, 0)).toThrow(/Bad params given to reduceCallbacks/);
      });

      it("should throw if given a non-function reduce", () => {
        expect(() => reduceCallbacks(() => { }, 5, 5)).toThrow(/Bad params given to reduceCallbacks/);
      });
    });

    describe("_setImmediate", () => {
      const { _setImmediate } = JobPrivate;

      it("should invoke the provided callback with args", () => (
        new Promise((resolve) => {
          const cb = (a, b) => {
            expect(a).toBe("foo");
            expect(b).toBe("bar");
            resolve();
          };
          _setImmediate(cb, "foo", "bar");
        })
      ));
    });

    return describe("_setInterval", () => {
      const { _setInterval, _clearInterval } = JobPrivate;

      it("should invoke the provided callback repeatedly with args", () => (
        new Promise((resolve) => {
          let cancel = null;
          let count = 0;
          const cb = (a, b) => {
            expect(a).toBe("foo");
            expect(b).toBe("bar");
            count += 1;
            if (count === 2) {
              _clearInterval(cancel);
              resolve();
            } else if (count > 2) {
              throw new Error("Interval called too many times");
            }
          };

          cancel = _setInterval(cb, 10, "foo", "bar");
        })
      ));
    });
  });

  describe("Job constructor", () => {
    const checkJob = (job) => {
      expect(job).toBeInstanceOf(Job);
      expect(job.root).toBe("root");
      expect(job.type).toBe("work");
      expect(job.data).toEqual({ foo: "bar" });
      expect(typeof job._doc).toBe("object");
      const doc = job._doc;
      expect(doc).not.toHaveProperty("_id");
      expect(doc.runId).toBeNull();
      expect(job.type).toBe(doc.type);
      expect(job.data).toEqual(doc.data);
      expect(typeof doc.status).toBe("string");
      expect(doc.updated).toBeInstanceOf(Date);
      expect(Array.isArray(doc.depends)).toBe(true);
      expect(Array.isArray(doc.resolved)).toBe(true);
      expect(typeof doc.priority).toBe("number");
      expect(typeof doc.retries).toBe("number");
      expect(typeof doc.retryWait).toBe("number");
      expect(typeof doc.retried).toBe("number");
      expect(typeof doc.retryBackoff).toBe("string");
      expect(doc.retryUntil).toBeInstanceOf(Date);
      expect(typeof doc.repeats).toBe("number");
      expect(typeof doc.repeatWait).toBe("number");
      expect(typeof doc.repeated).toBe("number");
      expect(doc.repeatUntil).toBeInstanceOf(Date);
      expect(doc.after).toBeInstanceOf(Date);
      expect(Array.isArray(doc.log)).toBe(true);
      expect(typeof doc.progress).toBe("object");
      expect(doc.created).toBeInstanceOf(Date);
    };

    it("should return a new valid Job object", () => {
      const job = new Job("root", "work", { foo: "bar" });
      checkJob(job);
    });

    it("should not work without 'new'", () => {
      expect(() => Job("root", "work", { foo: "bar" })).toThrow(/Cannot call a class as a function/);
    });

    it("should throw when given bad parameters", () => {
      expect(() => new Job()).toThrow(/new Job: bad parameter/);
    });

    it("should support using a valid job document", () => {
      const job = new Job("root", "work", { foo: "bar" });
      checkJob(job);
      const job2 = new Job("root", job.doc);
      checkJob(job2);
    });

    return it("should support using a valid oobject for root", () => {
      const job = new Job({
        root: "root"
      }, "work", { foo: "bar" });
      checkJob(job);
      const job2 = new Job({
        root: "root"
      }, job.doc);
      checkJob(job2);
    });
  });

  describe("job mutator method", () => {
    let job = null;
    let doc = null;

    beforeEach(() => {
      job = new Job("root", "work", {});
      doc = job._doc;
    });

    describe(".depends()", () => {
      it("should properly update the depends property", () => {
        const jobA = new Job("root", "work", {});
        jobA._doc._id = "foo";
        const jobB = new Job("root", "work", {});
        jobB._doc._id = "bar";
        const j = job.depends([jobA, jobB]);
        expect(j).toBe(job);
        expect(doc.depends).toEqual(["foo", "bar"]);
      });

      it("should accept a singlet Job", () => {
        const jobA = new Job("root", "work", {});
        jobA._doc._id = "foo";
        const j = job.depends(jobA);
        expect(j).toBe(job);
        expect(doc.depends).toEqual(["foo"]);
      });

      it("should accept an empty deps array and return the job unchanged", () => {
        const jobA = new Job("root", "work", {});
        jobA._doc._id = "foo";
        let j = job.depends(jobA);
        expect(j).toBe(job);
        expect(doc.depends).toEqual(["foo"]);
        j = job.depends([]);
        expect(j).toBe(job);
        expect(doc.depends).toEqual(["foo"]);
      });

      it("should clear dependencies when passed a falsy value", () => {
        const jobA = new Job("root", "work", {});
        jobA._doc._id = "foo";
        const j = job.depends(jobA);
        expect(j).toBe(job);
        expect(doc.depends).toEqual(["foo"]);
        job.depends(null);
        expect(doc.depends).toHaveLength(0);
      });

      it("should throw when given a bad parameter", () => {
        expect(() => job.depends("badness")).toThrow(/Bad input parameter/);
      });

      it("should throw when given an array containing non Jobs", () => {
        expect(() => job.depends(["Badness"])).toThrow(/Each provided object/);
      });

      it("should throw when given an array containing unsaved Jobs without an _id", () => {
        const jobA = new Job("root", "work", {});
        expect(() => job.depends([jobA])).toThrow(/Each provided object/);
      });
    });

    describe(".priority()", () => {
      it("should accept a numeric priority", () => {
        const j = job.priority(3);
        expect(j).toBe(job);
        expect(doc.priority).toBe(3);
      });

      it("should accept a valid string priority", () => {
        const j = job.priority("normal");
        expect(j).toBe(job);
        expect(doc.priority).toBe(Job.jobPriorities.normal);
      });

      it("should throw when given an invalid priority level", () => {
        expect(() => job.priority("super")).toThrow(/Invalid string priority level provided/);
      });

      it("should throw when given an invalid parameter", () => {
        expect(() => job.priority([])).toThrow(/priority must be an integer or valid priority level/);
      });

      it("should throw when given a non-integer", () => {
        expect(() => job.priority(3.14)).toThrow(/priority must be an integer or valid priority level/);
      });
    });

    describe(".retry()", () => {
      it("should accept a non-negative integer parameter", () => {
        const j = job.retry(3);
        expect(j).toBe(job);
        expect(doc.retries).toBe(3 + 1); // This is correct, it adds one.
        expect(doc.retryWait).toBe(5 * 60 * 1000);
        expect(doc.retryBackoff).toBe("constant");
      });

      it("should accept an option object", () => {
        const j = job.retry({
          retries: 3,
          until: new Date(new Date().valueOf() + 60000),
          wait: 5000,
          backoff: "exponential"
        });
        expect(j).toBe(job);
        expect(doc.retries).toBe(3 + 1);
        expect(doc.retryUntil > new Date()).toBeTruthy();
        expect(doc.retryWait).toBe(5000);
        expect(doc.retryBackoff).toBe("exponential");
      });

      it("should throw when given a bad parameter", () => {
        expect(() => job.retry("badness")).toThrow(/bad parameter: accepts either an integer/);
      });

      it("should throw when given a negative integer", () => {
        expect(() => job.retry(-1)).toThrow(/bad parameter: accepts either an integer/);
      });

      it("should throw when given a numeric non-integer", () => {
        expect(() => job.retry(3.14)).toThrow(/bad parameter: accepts either an integer/);
      });

      it("should throw when given bad options", () => {
        expect(() => job.retry({ retries: "badness" })).toThrow(/bad option: retries must be an integer/);
        expect(() => job.retry({ retries: -1 })).toThrow(/bad option: retries must be an integer/);
        expect(() => job.retry({ retries: 3.14 })).toThrow(/bad option: retries must be an integer/);
        expect(() => job.retry({ wait: "badness" })).toThrow(/bad option: wait must be an integer/);
        expect(() => job.retry({ wait: -1 })).toThrow(/bad option: wait must be an integer/);
        expect(() => job.retry({ wait: 3.14 })).toThrow(/bad option: wait must be an integer/);
        expect(() => job.retry({ backoff: "bogus" })).toThrow(/bad option: invalid retry backoff method/);
        expect(() => job.retry({ until: "bogus" })).toThrow(/bad option: until must be a Date object/);
      });
    });

    describe(".repeat()", () => {
      it("should accept a non-negative integer parameter", () => {
        const j = job.repeat(3);
        expect(j).toBe(job);
        expect(doc.repeats).toBe(3);
      });

      it("should accept an option object", () => {
        const j = job.repeat({
          repeats: 3,
          until: new Date(new Date().valueOf() + 60000),
          wait: 5000
        });
        expect(j).toBe(job);
        expect(doc.repeats).toBe(3);
        expect(doc.repeatUntil > new Date()).toBeTruthy();
        expect(doc.repeatWait).toBe(5000);
      });

      it("should accept an option object with later.js object", () => {
        const j = job.repeat({
          schedule: {
            schedules: [
              {
                h: [10]
              }
            ],
            exceptions: [],
            other() {
              return 0;
            }
          }
        });
        expect(j).toBe(job);
        expect(doc.repeatWait).toEqual({
          schedules: [
            {
              h: [10]
            }
          ],
          exceptions: []
        });
      });

      it("should throw when given a bad parameter", () => {
        expect(() => job.repeat("badness")).toThrow(/bad parameter: accepts either an integer/);
      });

      it("should throw when given a negative integer", () => {
        expect(() => job.repeat(-1)).toThrow(/bad parameter: accepts either an integer/);
      });

      it("should throw when given a numeric non-integer", () => {
        expect(() => job.repeat(3.14)).toThrow(/bad parameter: accepts either an integer/);
      });

      it("should throw when given bad options", () => {
        expect(() => job.repeat({ repeats: "badness" })).toThrow(/bad option: repeats must be an integer/);
        expect(() => job.repeat({ repeats: -1 })).toThrow(/bad option: repeats must be an integer/);
        expect(() => job.repeat({ repeats: 3.14 })).toThrow(/bad option: repeats must be an integer/);
        expect(() => job.repeat({ wait: "badness" })).toThrow(/bad option: wait must be an integer/);
        expect(() => job.repeat({ wait: -1 })).toThrow(/bad option: wait must be an integer/);
        expect(() => job.repeat({ wait: 3.14 })).toThrow(/bad option: wait must be an integer/);
        expect(() => job.repeat({ until: "bogus" })).toThrow(/bad option: until must be a Date object/);
        expect(() => job.repeat({ wait: 5, schedule: {} })).toThrow(/bad options: wait and schedule options are mutually exclusive/);
        expect(() => job.repeat({ schedule: "bogus" })).toThrow(/bad option, schedule option must be an object/);
        expect(() => job.repeat({ schedule: {} })).toThrow(/bad option, schedule object requires a schedules attribute of type Array/);
        expect(() => job.repeat({
          schedule: {
            schedules: 5
          }
        })).toThrow(/bad option, schedule object requires a schedules attribute of type Array/);
        expect(() => job.repeat({
          schedule: {
            schedules: [],
            exceptions: 5
          }
        })).toThrow(/bad option, schedule object exceptions attribute must be an Array/);
      });
    });

    describe(".after()", () => {
      it("should accept a valid Date", () => {
        const d = new Date();
        const j = job.after(d);
        expect(j).toBe(job);
        expect(doc.after).toBe(d);
      });

      it("should accept an undefined value", () => {
        const j = job.after();
        expect(j).toBe(job);
        expect(doc.after).toBeInstanceOf(Date);
        expect(doc.after <= new Date()).toBeTruthy();
      });

      return it("should throw if given a bad parameter", () => {
        expect(() => job.after({ foo: "bar" })).toThrow(/Bad parameter, after requires a valid Date object/);
        expect(() => job.after(123)).toThrow(/Bad parameter, after requires a valid Date object/);
        expect(() => job.after(false)).toThrow(/Bad parameter, after requires a valid Date object/);
      });
    });

    describe(".delay()", () => {
      it("should accept a valid delay", () => {
        const j = job.delay(5000);
        const delay = new Date().valueOf() + 5000;
        expect(j).toBe(job);
        expect(doc.after).toBeInstanceOf(Date);
        expect(doc.after.valueOf()).toBeGreaterThanOrEqual(delay);
        expect(doc.after.valueOf()).toBeLessThanOrEqual(delay + 1000);
      });

      it("should accept an undefined parameter", () => {
        const j = job.delay();
        const delay = new Date().valueOf();
        expect(j).toBe(job);
        expect(doc.after).toBeInstanceOf(Date);
        expect(doc.after.valueOf()).toBeGreaterThanOrEqual(delay);
        expect(doc.after.valueOf()).toBeLessThanOrEqual(delay + 1000);
      });

      return it("should throw when given an invalid parameter", () => {
        expect(() => job.delay(-1.234)).toThrow(/Bad parameter, delay requires a non-negative integer/);
        expect(() => job.delay(new Date())).toThrow(/Bad parameter, delay requires a non-negative integer/);
        expect(() => job.delay(false)).toThrow(/Bad parameter, delay requires a non-negative integer/);
      });
    });
  });

  describe("communicating", () => {
    let ddp = null;
    let originalDDPApply;

    beforeAll(() => {
      ddp = new DDP();
      Job.setDDP(ddp);
      originalDDPApply = Job._ddp_apply;
    });

    describe("job status method", () => {
      let job = null;
      let doc = null;

      beforeEach(() => {
        job = new Job("root", "work", {});
        doc = job._doc;
      });

      describe(".save()", () => {
        beforeAll(() => {
          originalDDPApply = Job._ddp_apply;
          // eslint-disable-next-line camelcase
          Job._ddp_apply = jest.fn(makeDdpStub((name, params) => {
            let res;
            if (name !== "root_jobSave") {
              throw new Error("Bad method name");
            }
            [doc] = params;
            const options = params[1];
            if (options.cancelRepeats) {
              throw new Error("cancelRepeats");
            }
            if (typeof doc === "object") {
              res = "newId";
            } else {
              res = null;
            }
            return [null, res];
          }));
        });

        it("should make valid DDP call when invoked", () => {
          const res = job.save();
          expect(res).toBe("newId");
        });

        it("should work with a callback", () => (
          new Promise((resolve) => {
            job.save((err, res) => {
              expect(res).toBe("newId");
              resolve();
            });
          })
        ));

        it("should properly pass cancelRepeats option", () => {
          expect(() => job.save({ cancelRepeats: true })).toThrow(/cancelRepeats/);
        });

        it("should properly pass cancelRepeats option with callback", () => {
          expect(() => job.save({ cancelRepeats: true }, () => {})).toThrow(/cancelRepeats/);
        });

        afterEach(() => {
          Job._ddp_apply.mockClear();
        });

        afterAll(() => {
          // eslint-disable-next-line camelcase
          Job._ddp_apply = originalDDPApply;
        });
      });

      describe(".refresh()", () => {
        beforeAll(() => {
          originalDDPApply = Job._ddp_apply;
          // eslint-disable-next-line camelcase
          Job._ddp_apply = jest.fn(makeDdpStub((name, params) => {
            let res;
            if (name !== "root_getJob") {
              throw new Error("Bad method name");
            }
            const id = params[0];
            const options = params[1];
            if (options.getLog) {
              throw new Error("getLog");
            }
            if (id === "thisId") {
              res = {
                foo: "bar"
              };
            } else {
              res = null;
            }
            return [null, res];
          }));
        });

        it("should make valid DDP call when invoked", () => {
          doc._id = "thisId";
          const res = job.refresh();
          expect(job._doc).toEqual({ foo: "bar" });
          expect(res).toBe(job);
        });

        it("should work with a callback", () => (
          new Promise((resolve) => {
            doc._id = "thisId";
            job.refresh((err, res) => {
              expect(job._doc).toEqual({ foo: "bar" });
              expect(res).toBe(job);
              resolve();
            });
          })
        ));

        it("shouldn't modify job when not found on server", () => {
          doc._id = "thatId";
          const res = job.refresh();
          expect(res).toBeFalsy();
          expect(job._doc).toEqual(doc);
        });

        it("should properly pass getLog option", () => {
          doc._id = "thisId";
          expect(() => job.refresh({ getLog: true })).toThrow(/getLog/);
        });

        it("should throw when called on an unsaved job", () => {
          expect(() => job.refresh()).toThrow(/on an unsaved job/);
        });

        afterEach(() => {
          Job._ddp_apply.mockClear();
        });

        afterAll(() => {
          // eslint-disable-next-line camelcase
          Job._ddp_apply = originalDDPApply;
        });
      });

      describe(".log()", () => {
        beforeAll(() => {
          originalDDPApply = Job._ddp_apply;
          // eslint-disable-next-line camelcase
          Job._ddp_apply = jest.fn(makeDdpStub((name, params) => {
            let res;
            if (name !== "root_jobLog") {
              throw new Error("Bad method name");
            }
            const id = params[0];
            const runId = params[1];
            const msg = params[2];
            const level = (params[3] && params[3].level) || "gerinfo";
            if ((id === "thisId") && (runId === "thatId") && (msg === "Hello") && Array.from(Job.jobLogLevels).includes(level)) {
              res = level;
            } else {
              res = false;
            }
            return [null, res];
          }));
        });

        it("should add a valid log entry to the local state when invoked before a job is saved", () => {
          const j = job.log("Hello", { level: "success" });
          const delay = new Date().valueOf();
          expect(j).toBe(job);
          const thisLog = doc.log[1]; //  [0] is the "Created" log message
          expect(thisLog.message).toBe("Hello");
          expect(thisLog.level).toBe("success");
          expect(thisLog.time).toBeInstanceOf(Date);

          expect(thisLog.time.valueOf()).toBeGreaterThanOrEqual(delay);
          expect(thisLog.time.valueOf()).toBeLessThanOrEqual(delay + 1000);
        });

        it("should make valid DDP call when invoked on a saved job", () => {
          doc._id = "thisId";
          doc.runId = "thatId";
          const res = job.log("Hello");
          expect(res).toBe("info");
        });

        it("should correctly pass level option", () => {
          doc._id = "thisId";
          doc.runId = "thatId";
          const res = job.log("Hello", { level: "danger" });
          expect(res).toBe("danger");
        });

        it("should work with a callback", () => {
          doc._id = "thisId";
          doc.runId = "thatId";

          return new Promise((resolve) => {
            job.log("Hello", {
              level: "success"
            }, (err, res) => {
              expect(res).toBe("success");
              resolve();
            });
          });
        });

        it("should throw when passed an invalid message", () => {
          doc._id = "thisId";
          doc.runId = "thatId";
          expect(() => job.log(43, { level: "danger" })).toThrow(/Log message must be a string/);
        });

        it("should throw when passed an invalid level", () => {
          doc._id = "thisId";
          doc.runId = "thatId";
          expect(() => job.log("Hello", { level: "blargh" })).toThrow(/Log level options must be one of Job.jobLogLevels/);
          expect(() => job.log("Hello", { level: [] })).toThrow(/Log level options must be one of Job.jobLogLevels/);
        });

        describe("echo option", () => {
          let ogConsoleInfo = global.console.info;
          let ogConsoleLog = global.console.log;
          let ogConsoleWarn = global.console.warn;
          let ogConsoleError = global.console.error;

          beforeAll(() => {
            global.console.info = jest.fn(() => {
              throw new Error("info");
            });

            global.console.log = jest.fn(() => {
              throw new Error("success");
            });

            global.console.warn = jest.fn(() => {
              throw new Error("warning");
            });

            global.console.error = jest.fn(() => {
              throw new Error("danger");
            });
          });

          it("should echo the log to the console at the level requested", () => {
            expect(() => job.log("Hello")).not.toThrow("echo occurred without being requested");
            expect(() => job.log("Hello", { echo: false })).not.toThrow("echo occurred when explicitly disabled");
            expect(() => job.log("Hello", { echo: true })).toThrow(/info/);
            expect(() => job.log("Hello", { echo: true, level: "info" })).toThrow(/info/);
            expect(() => job.log("Hello", { echo: true, level: "success" })).toThrow(/success/);
            expect(() => job.log("Hello", { echo: true, level: "warning" })).toThrow(/warning/);
            expect(() => job.log("Hello", { echo: true, level: "danger" })).toThrow(/danger/);
          });

          it("shouldn't echo the log to the console below the level requested", () => {
            expect(() => job.log("Hello", { echo: "warning" })).not.toThrow();
            expect(() => job.log("Hello", { echo: "warning", level: "info" })).not.toThrow();
            expect(() => job.log("Hello", { echo: "warning", level: "success" })).not.toThrow();
            expect(() => job.log("Hello", { echo: "warning", level: "warning" })).toThrow(/warning/);
            expect(() => job.log("Hello", { echo: "warning", level: "danger" })).toThrow(/danger/);
          });

          afterAll(() => {
            global.console.info = ogConsoleInfo;
            global.console.log = ogConsoleLog;
            global.console.warn = ogConsoleWarn;
            global.console.error = ogConsoleError;
          });
        });

        afterEach(() => {
          Job._ddp_apply.mockClear();
        });

        afterAll(() => {
          // eslint-disable-next-line camelcase
          Job._ddp_apply = originalDDPApply;
        });
      });

      describe(".progress()", () => {
        beforeAll(() => {
          originalDDPApply = Job._ddp_apply;
          // eslint-disable-next-line camelcase
          Job._ddp_apply = jest.fn(makeDdpStub((name, params) => {
            let res;
            if (name !== "root_jobProgress") {
              throw new Error("Bad method name");
            }
            const id = params[0];
            const runId = params[1];
            const completed = params[2];
            const total = params[3];
            if ((id === "thisId") && (runId === "thatId") && (typeof completed === "number") && (typeof total === "number") && (completed >= 0 && completed <= total) && (total > 0)) {
              res = (100 * completed) / total;
            } else {
              res = false;
            }
            return [null, res];
          }));
        });

        it("should add a valid progress update to the local state when invoked before a job is saved", () => {
          const j = job.progress(2.5, 10);
          expect(j).toBe(job);
          expect(doc.progress).toEqual({
            completed: 2.5,
            total: 10,
            percent: 25
          });
        });

        it("should make valid DDP call when invoked on a saved job", () => {
          doc._id = "thisId";
          doc.runId = "thatId";
          const res = job.progress(5, 10);
          expect(res).toBe(50);
        });

        it("should work with a callback", () => {
          doc._id = "thisId";
          doc.runId = "thatId";
          return new Promise((resolve) => {
            job.progress(7.5, 10, (err, res) => {
              expect(res).toBe(75);
              resolve();
            });
          });
        });

        describe("echo option", () => {
          let ogConsoleInfo = global.console.info;

          beforeAll(() => {
            global.console.info = jest.fn(() => {
              throw new Error("info");
            });
          });

          it("should progress updates to the console when requested", () => {
            expect(() => job.progress(0, 100)).not.toThrow();
            expect(() => job.progress(0, 100, { echo: false })).not.toThrow();
            expect(() => job.progress(0, 100, { echo: true })).toThrow(/info/);
          });

          return afterAll(() => {
            global.console.info = ogConsoleInfo;
          });
        });

        it("should throw when given invalid paramters", () => {
          expect(() => job.progress(true, 100)).toThrow(/job.progress: something is wrong with progress params/);
          expect(() => job.progress(0, "hundred")).toThrow(/job.progress: something is wrong with progress params/);
          expect(() => job.progress(-1, 100)).toThrow(/job.progress: something is wrong with progress params/);
          expect(() => job.progress(2, 1)).toThrow(/job.progress: something is wrong with progress params/);
          expect(() => job.progress(0, 0)).toThrow(/job.progress: something is wrong with progress params/);
          expect(() => job.progress(0, -1)).toThrow(/job.progress: something is wrong with progress params/);
          expect(() => job.progress(-2, -1)).toThrow(/job.progress: something is wrong with progress params/);
        });

        afterEach(() => {
          Job._ddp_apply.mockClear();
        });

        afterAll(() => {
          // eslint-disable-next-line camelcase
          Job._ddp_apply = originalDDPApply;
        });
      });

      describe(".done()", () => {
        beforeAll(() => {
          originalDDPApply = Job._ddp_apply;
          // eslint-disable-next-line camelcase
          Job._ddp_apply = jest.fn(makeDdpStub((name, params) => {
            let res;
            if (name !== "root_jobDone") {
              throw new Error("Bad method name");
            }
            const id = params[0];
            const runId = params[1];
            const result = params[2];
            const options = params[3];
            if ((id === "thisId") && (runId === "thatId") && (typeof result === "object")) {
              res = result;
            } else if (options.resultId) {
              res = result.resultId;
            } else {
              res = false;
            }
            return [null, res];
          }));
        });

        it("should make valid DDP call when invoked on a running job", () => {
          doc._id = "thisId";
          doc.runId = "thatId";
          const res = job.done();
          expect(res).toEqual({});
        });

        it("should properly handle a result object", () => {
          doc._id = "thisId";
          doc.runId = "thatId";
          const result = {
            foo: "bar",
            status: 0
          };
          const res = job.done(result);
          expect(res).toEqual(result);
        });

        it("should properly handle a non-object result", () => {
          doc._id = "thisId";
          doc.runId = "thatId";
          const result = "Done!";
          const res = job.done(result);
          expect(res).toEqual({ value: result });
        });

        it("should work with a callback", () => {
          doc._id = "thisId";
          doc.runId = "thatId";

          return new Promise((resolve) => {
            job.done((err, res) => {
              expect(res).toEqual({});
              resolve();
            });
          });
        });

        it("should throw when called on an unsaved job", () => {
          expect(() => job.done()).toThrow(/an unsaved or non-running job/);
        });

        it("should throw when called on a nonrunning job", () => {
          doc._id = "thisId";
          expect(() => job.done()).toThrow(/an unsaved or non-running job/);
        });

        it.skip("should properly pass the repeatId option", () => {
          doc._id = "someId";
          doc.runId = "otherId";
          return new Promise((resolve) => {
            job.done({
              repeatId: "testID"
            }, (err, res) => {
              expect(res).toBe("testID");
              resolve();
            });
          });
        });

        afterEach(() => {
          Job._ddp_apply.mockClear();
        });

        afterAll(() => {
          // eslint-disable-next-line camelcase
          Job._ddp_apply = originalDDPApply;
        });
      });

      describe(".fail()", () => {
        beforeAll(() => {
          originalDDPApply = Job._ddp_apply;
          // eslint-disable-next-line camelcase
          Job._ddp_apply = jest.fn(makeDdpStub((name, params) => {
            let res;
            if (name !== "root_jobFail") {
              throw new Error("Bad method name");
            }
            const id = params[0];
            const runId = params[1];
            const err = params[2];
            const options = params[3];
            if ((id === "thisId") && (runId === "thatId") && (typeof err === "object")) {
              if (options.fatal) {
                throw new Error("Fatal Error!");
              }
              res = err;
            } else {
              res = false;
            }
            return [null, res];
          }));
        });

        it("should make valid DDP call when invoked on a running job", () => {
          doc._id = "thisId";
          doc.runId = "thatId";
          const res = job.fail();
          expect(res).toEqual({ value: "No error information provided" });
        });

        it("should properly handle an error string", () => {
          doc._id = "thisId";
          doc.runId = "thatId";
          const err = "This is an error";
          const res = job.fail(err);
          expect(res).toEqual({ value: err });
        });

        it("should properly handle an error object", () => {
          doc._id = "thisId";
          doc.runId = "thatId";
          const err = {
            message: "This is an error"
          };
          const res = job.fail(err);
          expect(res).toEqual(err);
        });

        it("should work with a callback", () => {
          doc._id = "thisId";
          doc.runId = "thatId";
          return new Promise((resolve) => {
            job.fail((err, res) => {
              expect(res.value).toBe("No error information provided");
              resolve();
            });
          });
        });

        it("should properly handle the fatal option", () => {
          doc._id = "thisId";
          doc.runId = "thatId";
          expect(() => job.fail("Fatal error!", { fatal: true })).toThrow(/Fatal Error!/);
        });

        it("should throw when called on an unsaved job", () => {
          expect(() => job.fail()).toThrow(/an unsaved or non-running job/);
        });

        it("should throw when called on a nonrunning job", () => {
          doc._id = "thisId";
          expect(() => job.fail()).toThrow(/an unsaved or non-running job/);
        });

        afterEach(() => {
          Job._ddp_apply.mockClear();
        });

        afterAll(() => {
          // eslint-disable-next-line camelcase
          Job._ddp_apply = originalDDPApply;
        });
      });

      describe("job control operation", () => {
        const makeJobControl = (op, method) => describe(op, () => {
          beforeAll(() => {
            originalDDPApply = Job._ddp_apply;
            // eslint-disable-next-line camelcase
            Job._ddp_apply = jest.fn(makeDdpStub((name, params) => {
              let res;
              if (name !== `root_${method}`) {
                throw new Error(`Bad method name: ${name}`);
              }
              const id = params[0];
              if (id === "thisId") {
                res = true;
              } else {
                res = false;
              }
              return [null, res];
            }));
          });

          it("should properly invoke the DDP method", () => {
            expect(typeof job[op]).toBe("function");
            doc._id = "thisId";
            const res = job[op]();
            expect(res).toBeTruthy();
          });

          it("should return false if the id is not on the server", () => {
            expect(typeof job[op]).toBe("function");
            doc._id = "badId";
            const res = job[op]();
            expect(res).toBeFalsy();
          });

          it("should work with a callback", () => {
            expect(typeof job[op]).toBe("function");
            doc._id = "thisId";
            return new Promise((resolve) => {
              job[op]((err, res) => {
                expect(res).toBeTruthy();
                resolve();
              });
            });
          });

          if (["pause", "resume"].includes(op)) {
            it("should alter local state when called on an unsaved job", () => {
              const bad = "badStatus";
              doc.status = bad;
              const res = job[op]();
              expect(res).toBe(job);
              expect(doc.status).not.toBe(bad);
            });

            it("should alter local state when called on an unsaved job with callback", () => {
              const bad = "badStatus";
              doc.status = bad;
              return new Promise((resolve) => {
                job[op]((err, res) => {
                  expect(res).toBeTruthy();
                  expect(doc.status).not.toBe(bad);
                  expect(res).toBeTruthy();
                  resolve();
                });
              });
            });
          } else {
            it("should throw when called on an unsaved job", () => {
              expect(() => job[op]()).toThrow(/on an unsaved job/);
            });
          }

          afterEach(() => {
            Job._ddp_apply.mockClear();
          });

          afterAll(() => {
            // eslint-disable-next-line camelcase
            Job._ddp_apply = originalDDPApply;
          });
        });

        makeJobControl("pause", "jobPause");
        makeJobControl("resume", "jobResume");
        makeJobControl("ready", "jobReady");
        makeJobControl("cancel", "jobCancel");
        makeJobControl("restart", "jobRestart");
        makeJobControl("rerun", "jobRerun");
        makeJobControl("remove", "jobRemove");
      });
    // });

  //   return describe("class method", function () {

  //     describe("getWork", function () {

  //       before(() => sinon.stub(Job, "_ddp_apply").callsFake(makeDdpStub(function (name, params) {
  //         if (name !== "root_getWork") {
  //           throw new Error("Bad method name");
  //         }
  //         const type = params[0][0];
  //         const max = (params[1] != null
  //           ? params[1].maxJobs
  //           : undefined) != null
  //           ? (params[1] != null
  //             ? params[1].maxJobs
  //             : undefined)
  //           : 1;
  //         const res = (() => {
  //           switch (type) {
  //             case "work":
  //               return (__range__(1, max, true).map((i) => Job("root", type, { i: 1 })._doc));
  //             case "nowork":
  //               return [];
  //           }
  //         })();
  //         return [null, res];
  //       })));

  //       it("should make a DDP method call and return a Job by default without callback", function () {
  //         const res = Job.getWork("root", "work", {});
  //         return assert.instanceOf(res, Job);
  //       });

  //       it("should return undefined when no work is available without callback", function () {
  //         const res = Job.getWork("root", "nowork", {});
  //         return assert.isUndefined(res);
  //       });

  //       it("should return an array of Jobs when options.maxJobs > 1 without callback", function () {
  //         const res = Job.getWork("root", "work", { maxJobs: 2 });
  //         assert.isArray(res);
  //         assert.lengthOf(res, 2);
  //         return assert.instanceOf(res[0], Job);
  //       });

  //       it("should return an empty array when options.maxJobs > 1 and there is no work without callback", function () {
  //         const res = Job.getWork("root", "nowork", { maxJobs: 2 });
  //         assert.isArray(res);
  //         return assert.lengthOf(res, 0);
  //       });

  //       it("should throw when given on invalid value for the timeout option", function () {
  //         assert.throw((() => Job.getWork("root", "nowork", { workTimeout: "Bad" })), /must be a positive integer/);
  //         assert.throw((() => Job.getWork("root", "nowork", { workTimeout: 0 })), /must be a positive integer/);
  //         return assert.throw((() => Job.getWork("root", "nowork", { workTimeout: -1 })), /must be a positive integer/);
  //       });

  //       afterEach(() => Job._ddp_apply.resetHistory());

  //       return after(() => Job._ddp_apply.restore());
  //     });

  //     describe("makeJob", function () {

  //       const jobDoc = function () {
  //         const j = new Job("root", "work", {})._doc;
  //         j._id = {
  //           _str: "skljfdf9s0ujfsdfl3"
  //         };
  //         return j;
  //       };

  //       it("should return a valid job instance when called with a valid job document", function () {
  //         const res = new Job("root", jobDoc());
  //         return assert.instanceOf(res, Job);
  //       });

  //       return it("should throw when passed invalid params", function () {
  //         assert.throw((() => new Job()), /bad parameter/);
  //         assert.throw((() => new Job(5, jobDoc())), /bad parameter/);
  //         return assert.throw((() => new Job("work", {})), /bad parameter/);
  //       });
  //     });

  //     describe("get Job(s) by ID", function () {

  //       const getJobStub = function (name, params) {
  //         let res;
  //         let j;
  //         if (name !== "root_getJob") {
  //           throw new Error("Bad method name");
  //         }
  //         const ids = params[0];

  //         const one = function (id) {
  //           j = (() => {
  //             switch (id) {
  //               case "goodID":
  //                 return Job("root", "work", { i: 1 })._doc;
  //               default:
  //                 return undefined;
  //             }
  //           })();
  //           return j;
  //         };

  //         if (ids instanceof Array) {
  //           res = ((() => {
  //             const result = [];
  //             for (j of Array.from(ids)) {
  //               if (j === "goodID") {
  //                 result.push(one(j));
  //               }
  //             }
  //             return result;
  //           })());
  //         } else {
  //           res = one(ids);
  //         }

  //         return [null, res];
  //       };

  //       describe("getJob", function () {

  //         before(() => sinon.stub(Job, "_ddp_apply").callsFake(makeDdpStub(getJobStub)));

  //         it("should return a valid job instance when called with a good id", function () {
  //           const res = Job.getJob("root", "goodID");
  //           return assert.instanceOf(res, Job);
  //         });

  //         it("should return undefined when called with a bad id", function () {
  //           const res = Job.getJob("root", "badID");
  //           return assert.isUndefined(res);
  //         });

  //         afterEach(() => Job._ddp_apply.resetHistory());

  //         return after(() => Job._ddp_apply.restore());
  //       });

  //       return describe("getJobs", function () {

  //         before(() => sinon.stub(Job, "_ddp_apply").callsFake(makeDdpStub(getJobStub)));

  //         it("should return valid job instances for good IDs only", function () {
  //           const res = Job.getJobs("root", ["goodID", "badID", "goodID"]);
  //           assert(Job._ddp_apply.calledOnce, "getJob method called more than once");
  //           assert.isArray(res);
  //           assert.lengthOf(res, 2);
  //           assert.instanceOf(res[0], Job);
  //           return assert.instanceOf(res[1], Job);
  //         });

  //         it("should return an empty array for all bad IDs", function () {
  //           const res = Job.getJobs("root", ["badID", "badID", "badID"]);
  //           assert(Job._ddp_apply.calledOnce, "getJob method called more than once");
  //           assert.isArray(res);
  //           return assert.lengthOf(res, 0);
  //         });

  //         afterEach(() => Job._ddp_apply.resetHistory());

  //         return after(() => Job._ddp_apply.restore());
  //       });
  //     });

  //     describe("multijob operation", function () {

  //       const makeMulti = (op, method) => describe(op, function () {

  //         before(() => sinon.stub(Job, "_ddp_apply").callsFake(makeDdpStub(function (name, params) {
  //           if (name !== `root_${method}`) {
  //             throw new Error(`Bad method name: ${name}`);
  //           }
  //           const ids = params[0];
  //           return [
  //             null, ids.indexOf("goodID") !== -1
  //           ];
  //         })));

  //         it("should return true if there are any good IDs", function () {
  //           assert.isFunction(Job[op]);
  //           const res = Job[op]("root", ["goodID", "badID", "goodID"]);
  //           assert(Job._ddp_apply.calledOnce, `${op} method called more than once`);
  //           assert.isBoolean(res);
  //           return assert.isTrue(res);
  //         });

  //         it("should return false if there are all bad IDs", function () {
  //           assert.isFunction(Job[op]);
  //           const res = Job[op]("root", ["badID", "badID"]);
  //           assert(Job._ddp_apply.calledOnce, `${op} method called more than once`);
  //           assert.isBoolean(res);
  //           return assert.isFalse(res);
  //         });

  //         afterEach(() => Job._ddp_apply.resetHistory());

  //         return after(() => Job._ddp_apply.restore());
  //       });

  //       makeMulti("pauseJobs", "jobPause");
  //       makeMulti("resumeJobs", "jobResume");
  //       makeMulti("cancelJobs", "jobCancel");
  //       makeMulti("restartJobs", "jobRestart");
  //       return makeMulti("removeJobs", "jobRemove");
  //     });

  //     return describe("control method", function () {

  //       const makeControl = op => describe(op, function () {

  //         before(() => sinon.stub(Job, "_ddp_apply").callsFake(makeDdpStub(function (name, params) {
  //           if (name !== `root_${op}`) {
  //             throw new Error(`Bad method name: ${name}`);
  //           }
  //           return [null, true];
  //         })));

  //         it("should return a boolean", function () {
  //           assert.isFunction(Job[op]);
  //           const res = Job[op]("root");
  //           assert(Job._ddp_apply.calledOnce, `${op} method called more than once`);
  //           return assert.isBoolean(res);
  //         });

  //         afterEach(() => Job._ddp_apply.resetHistory());

  //         return after(() => Job._ddp_apply.restore());
  //       });

  //       makeControl("startJobs");
  //       makeControl("stopJobs");
  //       makeControl("startJobServer");
  //       return makeControl("shutdownJobServer");
  //     });
    });
  });
});

// //##########################################

// describe("JobQueue", function () {

//   const ddp = new DDP();
//   let failCalls = 0;
//   let doneCalls = 0;
//   let numJobs = 5;

//   before(function () {
//     Job._ddp_apply = undefined;
//     Job.setDDP(ddp);
//     return sinon.stub(Job, "_ddp_apply").callsFake(makeDdpStub(function (name, params) {
//       let err = null;
//       let res = null;
//       const makeJobDoc = function (idx) {
//         if (idx == null) {
//           idx = 0;
//         }
//         const job = new Job("root", "work", { idx });
//         const doc = job._doc;
//         doc._id = `thisId${idx}`;
//         doc.runId = `thatId${idx}`;
//         doc.status = "running";
//         return doc;
//       };
//       switch (name) {
//         case "root_jobDone":
//           doneCalls++;
//           res = true;
//           break;
//         case "root_jobFail":
//           failCalls++;
//           res = true;
//           break;
//         case "root_getWork":
//           var type = params[0][0];
//           var max = (params[1] != null
//             ? params[1].maxJobs
//             : undefined) != null
//             ? (params[1] != null
//               ? params[1].maxJobs
//               : undefined)
//             : 1;
//           if (numJobs === 0) {
//             res = [];
//           } else {
//             switch (type) {
//               case "noWork":
//                 res = [];
//                 break;
//               case "work":
//                 numJobs--;
//                 res = [makeJobDoc()];
//                 break;
//               case "workMax":
//                 if (max < numJobs) {
//                   max = numJobs;
//                 }
//                 numJobs -= max;
//                 res = (__range__(1, max, true).map((i) => makeJobDoc(i)));
//                 break;
//               case "returnError":
//                 err = new Error("MongoError: connection n to w.x.y.z:27017 timed out");
//                 break;
//             }
//           }
//           break;
//         default:
//           throw new Error(`Bad method name: ${name}`);
//       }
//       return [err, res];
//     }));
//   });

//   beforeEach(function () {
//     failCalls = 0;
//     doneCalls = 0;
//     return numJobs = 5;
//   });

//   it("should throw when an invalid options are used", function (done) {
//     assert.throws((() => Job.processJobs(42, "noWork", {}, function (job, cb) { })), /must be nonempty string/);
//     assert.throws((() => Job.processJobs("", "noWork", {}, function (job, cb) { })), /must be nonempty string/);
//     assert.throws((() => Job.processJobs("root", 42, {}, function (job, cb) { })), /must be nonempty string or array of nonempty strings/);
//     assert.throws((() => Job.processJobs("root", "", {}, function (job, cb) { })), /must be nonempty string or array of nonempty strings/);
//     assert.throws((() => Job.processJobs("root", [], {}, function (job, cb) { })), /must be nonempty string or array of nonempty strings/);
//     assert.throws((() => Job.processJobs("root", [""], {}, function (job, cb) { })), /must be nonempty string or array of nonempty strings/);
//     assert.throws((() => Job.processJobs("root", [
//       "noWork", ""
//     ], {}, function (job, cb) { })), /must be nonempty string or array of nonempty strings/);
//     assert.throws((() => Job.processJobs("root", "noWork", {
//       pollInterval: -1
//     }, function (job, cb) { })), /must be a positive integer/);
//     assert.throws((() => Job.processJobs("root", "noWork", {
//       concurrency: "Bad"
//     }, function (job, cb) { })), /must be a positive integer/);
//     assert.throws((() => Job.processJobs("root", "noWork", {
//       concurrency: -1
//     }, function (job, cb) { })), /must be a positive integer/);
//     assert.throws((() => Job.processJobs("root", "noWork", {
//       payload: "Bad"
//     }, function (job, cb) { })), /must be a positive integer/);
//     assert.throws((() => Job.processJobs("root", "noWork", {
//       payload: -1
//     }, function (job, cb) { })), /must be a positive integer/);
//     assert.throws((() => Job.processJobs("root", "noWork", {
//       prefetch: "Bad"
//     }, function (job, cb) { })), /must be a positive integer/);
//     assert.throws((() => Job.processJobs("root", "noWork", {
//       prefetch: -1
//     }, function (job, cb) { })), /must be a positive integer/);
//     assert.throws((() => Job.processJobs("root", "noWork", {
//       workTimeout: "Bad"
//     }, function (job, cb) { })), /must be a positive integer/);
//     assert.throws((() => Job.processJobs("root", "noWork", {
//       workTimeout: -1
//     }, function (job, cb) { })), /must be a positive integer/);
//     assert.throws((() => Job.processJobs("root", "noWork", {
//       callbackStrict: 1
//     }, function (job, cb) { })), /must be a boolean/);
//     assert.throws((() => Job.processJobs("root", "noWork", {
//       errorCallback: 1
//     }, function (job, cb) { })), /must be a function/);
//     return done();
//   });

//   it("should return a valid JobQueue when called", function (done) {
//     const q = Job.processJobs("root", "noWork", {
//       pollInterval: 100
//     }, function (job, cb) {
//       job.done();
//       return cb(null);
//     });
//     assert.instanceOf(q, Job.processJobs);
//     return q.shutdown({
//       quiet: true
//     }, function () {
//       assert.equal(doneCalls, 0);
//       assert.equal(failCalls, 0);
//       return done();
//     });
//   });

//   it("should return a valid JobQueue when called with array of job types", function (done) {
//     const q = Job.processJobs("root", [
//       "noWork", "noWork2"
//     ], {
//         pollInterval: 100
//       }, function (job, cb) {
//         job.done();
//         return cb(null);
//       });
//     assert.instanceOf(q, Job.processJobs);
//     return q.shutdown({
//       quiet: true
//     }, function () {
//       assert.equal(doneCalls, 0);
//       assert.equal(failCalls, 0);
//       return done();
//     });
//   });

//   it("should send shutdown notice to console when quiet is false", function (done) {
//     const jobConsole = Job.__get__("console");
//     const revert = Job.__set__({
//       console: {
//         info(...params) {
//           throw new Error("info");
//         },
//         log(...params) {
//           throw new Error("success");
//         },
//         warn(...params) {
//           throw new Error("warning");
//         },
//         error(...params) {
//           throw new Error("danger");
//         }
//       }
//     });
//     const q = Job.processJobs("root", "noWork", {
//       pollInterval: 100
//     }, function (job, cb) {
//       job.done();
//       return cb(null);
//     });
//     assert.instanceOf(q, Job.processJobs);
//     assert.throws((() => q.shutdown(() => done())), /warning/);
//     revert();
//     return q.shutdown({
//       quiet: true
//     }, function () {
//       assert.equal(doneCalls, 0);
//       assert.equal(failCalls, 0);
//       return done();
//     });
//   });

//   it("should invoke worker when work is returned", function (done) {
//     let q;
//     return q = Job.processJobs("root", "work", {
//       pollInterval: 100
//     }, function (job, cb) {
//       job.done();
//       q.shutdown({
//         quiet: true
//       }, function () {
//         assert.equal(doneCalls, 1);
//         assert.equal(failCalls, 0);
//         return done();
//       });
//       return cb(null);
//     });
//   });

//   it("should invoke worker when work is returned from a manual trigger", function (done) {
//     var q = Job.processJobs("root", "work", {
//       pollInterval: 0
//     }, function (job, cb) {
//       job.done();
//       q.shutdown({
//         quiet: true
//       }, function () {
//         assert.equal(doneCalls, 1);
//         assert.equal(failCalls, 0);
//         return done();
//       });
//       return cb(null);
//     });
//     assert.equal(q.pollInterval, Job.forever);
//     assert.isNull(q._interval);
//     return setTimeout(() => q.trigger(), 20);
//   });

//   it("should successfully start in paused state and resume", function (done) {
//     let flag = false;
//     var q = Job.processJobs("root", "work", {
//       pollInterval: 10
//     }, function (job, cb) {
//       assert.isTrue(flag);
//       job.done();
//       q.shutdown({
//         quiet: true
//       }, function () {
//         assert.equal(doneCalls, 1);
//         assert.equal(failCalls, 0);
//         return done();
//       });
//       return cb(null);
//     }).pause();
//     return setTimeout(function () {
//       flag = true;
//       return q.resume();
//     }, 20);
//   });

//   it("should successfully accept multiple jobs from getWork", function (done) {
//     let q;
//     let count = 5;
//     return q = Job.processJobs("root", "workMax", {
//       pollInterval: 100,
//       prefetch: 4
//     }, function (job, cb) {
//       assert.equal(q.length(), count - 1, "q.length is incorrect");
//       assert.equal(q.running(), 1, "q.running is incorrect");
//       if (count === 5) {
//         assert.isTrue(q.full(), "q.full should be true");
//         assert.isFalse(q.idle(), "q.idle should be false");
//       }
//       job.done();
//       count--;
//       if (count === 0) {
//         q.shutdown({
//           quiet: true
//         }, function () {
//           assert.equal(doneCalls, 5, "doneCalls is incorrect");
//           assert.equal(failCalls, 0, "failCalls is incorrect");
//           return done();
//         });
//       }
//       return cb(null);
//     });
//   });

//   it("should successfully accept and process multiple simultaneous jobs concurrently", function (done) {
//     let q;
//     let count = 0;
//     return q = Job.processJobs("root", "workMax", {
//       pollInterval: 100,
//       concurrency: 5
//     }, function (job, cb) {
//       count++;
//       return setTimeout(function () {
//         assert.equal(q.length(), 0);
//         assert.equal(q.running(), count);
//         count--;
//         job.done();
//         if (!(count > 0)) {
//           q.shutdown({
//             quiet: true
//           }, function () {
//             assert.equal(doneCalls, 5);
//             assert.equal(failCalls, 0);
//             return done();
//           });
//         }
//         return cb(null);
//       }, 25);
//     });
//   });

//   it("should successfully accept and process multiple simultaneous jobs in one worker", function (done) {
//     let q;
//     return q = Job.processJobs("root", "workMax", {
//       pollInterval: 100,
//       payload: 5
//     }, function (jobs, cb) {
//       assert.equal(jobs.length, 5);
//       assert.equal(q.length(), 0);
//       assert.equal(q.running(), 1);
//       for (let j of Array.from(jobs)) {
//         j.done();
//       }
//       q.shutdown({
//         quiet: true
//       }, function () {
//         assert.equal(doneCalls, 5);
//         assert.equal(failCalls, 0);
//         return done();
//       });
//       return cb();
//     });
//   });

//   it("should successfully accept and process multiple simultaneous jobs concurrently and within workers", function (done) {
//     let q;
//     let count = 0;
//     numJobs = 25;
//     return q = Job.processJobs("root", "workMax", {
//       pollInterval: 100,
//       payload: 5,
//       concurrency: 5
//     }, function (jobs, cb) {
//       count += jobs.length;
//       return setTimeout(function () {
//         assert.equal(q.length(), 0);
//         assert.equal(q.running(), count / 5);
//         count -= jobs.length;
//         for (let j of Array.from(jobs)) {
//           j.done();
//         }
//         if (!(count > 0)) {
//           q.shutdown({
//             quiet: true
//           }, function () {
//             assert.equal(doneCalls, 25);
//             assert.equal(failCalls, 0);
//             return done();
//           });
//         }
//         return cb(null);
//       }, 25);
//     });
//   });

//   it("should successfully perform a soft shutdown", function (done) {
//     let q;
//     let count = 5;
//     return q = Job.processJobs("root", "workMax", {
//       pollInterval: 100,
//       prefetch: 4
//     }, function (job, cb) {
//       count--;
//       assert.equal(q.length(), count);
//       assert.equal(q.running(), 1);
//       assert.isTrue(q.full());
//       job.done();
//       if (count === 4) {
//         q.shutdown({
//           quiet: true,
//           level: "soft"
//         }, function () {
//           assert(count === 0);
//           assert.equal(q.length(), 0);
//           assert.isFalse(Job._ddp_apply.calledWith("root_jobFail"));
//           assert.equal(doneCalls, 5);
//           assert.equal(failCalls, 0);
//           return done();
//         });
//       }
//       return cb(null);
//     });
//   });

//   it("should successfully perform a normal shutdown", function (done) {
//     let q;
//     let count = 5;
//     return q = Job.processJobs("root", "workMax", {
//       pollInterval: 100,
//       concurrency: 2,
//       prefetch: 3
//     }, (job, cb) => setTimeout(function () {
//       count--;
//       job.done();
//       if (count === 4) {
//         q.shutdown({
//           quiet: true,
//           level: "normal"
//         }, function () {
//           assert.equal(count, 3);
//           assert.equal(q.length(), 0);
//           assert.isTrue(Job._ddp_apply.calledWith("root_jobFail"));
//           assert.equal(doneCalls, 2);
//           assert.equal(failCalls, 3);
//           return done();
//         });
//       }
//       return cb(null);
//     }, 25));
//   });

//   it("should successfully perform a normal shutdown with both payload and concurrency", function (done) {
//     let q;
//     let count = 0;
//     numJobs = 25;
//     return q = Job.processJobs("root", "workMax", {
//       pollInterval: 100,
//       payload: 5,
//       concurrency: 2,
//       prefetch: 15
//     }, function (jobs, cb) {
//       count += jobs.length;
//       return setTimeout(function () {
//         assert.equal(q.running(), count / 5);
//         count -= jobs.length;
//         for (let j of Array.from(jobs)) {
//           j.done();
//         }
//         if (count === 5) {
//           q.shutdown({
//             quiet: true
//           }, function () {
//             assert.equal(q.length(), 0, "jobs remain in task list");
//             assert.equal(count, 0, "count is wrong value");
//             assert.isTrue(Job._ddp_apply.calledWith("root_jobFail"));
//             assert.equal(doneCalls, 10);
//             assert.equal(failCalls, 15);
//             return done();
//           });
//         }
//         return cb(null);
//       }, 25);
//     });
//   });

//   it("should successfully perform a hard shutdown", function (done) {
//     let q;
//     let count = 0;
//     let time = 20;
//     return q = Job.processJobs("root", "workMax", {
//       pollInterval: 100,
//       concurrency: 2,
//       prefetch: 3
//     }, function (job, cb) {
//       setTimeout(function () {
//         count++;
//         if (count === 1) {
//           job.done();
//           q.shutdown({
//             level: "hard",
//             quiet: true
//           }, function () {
//             assert.equal(q.length(), 0);
//             assert.equal(count, 1);
//             assert.isTrue(Job._ddp_apply.calledWith("root_jobFail"));
//             assert.equal(doneCalls, 1, "wrong number of .done() calls");
//             assert.equal(failCalls, 4, "wrong number of .fail() calls");
//             return done();
//           });
//           return cb(null);
//         }
//       }, // Other workers will never call back
//         time);
//       return time += 20;
//     });
//   });

//   it("should throw when using callbackStrict option and multiple callback invokes happen", function (done) {
//     let q;
//     return q = Job.processJobs("root", "work", {
//       callbackStrict: true,
//       pollInterval: 100,
//       concurrency: 1,
//       prefetch: 0
//     }, (job, cb) => setTimeout(function () {
//       job.done();
//       cb();
//       assert.throws(cb, /callback was invoked multiple times/);
//       return q.shutdown({
//         level: "hard",
//         quiet: true
//       }, function () {
//         assert.equal(doneCalls, 1);
//         assert.equal(failCalls, 0);
//         return done();
//       });
//     }, 25));
//   });

//   it("should throw when using callbackStrict option and multiple callback invokes happen 2", function (done) {
//     let q;
//     return q = Job.processJobs("root", "work", {
//       callbackStrict: true,
//       pollInterval: 100,
//       concurrency: 1,
//       prefetch: 0
//     }, (job, cb) => setTimeout(function () {
//       job.done(function () {
//         assert.throws(cb, /callback was invoked multiple times/);
//         return q.shutdown({
//           level: "hard",
//           quiet: true
//         }, function () {
//           assert.equal(doneCalls, 1);
//           assert.equal(failCalls, 0);
//           return done();
//         });
//       });
//       return cb();
//     }, 25));
//   });

//   it("should invoke errorCallback when an error is returned from getWork", function (done) {
//     let q;
//     const ecb = function (err, res) {
//       assert.instanceOf(err, Error);
//       return q.shutdown({
//         level: "hard",
//         quiet: true
//       }, function () {
//         assert.equal(doneCalls, 0);
//         assert.equal(failCalls, 0);
//         return done();
//       });
//     };

//     return q = Job.processJobs("root", "returnError", {
//       pollInterval: 100,
//       concurrency: 1,
//       prefetch: 0,
//       errorCallback: ecb
//     }, function (job, cb) { });
//   });

//   afterEach(() => Job._ddp_apply.resetHistory());

//   return after(() => Job._ddp_apply.restore());
// });
// function __range__(left, right, inclusive) {
//   let range = [];
//   let ascending = left < right;
//   let end = !inclusive
//     ? right
//     : ascending
//       ? right + 1
//       : right - 1;
//   for (let i = left; ascending
//     ? i < end
//     : i > end; ascending
//       ? i++
//       : i--) {
//     range.push(i);
//   }
//   return range;
// }
