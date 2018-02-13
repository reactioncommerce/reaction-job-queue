/* eslint-disable no-console */
export function createJobCollectionClass({ Meteor, JobCollectionBaseClass }) {
  class JobCollection extends JobCollectionBaseClass {
    constructor(root = "queue", options = {}) {
      super(root, options);

      this._toLog = this._toLog.bind(this);

      if (!(this instanceof JobCollection)) {
        return new JobCollection(root, options);
      }

      this.logConsole = false;
      this.isSimulation = true;

      if (!options.connection) {
        Meteor.methods(this._generateMethods());
      } else {
        options.connection.methods(this._generateMethods());
      }
    }

    _toLog(userId, method, message) {
      if (this.logConsole) {
        return console.log(`${new Date()}, ${userId}, ${method}, ${message}\n`);
      }
    }
  }

  return JobCollection;
}
