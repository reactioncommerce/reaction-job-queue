export default function (depends) {
  const { Meteor, Mongo, check, Match, later } = depends;
  const { createJobClass } = require("../common/jobFactory");
  const { createJobCollectionBaseClass } = require("../common/jobCollectionBaseFactory");
  const { createJobCollectionClass } = require("./jobCollectionFactory");

  // Create classes through dependency injecting
  const Job = createJobClass({ Meteor });
  const JobCollectionBaseClass = createJobCollectionBaseClass({ Meteor, Mongo, check, Match, later, Job });
  const JobCollection = createJobCollectionClass({ ...depends, Job, JobCollectionBaseClass });

  return {
    Job,
    JobCollection
  };
}
