#!/usr/bin/env node

var {Decider}  = require('aws-swf');
var {hostname} = require('os');
var bluebird   = require('bluebird');
var yargs      = require('yargs');
var logger      = require('winston');

var {load} = require('./common');

bluebird.config({
  cancellation: true
});

const wait = () => new Promise(resolve => setImmediate(resolve));

var options = yargs
  .usage('Usage: $0 [options]')
  .option('file', {
    alias: 'f',
    describe:  'Javascript file containing decider promise chain'
  })
  .option('domain', {
    alias: 'd',
    describe: 'Name of the SWF Domain to execute decisions within'
  })
  .option('taskList', {
    alias: 't',
    describe: 'Name of the SWF Task List to execute decisions for'
  })
  .option('identity', {
    alias: 'i',
    describe: 'Unique identifier of this decider instance',
    default: `decider-${hostname()}-${process.pid}`
  })
  .option('limit', {
    alias: 'l',
    describe: 'Limit the number of decider processes that can run concurrently'
  })
  .demand(['file', 'domain', 'taskList'])
  .argv;

const execute = (file, decisionTask) =>
  load(file)
    .then(module => {
      if (typeof module === 'function') {
        return module;
      }

      if (typeof module.default === 'function') {
        return module.default;
      }

      throw new TypeError(`${file} is not a function`);
    })
    .then(decider => ({
      decider: decider(
        decisionTask.eventList.workflow_input(),
        context(decisionTask)
      )
    }))
    .tap(wait);

const context = decisionTask => ({
  Promise: bluebird,

  activity: (name, input, options = {}) =>
    new bluebird.Promise((resolve, reject) => {
      if (!decisionTask.eventList.is_activity_scheduled(name)) {
        logger.info(`Scheduling activity '${name}'`);

        let schedule = Object.assign({
          name,
          input,
          activity: name
        }, options);

        return decisionTask.response.schedule(schedule);
      }

      if (decisionTask.eventList.has_activity_completed(name)) {
        logger.info(`Activity '${name}' has completed`);
        return resolve(decisionTask.eventList.results(name));
      }

      if (decisionTask.eventList.has_activity_failed(name)) {
        logger.error(`Activity '${name}' has failed`);
        return reject(decisionTask.eventList.results(name));
      }

      logger.info(`Waiting for activity '${name}' to finish`);
      return decisionTask.response.wait();
    }),

  timer: (name, seconds) =>
    new bluebird.Promise(resolve => {
      if (!decisionTask.eventList.timer_scheduled(name)) {
        logger.info(`Scheduling timer '${name}'`);

        return decisionTask.response.start_timer({
          delay: seconds
        }, {
          timerId: name
        });
      }

      if (decisionTask.eventList.timer_fired(name)) {
        logger.info(`Timer '${name}' has fired`);
        return resolve();
      }

      logger.info(`Waiting for timer '${name}'`);
      return decisionTask.response.wait();
    }),

  childWorkflow: (name, input, options) =>
    new bluebird.Promise((resolve, reject) => {
      if (!decisionTask.eventList.childworkflow_scheduled(name)) {
        logger.info(`Scheduling child workflow '${name}'`);

        let schedule = Object.assign({
          name,
          workflow: name
        }, options);

        return decisionTask.response.start_childworkflow(schedule, {input});
      }

      if (decisionTask.eventList.childworkflow_completed(name)) {
        logger.info(`Child workflow '${name}' has completed`);
        return resolve(decisionTask.eventList.childworkflow_results(name));
      }

      if (decisionTask.eventList.childworkflow_failed(name)) {
        logger.info(`Child workflow '${name}' has failed`);
        return reject(decisionTask.eventList.childworkflow_results(name));
      }

      logger.info(`Waiting for child workflow '${name}' to finish'`);
      return decisionTask.response.wait();
    }),

  signal: name =>
    new bluebird.Promise(resolve => {
      if (decisionTask.eventList.signal_arrived(name)) {
        logger.info(`Signal '${name}' has been received`);
        return resolve(decisionTask.eventList.signal_input(name));
      }

      logger.info(`Waiting for signal '${name}'`);
      return decisionTask.response.wait();
    })
});

const handleDecisionState = ({response}) => ({decider}) => {
  /**
   * The decider promise is still pending after this execution, so cancel
   * it to prevent it from being settled on a subsequent decision execution
   */
  if (decider.isPending()) {
    decider.cancel();
    return logger.info('Workflow execution is still pending');
  }

  /**
   * The decider promise has been fulfilled, which means that the workflow
   * should be marked as completed
   */
  if (decider.isFulfilled()) {
    response.stop({
      result: decider.value()
    });

    return logger.info('Workflow execution has succeeded');
  }

  /**
   * The decider promise has been rejected, which means that the workflow should
   * be marked as failed. Due to inconsistency in the `aws-swf` library, the
   * decision must be manually added to the decision list instead of using
   * `.fail()`
   */
  if (decider.isRejected()) {
    let reason = decider.reason();

    response.addDecision({
      decisionType: 'FailWorkflowExecution',
      failWorkflowExecutionDecisionAttributes: {
        reason,
        details: reason
      }
    });

    return logger.error('Workflow execution has failed');
  }
};

var decider = new Decider({
  domain: options.domain,
  identity: options.identity,
  taskList: {name: options.taskList},
  taskLimitation: options.limit,
  maximumPageSize: 500,
  reverseOrder: false
});

decider.on('decisionTask', decisionTask => {
  logger.info('Received decision task');

  execute(options.file, decisionTask)
    .tap(() => logger.info('Decision execution finished'))
    .catch(err => {
      logger.error('Decision execution failed', err);
      throw err;
    })
    .then(handleDecisionState(decisionTask))
    .then(() => {
      logger.info('Sending decision response');
      return bluebird.fromCallback(cb => decisionTask.response.send(cb));
    })
    .then(() => logger.info('All done!'));
});

decider.on('poll', () => logger.info('Polling for decision tasks...'));

logger.info(`Starting decider '${options.identity}' for task list '${options.taskList}' in domain '${options.domain}'`);

decider.start();
process.on('SIGINT', () => {
  logger.info('Caught SIGINT, polling will stop after current request...');
  decider.stop();
});
process.on('SIGTERM', () => {
  logger.info('Caught SIGTERM, polling will stop after current request...');
  decider.stop();
});
