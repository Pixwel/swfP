#!/usr/bin/env node

var {ActivityPoller} = require('aws-swf');
var {hostname} = require('os');
var resolve    = require('resolve');
var bluebird   = require('bluebird');
var yargs      = require('yargs');
var throttle   = require('lodash.throttle');
var logger     = require('winston');

const HEARTBEAT_PERIOD = 10;

var {load} = require('./common');

var options = yargs
  .usage('Usage: $0 [options]')
  .option('file', {
    alias: 'f',
    describe: 'Javascript file containing activities object'
  })
  .option('domain', {
    alias: 'd',
    describe: 'Name of the SWF Domain to execute activities within'
  })
  .option('taskList', {
    alias: 't',
    describe: 'Name of the SWF Task List to execute activities for'
  })
  .option('identity', {
    alias: 'i',
    describe: 'Unique identifier of this worker instance',
    default: `activity-${hostname()}-${process.pid}`
  })
  .option('limit', {
    alias: 'l',
    describe: 'Limit the number of worker processes that can run concurrently'
  })
  .demand(['file', 'domain', 'taskList'])
  .argv;

const parse = str => {
  try {
    return JSON.parse(str);
  } catch(e) {
    return str;
  }
};

const execute = (file, activityTask) =>
  load(file)
    .then(activities => {
      var {name} = activityTask.config.activityType;

      var activity = activities[name];
      if (!activity) {
        throw Error(`Activity '${name}' not defined in '${file}'`);
      }

      var input = parse(activityTask.config.input);
      var _context = context(activityTask);
      logger.info(`Executing activity '${name}'`, input);

      return bluebird.resolve(activity(input, _context))
        .finally(() => _context.heartbeat.cancel());
    });

const context = activityTask => ({
  Promise: bluebird,

  heartbeat: throttle(data => {
    logger.info('Sending heartbeat', data);
    activityTask.recordHeartbeat(data);
  }, HEARTBEAT_PERIOD * 1000)
});

var worker = new ActivityPoller({
  domain: options.domain,
  identity: options.identity,
  taskList: {name: options.taskList},
  taskLimitation: options.limit
});

worker.on('activityTask', activityTask => {
  logger.info('Received activity task');

  execute(options.file, activityTask)
    .tap(result => logger.info('Activity execution succeeded', result))
    .catch(err => {
      logger.error('Activity execution failed', err);
      activityTask.respondFailed(err.name, err.message);
      throw err;
    })
    .then(result => activityTask.respondCompleted(result));
});

worker.on('poll', () => logger.info('Polling for activity tasks...'));

logger.info(`Starting activity worker '${options.identity}' for task list '${options.taskList}' in domain '${options.domain}'`);

worker.start();
process.on('SIGINT', () => {
  logger.info('Caught SIGINT, polling will stop after current request...');
  worker.stop();
});
process.on('SIGTERM', () => {
  logger.info('Caught SIGTERM, polling will stop after current request...');
  worker.stop();
});
