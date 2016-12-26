/**
 * Created by ikebal on 06.09.16.
 */
const config = require('config');
const log_level = config.service.log_level;
const log4js = require('log4js');

log4js.configure({
  appenders: [{
    type: 'console',
    layout: {
      type: 'pattern',
      pattern: `%[ [%d{ISO8601}] [%p] [%h] [${config.service.name}] %] - %m%n`
    }
  }],
  replaceConsole: true
});

const logger = log4js.getLogger();

logger.setLevel(log_level);       // possible values: ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF

module.exports = logger;
