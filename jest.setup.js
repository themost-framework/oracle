require('dotenv').config();
const { JsonLogger } = require('@themost/json-logger');

if (typeof JsonLogger.prototype.setLogLevel !== 'function') {
    Object.assign(JsonLogger.prototype, {
        /**
         * @param {string} level
         */
        setLogLevel(level) {
            const logLevel = [ 'error', 'warn', 'info', 'verbose', 'debug' ].indexOf(level);
            if (logLevel > -1) {
                this.level = logLevel;
            }
        }
    })
}

const { TraceUtils } = require('@themost/common');
process.env.NODE_ENV = 'development';
TraceUtils.useLogger(new JsonLogger({
    format: 'raw'
}));
/* global jest */
jest.setTimeout(600000);