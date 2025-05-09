import oracledb from 'oracledb';
import async from 'async';
import util from 'util';
import _ from 'lodash';
import { SqlUtils, QueryExpression } from '@themost/query';
import { TraceUtils, LangUtils } from '@themost/common';
import { AsyncSeriesEventEmitter, before, after } from '@themost/events';
import { OracleFormatter } from './OracleFormatter';

oracledb.fetchAsString = [ oracledb.CLOB, oracledb.NCLOB ];

/**
 *
 * @returns {import('@themost/common').TraceLogger}
 */
function createLogger() {
    if (typeof TraceUtils.newLogger === 'function') {
        return TraceUtils.newLogger();
    }
    const [loggerProperty] = Object.getOwnPropertySymbols(TraceUtils);
    const logger = TraceUtils[loggerProperty];
    const newLogger = Object.create(TraceUtils[loggerProperty]);
    newLogger.options = Object.assign({}, logger.options);
    return newLogger;
}

/**
 *
 * @param {{target: OracleAdapter, query: string|QueryExpression, results: Array<*>}} event
 */
function onReceivingJsonObject(event) {
    if (typeof event.query === 'object' && event.query.$select) {
        // try to identify the usage of a $jsonObject dialect and format result as JSON
        const { $select: select } = event.query;
        if (select) {
            const attrs = Object.keys(select).reduce((previous, current) => {
                const fields = select[current];
                previous.push(...fields);
                return previous;
            }, []).filter((x) => {
                const [key] = Object.keys(x);
                if (typeof key !== 'string') {
                    return false;
                }
                return x[key].$jsonObject != null || x[key].$jsonArray != null  || x[key].$jsonGroupArray != null;
            }).map((x) => {
                return Object.keys(x)[0];
            });
            if (attrs.length > 0) {
                if (Array.isArray(event.results)) {
                    for(const result of event.results) {
                        attrs.forEach((attr) => {
                            if (Object.prototype.hasOwnProperty.call(result, attr) && typeof result[attr] === 'string') {
                                result[attr] = JSON.parse(result[attr]);
                            }
                        });
                    }
                }
            }
        }
    }
}

/**
 * @class
 * @augments {import('@themost/common').DataAdapterBase}
 * @property {string} connectString
 */
class OracleAdapter {
    /**
     * @constructor
     * @param {*} options
     */
    constructor(options) {
        this.options = options || { host:'localhost' };
        /**
         * Represents the database raw connection associated with this adapter
         * @type {*}
         */
        this.rawConnection = null;
        let connectString;
        //of options contains connectString parameter ignore all other params and define this as the database connection string
        if (options.connectString) { connectString = options.connectString; }
        Object.defineProperty(this, 'connectString', {
            get: function() {
                if (typeof connectString === 'string') {
                    return connectString;
                } else {
                    // get hostname or localhost
                    connectString = options.host || 'localhost';
                    //append port
                    if (typeof options.port !== 'undefined') { connectString += ':' + options.port; }
                    if (typeof options.service !== 'undefined') { connectString += '/' + options.service; }
                    if (typeof options.type !== 'undefined') { connectString += ':' + options.type; }
                    if (typeof options.instance !== 'undefined') { connectString += '/' + options.instance; }
                    return connectString;
                }
            }
        });

        this.executing = new AsyncSeriesEventEmitter();
        this.executed = new AsyncSeriesEventEmitter();
        this.executed.subscribe(onReceivingJsonObject);
        /**
         * create a new instance of logger
         * @type {import('@themost/common').TraceLogger}
         */
        this.logger = createLogger();
        // use log level from connection options, if any
        if (typeof this.options.logLevel === 'string' && this.options.logLevel.length) {
            // if the logger has level(string) function
            if (typeof this.logger.level === 'function') {
                // try to set log level
                this.logger.level(this.options.logLevel);
                // otherwise, check if logger has setLogLevel(string) function
            } else if (typeof this.logger.setLogLevel === 'function') {
                this.logger.setLogLevel(this.options.logLevel);
            }
        }

    }

    open(callback) {
        const self = this;
        callback = callback || function() {};
        if (self.rawConnection) {
            callback();
        }
        else {
            self.logger.debug('Opening database connection');
            oracledb.getConnection(
                {
                    user          : this.options.user,
                    password      : this.options.password,
                    connectString : this.connectString
                }, function(err, connection) {
                    if (err) {
                        return callback(err);
                    }
                    self.rawConnection = connection;
                    if (self.options.session) {
                        const executeOptions = {outFormat: oracledb.OBJECT, autoCommit: (typeof self.transaction === 'undefined') };
                        let sqls = [];
                        try {
                            //set session parameters
                            const session = self.options.session;
                            const keys = Object.keys(session);
                            if (keys.length === 0) {
                                return callback();
                            }
                            const formatter = self.getFormatter();
                            sqls.push.apply(sqls, keys.map((key) => {
                                return 'ALTER session SET ' + formatter.escapeName(key) + '=' + formatter.escape(session[key])
                            }));
                        } catch (error) {
                            return callback(error);
                        }
                        return async.eachSeries(sqls , function(sql, cb) {
                            self.rawConnection.execute(sql, [], executeOptions, function(err) {
                                if (err) {
                                    return cb(err);
                                }
                                return cb();
                            });
                        }, function(err) {
                            if (err) {
                                return callback(err);
                            }
                            return callback();
                        });
                    }
                    return callback();
                });
        }
    }

    /**
     * Opens a database connection
     */
    openAsync() {
        return new Promise((resolve, reject) => {
            return this.open(err => {
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
        });
    }

    close(callback) {
        const self = this;
        callback = callback || function() {};
        try {
            if (self.rawConnection)
            {
                self.logger.debug('Closing database connection');
                //close connection
                self.rawConnection.release(function(err) {
                    if (err) {
                        self.logger.debug('An error occurred while closing database connection.');
                        self.logger.debug(err);
                    }
                    self.logger.debug('Close database connection');
                    //destroy raw connection
                    self.rawConnection=null;
                    //and finally return
                    return callback();
                });
            }
            else {
                return callback();
            }

        }
        catch (err) {
            self.logger.debug('An error occurred while closing database connection');
            self.logger.debug(err);
            //call callback without error
            callback();
        }
    }

    /**
     * Closes the current database connection
     */
    closeAsync() {
        return new Promise((resolve, reject) => {
            return this.close(err => {
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
        });
    }

    /**
     * @param {string} query
     * @param {*=} values
     */
    prepare(query, values) {
        return SqlUtils.format(query,values);
    }

    static formatType(field) {
        // eslint-disable-next-line no-unused-vars
        const size = parseInt(field.size);
        let s;
        switch (field.type)
        {
            case 'Boolean':
                s = 'NUMBER(1,0)';
                break;
            case 'Byte':
                s = 'NUMBER(3,0)';
                break;
            case 'Float':
                s = 'NUMBER(19,4)';
                break;
            case 'Counter':
                return 'NUMBER(19,0)';
            case 'Currency':
                s =  'NUMBER(' + (field.size || 19) + ',4)';
                break;
            case 'Number':
            case 'Decimal':
                s =  'NUMBER';
                if ((field.size) && (field.scale)) {
                    s += '(' + field.size + ',' + field.scale + ')';
                }
                else {
                    s += '(19,4)';
                }
                break;
            case 'Date':
            case 'DateTime':
                s = 'TIMESTAMP(6) WITH LOCAL TIME ZONE';
                break;
            case 'Time':
                s = 'NUMBER(19,4)';
                break;
            case 'Long':
                s = 'NUMBER(19,0)';
                break;
            case 'Duration':
                s =field.size ? util.format('NVARCHAR2(%s)', field.size) : 'NVARCHAR2(48)';
                break;
            case 'Integer':
                s = 'NUMBER' + (size <= 38 ? '(' + size + ',0)':'(19,0)' );
                break;
            case 'URL':
            case 'Text':
                s = size > 0 ? util.format('NVARCHAR2(%s)', size) : 'NVARCHAR2(255)';
                break;
            case 'Note':
                // important note: if size is greater than 4000 then we use CLOB instead of NVARCHAR2
                if (size > 2000) {
                    s = 'NCLOB';
                } else {
                    s = size > 0 ? util.format('NVARCHAR2(%s)', size) : 'NVARCHAR2(2000)';
                }
                break;
            case 'Json':
                s = 'NCLOB';
                break
            case 'Image':
            case 'Binary':
                s ='LONG RAW';
                break;
            case 'Guid':
                s = 'VARCHAR(36)';
                break;
            case 'Short':
                s = 'NUMBER(5,0)';
                break;
            default:
                s = 'NUMBER(19,0)';
                break;
        }
        if (field.primary) {
            return s.concat(' NOT NULL');
        }
        else {
            return s.concat((typeof field.nullable=== 'undefined' || field.nullable === null) ? ' NULL': (field.nullable ? ' NULL': ' NOT NULL'));
        }
    }

    /**
     * Begins a transactional operation by executing the given function
     * @param executeFunc {function} The function to execute
     * @param callback {function(Error=)} The callback that contains the error -if any- and the results of the given operation
     */
    executeInTransaction(executeFunc, callback) {
        const self = this;
        try {
            // ensure parameters
            if (typeof executeFunc !== 'function') {
                // noinspection ExceptionCaughtLocallyJS
                throw new Error('Invalid argument. Expected a valid function that is going to be executed in transaction.');
            }
            callback = callback || function() {};
            return self.open(function(err) {
                if (err) {
                    // throw error
                    return callback(err);
                }
                // check if transaction is already open
                if (self.transaction) {
                    // and execute function
                    return executeFunc.call(self, function(err) {
                        callback(err);
                    });
                }
                // initialize dummy transaction object (for future use)
                self.transaction = { };
                // execute function
                return executeFunc.call(self, function(err) {
                    if (err) {
                        // rollback transaction
                        return self.rawConnection.rollback(function() {
                            // delete transaction object
                            delete self.transaction;
                            // use auto-close
                            return self.tryClose(function() {
                                // return error
                                return callback(err);
                            });
                        });
                    }
                    // commit transaction
                    return self.rawConnection.commit(function(err) {
                        // delete transaction object
                        delete self.transaction;
                        // use auto-close
                        return self.tryClose(function() {
                            return callback(err);
                        });
                    });
                });
            });
        } catch (error) {
            if (self.transaction) {
                return callback(error);
            }
            return self.tryClose(function() {
                return callback(error);
            });
        }
    }

    /**
     * Begins a data transaction and executes the given function
     * @param func {Function}
     */
    executeInTransactionAsync(func) {
        return new Promise((resolve, reject) => {
            return this.executeInTransaction((callback) => {
                return func.call(this).then(res => {
                    return callback(null, res);
                }).catch(err => {
                    return callback(err);
                });
            }, (err, res) => {
                if (err) {
                    return reject(err);
                }
                return resolve(res);
            });
        });
    }

    /**
     *
     * @param {string} name
     * @param {QueryExpression|*} query
     * @param {function(Error=)} callback
     */
    createView(name, query, callback) {
        this.view(name).create(query, callback);
    }

    /*
     * @param {DataModelMigration|*} obj An Object that represents the data model scheme we want to migrate
     * @param {function(Error=)} callback
     */
    migrate(obj, callback) {
        const self = this;
        callback = callback || function() {};
        if (typeof obj === 'undefined' || obj === null) { callback(); return; }
        /**
         * @type {*}
         */
        const migration = obj;

        const format = function(format, obj)
        {
            let result = format;
            if (/%t/.test(format))
                result = result.replace(/%t/g,OracleAdapter.formatType(obj));
            if (/%f/.test(format))
                result = result.replace(/%f/g,obj.name);
            return result;
        };


        async.waterfall([
            //1. Check migrations table existence
            function(cb) {
                if (OracleAdapter.supportMigrations) {
                    cb(null, true);
                    return;
                }
                self.table('migrations').exists(function(err, exists) {
                    if (err) { cb(err); return; }
                    cb(null, exists);
                });
            },
            //2. Create migrations table, if it does not exist
            function(arg, cb) {
                if (arg) { cb(null, 0); return; }
                //create migrations table

                async.eachSeries([
                    'CREATE TABLE "migrations"("id" NUMBER(10) NOT NULL, "appliesTo" NVARCHAR2(255) NOT NULL, "model" NVARCHAR2(255) NULL, ' +
                    '"description" NVARCHAR2(255),"version" NVARCHAR2(24) NOT NULL, CONSTRAINT "migrations_pk" PRIMARY KEY ("id"))',
                    'CREATE SEQUENCE "migrations_id_seq" START WITH 1 INCREMENT BY 1'
                ], function(s, cb0) {
                    self.execute(s, [], cb0);
                }, function(err) {
                    if (err) { return cb(err); }
                    OracleAdapter.supportMigrations=true;
                    return cb(null, 0);
                });
            },
            //3. Check if migration has already been applied (true=Table version is equal to migration version, false=Table version is older from migration version)
            function(arg, cb) {
                self.table(migration.appliesTo).version(function(err, version) {
                    if (err) { cb(err); return; }
                    cb(null, (version>=migration.version));
                });
            },
            //4a. Check table existence (-1=Migration has already been applied, 0=Table does not exist, 1=Table exists)
            function(arg, cb) {
                //migration has already been applied (set migration.updated=true)
                if (arg) {
                    migration['updated']=true;
                    cb(null, -1);
                }
                else {
                    self.table(migration.appliesTo).exists(function(err, exists) {
                        if (err) { cb(err); return; }
                        cb(null, exists ? 1 : 0);
                    });
                }
            },
            //5. Migrate target table (create or alter)
            function(arg, cb) {
                //migration has already been applied (args[0]=-1)
                if (arg < 0) {
                    cb(null, arg);
                }
                else if (arg === 0) {
                    self.table(migration.appliesTo).create(migration.add, function(err) {
                        if (err) { return cb(err); }
                        cb(null, 1);
                    });
                }
                else if (arg === 1) {
                    let column, newType, oldType;

                    //1. columns to be removed
                    if (_.isArray(migration.remove)) {
                        if (migration.remove.length>0) {
                            return cb(new Error('Data migration remove operation is not supported by this adapter.'));
                        }
                    }
                    //1. columns to be changed
                    if (_.isArray(migration.change)) {
                        if (migration.change.length>0) {
                            return cb(new Error('Data migration change operation is not supported by this adapter. Use add collection instead.'));
                        }
                    }

                    if (_.isArray(migration.add)) {
                        //init change collection
                        migration.change = [];
                        //get table columns
                        self.table(migration.appliesTo).columns(function(err, columns) {
                            if (err) { return cb(err); }
                            for (let i = 0; i < migration.add.length; i++) {
                                const x = migration.add[i];
                                column = _.find(columns, (y)=> {
                                    return y.name === x.name;
                                });
                                if (column) {
                                    //if column is primary key remove it from collection
                                    if (column.primary) {
                                        migration.add.splice(i, 1);
                                        i-=1;
                                    }
                                    else {
                                        // add exception for NCLOB size (remove it)
                                        if (column.type === 'NCLOB') {
                                            delete column.size;
                                        }
                                        newType = format('%t', x);
                                        if (column.precision !== null && column.scale !== null) {
                                            oldType = util.format('%s(%s,%s) %s', column.type.toUpperCase(), column.precision.toString(), column.scale.toString(), (column.nullable ? 'NULL' : 'NOT NULL'));
                                        }
                                        else if (/^TIMESTAMP\(\d+\) WITH LOCAL TIME ZONE$/i.test(column.type)) {
                                            oldType=util.format('%s %s', column.type.toUpperCase(), (column.nullable ? 'NULL' : 'NOT NULL'));
                                        }
                                        else if (column.size != null) {
                                            oldType = util.format('%s(%s) %s', column.type.toUpperCase(), column.size.toString(), (column.nullable ? 'NULL' : 'NOT NULL'));
                                        }
                                        else {
                                            oldType = util.format('%s %s', column.type.toUpperCase(), (column.nullable ? 'NULL' : 'NOT NULL'));
                                        }
                                        //remove column from collection
                                        migration.add.splice(i, 1);
                                        i-=1;
                                        if (newType !== oldType) {
                                            //add column to alter collection
                                            migration.change.push(x);
                                        }
                                    }
                                }
                            }
                            //alter table
                            const targetTable = self.table(migration.appliesTo);
                            //add new columns (if any)
                            targetTable.add(migration.add, function(err) {
                                if (err) { return cb(err); }
                                //modify columns (if any)
                                targetTable.change(migration.change, function(err) {
                                    if (err) { return cb(err); }
                                    cb(null, 1);
                                });
                            });
                        });
                    }
                    else {
                        cb(new Error('Invalid migration data.'));
                    }
                }
                else {
                    cb(new Error('Invalid table status.'));
                }
            },
            function(arg, cb) {
                if (arg>0) {
                    void self.selectIdentity('migrations', 'id', function(err, value) {
                        if (err) {
                            return cb(err);
                        }
                        //log migration to database
                        void self.execute('INSERT INTO "migrations"("id","appliesTo", "model", "version", "description") VALUES (?,?,?,?,?)', [
                            value,
                            migration.appliesTo,
                            migration.model,
                            migration.version,
                            migration.description 
                        ], function(err) {
                            if (err)  {
                                return cb(err);
                            }
                            return cb(null, 1);
                        });
                    });
                }
                else {
                    migration['updated'] = true;
                    return cb(null, arg);
                }
            }
        ], function(err) {
            callback(err);
        });

    }

    /**
     * Produces a new identity value for the given entity and attribute.
     * @param entity {String} The target entity name
     * @param attribute {String} The target attribute
     * @param callback {Function=}
     */
    selectIdentity(entity, attribute, callback) {

        const self = this;
        //format sequence name ([entity]_[attribute]_seg e.g. user_id_seq)
        let name = entity + '_' + attribute + '_seq';
        if (name.length>30)
        {
            name=entity.substring(0,26) + '_seq';
        }
        let owner;
        if (self.options && self.options.schema) {
            owner = self.options.schema;
        }
        let sql ='SELECT SEQUENCE_OWNER,SEQUENCE_NAME FROM ALL_SEQUENCES WHERE "SEQUENCE_NAME" = ?';
        if (owner) {
            sql += ' AND REGEXP_LIKE(SEQUENCE_OWNER,?,\'i\')';
        }
        //search for sequence
        self.execute(sql, [ name, owner ? '^' + owner + '$' : null ], function(err, result) {
            if (err) { return callback(err); }
            if (result.length===0) {
                self.execute(util.format('CREATE SEQUENCE "%s" START WITH 1 INCREMENT BY 1', name), [], function(err) {
                    if (err) { return callback(err); }
                    //get next value
                    self.execute(util.format('SELECT "%s".nextval AS "resultId" FROM DUAL', name), [], function(err, result) {
                        if (err) { return callback(err); }
                        callback(null, result[0]['resultId']);
                    });
                });
            }
            else {
                //get next value
                self.execute(util.format('SELECT "%s".nextval AS "resultId" FROM DUAL', name), [], function(err, result) {
                    if (err) { return callback(err); }
                    callback(null, result[0]['resultId']);
                });
            }
        });
    }

    selectIdentityAsync(entity, attribute) {
        const self = this;
        return new Promise(function(resolve, reject) {
            void self.selectIdentity(entity, attribute, function(err, value) {
                if (err) {
                    return reject(err);
                }
                return resolve(value);
            });
        });
    }

    resetIdentity(entity, attribute, callback) {
        const self = this;
        return self.selectIdentity(entity, attribute , function(err) {
            if (err) {
                return callback(err);
            }
            // get max value
            let sql = util.format('SELECT MAX("%s") AS "maxValue" FROM "%s"', attribute, entity)
            return self.execute(sql, [], function(err, results) {
                if (err) {
                    return callback(err);
                }
                const maxValue = (results && results.length && results[0].maxValue) + 1;
                if (maxValue) {
                    let name = entity + '_' + attribute + '_seq';
                    if (name.length>30) {
                        name=entity.substring(0,26) + '_seq';
                    }
                    sql = util.format('ALTER SEQUENCE "%s" RESTART START WITH %s INCREMENT BY 1', name, maxValue);
                    return self.execute(sql, [], function(err) {
                        if (err) {
                            return callback(err);
                        }
                        return callback();
                    });
                }
                return callback();
            });
        });
    }

    resetIdentityAsync(entity, attribute) {
        const self = this;
        return new Promise(function(resolve, reject) {
            void self.resetIdentity(entity, attribute, function(err) {
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
        });
    }

    /**
     * Produces a new counter auto increment value for the given entity and attribute.
     * @param entity {String} The target entity name
     * @param attribute {String} The target attribute
     * @param callback {Function=}
     */
    nextIdentity(entity, attribute, callback) {
        this.selectIdentity(entity, attribute , callback);
    }

    /**
     * Executes an operation against database and returns the results.
     * @param {*} batch
     * @param {function(Error=)} callback
     */
    executeBatch(callback) {
        callback = callback || function() {};
        callback(new Error('DataAdapter.executeBatch() is obsolete. Use DataAdapter.executeInTransaction() instead.'));
    }

    table(name) {
        const self = this;
        let owner;
        let table;
        const matches = /(\w+)\.(\w+)/.exec(name);
        if (matches) {
            //get schema owner (the first part of the string provided)
            owner = matches[1];
            //get table name (the second part of the string provided)
            table = matches[2];
        }
        else {
            //get table name (the whole string provided)
            table = name;
            //get schema name (from options)
            if (self.options && self.options.schema) {
                owner = self.options.schema;
            }
        }

        const format = function(format, obj)
        {
            let result = format;
            if (/%t/.test(format))
                result = result.replace(/%t/g,OracleAdapter.formatType(obj));
            if (/%f/.test(format))
                result = result.replace(/%f/g,obj.name);
            return result;
        };

        return {
            /**
             * @param {function(Error,Boolean=)} callback
             */
            exists:function(callback) {
                let sql;
                if (typeof owner === 'undefined' || owner === null) {
                    sql = 'SELECT COUNT(*) AS "count" FROM ALL_OBJECTS WHERE object_type IN (\'TABLE\') AND object_name = ?';
                }
                else {
                    sql = 'SELECT COUNT(*) AS "count" FROM ALL_OBJECTS WHERE object_type IN (\'TABLE\') AND object_name = ? AND REGEXP_LIKE(owner,?,\'i\')';
                }
                self.execute(sql, [table, '^' + owner + '$'], function(err, result) {
                    if (err) { callback(err); return; }
                    callback(null, (result[0].count>0));
                });
            },
            existsAsync: function () {
                return new Promise((resolve, reject) => {
                    this.exists((err, value) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(value);
                    });
                });
            },
            /**
             * @param {function(Error,string=)} callback
             */
            version:function(callback) {
                self.execute('SELECT MAX("version") AS "version" FROM "migrations" WHERE "appliesTo"=?',
                    [name], function(err, result) {
                        if (err) { return callback(err); }
                        if (result.length===0)
                            callback(null, '0.0');
                        else
                            callback(null, result[0].version || '0.0');
                    });
            },
            versionAsync: function () {
                return new Promise((resolve, reject) => {
                    this.version((err, value) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(value);
                    });
                });
            },
            /**
             * @param {function(Error,Boolean=)} callback
             */
            hasSequence:function(callback) {
                callback = callback || function() {};
                let sql;
                if (owner != null) {
                    sql = 'SELECT COUNT(*) AS "count" FROM ALL_SEQUENCES WHERE SEQUENCE_NAME=?  AND REGEXP_LIKE(owner,?,\'i\')';
                }
                else {
                    sql = 'SELECT COUNT(*) AS "count" FROM ALL_SEQUENCES WHERE SEQUENCE_NAME=?';
                }
                self.execute(sql,
                    [ table + '_seq', owner ? '^' + owner + '$' : null ], function(err, result) {
                        if (err) { callback(err); return; }
                        callback(null, (result[0].count>0));
                    });
            },
            /**
             * @param {function(Error=,Array=)} callback
             */
            columns:function(callback) {
                callback = callback || function() {};

                /*
                 SELECT c0.COLUMN_NAME AS "name", c0.DATA_TYPE AS "type", ROWNUM AS "ordinal",
                 c0.DATA_LENGTH AS "size", c0.DATA_SCALE AS "scale", CASE WHEN c0.NULLABLE='Y'
                 THEN 1 ELSE 0 END AS "nullable", CASE WHEN t0.CONSTRAINT_TYPE='P' THEN 1 ELSE 0 END AS "primaryKey"
                 FROM ALL_TAB_COLUMNS c0 LEFT JOIN (SELECT cols.table_name, cols.column_name, cols.owner, cons.constraint_type
                 FROM all_constraints cons, all_cons_columns cols WHERE cons.constraint_type = 'P'
                 AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner) t0 ON c0.TABLE_NAME=t0.TABLE_NAME
                 AND c0.OWNER=t0.OWNER AND c0.COLUMN_NAME=t0.COLUMN_NAME WHERE c0.TABLE_NAME = ?
                */

                let sql =
                    `SELECT c0.COLUMN_NAME AS "name", c0.DATA_TYPE AS "type", ROWNUM AS "ordinal", CASE WHEN c0."CHAR_LENGTH">0 THEN c0."CHAR_LENGTH" ELSE c0.DATA_LENGTH END as "size",
                    c0.DATA_SCALE AS "scale", c0.DATA_PRECISION AS "precision", CASE WHEN c0.NULLABLE='Y' THEN 1 ELSE 0 END AS "nullable", CASE WHEN t0.CONSTRAINT_TYPE='P' THEN 1 ELSE 0 END AS "primary"
                    FROM ALL_TAB_COLUMNS c0  LEFT JOIN   (
                    SELECT cols.table_name, cols.column_name, cols.owner, cons.constraint_type FROM all_constraints cons INNER JOIN all_cons_columns cols
                    ON cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner 
                    WHERE cols.table_name=? ) t0 ON c0.COLUMN_NAME=t0.COLUMN_NAME
                    WHERE c0.TABLE_NAME = ?`;
                if (owner) { 
                    sql += ' AND REGEXP_LIKE(c0.OWNER,?,\'i\')';
                }
                self.execute(sql, [name,name, '^' + owner + '$'], function(err, result) {
                        if (err) { callback(err); return; }
                        callback(null, result);
                    });
            },
            columnsAsync: function () {
                return new Promise((resolve, reject) => {
                    this.columns((err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            /**
             * @param {{name:string,type:string,primary:boolean|number,nullable:boolean|number,size:number, scale:number,precision:number,oneToMany:boolean}[]|*} fields
             * @param callback
             */
            create: function(fields, callback) {
                callback = callback || function() {};
                fields = fields || [];
                if (!_.isArray(fields)) {
                    return callback(new Error('Invalid argument type. Expected Array.'));
                }
                if (fields.length === 0) {
                    return callback(new Error('Invalid argument. Fields collection cannot be empty.'));
                }
                let strFields = _.map(
                    _.filter(fields, (x) => {
                        return !x.oneToMany;
                    }),
                    (x) => {
                        return format('"%f" %t', x);
                    }).join(', ');

                //get table qualified name
                let strTable = '';

                const formatter = self.getFormatter();
                if (typeof owner !== 'undefined') { strTable = formatter.escapeName(owner) + '.'; }
                strTable += formatter.escapeName(table);
                //add primary key constraint
                const strPKFields = _.map(_.filter(fields, (x) => {
                        return (x.primary === true || x.primary === 1);
                    }), (x) => {
                        return formatter.escapeName(x.name);
                }).join(', ');
                if (strPKFields.length>0) {
                    strFields += ', ' + util.format('CONSTRAINT "%s_pk" PRIMARY KEY (%s)', table, strPKFields);
                }
                const sql = util.format('CREATE TABLE %s (%s)', strTable, strFields);
                self.execute(sql, null, function(err) {
                    callback(err);
                });
            },
            createAsync: function (fields) {
                return new Promise((resolve, reject) => {
                    this.create(fields, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            /**
             * Alters the table by adding an array of fields
             * @param {{name:string,type:string,primary:boolean|number,nullable:boolean|number,size:number,oneToMany:boolean}[]|*} fields
             * @param callback
             */
            add:function(fields, callback) {
                callback = callback || function() {};
                fields = fields || [];
                if (!_.isArray(fields)) {
                    //invalid argument exception
                    return callback(new Error('Invalid argument type. Expected Array.'));
                }
                if (fields.length === 0) {
                    //do nothing
                    return callback();
                }
                const strFields = fields.map(function(x) {
                    return format('"%f" %t', x);
                }).join(', ');

                //get table qualified name
                let strTable = '';

                const formatter = self.getFormatter();
                if (typeof owner !== 'undefined') { strTable = formatter.escapeName(owner) + '.'; }
                strTable += formatter.escapeName(table);
                //generate SQL statement
                const sql = util.format('ALTER TABLE %s ADD (%s)', strTable, strFields);
                self.execute(sql, [], function(err) {
                    callback(err);
                });
            },
            addAsync: function (fields) {
                return new Promise((resolve, reject) => {
                    this.add(fields, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            /**
             * Alters the table by modifying an array of fields
             * @param {{name:string,type:string,primary:boolean|number,nullable:boolean|number,size:number,oneToMany:boolean}[]|*} fields
             * @param callback
             */
            change:function(fields, callback) {
                callback = callback || function() {};
                fields = fields || [];
                if (!_.isArray(fields)) {
                    //invalid argument exception
                    return callback(new Error('Invalid argument type. Expected Array.'));
                }
                if (fields.length === 0) {
                    //do nothing
                    return callback();
                }

                //get columns
                return this.columns((err, columns)=> {
                    if (err) {
                        return callback(err);
                    }

                    const strFields = fields.map(function(x) {
                        let column = columns.find((y)=> {
                            return y.name === x.name;
                        });
                        let res = format('"%f" %t', x);
                        if (column && (LangUtils.parseBoolean((typeof x.nullable === 'undefined') ? true : x.nullable) === LangUtils.parseBoolean(column.nullable))) {
                            res = res.replace(/\sNOT\sNULL$/,'').replace(/\sNULL$/,'');
                        }
                        return res;
                    }).join(', ');

                    //get table qualified name
                    let strTable = '';

                    const formatter = self.getFormatter();
                    if (typeof owner !== 'undefined') { strTable = formatter.escapeName(owner) + '.'; }
                    strTable += formatter.escapeName(table);
                    //generate SQL statement
                    const sql = util.format('ALTER TABLE %s MODIFY (%s)', strTable, strFields);
                    self.execute(sql, [], function(err) {
                        callback(err);
                    });

                });
            },
            changeAsync: function (fields) {
                return new Promise((resolve, reject) => {
                    this.change(fields, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            }
        };
    }

    view(name) {
        const self = this;
        let owner;
        // eslint-disable-next-line no-unused-vars
        let view;

        const matches = /(\w+)\.(\w+)/.exec(name);
        if (matches) {
            //get schema owner
            owner = matches[1];
            //get table name
            view = matches[2];
        }
        else {
            // eslint-disable-next-line no-unused-vars
            view = name;
            //get schema name (from options)
            if (self.options && self.options.schema) {
                owner = self.options.schema;
            }
        }
        return {
            /**
             * @param {function(Error,Boolean=)} callback
             */
            exists:function(callback) {
                let sql = 'SELECT COUNT(*) AS "count" FROM ALL_OBJECTS WHERE object_type IN (\'VIEW\') AND object_name = ?';
                if (owner != null) {
                    sql += ' AND REGEXP_LIKE(owner,?,\'i\')';
                }
                self.execute(sql, [name, '^' + (owner || '') + '$'], function(err, result) {
                    if (err) { callback(err); return; }
                    callback(null, (result[0].count>0));
                });
            },
            existsAsync: function () {
                return new Promise((resolve, reject) => {
                    this.exists((err, value) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(value);
                    });
                });
            },
            /**
             * @param {function(Error=)} callback
             */
            drop:function(callback) {
                callback = callback || function() {};
                self.open(function(err) {
                   if (err) { return callback(err); }

                    let sql = 'SELECT COUNT(*) AS "count" FROM ALL_OBJECTS WHERE object_type IN (\'VIEW\') AND object_name = ?';
                    if (typeof owner !== 'undefined') {
                        sql += ' AND REGEXP_LIKE(owner,?,\'i\')';
                    }
                    self.execute(sql, [name, '^' + (owner || '') + '$'], function(err, result) {
                        if (err) { return callback(err); }
                        const exists = (result[0].count>0);
                        if (exists) {
                            const sql = util.format('DROP VIEW "%s"', name);
                            self.execute(sql, undefined, function(err) {
                                if (err) { callback(err); return; }
                                callback();
                            });
                        }
                        else {
                            callback();
                        }
                    });
                });
            },
            dropAsync: function () {
                return new Promise((resolve, reject) => {
                    this.drop((err) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve();
                    });
                });
            },
            /**
             * @param {QueryExpression|*} q
             * @param {function(Error=)} callback
             */
            create:function(q, callback) {
                const thisArg = this;
                self.executeInTransaction(function(tr) {
                    thisArg.drop(function(err) {
                        if (err) { tr(err); return; }
                        try {
                            let sql = util.format('CREATE VIEW "%s" AS ', name);
                            const formatter = self.getFormatter();
                            sql += formatter.format(q);
                            self.execute(sql, [], tr);
                        }
                        catch(e) {
                            tr(e);
                        }
                    });
                }, function(err) {
                    callback(err);
                });
            },
            createAsync: function (q) {
                return new Promise((resolve, reject) => {
                    this.create(q, (err) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve();
                    });
                });
            }
        };
    }

    indexes(name) {

        const self = this, formatter = this.getFormatter();
        let owner;
        // eslint-disable-next-line no-unused-vars
        let table;
        const matches = /(\w+)\.(\w+)/.exec(name);
        if (matches) {
            //get schema owner
            owner = matches[1];
            //get table name
            table = matches[2];
        }
        else {
            // eslint-disable-next-line no-unused-vars
            table = name;
            // get schema name (from options)
            if (self.options && self.options.schema) {
                owner = self.options.schema;
            }
        }

        if (owner == null) {
            owner = self.options.user.toUpperCase();
        }

        return {
            list: function (callback) {
                /**
                 * @property {Array<{name: string,type:string,columns:Array<string>}>} _indexes
                 */
                const thisArg = this;
                if (Object.prototype.hasOwnProperty.call(thisArg, '_indexes')) {
                    return callback(null, thisArg._indexes);
                }
                self.execute(`SELECT "indexes"."INDEX_NAME" AS "name", "indexes"."INDEX_TYPE" AS "type", "constraints"."CONSTRAINT_TYPE" AS "constraint" FROM USER_INDEXES "indexes" LEFT JOIN USER_CONSTRAINTS "constraints" ON "indexes"."INDEX_NAME" = "constraints"."INDEX_NAME" AND "indexes"."TABLE_NAME" = "constraints"."TABLE_NAME" AND "indexes"."TABLE_OWNER" = "constraints"."OWNER" WHERE "indexes".TABLE_NAME = ${formatter.escape(table)} AND "indexes"."TABLE_OWNER" = ${formatter.escape(owner)}`, null, function (err, result) {
                    if (err) {
                        return callback(err);
                    }
                    const indexes = result.filter(function (x) {
                        return x.constraint !== 'P'; // Exclude primary key constraints
                    }).map(function (x) {
                        return {
                            name: x.name,
                            columns: []
                        };
                    });
                    self.execute(`SELECT "columns"."COLUMN_NAME" AS "name","columns"."INDEX_NAME" AS "index" FROM "USER_IND_COLUMNS" "columns" INNER JOIN "USER_INDEXES" "indexes" ON "indexes"."INDEX_NAME" = "columns"."INDEX_NAME" AND "indexes"."TABLE_NAME" = "columns"."TABLE_NAME" WHERE "indexes"."TABLE_NAME" = ${formatter.escape(table)} AND "indexes"."TABLE_OWNER" = ${formatter.escape(owner)}`, null, function (err, columns) {
                        if (err) {
                            return callback(err);
                        }
                        indexes.forEach(function (x) {
                           x.columns = columns.filter((y) => {
                               return y.index === x.name;
                           }).map((y) => {
                               return y.name;
                           });
                        });
                        thisArg._indexes = indexes;
                        return callback(null, indexes);
                    });
                });
            },
            listAsync: function() {
                return new Promise((resolve, reject) => {
                    this.list((err, results) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(results);
                    });
                });
            },
            /**
             * @param {string} name
             * @param {Array|string} columns
             * @param {Function} callback
             */
            create: function (name, columns, callback) {
                const cols = [];
                if (typeof columns === 'string') {
                    cols.push(columns);
                }
                else if (Array.isArray(columns)) {
                    cols.push.apply(cols, columns);
                }
                else {
                    return callback(new Error('Invalid parameter. Columns parameter must be a string or an array of strings.'));
                }
                const thisArg = this;
                void thisArg.list(function (err, indexes) {
                    if (err) {
                        return callback(err);
                    }
                    const ix = indexes.find(function (x) { return x.name === name; });
                    //format create index SQL statement
                    const sqlCreateIndex = `CREATE INDEX ${formatter.escapeName(name)} ON ${formatter.escapeName(table)}(${cols.map(function (x) { return formatter.escapeName(x); }).join(',')})`
                    if (typeof ix === 'undefined' || ix === null) {
                        return self.execute(sqlCreateIndex, [], (err, result) => {
                            return callback(err, result)
                        });
                    }
                    else {
                        let nCols = cols.length;
                        //enumerate existing columns
                        ix.columns.forEach(function (x) {
                            if (cols.indexOf(x) >= 0) {
                                //column exists in index
                                nCols -= 1;
                            }
                        });
                        if (nCols > 0) {
                            //drop index
                            thisArg.drop(name, function (err) {
                                if (err) {
                                    return callback(err);
                                }
                                //and create it
                                self.execute(sqlCreateIndex, [], callback);
                            });
                        }
                        else {
                            //do nothing
                            return callback();
                        }
                    }
                });
            },
            createAsync: function(name, columns) {
                return new Promise((resolve, reject) => {
                    this.create(name, columns, (err) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve();
                    });
                });
            },
            drop: function (name, callback) {
                if (typeof name !== 'string') {
                    return callback(new Error('Name must be a valid string.'));
                }
                void this.list(function (err, indexes) {
                    if (err) {
                        return callback(err);
                    }
                    const exists = indexes.find(function (x) { return x.name === name; });
                    if (exists == null) {
                        return callback();
                    }
                    //format drop index SQL statement
                    const sqlDropIndex = `DROP INDEX ${formatter.escapeName(name)}`;
                    void self.execute(sqlDropIndex, null, function (err) {
                        if (err) {
                            return callback(err);
                        }
                        return callback();
                    });
                });
            },
            dropAsync: function(name) {
                return new Promise((resolve, reject) => {
                    this.drop(name, (err) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve();
                    });
                });
            }
        };
    }

    getFormatter() {
        return new OracleFormatter();
    }

    /**
     * @param {function} callback 
     */
    tryClose(callback) {
        return callback();
    }

    @after(({target, args, result: results}, callback) => {
        const [query, params] = args;
        void target.executed.emit({
            target,
            query,
            params,
            results
        }).then(() => {
            return callback();
        }).catch((err) => {
            return callback(err);
        });
    })
    @before(({target, args}, callback) => {
        const [query, params] = args;
        void target.executing.emit({
            target,
            query,
            params
        }).then(() => {
            return callback();
        }).catch((err) => {
            return callback(err);
        });
    })
    /**
     * Executes a query against the underlying database
     * @param query {QueryExpression|string|*}
     * @param values {*=}
     * @param {function(Error=,*=)} callback
     */
    execute(query, values, callback) {
        const self = this;
        /**
         * @type {string}
         */
        let sql;
        try {
            if (typeof query === 'string') {
                // get raw sql statement
                sql = query;
            } else {
                // format query expression or any object that may be acted as query expression
                const formatter = self.getFormatter();
                sql = formatter.format(query);
            }
            // validate sql statement
            if (typeof sql !== 'string') {
                // noinspection ExceptionCaughtLocallyJS
                throw new Error('The executing command is of the wrong type or empty.');
            }
            // ensure connection
            self.open(function(err) {
                if (err) {
                    return callback(err);
                }
                // prepare statement - the traditional way
                const prepared = self.prepare(sql, values);
                self.logger.debug(`SQL ${prepared}`);
                // execute raw command
                self.rawConnection.execute(prepared,[], {outFormat: oracledb.OBJECT, autoCommit: (typeof self.transaction === 'undefined') }, function(err, result) {
                    self.tryClose(function() {
                        if (err) {
                            self.logger.error(`SQL Error ${prepared}`);
                            return callback(err);
                        }
                        if (result) {
                            return callback(null, result.rows);
                        }
                        return callback();
                    });
                });
            });
        }
        catch (error) {
            return self.tryClose(function() {
                return callback(error);
            });
        }
    }

    /**
     * @param query {*}
     * @param values {*}
     * @returns Promise<any>
     */
    executeAsync(query, values) {
        return new Promise((resolve, reject) => {
            return this.execute(query, values, (err, res) => {
                if (err) {
                    return reject(err);
                }
                return resolve(res);
            });
        });
    }
}

export {
    OracleAdapter
}

