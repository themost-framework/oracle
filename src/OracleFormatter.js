import util from 'util';
import _ from 'lodash';
import { SqlFormatter, QueryField, QueryExpression } from '@themost/query';
import isPlainObject from 'lodash/isPlainObject';
import isObjectLike from 'lodash/isObjectLike';
import isNative from 'lodash/isNative';

const SINGLE_QUOTE_ESCAPE = '\'\'';
const DOUBLE_QUOTE_ESCAPE = '"';
const SLASH_ESCAPE = '\\';
const NAME_FORMAT = '"$1"';
const TIMESTAMP_REGEX = /^\d{4}-[01]\d-[0-3]\d[T][0-2]\d:[0-5]\d:[0-5]\d\.\d+([+-][0-2]\d:[0-5]\d|[+-][0-9]\d:[0-9]\d)$/gm;


const objectToString = Function.prototype.toString.call(Object);

function isObjectDeep(any) {
    // check if it is a plain object
    let result = isPlainObject(any);
    if (result) {
        return result;
    }
    // check if it's object
    if (isObjectLike(any) === false) {
        return false;
    }
    // get prototype
    let proto = Object.getPrototypeOf(any);
    // if prototype exists, try to validate prototype recursively
    while(proto != null) {
        // get constructor
        const Ctor = Object.prototype.hasOwnProperty.call(proto, 'constructor')
            && proto.constructor;
        // check if constructor is native object constructor
        result = (typeof Ctor == 'function') && (Ctor instanceof Ctor)
            && Function.prototype.toString.call(Ctor) === objectToString;
        // if constructor is not object constructor and belongs to a native class
        if (result === false && isNative(Ctor) === true) {
            // return false
            return false;
        }
        // otherwise. get parent prototype and continue
        proto = Object.getPrototypeOf(proto);
    }
    // finally, return result
    return result;
}

function zeroPad(number, length) {
    number = number || 0;
    let res = number.toString();
    while (res.length < length) {
        res = '0' + res;
    }
    return res;
}

function instanceOf(any, ctor) {
    // validate constructor
    if (typeof ctor !== 'function') {
        return false
    }
    // validate with instanceof
    if (any instanceof ctor) {
        return true;
    }
    return !!(any && any.constructor && any.constructor.name === ctor.name);
}

/**
 * @class
 * @augments {SqlFormatter}
 */
class OracleFormatter extends SqlFormatter {
    /**
     * @constructor
     */
    constructor() {
        super();
        this.settings = {
            nameFormat:NAME_FORMAT,
            forceAlias:true,
            useAliasKeyword: false,
            jsonDateFormat: 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"', // default json date format YYYY-MM-DDTHH:mm:ss.sssZ
        };
        // try to validate if JSON.stringify returns a date as string using timestamp with timezone
        // e.g. 2020-12-14T12:45:00.000+02:00
        const date = JSON.stringify(new Date());
        if (TIMESTAMP_REGEX.test(date) === true) {
            this.settings.jsonDateFormat = 'YYYY-MM-DD"T"HH24:MI:SS.FF3TZH:TZM';
        }
    }

    escapeName(name) {
        if (typeof name === 'string')
            return name.replace(/(\w+)/ig, this.settings.nameFormat);
        return name;
    }

    /**
     * Escapes an object or a value and returns the equivalent sql value.
     * @param {*} value - A value that is going to be escaped for SQL statements
     * @param {boolean=} unquoted - An optional value that indicates whether the resulted string will be quoted or not.
     * returns {string} - The equivalent SQL string value
     */
    escape(value, unquoted) {
        if (typeof value === 'boolean') { return value ? '1' : '0'; }
        if (value instanceof Date) {
            return util.format('TO_TIMESTAMP_TZ(%s, \'YYYY-MM-DD HH24:MI:SS.FF3TZH:TZM\')', this.escapeDate(value));
        }
        // if (typeof value === 'string' && LangUtils.isDate(value)) {
        //     return util.format('TO_TIMESTAMP_TZ(%s, \'YYYY-MM-DD HH24:MI:SS.FF3TZH:TZM\')', this.escapeDate(new Date(value)));
        // }
        // serialize array of objects as json array
        if (Array.isArray(value)) {
            // find first non-object value
            const index = value.filter((x) => {
                return x != null;
            }).findIndex((x) => {
                return isObjectDeep(x) === false;
            });
            // if all values are objects
            if (index === -1) {
                return this.escape(JSON.stringify(value)); // return as json array
            }
        }
        let res = super.escape.bind(this)(value, unquoted);
        if (typeof value === 'string') {
            if (/\\'/g.test(res)) {
                //escape single quote (that is already escaped)
                res = res.replace(/\\'/g, SINGLE_QUOTE_ESCAPE);
            }
            if (/\\"/g.test(res))
            //escape double quote (that is already escaped)
                res = res.replace(/\\"/g, DOUBLE_QUOTE_ESCAPE);
            if (/\\\\/g.test(res))
            //escape slash (that is already escaped)
                res = res.replace(/\\\\/g, SLASH_ESCAPE);
        }
        return res;
    }

    /**
     * @param {Date|*} val
     * @returns {string}
     */
    escapeDate(val) {
        const year   = val.getFullYear();
        const month  = zeroPad(val.getMonth() + 1, 2);
        const day    = zeroPad(val.getDate(), 2);
        const hour   = zeroPad(val.getHours(), 2);
        const minute = zeroPad(val.getMinutes(), 2);
        const second = zeroPad(val.getSeconds(), 2);
        //var millisecond = zeroPad(dt.getMilliseconds(), 3);
        //format timezone
        const offset = (new Date()).getTimezoneOffset(), timezone = (offset<=0 ? '+' : '-') + zeroPad(-Math.floor(offset/60),2) + ':' + zeroPad(offset%60,2);
        return '\'' + year + '-' + month + '-' + day + ' ' + hour + ':' + minute + ':' + second + '.' + zeroPad(val.getMilliseconds(), 3) + timezone + '\'';
    }


    /**
     * Formats a fixed query expression where select fields are constants e.g. SELECT 1 AS `id`,'John' AS `givenName` etc
     * @param obj {QueryExpression|*}
     * @returns {string}
     */
    formatFixedSelect(obj) {
        let self = this;
        let fields = obj.fields();
        return 'SELECT ' + _.map(fields, function(x) { return self.format(x,'%f'); }).join(', ') + ' FROM DUAL';
    }

    /**
     *
     * @param {QueryExpression} obj
     * @returns {string}
     */
    formatLimitSelect(obj) {

        let sql;
        const self=this;
        let take = parseInt(obj.$take) || 0;
        let skip = parseInt(obj.$skip) || 0;
        if (take<=0) {
            sql=self.formatSelect(obj);
        }
        else {
            //add row_number with order
            const keys = Object.keys(obj.$select);
            if (keys.length === 0)
                throw new Error('Entity is missing');
            //get select fields
            let selectFields = obj.$select[keys[0]];
            //get order
            let order = obj.$order;
            //add row index field
            selectFields.push({
                '__RowIndex': {
                  $row_index: order
                }
            });
            //remove order
            if (order) {
                delete obj.$order;
            }
            //get sub query
            const subQuery = self.formatSelect(obj);
            //add order again
            if (order) {
                obj.$order = order;
            }
            //remove row index field
            selectFields.pop();
            const fields = [];
            _.forEach(selectFields, (x) => {
                if (typeof x === 'string') {
                    fields.push(new QueryField(x));
                }
                else {
                    /**
                     * @type QueryField
                     */
                    let field = Object.assign(new QueryField(), x);
                    fields.push(field.as() || field.getName());
                }
            });
            sql = util.format('SELECT %s FROM (%s) t0 WHERE "__RowIndex" BETWEEN %s AND %s', _.map(fields, (x) => {
                return self.format(x, '%f');
            }).join(', '), subQuery, skip + 1, skip + take);
        }
        return sql;

    }
    isLogical(obj) {
        let prop;
        // eslint-disable-next-line no-unused-vars
        for(let key in obj) {
            if (Object.prototype.hasOwnProperty.call(obj, key)) {
                prop = key;
                break;
            }
        }
        return (/^\$(and|or|not|nor)$/g.test(prop));
    }
    /**
     * Implements [a & b] bitwise and expression formatter.
     * @param p0 {*}
     * @param p1 {*}
     */
    $bit(p0, p1)
    {
        return util.format('BITAND(%s, %s)', this.escape(p0), this.escape(p1));
    }

    /**
     * Implements indexOf(str,substr) expression formatter.
     * @param {string} p0 The source string
     * @param {string} p1 The string to search for
     * @returns {string}
     */
    $indexof(p0, p1) {
        return util.format('(INSTR(%s,%s)-1)', this.escape(p0), this.escape(p1));
    }

    $indexOf(p0, p1) {
        return util.format('(INSTR(%s,%s)-1)', this.escape(p0), this.escape(p1));
    }

    /**
     * Implements contains(a,b) expression formatter.
     * @param {string} p0 The source string
     * @param {string} p1 The string to search for
     * @returns {string}
     */
    $text(p0, p1) {
        return util.format('(INSTR(%s,%s)-1)>=0', this.escape(p0), this.escape(p1));
    }

    /**
     * Implements concat(a,b) expression formatter.
     * @param {...*} arg
     * @returns {string}
     */
    // eslint-disable-next-line no-unused-vars
    $concat(arg) {
        const args = Array.from(arguments);
        if (args.length < 2) {
            throw new Error('Concat method expects two or more arguments');
        }
        let result = '(';
        result += Array.from(args).map((arg) => {
            return `COALESCE(TO_CHAR(${this.escape(arg)}),'')`
        }).join(' || ');
        result += ')';
        return result;
    }

    /**
     * Implements substring(str,pos) expression formatter.
     * @param {String} p0 The source string
     * @param {Number} pos The starting position
     * @param {Number=} length The length of the resulted string
     * @returns {string}
     */
    $substring(p0, pos, length) {
        if (length)
            return util.format('SUBSTR(%s,%s,%s)', this.escape(p0), pos.valueOf()+1, length.valueOf());
        else
            return util.format('SUBSTR(%s,%s)', this.escape(p0), pos.valueOf()+1);
    }

    /**
     * Implements length(a) expression formatter.
     * @param {*} p0
     * @returns {string}
     */
    $length(p0) {
        return util.format('LENGTH(%s)', this.escape(p0));
    }

    /**
     * @param {...*} p0
     * @return {*}
     */
    // eslint-disable-next-line no-unused-vars
    $row_index() {
        let args = Array.from(arguments).filter(function(x) {
            return x != null;
        });
        return util.format('ROW_NUMBER() OVER(%s)', (args && args.length) ? this.format(args, '%o') : 'ORDER BY NULL');
    }

    $ceiling(p0) {
        return util.format('CEIL(%s)', this.escape(p0));
    }

    $startswith(p0, p1) {
        //validate params
        if ( _.isNil(p0) ||  _.isNil(p1))
            return '';
        return 'REGEXP_COUNT(' + this.escape(p0) + ',\'^' + this.escape(p1, true) + '\', 1, \'i\')';
    }

    $contains(p0, p1) {
        //validate params
        if ( _.isNil(p0) ||  _.isNil(p1))
            return '';
        //(CASE WHEN REGEXP_COUNT(x, 'text', 1, 'i') > 0 THEN 1 ELSE 0 END)
        return '(CASE WHEN REGEXP_COUNT(' + this.escape(p0) + ',\'' + this.escape(p1, true) + '\', 1, \'i\') > 0 THEN 1 ELSE 0 END)';
    }

    $endswith(p0, p1) {
        //validate params
        if ( _.isNil(p0) ||  _.isNil(p1))
            return '';
        return 'REGEXP_COUNT(' + this.escape(p0) + ',\'' + this.escape(p1, true) + '$\', 1, \'i\')';
    }

    $day(p0) {
        if (Object.prototype.hasOwnProperty.call(p0, '$jsonGet')) {
            return `EXTRACT(DAY FROM TO_TIMESTAMP_TZ(${this.escape(p0)}, '${this.settings.jsonDateFormat}'))`;
        }
        return `EXTRACT(DAY FROM ${this.escape(p0)})`;
    }

    $month(p0) {
        if (Object.prototype.hasOwnProperty.call(p0, '$jsonGet')) {
            return util.format('EXTRACT(MONTH FROM TO_TIMESTAMP_TZ(%s, \'%s\'))', this.escape(p0), this.settings.jsonDateFormat);
        }
        return util.format('EXTRACT(MONTH FROM %s)', this.escape(p0)) ;
    }

    $year(p0) {
        if (Object.prototype.hasOwnProperty.call(p0, '$jsonGet')) {
            // try to get json date value
            // important note: we are expecting to have a datetime value like '2023-10-01T00:00:00.000Z'
            // so we need to convert it to a date value
            // and then extract the year
            return util.format('EXTRACT(YEAR FROM TO_TIMESTAMP_TZ(%s, \'%s\'))', this.escape(p0), this.settings.jsonDateFormat);
        }
        return util.format('EXTRACT(YEAR FROM %s)', this.escape(p0));
    }

    $hour(p0) {
        if (Object.prototype.hasOwnProperty.call(p0, '$jsonGet')) {
            return util.format('EXTRACT(HOUR FROM TO_TIMESTAMP_TZ(%s, \'%s\'))', this.escape(p0), this.settings.jsonDateFormat);
        }
        return util.format('EXTRACT(HOUR FROM %s)', this.escape(p0)) ;
    }

    $minute(p0) {
        if (Object.prototype.hasOwnProperty.call(p0, '$jsonGet')) {
            return util.format('EXTRACT(MINUTE FROM TO_TIMESTAMP_TZ(%s, \'%s\'))', this.escape(p0), this.settings.jsonDateFormat);
        }
        return util.format('EXTRACT(MINUTE FROM %s)', this.escape(p0)) ;
    }

    $second(p0) {
        if (Object.prototype.hasOwnProperty.call(p0, '$jsonGet')) {
            return util.format('EXTRACT(SECOND FROM TO_TIMESTAMP_TZ(%s, \'%s\'))', this.escape(p0), this.settings.jsonDateFormat);
        }
        return util.format('EXTRACT(SECOND FROM %s)', this.escape(p0)) ;
    }

    $date(p0) {
        //alternative date solution: 'TO_TIMESTAMP_TZ(TO_CHAR(%s, 'YYYY-MM-DD'),'YYYY-MM-DD')'
        return util.format('TO_CHAR(TRUNC(%s), \'YYYY-MM-DD\')', this.escape(p0)) ;
    }

    /**
     * Implements contains(a,b) expression formatter.
     * @param {*} p0 The source string
     * @param {string|*} p1 The string to search for
     * @returns {string}
     */
    $regex(p0, p1) {
        //validate params
        if ( _.isNil(p0) ||  _.isNil(p1))
            return '';
        return 'REGEXP_LIKE(' + this.escape(p0) + ',\'' + this.escape(p1, true) + '\')';
    }

    /**
     * @deprecated Use $ifNull() instead
     * @param {*} p0 
     * @param {*} p1 
     * @returns 
     */
    $ifnull(p0, p1) {
        return this.$ifNull(p0, p1) ;
    }

    $ifNull(p0, p1) {
        return util.format('NVL(%s, %s)', this.escape(p0), this.escape(p1)) ;
    }

    $cond(ifExpr, thenExpr, elseExpr) {
        // validate ifExpr which should an instance of QueryExpression or a comparison expression
        let ifExpression;
        if (instanceOf(ifExpr, QueryExpression)) {
            ifExpression = this.formatWhere(ifExpr.$where);
        } else if (this.isComparison(ifExpr) || this.isLogical(ifExpr)) {
            ifExpression = this.formatWhere(ifExpr);
        } else {
            throw new Error('Condition parameter should be an instance of query or comparison expression');
        }
        return util.format('(CASE WHEN %s THEN %s ELSE %s END)', ifExpression, this.escape(thenExpr), this.escape(elseExpr));
    }

    $toString(p0) {
        return util.format('TO_NCHAR(%s)', this.escape(p0)) ;
    }

    $uuid() {
        return 'REGEXP_REPLACE(SYS_GUID(), \'(.{8})(.{4})(.{4})(.{4})(.{12})\', \'\\1-\\2-\\3-\\4-\\5\')';
    }

    $toGuid(p0) {
        return `REGEXP_REPLACE(STANDARD_HASH(TO_CHAR(${this.escape(p0)}),\'MD5\'), \'(.{8})(.{4})(.{4})(.{4})(.{12})\', \'\\1-\\2-\\3-\\4-\\5\')`;
    }

    /**
     *
     * @param {('date'|'datetime'|'timestamp')} type
     * @returns
     */
    $getDate(type) {
        switch (type) {
            case 'date':
                return 'TRUNC(SYSDATE)';
            case 'datetime':
                return 'SYSDATE';
            case 'timestamp':
                return 'CAST(SYSDATE AS TIMESTAMP WITH LOCAL TIME ZONE)';
            default:
                return 'CAST(SYSDATE AS TIMESTAMP WITH LOCAL TIME ZONE)';
        }
    }


    $toInt(expr) {
        return `FLOOR(CAST(${this.escape(expr)} as DECIMAL(19,8)))`;
    }

    $toDouble(expr) {
        return this.$toDecimal(expr, 19, 8);
    }

    // noinspection JSCheckFunctionSignatures
    /**
     * @param {*} expr
     * @param {number=} precision
     * @param {number=} scale
     * @returns
     */
    $toDecimal(expr, precision, scale) {
        const p = typeof precision === 'number' ? Math.floor(precision) : 19;
        const s = typeof scale === 'number' ? Math.floor(scale) : 8;
        return `CAST(${this.escape(expr)} AS DECIMAL(${p},${s}))`;
    }

    $toLong(expr) {
        return `CAST(${this.escape(expr)} AS NUMBER(19))`;
    }

    /**
     * @param {*} expr
     * @return {string}
     */
    $jsonGet(expr) {
        if (typeof expr.$name !== 'string') {
            throw new Error('Invalid json expression. Expected a string');
        }
        const parts = expr.$name.split('.');
        const extract = this.escapeName(parts.splice(0, 2).join('.'));
        return `JSON_VALUE(${extract}, '$.${parts.join('.')}')`;
    }

    /**
     * @param {*} expr
     * @return {string}
     */
    $jsonEach(expr) {
        return `JSON_TABLE(${this.escapeName(expr)})`;
    }

    /**
     * @param {{ $jsonGet: Array<*> }} expr
     */
    $jsonGroupArray(expr) {
        const [key] = Object.keys(expr);
        if (key !== '$jsonObject') {
            throw new Error('Invalid json group array expression. Expected a json object expression');
        }
        return `JSON_ARRAYAGG(${this.escape(expr)})`;
    }

    /**
     * @param {...*} expr
     */
    // eslint-disable-next-line no-unused-vars
    $jsonObject() {
        // expected an array of QueryField objects
        const args = Array.from(arguments).reduce((previous, current) => {
            // get the first key of the current object
            let [name] = Object.keys(current);
            let value;
            // if the name is not a string then throw an error
            if (typeof name !== 'string') {
                throw new Error('Invalid json object expression. The attribute name cannot be determined.');
            }
            // if the given name is a dialect function (starts with $) then use the current value as is
            // otherwise create a new QueryField object
            if (name.startsWith('$')) {
                value = new QueryField(current[name]);
                name = value.getName();
            } else {
                value = current instanceof QueryField ? new QueryField(current[name]) : current[name];
            }
            // escape json attribute name and value
            previous.push(this.escape(name) + ':' + this.escape(value));
            return previous;
        }, []);
        return `json_object(${args.join(',')})`;
    }

    $jsonArray(expr) {
        if (expr == null) {
            throw new Error('The given query expression cannot be null');
        }
        if (expr instanceof QueryField) {
            // escape expr as field and waiting for parsing results as json array
            return this.escape(expr);
        }
        // treat expr as select expression
        if (expr.$select) {
            // get select fields
            const args = Object.keys(expr.$select).reduce((previous, key) => {
                previous.push.apply(previous, expr.$select[key]);
                return previous;
            }, []);
            const [key] = Object.keys(expr.$select);
            // prepare select expression to return json array
            expr.$select[key] = [
                {
                    $jsonGroupArray: [ // use json_group_array function
                        {
                            $jsonObject: args // use json_object function
                        }
                    ]
                }
            ];
            return `(${this.format(expr)})`;
        }
        // treat expression as query field
        if (Object.prototype.hasOwnProperty.call(expr, '$name')) {
            return this.escape(expr);
        }
        // treat expression as value
        if (Object.prototype.hasOwnProperty.call(expr, '$value')) {
            if (Array.isArray(expr.$value)) {
                return this.escape(JSON.stringify(expr.$value));
            }
            return this.escape(expr);
        }
        if (Object.prototype.hasOwnProperty.call(expr, '$literal')) {
            if (Array.isArray(expr.$literal)) {
                return this.escape(JSON.stringify(expr.$literal));
            }
            return this.escape(expr);
        }
        throw new Error('Invalid json array expression. Expected a valid select expression');
    }

}

export {
    OracleFormatter
}
