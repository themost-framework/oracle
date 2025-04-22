import { OracleFormatter } from './OracleFormatter';
import { OracleAdapter } from './OracleAdapter';

function createInstance(options) {
    return new OracleAdapter(options);
}

export {
    OracleAdapter,
    OracleFormatter,
    createInstance
}