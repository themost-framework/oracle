
import { QueryExpression } from "@themost/query";
import {DataAdapterIndexes, TraceLogger} from "@themost/common";
import {AsyncSeriesEventEmitter} from "@themost/events";

declare module "@themost/common" {
    export class TraceUtils {
        static newLogger(): TraceLogger;
    }

    export class TraceLogger {
        setLogLevel(level: string): void;
    }
}

export declare interface OracleAdapterTable {
    create(fields: Array<any>, callback: (err: Error) => void): void;
    createAsync(fields: Array<any>): Promise<void>;
    add(fields: Array<any>, callback: (err: Error) => void): void;
    addAsync(fields: Array<any>): Promise<void>;
    change(fields: Array<any>, callback: (err: Error) => void): void;
    changeAsync(fields: Array<any>): Promise<void>;
    exists(callback: (err: Error, result: boolean) => void): void;
    existsAsync(): Promise<boolean>;
    version(callback: (err: Error, result: string) => void): void;
    versionAsync(): Promise<string>;
    columns(callback: (err: Error, result: Array<any>) => void): void;
    columnsAsync(): Promise<Array<any>>;
}

export declare interface OracleAdapterView {
    create(query: any, callback: (err: Error) => void): void;
    createAsync(query: any): Promise<void>;
    exists(callback: (err: Error, result: boolean) => void): void;
    existsAsync(): Promise<boolean>;
    drop(callback: (err: Error) => void): void;
    dropAsync(): Promise<void>;
}

export declare interface OracleAdapterMigration {
    add: Array<any>;
    change?: Array<any>;
    appliesTo: string;
    version: string;
}

export declare class OracleAdapter {

    public logger: TraceLogger;
    public executing: AsyncSeriesEventEmitter<{target: this, query: (string|QueryExpression), params?: unknown[]}>;
    public executed: AsyncSeriesEventEmitter<{target: this, query: (string|QueryExpression), params?: unknown[], results: uknown[]}>;


    static formatType(field: any): string;
    formatType(field: any): string;
    open(callback: (err: Error) => void): void;
    close(callback: (err: Error) => void): void;
    openAsync(): Promise<void>;
    closeAsync(): Promise<void>;
    prepare(query: any, values?: Array<any>): any;
    createView(name: string, query: any, callback: (err: Error) => void): void;
    executeInTransaction(func: any, callback: (err: Error) => void): void;
    executeInTransactionAsync(func: Promise<any>): Promise<any>;
    migrate(obj: OracleAdapterMigration, callback: (err: Error) => void): void;
    selectIdentity(entity: string, attribute: string, callback: (err: Error, value: any) => void): void;
    selectIdentityAsync(entity: string, attribute: string): Promise<any>;
    execute(query: any, values: any, callback: (err: Error, value: any) => void): void;
    executeAsync(query: any, values: any): Promise<any>;
    executeAsync<T>(query: any, values: any): Promise<Array<T>>;
    table(name: string): OracleAdapterTable;
    view(name: string): OracleAdapterView;
    resetIdentity(entity: string, attribute: string, callback: (err: Error) => void): void;
    resetIdentityAsync(entity: string, attribute: string): Promise<void>;
    indexes(name: string): DataAdapterIndexes;
    getFormatter(): SqlFormatter;
}
